package resources

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

func configureSnapshotBackupFolder(t *testing.T, ctx context.Context, s *storageYDB) {
	t.Helper()
	_, err := s.db.ExecuteRW(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        update snapshots set folder_id = "folder" where id = "snapshot";
    `, s.snapshotsPath))
	require.NoError(t, err)
	require.NoError(t, s.SetSnapshotBackupFolder(ctx, "folder", "generation", true))
	claimed, err := s.ClaimSnapshotBackupFolder(ctx, "folder", "generation", "coordinator")
	require.NoError(t, err)
	require.True(t, claimed)
}

func createFolderBackupSnapshot(t *testing.T, ctx context.Context, s *storageYDB, id, folder string, creatingAt time.Time) {
	t.Helper()
	_, err := s.CreateSnapshot(ctx, SnapshotMeta{ID: id, FolderID: folder, Disk: &types.Disk{DiskId: "disk", ZoneId: "zone"}, CreateRequest: &wrappers.UInt64Value{Value: 1}, CreateTaskID: id + "_create", CreatingAt: creatingAt})
	require.NoError(t, err)
	require.NoError(t, s.SnapshotCreated(ctx, id, "checkpoint", time.Now(), 4096, 4096))
}

func TestSnapshotBackupReservationSerializesWithDelete(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(fmt.Sprint(accepted), func(t *testing.T) {
			ctx, s := newSnapshotBackupTestStorage(t)
			configureSnapshotBackupFolder(t, ctx, s)
			meta, err := s.GetSnapshotMeta(ctx, "snapshot")
			require.NoError(t, err)
			require.Empty(t, meta.BackupTaskID)
			if accepted {
				meta, err = s.ReserveSnapshotBackup(ctx, "snapshot", "owner", "coordinator", false)
				require.NoError(t, err)
				require.Equal(t, "owner", meta.BackupTaskID)
			}
			deleting, err := s.DeleteSnapshot(ctx, "snapshot", "delete", time.Now())
			require.NoError(t, err)
			require.False(t, deleting.Ready)
			meta, err = s.ReserveSnapshotBackup(ctx, "snapshot", "owner", "coordinator", false)
			require.NoError(t, err)
			if !accepted {
				require.Nil(t, meta)
				require.Empty(t, deleting.BackupTaskID)
				require.NoError(t, s.SnapshotDeleted(ctx, "snapshot", time.Now()))
				return
			}
			require.NotNil(t, meta)
			require.False(t, meta.Ready)
			require.Equal(t, "owner", deleting.BackupTaskID)
			require.True(t, errors.Is(s.SnapshotDeleted(ctx, "snapshot", time.Now()), errors.NewInterruptExecutionError()))
			require.NoError(t, s.ReleaseSnapshotBackup(ctx, "snapshot", "stale", true))
			meta, err = s.GetSnapshotMeta(ctx, "snapshot")
			require.NoError(t, err)
			require.Equal(t, "owner", meta.BackupTaskID)
			require.False(t, meta.BackupCompleted)
			require.NoError(t, s.ReleaseSnapshotBackup(ctx, "snapshot", "owner", true))
			meta, err = s.GetSnapshotMeta(ctx, "snapshot")
			require.NoError(t, err)
			require.Empty(t, meta.BackupTaskID)
			require.True(t, meta.BackupCompleted)
			require.NoError(t, s.SnapshotDeleted(ctx, "snapshot", time.Now()))
		})
	}
}

func TestSnapshotBackupStartRacesWithDelete(t *testing.T) {
	ctx, s := newSnapshotBackupTestStorage(t)
	configureSnapshotBackupFolder(t, ctx, s)
	start := make(chan struct{})
	type result struct {
		meta *SnapshotMeta
		err  error
	}
	backup := make(chan result, 1)
	deleted := make(chan error, 1)
	go func() {
		<-start
		meta, err := s.ReserveSnapshotBackup(ctx, "snapshot", "owner", "coordinator", false)
		backup <- result{meta, err}
	}()
	go func() { <-start; _, err := s.DeleteSnapshot(ctx, "snapshot", "delete", time.Now()); deleted <- err }()
	close(start)
	r := <-backup
	require.NoError(t, r.err)
	require.NoError(t, <-deleted)
	meta, err := s.GetSnapshotMeta(ctx, "snapshot")
	require.NoError(t, err)
	require.False(t, meta.Ready)
	require.Equal(t, r.meta != nil, meta.BackupTaskID != "")
	if r.meta != nil {
		require.True(t, errors.Is(s.SnapshotDeleted(ctx, "snapshot", time.Now()), errors.NewInterruptExecutionError()))
		require.NoError(t, s.ReleaseSnapshotBackup(ctx, "snapshot", "owner", true))
	}
	require.NoError(t, s.SnapshotDeleted(ctx, "snapshot", time.Now()))
}

func TestSnapshotBackupFolderHoldsOnlyItsActiveCopy(t *testing.T) {
	ctx, s := newSnapshotBackupTestStorage(t)
	configureSnapshotBackupFolder(t, ctx, s)
	createFolderBackupSnapshot(t, ctx, s, "other", "folder", time.Now())
	meta, err := s.ReserveSnapshotBackup(ctx, "snapshot", "owner", "coordinator", false)
	require.NoError(t, err)
	require.NotNil(t, meta)
	_, err = s.ReserveSnapshotBackup(ctx, "other", "second", "coordinator", false)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	other, err := s.GetSnapshotMeta(ctx, "other")
	require.NoError(t, err)
	require.Empty(t, other.BackupTaskID)
	// Queued resources are not protected in advance and can be deleted.
	_, err = s.DeleteSnapshot(ctx, "other", "delete", time.Now())
	require.NoError(t, err)
	require.NoError(t, s.SnapshotDeleted(ctx, "other", time.Now()))
	require.NoError(t, s.ReleaseSnapshotBackup(ctx, "snapshot", "owner", false))
	createFolderBackupSnapshot(t, ctx, s, "next", "folder", time.Now())
	meta, err = s.ReserveSnapshotBackup(ctx, "next", "second", "coordinator", false)
	require.NoError(t, err)
	require.NotNil(t, meta)
}

func TestSnapshotBackfillPagesSelectReadyResourcesAndRepairCursorOmissions(t *testing.T) {
	ctx, s := newSnapshotBackupTestStorage(t)
	configureSnapshotBackupFolder(t, ctx, s)
	createFolderBackupSnapshot(t, ctx, s, "a", "folder", time.Now())
	createFolderBackupSnapshot(t, ctx, s, "b", "folder", time.Now())
	createFolderBackupSnapshot(t, ctx, s, "another-folder", "other", time.Now())
	_, err := s.CreateSnapshot(ctx, SnapshotMeta{ID: "c", FolderID: "folder", Disk: &types.Disk{DiskId: "disk", ZoneId: "zone"}, CreateRequest: &wrappers.UInt64Value{Value: 1}, CreateTaskID: "c_create", CreatingAt: time.Now()})
	require.NoError(t, err)
	ids, err := s.ListSnapshotBackupCandidates(ctx, "folder", "", 1)
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, ids)
	ids, err = s.ListSnapshotBackupCandidates(ctx, "folder", "a", 100)
	require.NoError(t, err)
	require.Equal(t, []string{"b", "snapshot"}, ids)
	require.NoError(t, s.SnapshotCreated(ctx, "c", "checkpoint", time.Now(), 4096, 4096))
	queue, err := s.ListSnapshotsToBackup(ctx, 100)
	require.NoError(t, err)
	require.Contains(t, queue, SnapshotBackupRequest{SnapshotID: "c"})
	require.NotContains(t, queue, SnapshotBackupRequest{SnapshotID: "another-folder"})
	ids, err = s.ListSnapshotBackupCandidates(ctx, "folder", "b", 1)
	require.NoError(t, err)
	require.Equal(t, []string{"c"}, ids)
	_, err = s.DeleteSnapshot(ctx, "b", "delete", time.Now())
	require.NoError(t, err)
	ids, err = s.ListSnapshotBackupCandidates(ctx, "folder", "a", 1)
	require.NoError(t, err)
	require.Equal(t, []string{"c"}, ids)
	// Resetting the ID cursor catches resources that became ready behind it.
	ids, err = s.ListSnapshotBackupCandidates(ctx, "folder", "", 1)
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, ids)
}

func TestSnapshotBackupFolderDisableKeepsOwnershipUntilAcceptedCopyEnds(t *testing.T) {
	ctx, s := newSnapshotBackupTestStorage(t)
	configureSnapshotBackupFolder(t, ctx, s)
	meta, err := s.ReserveSnapshotBackup(ctx, "snapshot", "owner", "coordinator", false)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.NoError(t, s.SetSnapshotBackupFolder(ctx, "folder", "generation", false))
	folders, err := s.ListSnapshotBackupFolders(ctx)
	require.NoError(t, err)
	require.Empty(t, folders)
	createFolderBackupSnapshot(t, ctx, s, "during-disable", "folder", time.Now())
	queue, err := s.ListSnapshotsToBackup(ctx, 100)
	require.NoError(t, err)
	require.NotContains(t, queue, SnapshotBackupRequest{SnapshotID: "during-disable"})
	rejected, err := s.ReserveSnapshotBackup(ctx, "during-disable", "new-owner", "coordinator", false)
	require.NoError(t, err)
	require.Nil(t, rejected)
	meta, err = s.ReserveSnapshotBackup(ctx, "snapshot", "owner", "coordinator", false)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.True(t, errors.Is(s.SetSnapshotBackupFolder(ctx, "folder", "next", true), errors.NewInterruptExecutionError()))
	require.NoError(t, s.ReleaseSnapshotBackupFolder(ctx, "folder", "coordinator"))
	require.True(t, errors.Is(s.SetSnapshotBackupFolder(ctx, "folder", "next", true), errors.NewInterruptExecutionError()))
	require.NoError(t, s.ReleaseSnapshotBackup(ctx, "snapshot", "owner", true))
	require.True(t, errors.Is(s.SetSnapshotBackupFolder(ctx, "folder", "next", true), errors.NewInterruptExecutionError()))
	require.NoError(t, s.ReleaseSnapshotBackupFolder(ctx, "folder", "coordinator"))
	require.True(t, errors.Is(s.SetSnapshotBackupFolder(ctx, "folder", "generation", true), errors.NewEmptyNonRetriableError()))
	require.NoError(t, s.SetSnapshotBackupFolder(ctx, "folder", "next", true))
	claimed, err := s.ClaimSnapshotBackupFolder(ctx, "folder", "next", "replacement")
	require.NoError(t, err)
	require.True(t, claimed)
	createFolderBackupSnapshot(t, ctx, s, "new", "folder", time.Now())
	meta, err = s.ReserveSnapshotBackup(ctx, "new", "late-owner", "coordinator", false)
	require.NoError(t, err)
	require.Nil(t, meta)
	meta, err = s.ReserveSnapshotBackup(ctx, "new", "replacement-owner", "replacement", false)
	require.NoError(t, err)
	require.NotNil(t, meta)
}

func TestSnapshotBackupFolderSelectionIsIdempotent(t *testing.T) {
	ctx, s := newSnapshotBackupTestStorage(t)
	require.NoError(t, s.SetSnapshotBackupFolder(ctx, "folder", "first", true))
	require.NoError(t, s.SetSnapshotBackupFolder(ctx, "folder", "second", true))
	folders, err := s.ListSnapshotBackupFolders(ctx)
	require.NoError(t, err)
	require.Equal(t, []SnapshotBackupFolder{{FolderID: "folder", Generation: "first"}}, folders)
	require.NoError(t, s.SetSnapshotBackupFolder(ctx, "folder", "", false))
	require.NoError(t, s.SetSnapshotBackupFolder(ctx, "folder", "third", true))
	// A delayed disable for the old generation cannot stop the new one.
	require.NoError(t, s.SetSnapshotBackupFolder(ctx, "folder", "first", false))
	folders, err = s.ListSnapshotBackupFolders(ctx)
	require.NoError(t, err)
	require.Equal(t, []SnapshotBackupFolder{{FolderID: "folder", Generation: "third"}}, folders)
}

func TestSnapshotBackupPlanHoldSerializesWithDelete(t *testing.T) {
	for _, held := range []bool{false, true} {
		t.Run(fmt.Sprint(held), func(t *testing.T) {
			ctx, s := newSnapshotBackupTestStorage(t)
			configureSnapshotBackupFolder(t, ctx, s)
			if held {
				meta, err := s.HoldSnapshotBackupPlan(ctx, "snapshot", "plan", "coordinator")
				require.NoError(t, err)
				require.Equal(t, "plan", meta.BackupPlanID)
				require.Empty(t, meta.BackupTaskID, "preparation is not an accepted copy")
			}
			deleting, err := s.DeleteSnapshot(ctx, "snapshot", "delete", time.Now())
			require.NoError(t, err)
			meta, err := s.HoldSnapshotBackupPlan(ctx, "snapshot", "plan", "coordinator")
			require.NoError(t, err)
			if !held {
				require.Nil(t, meta)
				require.Empty(t, deleting.BackupPlanID)
				require.NoError(t, s.SnapshotDeleted(ctx, "snapshot", time.Now()))
				return
			}
			require.Equal(t, "plan", meta.BackupPlanID)
			require.Equal(t, "plan", deleting.BackupPlanID)
			require.True(t, errors.Is(s.SnapshotDeleted(ctx, "snapshot", time.Now()), errors.NewInterruptExecutionError()))
			// A deleting leaf is not accepted, but a held ancestor needed by a
			// live descendant can still be copied before deletion completes.
			leaf, err := s.ReserveSnapshotBackup(ctx, "snapshot", "plan", "coordinator", false)
			require.NoError(t, err)
			require.Nil(t, leaf)
			ancestor, err := s.ReserveSnapshotBackup(ctx, "snapshot", "plan", "coordinator", true)
			require.NoError(t, err)
			require.Equal(t, "plan", ancestor.BackupTaskID)
			require.NoError(t, s.ReleaseSnapshotBackup(ctx, "snapshot", "stale", true))
			meta, err = s.GetSnapshotMeta(ctx, "snapshot")
			require.NoError(t, err)
			require.Equal(t, "plan", meta.BackupPlanID)
			require.False(t, meta.BackupCompleted)
			require.NoError(t, s.ReleaseSnapshotBackup(ctx, "snapshot", "plan", true))
			require.NoError(t, s.FinishSnapshotBackupPlan(ctx, "folder", "plan"))
			require.NoError(t, s.SnapshotDeleted(ctx, "snapshot", time.Now()))
		})
	}
}

func TestSnapshotBackupPlanProtectsMultipleSourcesAndPrunesWithoutCompletingCopy(t *testing.T) {
	ctx, s := newSnapshotBackupTestStorage(t)
	configureSnapshotBackupFolder(t, ctx, s)
	createFolderBackupSnapshot(t, ctx, s, "ancestor", "folder", time.Now())
	for _, id := range []string{"snapshot", "ancestor"} {
		meta, err := s.HoldSnapshotBackupPlan(ctx, id, "plan", "coordinator")
		require.NoError(t, err)
		require.Equal(t, "plan", meta.BackupPlanID)
	}
	_, err := s.HoldSnapshotBackupPlan(ctx, "ancestor", "other-plan", "coordinator")
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	_, err = s.DeleteSnapshot(ctx, "snapshot", "delete", time.Now())
	require.NoError(t, err)
	require.NoError(t, s.ReleaseSnapshotBackup(ctx, "snapshot", "plan", false))
	leaf, err := s.GetSnapshotMeta(ctx, "snapshot")
	require.NoError(t, err)
	require.Empty(t, leaf.BackupPlanID)
	require.False(t, leaf.BackupCompleted)
	require.NoError(t, s.SnapshotDeleted(ctx, "snapshot", time.Now()))
	ancestor, err := s.GetSnapshotMeta(ctx, "ancestor")
	require.NoError(t, err)
	require.Equal(t, "plan", ancestor.BackupPlanID)
	// Disable/re-enable cannot replace a coordinator while graph holds remain.
	require.NoError(t, s.SetSnapshotBackupFolder(ctx, "folder", "generation", false))
	require.NoError(t, s.ReleaseSnapshotBackupFolder(ctx, "folder", "coordinator"))
	require.True(t, errors.Is(s.SetSnapshotBackupFolder(ctx, "folder", "next", true), errors.NewInterruptExecutionError()))
	require.True(t, errors.Is(s.FinishSnapshotBackupPlan(ctx, "folder", "plan"), errors.NewInterruptExecutionError()))
	require.NoError(t, s.ReleaseSnapshotBackup(ctx, "ancestor", "plan", false))
	require.NoError(t, s.FinishSnapshotBackupPlan(ctx, "folder", "plan"))
	require.NoError(t, s.ReleaseSnapshotBackupFolder(ctx, "folder", "coordinator"))
	require.NoError(t, s.SetSnapshotBackupFolder(ctx, "folder", "next", true))
}

func TestSnapshotBackupPlanHoldRacesWithDelete(t *testing.T) {
	ctx, s := newSnapshotBackupTestStorage(t)
	configureSnapshotBackupFolder(t, ctx, s)
	start := make(chan struct{})
	type result struct {
		meta *SnapshotMeta
		err  error
	}
	hold := make(chan result, 1)
	deleted := make(chan error, 1)
	go func() {
		<-start
		meta, err := s.HoldSnapshotBackupPlan(ctx, "snapshot", "plan", "coordinator")
		hold <- result{meta, err}
	}()
	go func() { <-start; _, err := s.DeleteSnapshot(ctx, "snapshot", "delete", time.Now()); deleted <- err }()
	close(start)
	r := <-hold
	require.NoError(t, r.err)
	require.NoError(t, <-deleted)
	meta, err := s.GetSnapshotMeta(ctx, "snapshot")
	require.NoError(t, err)
	require.False(t, meta.Ready)
	require.Equal(t, r.meta != nil, meta.BackupPlanID != "")
	if r.meta != nil {
		require.True(t, errors.Is(s.SnapshotDeleted(ctx, "snapshot", time.Now()), errors.NewInterruptExecutionError()))
		require.NoError(t, s.ReleaseSnapshotBackup(ctx, "snapshot", "plan", false))
		require.NoError(t, s.FinishSnapshotBackupPlan(ctx, "folder", "plan"))
	}
	require.NoError(t, s.SnapshotDeleted(ctx, "snapshot", time.Now()))
}

func TestPruningBackupPlanLeafDoesNotReleaseActiveCopySlot(t *testing.T) {
	ctx, s := newSnapshotBackupTestStorage(t)
	configureSnapshotBackupFolder(t, ctx, s)
	createFolderBackupSnapshot(t, ctx, s, "ancestor", "folder", time.Now())
	for _, id := range []string{"snapshot", "ancestor"} {
		_, err := s.HoldSnapshotBackupPlan(ctx, id, "plan", "coordinator")
		require.NoError(t, err)
	}
	_, err := s.ReserveSnapshotBackup(ctx, "ancestor", "plan", "coordinator", true)
	require.NoError(t, err)
	require.NoError(t, s.ReleaseSnapshotBackup(ctx, "snapshot", "plan", false))
	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        select active_owner, active_plan from backup_folders where folder_id = "folder";
    `, s.snapshotsPath))
	require.NoError(t, err)
	defer res.Close()
	require.True(t, res.NextResultSet(ctx))
	require.True(t, res.NextRow())
	var owner, plan string
	require.NoError(t, res.ScanWithDefaults(&owner, &plan))
	require.Equal(t, "plan", owner)
	require.Equal(t, "plan", plan)
	require.NoError(t, res.Err())
}
