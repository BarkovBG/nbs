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

func createBackupSnapshot(t *testing.T, ctx context.Context, s *storageYDB, id, folder string, creatingAt time.Time) {
	t.Helper()
	_, err := s.CreateSnapshot(ctx, SnapshotMeta{ID: id, FolderID: folder, Disk: &types.Disk{DiskId: "disk", ZoneId: "zone"}, CreateRequest: &wrappers.UInt64Value{Value: 1}, CreateTaskID: id + "_create", CreatingAt: creatingAt})
	require.NoError(t, err)
	require.NoError(t, s.SnapshotCreated(ctx, id, "checkpoint", time.Now(), 4096, 4096))
}

func TestSnapshotBackupReservationSerializesWithDelete(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(fmt.Sprint(accepted), func(t *testing.T) {
			ctx, s := newSnapshotBackupTestStorage(t)
			require.NoError(t, s.ClaimSnapshotBackupPlan(ctx, "owner"))
			meta, err := s.GetSnapshotMeta(ctx, "snapshot")
			require.NoError(t, err)
			require.Empty(t, meta.BackupTaskID)
			if accepted {
				_, err = s.HoldSnapshotBackupPlan(ctx, "snapshot", "owner")
				require.NoError(t, err)
				meta, err = s.ReserveSnapshotBackup(ctx, "snapshot", "owner", false)
				require.NoError(t, err)
				require.Equal(t, "owner", meta.BackupTaskID)
			}
			deleting, err := s.DeleteSnapshot(ctx, "snapshot", "delete", time.Now())
			require.NoError(t, err)
			require.False(t, deleting.Ready)
			meta, err = s.ReserveSnapshotBackup(ctx, "snapshot", "owner", false)
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
	require.NoError(t, s.ClaimSnapshotBackupPlan(ctx, "owner"))
	_, err := s.HoldSnapshotBackupPlan(ctx, "snapshot", "owner")
	require.NoError(t, err)
	start := make(chan struct{})
	type result struct {
		meta *SnapshotMeta
		err  error
	}
	backup := make(chan result, 1)
	deleted := make(chan error, 1)
	go func() {
		<-start
		meta, err := s.ReserveSnapshotBackup(ctx, "snapshot", "owner", false)
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
	require.NoError(t, s.ReleaseSnapshotBackup(ctx, "snapshot", "owner", false))
	require.NoError(t, s.SnapshotDeleted(ctx, "snapshot", time.Now()))
}

func TestSnapshotBackupPlanHoldSerializesWithDelete(t *testing.T) {
	for _, held := range []bool{false, true} {
		t.Run(fmt.Sprint(held), func(t *testing.T) {
			ctx, s := newSnapshotBackupTestStorage(t)
			require.NoError(t, s.ClaimSnapshotBackupPlan(ctx, "plan"))
			if held {
				meta, err := s.HoldSnapshotBackupPlan(ctx, "snapshot", "plan")
				require.NoError(t, err)
				require.Equal(t, "plan", meta.BackupPlanID)
				require.Empty(t, meta.BackupTaskID, "preparation is not an accepted copy")
			}
			deleting, err := s.DeleteSnapshot(ctx, "snapshot", "delete", time.Now())
			require.NoError(t, err)
			meta, err := s.HoldSnapshotBackupPlan(ctx, "snapshot", "plan")
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
			leaf, err := s.ReserveSnapshotBackup(ctx, "snapshot", "plan", false)
			require.NoError(t, err)
			require.Nil(t, leaf)
			ancestor, err := s.ReserveSnapshotBackup(ctx, "snapshot", "plan", true)
			require.NoError(t, err)
			require.Equal(t, "plan", ancestor.BackupTaskID)
			require.NoError(t, s.ReleaseSnapshotBackup(ctx, "snapshot", "stale", true))
			meta, err = s.GetSnapshotMeta(ctx, "snapshot")
			require.NoError(t, err)
			require.Equal(t, "plan", meta.BackupPlanID)
			require.False(t, meta.BackupCompleted)
			require.NoError(t, s.ReleaseSnapshotBackup(ctx, "snapshot", "plan", true))
			require.NoError(t, s.FinishSnapshotBackupPlan(ctx, "plan"))
			require.NoError(t, s.SnapshotDeleted(ctx, "snapshot", time.Now()))
		})
	}
}

func TestSnapshotBackupPlanHoldRacesWithDelete(t *testing.T) {
	ctx, s := newSnapshotBackupTestStorage(t)
	require.NoError(t, s.ClaimSnapshotBackupPlan(ctx, "plan"))
	start := make(chan struct{})
	type result struct {
		meta *SnapshotMeta
		err  error
	}
	hold := make(chan result, 1)
	deleted := make(chan error, 1)
	go func() {
		<-start
		meta, err := s.HoldSnapshotBackupPlan(ctx, "snapshot", "plan")
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
		require.NoError(t, s.FinishSnapshotBackupPlan(ctx, "plan"))
	}
	require.NoError(t, s.SnapshotDeleted(ctx, "snapshot", time.Now()))
}

func TestSnapshotBackupQueueSerializesIntersectingPlansAndRejectsStaleCleanup(t *testing.T) {
	ctx, s := newSnapshotBackupTestStorage(t)
	createBackupSnapshot(t, ctx, s, "ancestor", "another-folder", time.Now())
	require.NoError(t, s.ClaimSnapshotBackupPlan(ctx, "first"))
	for _, id := range []string{"snapshot", "ancestor"} {
		meta, err := s.HoldSnapshotBackupPlan(ctx, id, "first")
		require.NoError(t, err)
		require.Equal(t, "first", meta.BackupPlanID)
	}
	require.NoError(t, s.ClaimSnapshotBackupPlan(ctx, "first"), "retry resumes the same plan")
	require.True(t, errors.Is(s.ClaimSnapshotBackupPlan(ctx, "second"), errors.NewInterruptExecutionError()))
	require.True(t, errors.Is(s.FinishSnapshotBackupPlan(ctx, "first"), errors.NewInterruptExecutionError()))
	require.NoError(t, s.ReleaseSnapshotBackup(ctx, "snapshot", "first", false))
	require.True(t, errors.Is(s.FinishSnapshotBackupPlan(ctx, "first"), errors.NewInterruptExecutionError()))
	require.NoError(t, s.ReleaseSnapshotBackup(ctx, "ancestor", "first", false))
	require.NoError(t, s.FinishSnapshotBackupPlan(ctx, "first"))
	require.NoError(t, s.ClaimSnapshotBackupPlan(ctx, "second"))
	_, err := s.HoldSnapshotBackupPlan(ctx, "ancestor", "second")
	require.NoError(t, err)
	require.NoError(t, s.FinishSnapshotBackupPlan(ctx, "first"), "stale cancellation cannot release the new queue slot")
	require.NoError(t, s.ReleaseSnapshotBackup(ctx, "ancestor", "first", true))
	meta, err := s.GetSnapshotMeta(ctx, "ancestor")
	require.NoError(t, err)
	require.Equal(t, "second", meta.BackupPlanID)
	require.False(t, meta.BackupCompleted)
	require.True(t, errors.Is(s.ClaimSnapshotBackupPlan(ctx, "third"), errors.NewInterruptExecutionError()))
}
