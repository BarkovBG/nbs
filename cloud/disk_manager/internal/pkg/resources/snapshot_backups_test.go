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
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

func configureSnapshotBackupFolder(t *testing.T, ctx context.Context, s *storageYDB) {
	t.Helper()
	_, err := s.db.ExecuteRW(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        update snapshots set folder_id = "folder" where id = "snapshot";
        upsert into backup_folders (folder_id, generation, prepared) values ("folder", "generation", true);
    `, s.snapshotsPath))
	require.NoError(t, err)
	claimed, err := s.ClaimSnapshotBackupFolder(ctx, "folder", "generation", "coordinator")
	require.NoError(t, err)
	require.True(t, claimed)
	require.NoError(t, s.SaveSnapshotBackupPlan(ctx, SnapshotBackupPlan{FolderID: "folder", SnapshotID: "snapshot", OwnerID: "owner", CoordinatorID: "coordinator", Prepared: true, CreatingAt: time.Now()}))
}

func TestSnapshotBackupReservationSerializesWithDelete(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(fmt.Sprint(accepted), func(t *testing.T) {
			ctx, s := newSnapshotBackupTestStorage(t)
			configureSnapshotBackupFolder(t, ctx, s)
			meta, err := s.ReserveSnapshotBackup(ctx, "snapshot", "owner", false)
			require.NoError(t, err)
			require.NotNil(t, meta)
			if accepted {
				meta, err = s.ReserveSnapshotBackup(ctx, "snapshot", "owner", true)
				require.NoError(t, err)
				require.True(t, meta.BackupStarted)
			}
			deleting, err := s.DeleteSnapshot(ctx, "snapshot", "delete", time.Now())
			require.NoError(t, err)
			require.False(t, deleting.Ready)
			require.Equal(t, "owner", deleting.BackupTaskID)
			err = s.SnapshotDeleted(ctx, "snapshot", time.Now())
			require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
			meta, err = s.ReserveSnapshotBackup(ctx, "snapshot", "owner", true)
			require.NoError(t, err)
			if accepted {
				require.NotNil(t, meta)
				require.False(t, meta.Ready)
			} else {
				require.Nil(t, meta)
			}
			require.NoError(t, s.ReleaseSnapshotBackup(ctx, "snapshot", "stale", true))
			meta, err = s.GetSnapshotMeta(ctx, "snapshot")
			require.NoError(t, err)
			require.Equal(t, "owner", meta.BackupTaskID)
			require.False(t, meta.BackupCompleted)
			require.NoError(t, s.ReleaseSnapshotBackup(ctx, "snapshot", "owner", accepted))
			meta, err = s.GetSnapshotMeta(ctx, "snapshot")
			require.NoError(t, err)
			require.Empty(t, meta.BackupTaskID)
			require.Equal(t, accepted, meta.BackupCompleted)
			require.NoError(t, s.SnapshotDeleted(ctx, "snapshot", time.Now()))
		})
	}
}

func TestSnapshotBackupStartRacesWithDelete(t *testing.T) {
	ctx, s := newSnapshotBackupTestStorage(t)
	configureSnapshotBackupFolder(t, ctx, s)
	_, err := s.ReserveSnapshotBackup(ctx, "snapshot", "owner", false)
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
		m, e := s.ReserveSnapshotBackup(ctx, "snapshot", "owner", true)
		backup <- result{m, e}
	}()
	go func() { <-start; _, e := s.DeleteSnapshot(ctx, "snapshot", "delete", time.Now()); deleted <- e }()
	close(start)
	r := <-backup
	require.NoError(t, r.err)
	require.NoError(t, <-deleted)
	meta, err := s.GetSnapshotMeta(ctx, "snapshot")
	require.NoError(t, err)
	require.False(t, meta.Ready)
	require.Equal(t, r.meta != nil, meta.BackupStarted)
	require.Equal(t, "owner", meta.BackupTaskID)
}

func createFolderBackupSnapshot(t *testing.T, ctx context.Context, s *storageYDB, id, folder string, creatingAt time.Time) {
	t.Helper()
	_, err := s.CreateSnapshot(ctx, SnapshotMeta{ID: id, FolderID: folder, Disk: &types.Disk{DiskId: "disk", ZoneId: "zone"}, CreateRequest: &wrappers.UInt64Value{Value: 1}, CreateTaskID: id + "_create", CreatingAt: creatingAt})
	require.NoError(t, err)
	require.NoError(t, s.SnapshotCreated(ctx, id, "checkpoint", time.Now(), 4096, 4096))
}

func TestSnapshotBackupFolderPreparationAndSerialization(t *testing.T) {
	ctx, s := newSnapshotBackupTestStorage(t)
	configureSnapshotBackupFolder(t, ctx, s)
	createFolderBackupSnapshot(t, ctx, s, "other", "folder", time.Now())
	require.NoError(t, s.SaveSnapshotBackupPlan(ctx, SnapshotBackupPlan{FolderID: "folder", SnapshotID: "other", OwnerID: "second", CoordinatorID: "coordinator", Prepared: true, CreatingAt: time.Now(), BaseSnapshotID: "snapshot"}))
	plans, err := s.ListSnapshotBackupPlans(ctx, "folder", 10, true, "")
	require.NoError(t, err)
	require.Len(t, plans, 1)
	require.Equal(t, "snapshot", plans[0].SnapshotID)
	require.NoError(t, s.SetSnapshotBackupFolderPrepared(ctx, "folder", "generation", false))
	_, err = s.ReserveSnapshotBackup(ctx, "snapshot", "owner", false)
	require.NoError(t, err)
	_, err = s.ReserveSnapshotBackup(ctx, "snapshot", "owner", true)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	require.NoError(t, s.SetSnapshotBackupFolderPrepared(ctx, "folder", "generation", true))
	_, err = s.ReserveSnapshotBackup(ctx, "snapshot", "owner", true)
	require.NoError(t, err)
	_, err = s.ReserveSnapshotBackup(ctx, "other", "second", false)
	require.NoError(t, err)
	_, err = s.ReserveSnapshotBackup(ctx, "other", "second", true)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	require.NoError(t, s.ReleaseSnapshotBackup(ctx, "snapshot", "owner", true))
	require.NoError(t, s.RemoveSnapshotBackupPlan(ctx, "folder", "snapshot", "stale"))
	plans, err = s.ListSnapshotBackupPlans(ctx, "folder", 10, true, "")
	require.NoError(t, err)
	require.Len(t, plans, 1)
	require.Equal(t, "snapshot", plans[0].SnapshotID)
	require.NoError(t, s.RemoveSnapshotBackupPlan(ctx, "folder", "snapshot", "owner"))
	plans, err = s.ListSnapshotBackupPlans(ctx, "folder", 10, true, "")
	require.NoError(t, err)
	require.Len(t, plans, 1)
	require.Equal(t, "other", plans[0].SnapshotID)
	_, err = s.ReserveSnapshotBackup(ctx, "other", "second", true)
	require.NoError(t, err)
}

func TestSnapshotBackfillPagesExcludeOtherFoldersAndNewResources(t *testing.T) {
	ctx, s := newSnapshotBackupTestStorage(t)
	configureSnapshotBackupFolder(t, ctx, s)
	cutoff := time.Now()
	createFolderBackupSnapshot(t, ctx, s, "a", "folder", cutoff.Add(-time.Hour))
	createFolderBackupSnapshot(t, ctx, s, "b", "folder", cutoff.Add(-time.Minute))
	createFolderBackupSnapshot(t, ctx, s, "c", "folder", cutoff.Add(time.Hour))
	createFolderBackupSnapshot(t, ctx, s, "another-folder", "other", cutoff.Add(-time.Hour))
	ids, err := s.ListSnapshotBackupCandidates(ctx, "folder", "", cutoff, 1)
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, ids)
	ids, err = s.ListSnapshotBackupCandidates(ctx, "folder", "a", cutoff, 1)
	require.NoError(t, err)
	require.Equal(t, []string{"b"}, ids)
	// New ready resources are already queued even while their folder is preparing.
	queue, err := s.ListSnapshotsToBackup(ctx, 100)
	require.NoError(t, err)
	require.Contains(t, queue, SnapshotBackupRequest{SnapshotID: "c"})
	require.NotContains(t, queue, SnapshotBackupRequest{SnapshotID: "another-folder"})
	_, err = s.DeleteSnapshot(ctx, "b", "delete", time.Now())
	require.NoError(t, err)
	ids, err = s.ListSnapshotBackupCandidates(ctx, "folder", "a", cutoff, 1)
	require.NoError(t, err)
	require.Equal(t, []string{"snapshot"}, ids)
}

func TestSnapshotFolderCoordinatorCannotAdoptForeignPendingWork(t *testing.T) {
	ctx, s := newSnapshotBackupTestStorage(t)
	configureSnapshotBackupFolder(t, ctx, s)
	_, err := s.db.ExecuteRW(ctx, fmt.Sprintf(`--!syntax_v1
        pragma TablePathPrefix = "%v";
        update backup_folders set generation = "next", coordinator_task_id = "" where folder_id = "folder";
    `, s.snapshotsPath))
	require.NoError(t, err)
	_, err = s.ClaimSnapshotBackupFolder(ctx, "folder", "next", "replacement")
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	require.NoError(t, s.RemoveSnapshotBackupPlan(ctx, "folder", "snapshot", "owner"))
	claimed, err := s.ClaimSnapshotBackupFolder(ctx, "folder", "next", "replacement")
	require.NoError(t, err)
	require.True(t, claimed)
	_, err = s.db.ExecuteRW(ctx, fmt.Sprintf(`--!syntax_v1
        pragma TablePathPrefix = "%v";
        declare $folder as Utf8;
        delete from backup_folders where folder_id = $folder;
    `, s.snapshotsPath), persistence.ValueParam("$folder", persistence.UTF8Value("folder")))
	require.NoError(t, err)
	claimed, err = s.ClaimSnapshotBackupFolder(ctx, "folder", "next", "replacement")
	require.NoError(t, err)
	require.False(t, claimed)
}

func TestSnapshotBackupFolderSelectionIsIdempotent(t *testing.T) {
	ctx, s := newSnapshotBackupTestStorage(t)
	require.NoError(t, s.SetSnapshotBackupFolder(ctx, "folder", "first", true))
	require.NoError(t, s.SetSnapshotBackupFolder(ctx, "folder", "second", true))
	folders, err := s.ListSnapshotBackupFolders(ctx)
	require.NoError(t, err)
	require.Equal(t, []SnapshotBackupFolder{{FolderID: "folder", Generation: "first"}}, folders)
	require.NoError(t, s.SetSnapshotBackupFolder(ctx, "folder", "", false))
	folders, err = s.ListSnapshotBackupFolders(ctx)
	require.NoError(t, err)
	require.Empty(t, folders)
	require.NoError(t, s.SetSnapshotBackupFolder(ctx, "folder", "third", true))
	folders, err = s.ListSnapshotBackupFolders(ctx)
	require.NoError(t, err)
	require.Equal(t, []SnapshotBackupFolder{{FolderID: "folder", Generation: "third"}}, folders)
}
