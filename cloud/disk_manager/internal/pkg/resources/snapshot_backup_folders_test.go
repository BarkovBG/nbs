package resources

import (
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

func TestSnapshotBackupFolderSelectionFencesOldProducerAndKeepsManualQueue(t *testing.T) {
	ctx, storage := newSnapshotBackupTestStorage(t)
	require.NoError(t, storage.SetSnapshotBackupFolder(ctx, "folder", "first", true))
	require.NoError(t, storage.SetSnapshotBackupFolder(ctx, "folder", "ignored", true))
	folders, err := storage.ListSnapshotBackupFolders(ctx)
	require.NoError(t, err)
	require.Equal(t, []SnapshotBackupFolder{{FolderID: "folder", Generation: "first"}}, folders)
	require.NoError(t, storage.EnqueueSnapshotBackup(ctx, "snapshot", "manual"))
	selected, err := storage.EnqueueSnapshotBackupForFolder(ctx, "snapshot", "producer", "folder", "first")
	require.NoError(t, err)
	require.True(t, selected)
	queue, err := storage.ListSnapshotsToBackup(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []SnapshotBackupRequest{{SnapshotID: "snapshot", BackupID: "manual"}}, queue)
	require.NoError(t, storage.SetSnapshotBackupFolder(ctx, "folder", "first", false))
	selected, err = storage.EnqueueSnapshotBackupForFolder(ctx, "snapshot", "old", "folder", "first")
	require.NoError(t, err)
	require.False(t, selected)
	require.NoError(t, storage.SetSnapshotBackupFolder(ctx, "folder", "second", true))
	// An old cancellation cannot disable the new enable cycle.
	require.NoError(t, storage.SetSnapshotBackupFolder(ctx, "folder", "first", false))
	selected, err = storage.EnqueueSnapshotBackupForFolder(ctx, "snapshot", "old", "folder", "first")
	require.NoError(t, err)
	require.False(t, selected)
	folders, err = storage.ListSnapshotBackupFolders(ctx)
	require.NoError(t, err)
	require.Equal(t, []SnapshotBackupFolder{{FolderID: "folder", Generation: "second"}}, folders)
}

func TestSnapshotBackupFolderPagesAndNewReadySnapshotsUseSameQueue(t *testing.T) {
	ctx, storage := newSnapshotBackupTestStorage(t)
	require.NoError(t, storage.SetSnapshotBackupFolder(ctx, "folder", "cycle", true))
	createBackupSnapshot(t, ctx, storage, "a", "folder", time.Now())
	createBackupSnapshot(t, ctx, storage, "b", "folder", time.Now())
	createBackupSnapshot(t, ctx, storage, "other", "other-folder", time.Now())
	_, err := storage.CreateSnapshot(ctx, SnapshotMeta{ID: "c", FolderID: "folder", Disk: &types.Disk{DiskId: "disk", ZoneId: "zone"}, CreateRequest: &wrappers.UInt64Value{Value: 1}, CreateTaskID: "c_create", CreatingAt: time.Now()})
	require.NoError(t, err)
	ids, err := storage.ListSnapshotBackupCandidates(ctx, "folder", "", 1)
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, ids)
	ids, err = storage.ListSnapshotBackupCandidates(ctx, "folder", "b", 10)
	require.NoError(t, err)
	require.Equal(t, []string{"snapshot"}, ids)
	require.NoError(t, storage.SnapshotCreated(ctx, "c", "cp", time.Now(), 0, 0))
	queue, err := storage.ListSnapshotsToBackup(ctx, 10)
	require.NoError(t, err)
	require.Contains(t, queue, SnapshotBackupRequest{SnapshotID: "c", BackupID: "cycle"})
	require.NotContains(t, queue, SnapshotBackupRequest{SnapshotID: "other", BackupID: "cycle"})
	ids, err = storage.ListSnapshotBackupCandidates(ctx, "folder", "b", 1)
	require.NoError(t, err)
	require.Equal(t, []string{"c"}, ids)
	_, err = storage.DeleteSnapshot(ctx, "c", "delete", time.Now())
	require.NoError(t, err)
	selected, err := storage.EnqueueSnapshotBackupForFolder(ctx, "c", "scan", "folder", "cycle")
	require.NoError(t, err)
	require.True(t, selected)
	queue, err = storage.ListSnapshotsToBackup(ctx, 10)
	require.NoError(t, err)
	require.NotContains(t, queue, SnapshotBackupRequest{SnapshotID: "c", BackupID: "scan"})
	// Resetting a completed page pass discovers resources behind the cursor.
	ids, err = storage.ListSnapshotBackupCandidates(ctx, "folder", "", 1)
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, ids)
}

func TestDisabledFolderDoesNotBlockManualSingleSnapshotBackup(t *testing.T) {
	ctx, storage := newSnapshotBackupTestStorage(t)
	require.NoError(t, storage.SetSnapshotBackupFolder(ctx, "folder", "cycle", true))
	require.NoError(t, storage.SetSnapshotBackupFolder(ctx, "folder", "cycle", false))
	require.NoError(t, storage.EnqueueSnapshotBackup(ctx, "snapshot", "manual"))
	queue, err := storage.ListSnapshotsToBackup(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []SnapshotBackupRequest{{SnapshotID: "snapshot", BackupID: "manual"}}, queue)
}
