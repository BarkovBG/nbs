package resources

import (
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

func TestEnqueueBackupBatch(t *testing.T) {
	for _, kind := range []string{"snapshots", "images"} {
		t.Run(kind, func(t *testing.T) {
			ctx, storage := newSnapshotBackupTestStorage(t)
			storage.backupEnabled = false
			cutoff := time.Unix(1700000000, 0)
			for _, id := range []string{"a", "b", "c", "d", "future", "creating", "deleted"} {
				storage.backupEnabled = id == "b"
				created := cutoff.Add(-time.Hour)
				if id == "future" {
					created = cutoff.Add(time.Hour)
				}
				if kind == "snapshots" {
					_, err := storage.CreateSnapshot(ctx, SnapshotMeta{
						ID: id, Disk: &types.Disk{DiskId: "disk", ZoneId: "zone"},
						CreateTaskID: "create_" + id, CreatingAt: created,
						CreateRequest: &wrappers.UInt64Value{Value: 1},
					})
					require.NoError(t, err)
					if id != "creating" {
						require.NoError(t, storage.SnapshotCreated(ctx, id, "cp", created, 0, 0))
					}
					if id == "deleted" {
						_, err = storage.DeleteSnapshot(ctx, id, "delete", cutoff)
						require.NoError(t, err)
					}
				} else {
					_, err := storage.CreateImage(ctx, ImageMeta{
						ID: id, CreateTaskID: "create_" + id, CreatingAt: created,
						CreateRequest: &wrappers.UInt64Value{Value: 1},
					})
					require.NoError(t, err)
					if id != "creating" {
						require.NoError(t, storage.ImageCreated(ctx, id, "cp", created, 0, 0))
					}
					if id == "deleted" {
						_, err = storage.DeleteImage(ctx, id, "delete", cutoff)
						require.NoError(t, err)
					}
				}
			}
			if kind == "snapshots" {
				require.NoError(t, storage.EnqueueSnapshotBackup(ctx, "a", "done"))
				require.NoError(t, storage.SnapshotBackupScheduled(ctx, "a", "done"))
				require.NoError(t, storage.EnqueueSnapshotBackup(ctx, "c", "existing"))
			} else {
				require.NoError(t, storage.EnqueueImageBackup(ctx, "a", "done"))
				require.NoError(t, storage.ImageBackupScheduled(ctx, "a", "done"))
				require.NoError(t, storage.EnqueueImageBackup(ctx, "c", "existing"))
			}
			page, err := storage.EnqueueBackupBatch(ctx, kind, "", cutoff, 2, "backfill")
			require.NoError(t, err)
			require.Equal(t, []BackupRequest{{ResourceID: "b"}, {ResourceID: "c", BackupID: "existing"}}, page)
			repeated, err := storage.EnqueueBackupBatch(ctx, kind, "", cutoff, 2, "another")
			require.NoError(t, err)
			require.Equal(t, page, repeated)
			page, err = storage.EnqueueBackupBatch(ctx, kind, "c", cutoff, 2, "backfill")
			require.NoError(t, err)
			require.Equal(t, []BackupRequest{{ResourceID: "d", BackupID: "backfill"}}, page)
			page, err = storage.EnqueueBackupBatch(ctx, kind, "d", cutoff, 2, "backfill")
			require.NoError(t, err)
			require.Empty(t, page)
		})
	}
}
