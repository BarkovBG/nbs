package resources

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

func TestSnapshotBackupReservationSerializesWithDelete(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(fmt.Sprint(accepted), func(t *testing.T) {
			ctx, s := newSnapshotBackupTestStorage(t)
			meta, err := s.GetSnapshotMeta(ctx, "snapshot")
			require.NoError(t, err)
			require.Empty(t, meta.BackupTaskID)
			if accepted {
				meta, err = s.ReserveSnapshotBackup(ctx, "snapshot", "owner")
				require.NoError(t, err)
				require.Equal(t, "owner", meta.BackupTaskID)
			}
			deleting, err := s.DeleteSnapshot(ctx, "snapshot", "delete", time.Now())
			require.NoError(t, err)
			require.False(t, deleting.Ready)
			meta, err = s.ReserveSnapshotBackup(ctx, "snapshot", "owner")
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
	start := make(chan struct{})
	type result struct {
		meta *SnapshotMeta
		err  error
	}
	backup := make(chan result, 1)
	deleted := make(chan error, 1)
	go func() {
		<-start
		meta, err := s.ReserveSnapshotBackup(ctx, "snapshot", "owner")
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
