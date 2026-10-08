package dataplane

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func newBackupChunksTask(
	storage snapshot_storage.Storage,
	follower testFollower,
) *backupChunksTask {

	return &backupChunksTask{
		storage:       storage,
		backupS3:      follower.backupS3,
		batchSize:     10,
		inflightLimit: 2,
		registry:      metrics.NewEmptyRegistry(),
		state:         &protos.BackupChunksTaskState{},
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestBackupChunksTask(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	chunkID := createSnapshotWithChunk(t, ctx, storage, "snap1")

	entries := []snapshot_storage.BackupChunkQueueEntry{
		{SnapshotID: "snap1", ChunkID: chunkID, StoredInS3: true},
	}
	err := enqueueBackupChunks(ctx, storage, entries)
	require.NoError(t, err)

	task := newBackupChunksTask(storage, follower)
	execCtx := mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("worker")

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	chunkBlob, err := storage.ReadChunkBlob(
		ctx,
		chunkID,
		true, // storedInS3
	)
	require.NoError(t, err)

	object, err := follower.getObject(ctx, backup.ChunkKey(chunkID))
	require.NoError(t, err)
	require.Equal(t, chunkBlob.Data, object.Data)
	require.Equal(
		t,
		*chunks.NewS3Object(chunkBlob).Metadata["Checksum"],
		*object.Metadata["Checksum"],
	)

	queue, err := storage.GetQueuedChunksToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)
}

func TestBackupChunksTaskCopiesSeveralBatches(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	chunk0 := createSnapshotWithChunk(t, ctx, storage, "snap1")
	chunk1 := createSnapshotWithChunk(t, ctx, storage, "snap2")

	entries := []snapshot_storage.BackupChunkQueueEntry{
		{SnapshotID: "snap1", ChunkID: chunk0, StoredInS3: true},
		{SnapshotID: "snap2", ChunkID: chunk1, StoredInS3: true},
	}
	err := enqueueBackupChunks(ctx, storage, entries)
	require.NoError(t, err)

	task := newBackupChunksTask(storage, follower)
	task.batchSize = 1
	execCtx := mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("worker")

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	for _, chunkID := range []string{chunk0, chunk1} {
		_, err = follower.getObject(ctx, backup.ChunkKey(chunkID))
		require.NoError(t, err)
	}

	queue, err := storage.GetQueuedChunksToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)
}

func TestBackupChunksTaskGoesOnPastMissingChunk(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	chunkID := createSnapshotWithChunk(t, ctx, storage, "snap2")

	missing := snapshot_storage.BackupChunkQueueEntry{
		SnapshotID: "snap1",
		ChunkID:    "task.snap1.0",
		StoredInS3: true,
	}
	entries := []snapshot_storage.BackupChunkQueueEntry{
		missing,
		{SnapshotID: "snap2", ChunkID: chunkID, StoredInS3: true},
	}
	err := enqueueBackupChunks(ctx, storage, entries)
	require.NoError(t, err)

	task := newBackupChunksTask(storage, follower)
	execCtx := mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("worker")

	err = task.Run(ctx, execCtx)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	_, err = follower.getObject(ctx, backup.ChunkKey(chunkID))
	require.NoError(t, err)

	queue, err := storage.GetQueuedChunksToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)
	err = storage.CheckBackupChunksCompleted(ctx, "snap1")
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	require.NoError(t, storage.CheckBackupChunksCompleted(ctx, "snap2"))

}

func TestBackupChunksTaskRecoversClaimsAfterRestart(t *testing.T) {
	ctx := test.NewContext()
	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()
	follower := newTestFollower(t, ctx)
	chunkID := createSnapshotWithChunk(t, ctx, storage, "snap1")
	require.NoError(t, storage.HoldSnapshotForBackup(ctx, "snap1", "backup"))
	entries := []snapshot_storage.BackupChunkQueueEntry{{
		SnapshotID: "snap1", ChunkID: chunkID, StoredInS3: true,
	}}
	require.NoError(t, storage.EnqueueBackupChunks(ctx, "snap1", entries))
	claimed, err := storage.ClaimBackupChunks(ctx, "worker", entries)
	require.NoError(t, err)
	require.Equal(t, entries, claimed)
	err = storage.DrainSnapshotBackup(ctx, "snap1", "backup", 10)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	// Simulate a worker restart after it registered its batch. Cancellation
	// removed the queue, so the resumed worker must only release its claims.
	task := newBackupChunksTask(storage, follower)
	execCtx := mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("worker")
	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	require.NoError(t, storage.DrainSnapshotBackup(ctx, "snap1", "backup", 10))
	_, err = follower.getObject(ctx, backup.ChunkKey(chunkID))
	require.Error(t, err)
}

func TestBackupChunksTaskChecksFollowerBeforeReadingSource(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusForbidden, http.StatusServiceUnavailable} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodHead, r.Method)
				w.WriteHeader(status)
			}))
			defer server.Close()
			client, err := persistence.NewS3Client(server.URL, "region", persistence.NewS3Credentials("test", "test"), time.Second, metrics.NewEmptyRegistry(), 0, nil, nil)
			require.NoError(t, err)
			storage := storage_mocks.NewStorageMock()
			task := &backupChunksTask{storage: storage, backupS3: backup.NewS3(client, "bucket", "prefix"), registry: metrics.NewEmptyRegistry()}
			err = task.copyChunk(test.NewContext(), snapshot_storage.BackupChunkQueueEntry{SnapshotID: "snapshot", ChunkID: "chunk"})
			if status == http.StatusOK {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			// No expectations: any source read would fail this test. Errors on
			// HEAD must also prevent reads/PUT and completion of the queue entry.
			storage.AssertExpectations(t)
		})
	}
}
