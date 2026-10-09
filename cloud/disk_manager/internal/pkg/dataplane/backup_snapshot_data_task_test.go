package dataplane

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

const backupTestBucket = "chunks-backup"

type testFollower struct {
	s3           *persistence.S3Client
	backupS3     *backup.S3
	encryptedDEK []byte
}

func newTestFollower(t *testing.T, ctx context.Context) testFollower {
	s3, err := test.NewS3Client()
	require.NoError(t, err)

	exists, err := s3.BucketExists(ctx, backupTestBucket)
	require.NoError(t, err)
	if !exists {
		err = s3.CreateBucket(ctx, backupTestBucket)
		require.NoError(t, err)
	}

	backupS3, err := backup.NewS3(
		s3,
		backupTestBucket,
		t.Name(),
		"kek1",
		make([]byte, 32),
	)
	require.NoError(t, err)

	encryptedDEK, err := backupS3.NewEncryptedDEK()
	require.NoError(t, err)

	return testFollower{
		s3:           s3,
		backupS3:     backupS3,
		encryptedDEK: encryptedDEK,
	}
}

func (f testFollower) getObject(
	ctx context.Context,
	key string,
) (persistence.S3Object, error) {

	return f.backupS3.GetObject(ctx, key)
}

func (f testFollower) getRawObject(
	ctx context.Context,
	key string,
) (persistence.S3Object, error) {

	return f.s3.GetObject(ctx, backupTestBucket, f.backupS3.Key(key))
}

func newBackupSnapshotDataTask(
	storage snapshot_storage.Storage,
	follower testFollower,
	snapshotID string,
) *backupSnapshotDataTask {

	return &backupSnapshotDataTask{
		storage:   storage,
		backupS3:  follower.backupS3,
		batchSize: 1000,
		request: &protos.BackupSnapshotDataRequest{
			SnapshotId:   snapshotID,
			EncryptedDek: follower.encryptedDEK,
		},
		state: &protos.BackupSnapshotDataTaskState{},
	}
}

func newBackupExecutionContext(
	ctx context.Context,
) *mocks.ExecutionContextMock {

	execCtx := mocks.NewExecutionContextMock()
	execCtx.On("SaveState", ctx).Return(nil)
	execCtx.On("GetTaskID").Return("backup")
	return execCtx
}

func enqueueBackupChunks(
	ctx context.Context,
	storage snapshot_storage.Storage,
	entries []snapshot_storage.BackupChunkQueueEntry,
) error {

	for _, entry := range entries {
		err := storage.EnqueueBackupChunks(
			ctx,
			entry.SnapshotID,
			[]snapshot_storage.BackupChunkQueueEntry{entry},
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func createSnapshotWithChunk(
	t *testing.T,
	ctx context.Context,
	storage snapshot_storage.Storage,
	snapshotID string,
) string {

	_, err := storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{ID: snapshotID},
	)
	require.NoError(t, err)

	chunkID, err := storage.WriteChunk(
		ctx,
		"task",
		snapshotID,
		dataplane_common.Chunk{Index: 0, Data: []byte("abc")},
		true, // useS3
	)
	require.NoError(t, err)

	err = storage.SnapshotCreated(
		ctx,
		snapshotID,
		4096, // size
		4096, // storageSize
		1,    // chunkCount
		nil,  // encryption
	)
	require.NoError(t, err)

	return chunkID
}

func readBackupChunkMap(
	t *testing.T,
	ctx context.Context,
	follower testFollower,
	snapshotID string,
) *protos.BackupChunkMap {

	object, err := follower.getObject(ctx, backup.ChunkMapKey(snapshotID))
	require.NoError(t, err)

	chunkMap := &protos.BackupChunkMap{}
	err = proto.Unmarshal(object.Data, chunkMap)
	require.NoError(t, err)

	raw, err := follower.getRawObject(ctx, backup.ChunkMapKey(snapshotID))
	require.NoError(t, err)
	require.NotEqual(t, object.Data, raw.Data)
	require.Equal(t, "kek1", *raw.Metadata["Key-Id"])

	return chunkMap
}

////////////////////////////////////////////////////////////////////////////////

func TestBackupSnapshotDataTask(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)

	disk := &types.Disk{ZoneId: "zone", DiskId: "disk1"}
	_, err := storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{ID: "snap1", Disk: disk},
	)
	require.NoError(t, err)

	chunk0, err := storage.WriteChunk(
		ctx,
		"task",
		"snap1",
		dataplane_common.Chunk{Index: 0, Data: []byte("abc")},
		true, // useS3
	)
	require.NoError(t, err)

	_, err = storage.WriteChunk(
		ctx,
		"task",
		"snap1",
		dataplane_common.Chunk{Index: 1, Zero: true},
		true, // useS3
	)
	require.NoError(t, err)

	err = storage.SnapshotCreated(
		ctx,
		"snap1",
		8192, // size
		4096, // storageSize
		2,    // chunkCount
		nil,  // encryption
	)
	require.NoError(t, err)

	execCtx := newBackupExecutionContext(ctx)
	task := newBackupSnapshotDataTask(storage, follower, "snap1")
	task.request.MetaKey = backup.SnapshotMetaKey("disk1", "snap1")
	task.request.Meta = []byte(`{"id":"snap1"}`)

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	_, err = storage.DeletingSnapshot(ctx, "snap1", "delete")
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	_, err = follower.getObject(ctx, backup.ChunkMapKey("snap1"))
	require.Error(t, err)

	queue, err := storage.GetQueuedChunksToBackup(ctx, 10)
	require.NoError(t, err)
	require.Equal(
		t,
		[]snapshot_storage.BackupChunkQueueEntry{
			{
				SnapshotID:   "snap1",
				ChunkID:      chunk0,
				StoredInS3:   true,
				EncryptedDEK: follower.encryptedDEK,
			},
		},
		queue,
	)

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	err = storage.ChunksBackupCompleted(ctx, queue)
	require.NoError(t, err)

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)

	object, err := follower.getObject(ctx, task.request.MetaKey)
	require.NoError(t, err)
	require.Equal(t, task.request.Meta, object.Data)

	chunkMap := readBackupChunkMap(t, ctx, follower, "snap1")
	require.Equal(t, []string{chunk0, ""}, chunkMap.ChunkIds)

	cleared, err := storage.ClearCompletedBackupChunks(ctx, "snap1", 10)
	require.NoError(t, err)
	require.Zero(t, cleared)

	state, err := task.Save()
	require.NoError(t, err)
	request, err := proto.Marshal(task.request)
	require.NoError(t, err)
	resumed := newBackupSnapshotDataTask(storage, follower, "snap1")
	require.NoError(t, resumed.Load(request, state))

	_, err = storage.DeletingSnapshot(ctx, "snap1", "delete")
	require.NoError(t, err)

	err = resumed.Run(ctx, execCtx)
	require.NoError(t, err)
}

func TestBackupSnapshotDataTaskBacksUpInheritedChunksAfterCreatorDeletion(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)

	disk := &types.Disk{ZoneId: "zone", DiskId: "disk1"}
	_, err := storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{
			ID:           "snap1",
			Disk:         disk,
			CheckpointID: "checkpoint1",
		},
	)
	require.NoError(t, err)

	chunk0, err := storage.WriteChunk(
		ctx,
		"task1",
		"snap1",
		dataplane_common.Chunk{Index: 0, Data: []byte("abc")},
		true, // useS3
	)
	require.NoError(t, err)

	err = storage.SnapshotCreated(
		ctx,
		"snap1",
		4096, // size
		4096, // storageSize
		1,    // chunkCount
		nil,  // encryption
	)
	require.NoError(t, err)

	snap2, err := storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{
			ID:           "snap2",
			Disk:         disk,
			CheckpointID: "checkpoint2",
		},
	)
	require.NoError(t, err)
	require.Equal(t, "snap1", snap2.BaseSnapshotID)

	err = storage.ShallowCopyChunk(
		ctx,
		snapshot_storage.ChunkMapEntry{
			ChunkIndex: 0,
			ChunkID:    chunk0,
			StoredInS3: true,
		},
		"snap2",
	)
	require.NoError(t, err)

	chunk1, err := storage.WriteChunk(
		ctx,
		"task2",
		"snap2",
		dataplane_common.Chunk{Index: 1, Data: []byte("def")},
		true, // useS3
	)
	require.NoError(t, err)

	err = storage.SnapshotCreated(
		ctx,
		"snap2",
		8192, // size
		8192, // storageSize
		2,    // chunkCount
		nil,  // encryption
	)
	require.NoError(t, err)

	// C inherits both A's and B's chunks. Neither ancestor has a backup,
	// and both may disappear before backfilling C.
	_, err = storage.CreateSnapshot(ctx, snapshot_storage.SnapshotMeta{
		ID: "snap3", Disk: disk, CheckpointID: "checkpoint3",
	})
	require.NoError(t, err)
	for index, id := range []string{chunk0, chunk1} {
		require.NoError(t, storage.ShallowCopyChunk(ctx, snapshot_storage.ChunkMapEntry{
			ChunkIndex: uint32(index), ChunkID: id, StoredInS3: true,
		}, "snap3"))
	}
	require.NoError(t, storage.SnapshotCreated(ctx, "snap3", 8192, 8192, 2, nil))
	finishSnapshotDeletion(t, ctx, storage, "snap1")
	finishSnapshotDeletion(t, ctx, storage, "snap2")

	execCtx := newBackupExecutionContext(ctx)
	task := newBackupSnapshotDataTask(storage, follower, "snap3")

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	queue, err := storage.GetQueuedChunksToBackup(ctx, 10)
	require.NoError(t, err)
	require.ElementsMatch(
		t,
		[]snapshot_storage.BackupChunkQueueEntry{
			{SnapshotID: "snap3", ChunkID: chunk0, StoredInS3: true, EncryptedDEK: follower.encryptedDEK},
			{SnapshotID: "snap3", ChunkID: chunk1, StoredInS3: true, EncryptedDEK: follower.encryptedDEK},
		},
		queue,
	)

	workerCtx := mocks.NewExecutionContextMock()
	workerCtx.On("GetTaskID").Return("worker")
	err = newBackupChunksTask(storage, follower).Run(ctx, workerCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	for _, id := range []string{chunk0, chunk1} {
		object, err := follower.getObject(ctx, backup.ChunkKey(id))
		require.NoError(t, err)
		blob, err := storage.ReadChunkBlob(ctx, id, true)
		require.NoError(t, err)
		require.Equal(t, blob.Data, object.Data)
		require.Equal(t, *chunks.NewS3Object(blob).Metadata["Checksum"], *object.Metadata["Checksum"])
	}

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)

	chunkMap := readBackupChunkMap(t, ctx, follower, "snap3")
	require.Equal(t, []string{chunk0, chunk1}, chunkMap.ChunkIds)
}

func TestBackupSnapshotDataTaskEnqueuesInBatches(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)

	_, err := storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{ID: "snap1"},
	)
	require.NoError(t, err)

	chunk0, err := storage.WriteChunk(
		ctx,
		"task",
		"snap1",
		dataplane_common.Chunk{Index: 0, Data: []byte("abc")},
		true, // useS3
	)
	require.NoError(t, err)

	chunk1, err := storage.WriteChunk(
		ctx,
		"task",
		"snap1",
		dataplane_common.Chunk{Index: 1, Data: []byte("def")},
		true, // useS3
	)
	require.NoError(t, err)

	err = storage.SnapshotCreated(
		ctx,
		"snap1",
		8192, // size
		8192, // storageSize
		2,    // chunkCount
		nil,  // encryption
	)
	require.NoError(t, err)

	execCtx := newBackupExecutionContext(ctx)
	task := newBackupSnapshotDataTask(storage, follower, "snap1")
	task.batchSize = 1

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	require.EqualValues(t, 2, task.state.MilestoneChunkIndex)

	queue, err := storage.GetQueuedChunksToBackup(ctx, 10)
	require.NoError(t, err)
	require.ElementsMatch(
		t,
		[]snapshot_storage.BackupChunkQueueEntry{
			{
				SnapshotID:   "snap1",
				ChunkID:      chunk0,
				StoredInS3:   true,
				EncryptedDEK: follower.encryptedDEK,
			},
			{
				SnapshotID:   "snap1",
				ChunkID:      chunk1,
				StoredInS3:   true,
				EncryptedDEK: follower.encryptedDEK,
			},
		},
		queue,
	)

	err = storage.ChunksBackupCompleted(ctx, queue[:1])
	require.NoError(t, err)

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	err = storage.ChunksBackupCompleted(ctx, queue[1:])
	require.NoError(t, err)

	resumed := newBackupSnapshotDataTask(storage, follower, "snap1")
	resumed.batchSize = 1
	resumed.state.MilestoneChunkIndex = 1

	for i := 0; i < 2; i++ {
		err = resumed.Run(ctx, execCtx)
		require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	}
	err = resumed.Run(ctx, execCtx)
	require.NoError(t, err)

	queue, err = storage.GetQueuedChunksToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)

	completed, err := storage.GetBackedUpChunkCount(ctx, "snap1")
	require.NoError(t, err)
	require.Zero(t, completed)
}

func TestBackupSnapshotDataTaskBacksUpChunkStoredInYDB(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)

	_, err := storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{ID: "snap1"},
	)
	require.NoError(t, err)

	chunkID, err := storage.WriteChunk(
		ctx,
		"task",
		"snap1",
		dataplane_common.Chunk{Index: 0, Data: []byte("abc")},
		false, // useS3
	)
	require.NoError(t, err)

	err = storage.SnapshotCreated(
		ctx,
		"snap1",
		4096, // size
		4096, // storageSize
		1,    // chunkCount
		nil,  // encryption
	)
	require.NoError(t, err)

	execCtx := newBackupExecutionContext(ctx)
	task := newBackupSnapshotDataTask(storage, follower, "snap1")

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	queue, err := storage.GetQueuedChunksToBackup(ctx, 10)
	require.NoError(t, err)
	require.Equal(
		t,
		[]snapshot_storage.BackupChunkQueueEntry{
			{
				SnapshotID:   "snap1",
				ChunkID:      chunkID,
				EncryptedDEK: follower.encryptedDEK,
			},
		},
		queue,
	)

	chunksTask := newBackupChunksTask(storage, follower)
	err = chunksTask.Run(ctx, newBackupExecutionContext(ctx))
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	chunkBlob, err := storage.ReadChunkBlob(
		ctx,
		chunkID,
		false, // storedInS3
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

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)

	chunkMap := readBackupChunkMap(t, ctx, follower, "snap1")
	require.Equal(t, []string{chunkID}, chunkMap.ChunkIds)
}

func TestBackupSnapshotDataTaskFailsOnChunkIndexOutOfRange(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)

	_, err := storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{ID: "snap1"},
	)
	require.NoError(t, err)

	for index := uint32(0); index < 2; index++ {
		_, err = storage.WriteChunk(
			ctx,
			"task",
			"snap1",
			dataplane_common.Chunk{Index: index, Data: []byte("abc")},
			true, // useS3
		)
		require.NoError(t, err)
	}

	err = storage.SnapshotCreated(
		ctx,
		"snap1",
		4096, // size
		4096, // storageSize
		1,    // chunkCount
		nil,  // encryption
	)
	require.NoError(t, err)

	execCtx := newBackupExecutionContext(ctx)
	task := newBackupSnapshotDataTask(storage, follower, "snap1")

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewEmptyRetriableError()))

	_, err = follower.getObject(ctx, backup.ChunkMapKey("snap1"))
	require.Error(t, err)

	_, err = storage.DeletingSnapshot(ctx, "snap1", "delete")
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	require.NoError(t, task.Cancel(ctx, execCtx))
	_, err = storage.DeletingSnapshot(ctx, "snap1", "delete")
	require.NoError(t, err)
}

func TestBackupSnapshotDataTaskBacksUpShallowCopyWithoutCreatorBackup(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	chunk0 := createSnapshotWithChunk(t, ctx, storage, "snap1")

	// The source has never been backed up. The image must copy its data itself.
	_, err := storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{ID: "image1"},
	)
	require.NoError(t, err)

	err = storage.ShallowCopyChunk(
		ctx,
		snapshot_storage.ChunkMapEntry{
			ChunkIndex: 0,
			ChunkID:    chunk0,
			StoredInS3: true,
		},
		"image1",
	)
	require.NoError(t, err)

	err = storage.SnapshotCreated(
		ctx,
		"image1",
		4096, // size
		4096, // storageSize
		1,    // chunkCount
		nil,  // encryption
	)
	require.NoError(t, err)

	execCtx := newBackupExecutionContext(ctx)
	task := newBackupSnapshotDataTask(storage, follower, "image1")

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	workerCtx := mocks.NewExecutionContextMock()
	workerCtx.On("GetTaskID").Return("worker")
	err = newBackupChunksTask(storage, follower).Run(ctx, workerCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	require.NoError(t, task.Run(ctx, execCtx))
	_, err = follower.getObject(ctx, backup.ChunkKey(chunk0))
	require.NoError(t, err)

	queue, err := storage.GetQueuedChunksToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)

	chunkMap := readBackupChunkMap(t, ctx, follower, "image1")
	require.Equal(t, []string{chunk0}, chunkMap.ChunkIds)
}

func TestBackupSnapshotDataTaskFailsOnDeletedSnapshot(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	createSnapshotWithChunk(t, ctx, storage, "snap1")

	_, err := storage.DeletingSnapshot(ctx, "snap1", "deleteTaskID")
	require.NoError(t, err)

	execCtx := newBackupExecutionContext(ctx)
	task := newBackupSnapshotDataTask(storage, follower, "snap1")

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	queue, err := storage.GetQueuedChunksToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)
}

func TestBackupSnapshotDataTaskRejectsBadDEK(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	createSnapshotWithChunk(t, ctx, storage, "snap1")

	execCtx := newBackupExecutionContext(ctx)
	task := newBackupSnapshotDataTask(storage, follower, "snap1")
	task.request.EncryptedDek = []byte("bad")

	err := task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	require.Zero(t, task.state.MilestoneChunkIndex)

	queue, err := storage.GetQueuedChunksToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)
}

func TestBackupSnapshotDataTaskRetriesBadDEKForAcceptedCPOwner(t *testing.T) {
	ctx := test.NewContext()
	storage := storage_mocks.NewStorageMock()
	task := newBackupSnapshotDataTask(storage, newTestFollower(t, ctx), "snapshot")
	task.request.OwnerId = "accepted"
	task.request.EncryptedDek = []byte("bad")
	err := task.Run(ctx, newBackupExecutionContext(ctx))
	retriable := errors.NewEmptyRetriableError()
	require.True(t, errors.As(err, &retriable))
	require.True(t, retriable.IgnoreRetryLimit)
	require.False(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	storage.AssertNotCalled(t, "HoldSnapshotForBackup", mock.Anything, mock.Anything, mock.Anything)
}

func TestBackupSnapshotDataTaskCancellationDrainsWorkers(t *testing.T) {
	ctx := test.NewContext()
	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()
	follower := newTestFollower(t, ctx)
	createSnapshotWithChunk(t, ctx, storage, "snap1")
	task := newBackupSnapshotDataTask(storage, follower, "snap1")
	execCtx := newBackupExecutionContext(ctx)

	err := task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	queue, err := storage.GetQueuedChunksToBackup(ctx, 10)
	require.NoError(t, err)
	claimed, err := storage.ClaimBackupChunks(ctx, "worker", queue)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	err = task.Cancel(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	_, err = storage.DeletingSnapshot(ctx, "snap1", "delete")
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	queue, err = storage.GetQueuedChunksToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)

	// A worker cancelled after a failed execution also releases its claims.
	worker := newBackupChunksTask(storage, follower)
	workerCtx := mocks.NewExecutionContextMock()
	workerCtx.On("GetTaskID").Return("worker")
	require.NoError(t, worker.Cancel(ctx, workerCtx))
	require.NoError(t, task.Cancel(ctx, execCtx))
	_, err = storage.DeletingSnapshot(ctx, "snap1", "delete")
	require.NoError(t, err)
	// Retrying cancellation after deletion is harmless.
	require.NoError(t, task.Cancel(ctx, execCtx))
}

func TestBackupSnapshotDataTaskCancellationRetriesCleanupErrors(t *testing.T) {
	ctx := test.NewContext()
	storage := storage_mocks.NewStorageMock()
	storage.On("DrainSnapshotBackup", mock.Anything, "snap1", "backup", 1000).Return(
		errors.NewNonRetriableErrorf("storage failure"),
	).Once()
	task := newBackupSnapshotDataTask(storage, testFollower{}, "snap1")
	err := task.Cancel(ctx, newBackupExecutionContext(ctx))
	require.True(t, errors.Is(err, errors.NewEmptyRetriableError()))
	require.False(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	storage.AssertNotCalled(t, "ReleaseSnapshotForBackup", mock.Anything, "snap1", "backup")
	storage.AssertExpectations(t)
}

func TestBackupSnapshotDataTaskHandlesRepeatedChunkIDs(t *testing.T) {
	for _, batchSize := range []int{1, 1000} {
		t.Run(fmt.Sprint(batchSize), func(t *testing.T) {
			ctx := test.NewContext()
			storage, closeFunc := newStorage(t, ctx)
			defer closeFunc()
			follower := newTestFollower(t, ctx)
			chunkID := createSnapshotWithChunk(t, ctx, storage, "source")
			_, err := storage.CreateSnapshot(ctx, snapshot_storage.SnapshotMeta{ID: "copy"})
			require.NoError(t, err)
			for index := uint32(0); index < 2; index++ {
				require.NoError(t, storage.ShallowCopyChunk(ctx, snapshot_storage.ChunkMapEntry{
					ChunkIndex: index, ChunkID: chunkID, StoredInS3: true,
				}, "copy"))
			}
			require.NoError(t, storage.SnapshotCreated(ctx, "copy", 12288, 8192, 3, nil))
			task := newBackupSnapshotDataTask(storage, follower, "copy")
			task.batchSize = batchSize
			execCtx := newBackupExecutionContext(ctx)
			err = task.Run(ctx, execCtx)
			require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
			queue, err := storage.GetQueuedChunksToBackup(ctx, 10)
			require.NoError(t, err)
			require.Len(t, queue, 1)
			workerCtx := mocks.NewExecutionContextMock()
			workerCtx.On("GetTaskID").Return("worker")
			err = newBackupChunksTask(storage, follower).Run(ctx, workerCtx)
			require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
			for i := 0; i < 3; i++ {
				err = task.Run(ctx, execCtx)
				if err == nil {
					break
				}
				require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
			}
			require.NoError(t, err)
			require.Equal(t, []string{chunkID, chunkID, ""}, readBackupChunkMap(t, ctx, follower, "copy").ChunkIds)
		})
	}
}

type failingBackupChunkStorage struct {
	snapshot_storage.Storage
}

func (s failingBackupChunkStorage) ReadChunkBlob(
	ctx context.Context,
	chunkID string,
	storedInS3 bool,
) (chunks.ChunkBlob, error) {

	return chunks.ChunkBlob{}, errors.NewNonRetriableErrorf("source chunk is corrupt")
}

func TestBackupSnapshotDataTaskRetainsHoldUntilExplicitCancellationOnChunkError(t *testing.T) {
	ctx := test.NewContext()
	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()
	follower := newTestFollower(t, ctx)
	createSnapshotWithChunk(t, ctx, storage, "snapshot")
	task := newBackupSnapshotDataTask(storage, follower, "snapshot")
	execCtx := newBackupExecutionContext(ctx)
	err := task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	workerCtx := mocks.NewExecutionContextMock()
	workerCtx.On("GetTaskID").Return("worker")
	worker := newBackupChunksTask(failingBackupChunkStorage{Storage: storage}, follower)
	err = worker.Run(ctx, workerCtx)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewEmptyRetriableError()))
	_, err = follower.getObject(ctx, backup.ChunkMapKey("snapshot"))
	require.Error(t, err)
	require.NoError(t, task.Cancel(ctx, execCtx))
	_, err = storage.DeletingSnapshot(ctx, "snapshot", "delete")
	require.NoError(t, err)
}

func TestBackupSnapshotDataTaskRecoversFailedChunksWithoutReleasingSource(t *testing.T) {
	ctx := test.NewContext()
	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()
	follower := newTestFollower(t, ctx)
	createSnapshotWithChunk(t, ctx, storage, "snapshot")
	task := newBackupSnapshotDataTask(storage, follower, "snapshot")
	execCtx := newBackupExecutionContext(ctx)
	require.True(t, errors.Is(task.Run(ctx, execCtx), errors.NewInterruptExecutionError()))
	workerCtx := mocks.NewExecutionContextMock()
	workerCtx.On("GetTaskID").Return("worker")
	worker := newBackupChunksTask(failingBackupChunkStorage{Storage: storage}, follower)
	require.True(t, errors.Is(worker.Run(ctx, workerCtx), errors.NewEmptyNonRetriableError()))
	require.True(t, errors.Is(task.Run(ctx, execCtx), errors.NewEmptyRetriableError()))
	_, err := storage.DeletingSnapshot(ctx, "snapshot", "delete")
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	// A subsequent retry requeues failed IDs while keeping the same hold/DEK.
	require.True(t, errors.Is(task.Run(ctx, execCtx), errors.NewInterruptExecutionError()))
	require.True(t, errors.Is(newBackupChunksTask(storage, follower).Run(ctx, workerCtx), errors.NewInterruptExecutionError()))
	require.NoError(t, task.Run(ctx, execCtx))
	_, err = storage.DeletingSnapshot(ctx, "snapshot", "delete")
	require.NoError(t, err)
}
