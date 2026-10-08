package dataplane

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

func TestPrepareSnapshotBackupValidatesInheritedChunksAndKeepsHold(t *testing.T) {
	ctx := context.Background()
	store := storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("SaveState", ctx).Return(nil)
	store.On("HoldSnapshotForBackup", ctx, "child", "owner").Return(nil).Once()
	store.On("CheckSnapshotReady", ctx, "child").Return(storage.SnapshotMeta{ID: "child", BaseSnapshotID: "base", ChunkCount: 3}, nil).Once()
	entries := make(chan storage.ChunkMapEntry, 3)
	entries <- storage.ChunkMapEntry{ChunkIndex: 0, ChunkID: "base.chunk"}
	entries <- storage.ChunkMapEntry{ChunkIndex: 1}
	entries <- storage.ChunkMapEntry{ChunkIndex: 2, ChunkID: "child.chunk", StoredInS3: true}
	close(entries)
	readErrors := make(chan error, 1)
	readErrors <- nil
	store.On("ReadChunkMap", mock.Anything, "child", uint32(0)).Return((<-chan storage.ChunkMapEntry)(entries), (<-chan error)(readErrors)).Once()
	store.On("ReadChunkBlob", ctx, "base.chunk", false).Return(chunks.ChunkBlob{}, nil).Once()
	store.On("ReadChunkBlob", ctx, "child.chunk", true).Return(chunks.ChunkBlob{}, nil).Once()
	task := &prepareSnapshotBackupTask{storage: store, request: &protos.PrepareSnapshotBackupRequest{SnapshotId: "child", OwnerId: "owner"}, state: &protos.PrepareSnapshotBackupState{}}
	require.NoError(t, task.Run(ctx, execCtx))
	require.True(t, task.state.Prepared)
	require.Equal(t, "base", task.GetResponse().(*protos.PrepareSnapshotBackupResponse).BaseSnapshotId)
	saved, err := task.Save()
	require.NoError(t, err)
	request, err := proto.Marshal(task.request)
	require.NoError(t, err)
	resumed := &prepareSnapshotBackupTask{storage: store}
	require.NoError(t, resumed.Load(request, saved))
	require.NoError(t, resumed.Run(ctx, execCtx))
	store.AssertNotCalled(t, "ReleaseSnapshotForBackup", mock.Anything, mock.Anything, mock.Anything)
	store.On("ReleaseSnapshotForBackup", ctx, "child", "owner").Return(nil).Once()
	require.NoError(t, resumed.Cancel(ctx, execCtx))
	mock.AssertExpectationsForObjects(t, store, execCtx)
}

func TestPrepareSnapshotBackupKeepsSourceOnMissingChunk(t *testing.T) {
	ctx := context.Background()
	store := storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	store.On("HoldSnapshotForBackup", ctx, "child", "owner").Return(nil)
	store.On("CheckSnapshotReady", ctx, "child").Return(storage.SnapshotMeta{ID: "child", ChunkCount: 1}, nil)
	entries := make(chan storage.ChunkMapEntry, 1)
	entries <- storage.ChunkMapEntry{ChunkID: "missing"}
	close(entries)
	readErrors := make(chan error, 1)
	readErrors <- nil
	store.On("ReadChunkMap", mock.Anything, "child", uint32(0)).Return((<-chan storage.ChunkMapEntry)(entries), (<-chan error)(readErrors))
	store.On("ReadChunkBlob", ctx, "missing", false).Return(chunks.ChunkBlob{}, errors.NewNonRetriableErrorf("missing"))
	task := &prepareSnapshotBackupTask{storage: store, request: &protos.PrepareSnapshotBackupRequest{SnapshotId: "child", OwnerId: "owner"}, state: &protos.PrepareSnapshotBackupState{}}
	err := task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewEmptyRetriableError()))
	require.False(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	require.False(t, task.state.Prepared)
	store.AssertNotCalled(t, "ReleaseSnapshotForBackup", mock.Anything, mock.Anything, mock.Anything)
	mock.AssertExpectationsForObjects(t, store, execCtx)
}
