package dataplane

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

func TestSnapshotBackupLockKeepsChunkCreatorsUntilReleaseAfterRestart(t *testing.T) {
	ctx := test.NewContext()
	s := storage_mocks.NewStorageMock()
	execCtx := mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("lock-task")
	execCtx.On("HasEvent", ctx, int64(protos.SnapshotBackupLockEvent_RELEASE)).Return(false)
	execCtx.On("SaveState", mock.Anything).Return(nil)
	s.On("GetSnapshotMeta", ctx, "C").Return(&storage.SnapshotMeta{ID: "C", Ready: true, ChunkCount: 5}, nil).Once()
	// Only C is held. Its existing chunk references protect A's/B's blobs.
	s.On("HoldSnapshotForBackup", ctx, "C", "lock-task").Return(nil).Once()
	entries := make(chan storage.ChunkMapEntry, 5)
	for i, id := range []string{"task.A.0", "task.A.1", "task.C.2", "task.B.3", ""} {
		entries <- storage.ChunkMapEntry{ChunkIndex: uint32(i), ChunkID: id}
	}
	close(entries)
	readErrors := make(chan error, 1)
	readErrors <- nil
	s.On("ReadChunkMap", mock.Anything, "C", uint32(0)).Return((<-chan storage.ChunkMapEntry)(entries), (<-chan error)(readErrors)).Once()
	task := &lockSnapshotForBackupTask{storage: s, request: &protos.LockSnapshotForBackupRequest{SnapshotId: "C"}, state: &protos.LockSnapshotForBackupState{}}
	require.True(t, errors.Is(task.Run(ctx, execCtx), errors.NewInterruptExecutionError()))
	message, err := task.GetMetadata(ctx)
	require.NoError(t, err)
	metadata := message.(*protos.LockSnapshotForBackupState)
	require.True(t, metadata.Ready)
	require.True(t, metadata.Locked)
	require.Equal(t, []string{"A", "B"}, metadata.SnapshotIds)
	s.AssertNotCalled(t, "ReleaseSnapshotForBackup", mock.Anything, mock.Anything, mock.Anything)
	request, err := proto.Marshal(task.request)
	require.NoError(t, err)
	state, err := task.Save()
	require.NoError(t, err)
	resumed := &lockSnapshotForBackupTask{storage: s}
	require.NoError(t, resumed.Load(request, state))
	require.True(t, errors.Is(resumed.Run(ctx, execCtx), errors.NewInterruptExecutionError()))
	execCtx.ExpectedCalls = nil
	execCtx.On("GetTaskID").Return("lock-task")
	execCtx.On("HasEvent", ctx, int64(protos.SnapshotBackupLockEvent_RELEASE)).Return(true)
	s.On("ReleaseSnapshotForBackup", ctx, "C", "lock-task").Return(nil).Once()
	require.NoError(t, resumed.Run(ctx, execCtx))
	s.AssertExpectations(t)
}

func TestSnapshotBackupLockRejectsDeletionThatWonAdmission(t *testing.T) {
	ctx := test.NewContext()
	s := storage_mocks.NewStorageMock()
	execCtx := mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("lock-task")
	execCtx.On("HasEvent", ctx, int64(protos.SnapshotBackupLockEvent_RELEASE)).Return(false)
	execCtx.On("SaveState", mock.Anything).Return(nil)
	s.On("GetSnapshotMeta", ctx, "C").Return(&storage.SnapshotMeta{ID: "C", Ready: true}, nil).Once()
	s.On("HoldSnapshotForBackup", ctx, "C", "lock-task").Return(errors.NewNonRetriableErrorf("deleting")).Once()
	s.On("GetSnapshotMeta", ctx, "C").Return(&storage.SnapshotMeta{ID: "C"}, nil).Once()
	task := &lockSnapshotForBackupTask{storage: s, request: &protos.LockSnapshotForBackupRequest{SnapshotId: "C"}, state: &protos.LockSnapshotForBackupState{}}
	require.NoError(t, task.Run(ctx, execCtx))
	require.True(t, task.state.Ready)
	require.False(t, task.state.Locked)
	s.AssertNotCalled(t, "ReadChunkMap", mock.Anything, mock.Anything, mock.Anything)
	s.AssertExpectations(t)
}

func TestSnapshotBackupLockRetriesFailedReleaseAndCancellation(t *testing.T) {
	ctx := test.NewContext()
	s := storage_mocks.NewStorageMock()
	execCtx := mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("lock-task")
	execCtx.On("HasEvent", ctx, int64(protos.SnapshotBackupLockEvent_RELEASE)).Return(true)
	s.On("ReleaseSnapshotForBackup", ctx, "C", "lock-task").Return(errors.NewNonRetriableErrorf("db unavailable")).Once()
	s.On("ReleaseSnapshotForBackup", ctx, "C", "lock-task").Return(nil).Once()
	task := &lockSnapshotForBackupTask{storage: s, request: &protos.LockSnapshotForBackupRequest{SnapshotId: "C"}, state: &protos.LockSnapshotForBackupState{}}
	require.True(t, errors.Is(task.Run(ctx, execCtx), errors.NewEmptyRetriableError()))
	require.NoError(t, task.Cancel(ctx, execCtx))
	s.AssertNotCalled(t, "HoldSnapshotForBackup", mock.Anything, mock.Anything, mock.Anything)
	s.AssertExpectations(t)
}

func TestSnapshotBackupCopyDrainsAndReleasesOnlyItsOwnReference(t *testing.T) {
	ctx := test.NewContext()
	s := storage_mocks.NewStorageMock()
	execCtx := mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("copy-task")
	s.On("DrainSnapshotBackup", ctx, "C", "copy-task", 1000).Return(errors.NewInterruptExecutionError()).Once()
	s.On("DrainSnapshotBackup", ctx, "C", "copy-task", 1000).Return(nil)
	s.On("ReleaseSnapshotForBackup", ctx, "C", "copy-task").Return(nil)
	task := &backupSnapshotDataTask{storage: s, batchSize: 1000, request: &protos.BackupSnapshotDataRequest{SnapshotId: "C"}, state: &protos.BackupSnapshotDataTaskState{Finished: true}}
	require.True(t, errors.Is(task.Run(ctx, execCtx), errors.NewInterruptExecutionError()))
	s.AssertNotCalled(t, "ReleaseSnapshotForBackup", mock.Anything, mock.Anything, mock.Anything)
	require.NoError(t, task.Run(ctx, execCtx))
	require.NoError(t, task.Cancel(ctx, execCtx))
	s.AssertNotCalled(t, "ReleaseSnapshotForBackup", ctx, "C", "lock-task")
	s.AssertExpectations(t)
}

func TestSnapshotBackupLockDoesNotExposeDependenciesBeforeSuccessfulStateSave(t *testing.T) {
	ctx := test.NewContext()
	s := storage_mocks.NewStorageMock()
	execCtx := mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("lock-task")
	execCtx.On("HasEvent", ctx, int64(protos.SnapshotBackupLockEvent_RELEASE)).Return(false)
	execCtx.On("SaveState", mock.Anything).Return(errors.NewRetriableErrorf("save failed")).Once()
	execCtx.On("SaveState", mock.Anything).Return(nil).Once()
	s.On("GetSnapshotMeta", ctx, "C").Return(&storage.SnapshotMeta{ID: "C", Ready: true}, nil).Once()
	s.On("HoldSnapshotForBackup", ctx, "C", "lock-task").Return(nil).Once()
	entries := make(chan storage.ChunkMapEntry)
	close(entries)
	readErrors := make(chan error, 1)
	readErrors <- nil
	s.On("ReadChunkMap", mock.Anything, "C", uint32(0)).Return((<-chan storage.ChunkMapEntry)(entries), (<-chan error)(readErrors)).Once()
	task := &lockSnapshotForBackupTask{storage: s, request: &protos.LockSnapshotForBackupRequest{SnapshotId: "C"}, state: &protos.LockSnapshotForBackupState{}}
	require.True(t, errors.Is(task.Run(ctx, execCtx), errors.NewEmptyRetriableError()))
	// A repeated invocation must retry SaveState rather than wait forever
	// with readiness only in memory. The scheduler loads metadata from storage.
	require.True(t, errors.Is(task.Run(ctx, execCtx), errors.NewInterruptExecutionError()))
	s.AssertNotCalled(t, "ReleaseSnapshotForBackup", mock.Anything, mock.Anything, mock.Anything)
	mock.AssertExpectationsForObjects(t, s, execCtx)
}
