package dataplane

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

func TestSnapshotBackupDependenciesUseChunkCreators(t *testing.T) {
	ctx := test.NewContext()
	s := storage_mocks.NewStorageMock()
	execCtx := mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("reader")
	execCtx.On("SaveState", mock.Anything).Return(nil)
	s.On("GetSnapshotMeta", ctx, "C").Return(&storage.SnapshotMeta{ID: "C", Ready: true, ChunkCount: 5}, nil)
	s.On("HoldSnapshotForBackup", ctx, "C", "reader").Return(nil).Once()
	entries := make(chan storage.ChunkMapEntry, 5)
	for i, id := range []string{"task.A.0", "task.A.1", "task.C.2", "task.B.3", ""} {
		entries <- storage.ChunkMapEntry{ChunkIndex: uint32(i), ChunkID: id}
	}
	close(entries)
	readErrors := make(chan error, 1)
	readErrors <- nil
	s.On("ReadChunkMap", mock.Anything, "C", uint32(0)).Return((<-chan storage.ChunkMapEntry)(entries), (<-chan error)(readErrors)).Once()
	s.On("ReleaseSnapshotForBackup", ctx, "C", "reader").Return(nil)
	task := &getSnapshotBackupDependenciesTask{storage: s, request: &protos.GetSnapshotBackupDependenciesRequest{SnapshotId: "C"}, state: &protos.GetSnapshotBackupDependenciesState{}}
	require.NoError(t, task.Run(ctx, execCtx))
	require.Equal(t, []string{"A", "B"}, task.GetResponse().(*protos.GetSnapshotBackupDependenciesResponse).SnapshotIds)
	// Repeating a finished reader only releases ownership; it never rereads.
	require.NoError(t, task.Run(ctx, execCtx))
	mock.AssertExpectationsForObjects(t, s, execCtx)
}
