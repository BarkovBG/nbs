package snapshots

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

func TestBackupFolderProducerResumesCursorAndStartsFreshRound(t *testing.T) {
	ctx := context.Background()
	storage := resources_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("producer")
	execCtx.On("SaveState", mock.Anything).Return(nil)
	storage.On("ListSnapshotBackupFolders", ctx).Return([]resources.SnapshotBackupFolder{{FolderID: "folder", Generation: "cycle"}}, nil)
	storage.On("ListSnapshotBackupCandidates", ctx, "folder", "", 100).Return([]string{"C", "D"}, nil).Once()
	storage.On("EnqueueSnapshotBackupForFolder", ctx, "C", "producer_cycle_1", "folder", "cycle").Return(true, nil).Once()
	storage.On("EnqueueSnapshotBackupForFolder", ctx, "D", "producer_cycle_1", "folder", "cycle").Return(false, errors.NewRetriableErrorf("temporary queue failure")).Once()
	task := &backupFolderTask{storage: storage, request: &protos.BackupFolderRequest{FolderId: "folder", Generation: "cycle"}, state: &protos.BackupFolderState{}}
	require.Error(t, task.Run(ctx, execCtx))
	require.Equal(t, "C", task.state.AfterId)
	request, err := proto.Marshal(task.request)
	require.NoError(t, err)
	state, err := task.Save()
	require.NoError(t, err)
	require.NoError(t, task.Load(request, state))
	storage.On("ListSnapshotBackupCandidates", ctx, "folder", "C", 100).Return([]string{"D"}, nil).Once()
	storage.On("EnqueueSnapshotBackupForFolder", ctx, "D", "producer_cycle_1", "folder", "cycle").Return(true, nil).Once()
	require.True(t, errors.Is(task.Run(ctx, execCtx), errors.NewInterruptExecutionError()))
	storage.On("ListSnapshotBackupCandidates", ctx, "folder", "D", 100).Return([]string(nil), nil).Once()
	require.True(t, errors.Is(task.Run(ctx, execCtx), errors.NewInterruptExecutionError()))
	require.Empty(t, task.state.AfterId)
	require.EqualValues(t, 2, task.state.Round)
	storage.On("ListSnapshotBackupCandidates", ctx, "folder", "", 100).Return([]string{"C"}, nil).Once()
	storage.On("EnqueueSnapshotBackupForFolder", ctx, "C", "producer_cycle_2", "folder", "cycle").Return(true, nil).Once()
	require.True(t, errors.Is(task.Run(ctx, execCtx), errors.NewInterruptExecutionError()))
	storage.AssertExpectations(t)
}

func TestBackupFolderProducerStopsWhenSelectionChangesDuringPage(t *testing.T) {
	ctx := context.Background()
	storage := resources_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("producer")
	storage.On("ListSnapshotBackupFolders", ctx).Return([]resources.SnapshotBackupFolder{{FolderID: "folder", Generation: "old"}}, nil)
	storage.On("ListSnapshotBackupCandidates", ctx, "folder", "", 100).Return([]string{"C", "D"}, nil)
	storage.On("EnqueueSnapshotBackupForFolder", ctx, "C", "producer_old_1", "folder", "old").Return(false, nil).Once()
	task := &backupFolderTask{storage: storage, request: &protos.BackupFolderRequest{FolderId: "folder", Generation: "old"}, state: &protos.BackupFolderState{Round: 1}}
	require.NoError(t, task.Run(ctx, execCtx))
	storage.AssertNotCalled(t, "EnqueueSnapshotBackupForFolder", mock.Anything, "D", mock.Anything, mock.Anything, mock.Anything)
	storage.AssertExpectations(t)
}
