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
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

func TestBackupFolderStartsFirstCopyWithoutPreparingOtherSnapshots(t *testing.T) {
	ctx := context.Background()
	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("coordinator")
	execCtx.On("SaveState", ctx).Return(nil)
	storage.On("ListSnapshotBackupFolders", ctx).Return([]resources.SnapshotBackupFolder{{FolderID: "folder", Generation: "generation"}}, nil)
	storage.On("ClaimSnapshotBackupFolder", ctx, "folder", "generation", "coordinator").Return(true, nil)
	storage.On("ListSnapshotBackupCandidates", ctx, "folder", "", 1).Return([]string{"a"}, nil).Once()
	scheduler.On("ScheduleTask", mock.Anything, "snapshots.BackupSnapshot", "", mock.MatchedBy(func(r *protos.BackupSnapshotRequest) bool {
		return r.SnapshotId == "a" && r.BackupId == "coordinator_1_a" && r.CoordinatorId == "coordinator"
	})).Return("copy-a", nil).Once()
	scheduler.On("WaitTask", ctx, execCtx, "copy-a").Return((*protos.BackupSnapshotResponse)(nil), errors.NewInterruptExecutionError()).Once()
	task := &backupFolderTask{storage: storage, scheduler: scheduler, request: &protos.BackupFolderRequest{FolderId: "folder", Generation: "generation"}, state: &protos.BackupFolderState{}}
	require.True(t, errors.Is(task.Run(ctx, execCtx), errors.NewInterruptExecutionError()))
	require.Equal(t, "a", task.state.SnapshotId)
	require.Equal(t, "copy-a", task.state.BackupTaskId)
	require.Empty(t, task.state.AfterId)
	storage.AssertNotCalled(t, "ListSnapshotBackupCandidates", ctx, "folder", "a", 1)
	// Once the first copy is fully finished, the next ready resource is taken.
	scheduler.On("WaitTask", ctx, execCtx, "copy-a").Return(&protos.BackupSnapshotResponse{Completed: true}, nil).Once()
	storage.On("ListSnapshotBackupCandidates", ctx, "folder", "a", 1).Return([]string{"b"}, nil).Once()
	scheduler.On("ScheduleTask", mock.Anything, "snapshots.BackupSnapshot", "", mock.MatchedBy(func(r *protos.BackupSnapshotRequest) bool {
		return r.SnapshotId == "b" && r.BackupId == "coordinator_2_b"
	})).Return("copy-b", nil).Once()
	scheduler.On("WaitTask", ctx, execCtx, "copy-b").Return(&protos.BackupSnapshotResponse{Completed: true}, nil).Once()
	storage.On("ListSnapshotBackupCandidates", ctx, "folder", "b", 1).Return([]string(nil), nil).Once()
	storage.On("ListSnapshotBackupCandidates", ctx, "folder", "", 1).Return([]string(nil), nil).Once()
	require.True(t, errors.Is(task.Run(ctx, execCtx), errors.NewInterruptExecutionError()))
	require.Empty(t, task.state.SnapshotId)
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
}

func TestBackupFolderRecoversChildWhoseIDWasNotSaved(t *testing.T) {
	ctx := context.Background()
	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("coordinator")
	storage.On("ListSnapshotBackupFolders", ctx).Return([]resources.SnapshotBackupFolder{{FolderID: "folder", Generation: "generation"}}, nil)
	storage.On("ClaimSnapshotBackupFolder", ctx, "folder", "generation", "coordinator").Return(true, nil)
	storage.On("ListSnapshotBackupCandidates", ctx, "folder", "", 1).Return([]string{"a"}, nil).Once()
	task := &backupFolderTask{storage: storage, scheduler: scheduler, request: &protos.BackupFolderRequest{FolderId: "folder", Generation: "generation"}, state: &protos.BackupFolderState{}}
	var saved []byte
	execCtx.On("SaveState", ctx).Run(func(mock.Arguments) {
		var err error
		saved, err = task.Save()
		require.NoError(t, err)
	}).Return(nil).Once()
	execCtx.On("SaveState", ctx).Return(errors.NewRetriableErrorf("lost child checkpoint")).Once()
	scheduler.On("ScheduleTask", mock.MatchedBy(func(ctx context.Context) bool {
		return headers.GetIdempotencyKey(ctx) == "backup_snapshot_a_coordinator_1_a"
	}), "snapshots.BackupSnapshot", "", mock.MatchedBy(func(r *protos.BackupSnapshotRequest) bool {
		return r.SnapshotId == "a" && r.BackupId == "coordinator_1_a" && r.CoordinatorId == "coordinator"
	})).Return("same-child", nil).Twice()
	require.True(t, errors.Is(task.Run(ctx, execCtx), errors.NewEmptyRetriableError()))
	serialized, err := proto.Marshal(task.request)
	require.NoError(t, err)
	resumed := &backupFolderTask{storage: storage, scheduler: scheduler}
	require.NoError(t, resumed.Load(serialized, saved))
	require.Empty(t, resumed.state.BackupTaskId)
	resumedCtx := tasks_mocks.NewExecutionContextMock()
	resumedCtx.On("GetTaskID").Return("coordinator")
	resumedCtx.On("SaveState", ctx).Return(nil)
	scheduler.On("WaitTask", ctx, resumedCtx, "same-child").Return(&protos.BackupSnapshotResponse{Completed: true}, nil).Once()
	storage.On("ListSnapshotBackupCandidates", ctx, "folder", "a", 1).Return([]string(nil), nil).Once()
	storage.On("ListSnapshotBackupCandidates", ctx, "folder", "", 1).Return([]string(nil), nil).Once()
	require.True(t, errors.Is(resumed.Run(ctx, resumedCtx), errors.NewInterruptExecutionError()))
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx, resumedCtx)
}

func TestBackupFolderDisableFinishesCurrentChildBeforeReleasingCoordinator(t *testing.T) {
	for _, completed := range []bool{false, true} {
		t.Run(map[bool]string{false: "excluded-before-admission", true: "accepted-copy"}[completed], func(t *testing.T) {
			ctx := context.Background()
			storage := resources_mocks.NewStorageMock()
			scheduler := tasks_mocks.NewSchedulerMock()
			execCtx := tasks_mocks.NewExecutionContextMock()
			execCtx.On("GetTaskID").Return("coordinator")
			execCtx.On("SaveState", ctx).Return(nil)
			storage.On("ListSnapshotBackupFolders", ctx).Return([]resources.SnapshotBackupFolder(nil), nil).Once()
			scheduler.On("ScheduleTask", mock.Anything, "snapshots.BackupSnapshot", "", mock.MatchedBy(func(r *protos.BackupSnapshotRequest) bool {
				return r.BackupId == "owner" && r.CoordinatorId == "coordinator"
			})).Return("copy-a", nil).Once()
			scheduler.On("WaitTask", ctx, execCtx, "copy-a").Return(&protos.BackupSnapshotResponse{Completed: completed}, nil).Once()
			storage.On("ReleaseSnapshotBackupFolder", ctx, "folder", "coordinator").Return(nil).Once()
			task := &backupFolderTask{storage: storage, scheduler: scheduler, request: &protos.BackupFolderRequest{FolderId: "folder", Generation: "generation"}, state: &protos.BackupFolderState{SnapshotId: "a", OwnerId: "owner"}}
			require.NoError(t, task.Run(ctx, execCtx))
			scheduler.AssertNotCalled(t, "CancelTask", mock.Anything, mock.Anything)
			storage.AssertNotCalled(t, "ListSnapshotBackupCandidates", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
		})
	}
}

func TestBackupFolderCancellationWaitsForChildCleanup(t *testing.T) {
	ctx := context.Background()
	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("coordinator")
	execCtx.On("SaveState", ctx).Return(nil)
	storage.On("SetSnapshotBackupFolder", ctx, "folder", "generation", false).Return(nil)
	scheduler.On("ScheduleTask", mock.Anything, "snapshots.BackupSnapshot", "", mock.Anything).Return("copy-a", nil).Once()
	scheduler.On("CancelTask", ctx, "copy-a").Return("", nil)
	scheduler.On("WaitTaskEnded", ctx, "copy-a").Return(errors.NewInterruptExecutionError()).Once()
	scheduler.On("WaitTaskEnded", ctx, "copy-a").Return(nil).Once()
	task := &backupFolderTask{storage: storage, scheduler: scheduler, request: &protos.BackupFolderRequest{FolderId: "folder", Generation: "generation"}, state: &protos.BackupFolderState{SnapshotId: "a", OwnerId: "owner"}}
	require.True(t, errors.Is(task.Cancel(ctx, execCtx), errors.NewInterruptExecutionError()))
	storage.AssertNotCalled(t, "ReleaseSnapshotBackupFolder", mock.Anything, mock.Anything, mock.Anything)
	storage.On("ReleaseSnapshotBackupFolder", ctx, "folder", "coordinator").Return(nil).Once()
	require.NoError(t, task.Cancel(ctx, execCtx))
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
}
