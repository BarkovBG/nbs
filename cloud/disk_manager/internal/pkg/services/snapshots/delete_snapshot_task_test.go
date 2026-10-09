package snapshots

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

func TestDeleteSnapshotWaitsForCPReservationBeforeSchedulingDP(t *testing.T) {
	ctx := context.Background()
	store := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("delete")
	store.On("DeleteSnapshot", ctx, "snapshot", "delete", mock.Anything).Return(&resources.SnapshotMeta{ID: "snapshot", DeleteTaskID: "delete", BackupTaskID: "owner"}, nil).Twice()
	task := &deleteSnapshotTask{storage: store, scheduler: scheduler, request: &protos.DeleteSnapshotRequest{SnapshotId: "snapshot"}, state: &protos.DeleteSnapshotTaskState{}}
	require.True(t, errors.Is(task.Run(ctx, execCtx), errors.NewInterruptExecutionError()))
	require.True(t, errors.Is(task.Cancel(ctx, execCtx), errors.NewInterruptExecutionError()))
	scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	store.AssertNotCalled(t, "SnapshotDeleted", mock.Anything, mock.Anything, mock.Anything)
	mock.AssertExpectationsForObjects(t, store, scheduler, execCtx)
}
