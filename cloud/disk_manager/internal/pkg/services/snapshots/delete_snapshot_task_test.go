package snapshots

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestDeleteSnapshotPausesDataplaneWhenZoneIsLost(t *testing.T) {
	ctx := logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
	scheduler := tasks_mocks.NewSchedulerMock()
	storage := resources_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	task := &deleteSnapshotTask{
		scheduler:  scheduler,
		storage:    storage,
		nbsFactory: nbsFactory,
		request: &protos.DeleteSnapshotRequest{
			SnapshotId: "snapshot",
		},
		state: &protos.DeleteSnapshotTaskState{},
	}

	execCtx.On("GetTaskID").Return("delete-snapshot")
	storage.On(
		"DeleteSnapshot",
		ctx,
		"snapshot",
		"delete-snapshot",
		mock.Anything,
	).Return(&resources.SnapshotMeta{
		ID:           "snapshot",
		Disk:         &types.Disk{ZoneId: "zone", DiskId: "disk"},
		CheckpointID: "checkpoint",
		DeleteTaskID: "delete-snapshot",
	}, nil)
	nbsFactory.On("IsZoneLost", "zone").Return(true)
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"dataplane.DeleteSnapshot",
		mock.Anything,
		mock.Anything,
	).Return("dataplane-task", nil)
	scheduler.On("PauseTask", ctx, "dataplane-task").Return(nil)
	storage.On("SnapshotDeleted", ctx, "snapshot", mock.Anything).Return(nil)

	require.NoError(t, task.Run(ctx, execCtx))
	scheduler.AssertNotCalled(t, "WaitTask", mock.Anything, mock.Anything, mock.Anything)
	mock.AssertExpectationsForObjects(t, scheduler, storage, nbsFactory, execCtx)
}
