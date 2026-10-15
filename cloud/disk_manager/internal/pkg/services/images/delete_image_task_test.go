package images

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	pools_service_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/mocks"
	pools_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestDeleteImageSchedulesRetireBaseDisksIdempotently(t *testing.T) {
	zoneID := "zone-a"

	ctx := logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
	scheduler := tasks_mocks.NewSchedulerMock()
	storage := resources_storage_mocks.NewStorageMock()
	poolService := pools_service_mocks.NewServiceMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	config := &config.ImagesConfig{
		DefaultDiskPoolConfigs: []*config.DiskPoolConfig{
			&config.DiskPoolConfig{ZoneId: &zoneID},
		},
	}

	request := &protos.DeleteImageRequest{
		ImageId: "image",
	}

	task := &deleteImageTask{
		config:      config,
		scheduler:   scheduler,
		storage:     storage,
		poolService: poolService,
		request:     request,
		state:       &protos.DeleteImageTaskState{},
	}

	imageSize := uint64(100)

	execCtx.On("GetTaskID").Return("delete_image_task")
	poolService.On("ImageDeleting", mock.Anything, mock.Anything).Return(mock.Anything, nil)
	scheduler.On("WaitTask", mock.Anything, mock.Anything).Return(mock.Anything, nil)
	scheduler.On("WaitTaskEnded", mock.Anything, mock.Anything).Maybe().Return(nil)
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"dataplane.DeleteSnapshot",
		mock.Anything,
		mock.Anything,
	).Return(mock.Anything, nil)
	storage.On("ImageDeleted", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	storage.On(
		"DeleteImage",
		ctx,
		"image",
		"delete_image_task", // taskID
		mock.Anything,       // deletingAt
	).Return(&resources.ImageMeta{
		ID:           "image",
		Size:         imageSize,
		DeleteTaskID: "delete_image_task",
	}, nil)

	// Should schedule retire base disks task only during the first run.
	scheduler.On(
		"ScheduleTask",
		mock.Anything, // ctx
		"pools.RetireBaseDisks",
		mock.Anything, // description
		&pools_protos.RetireBaseDisksRequest{
			ImageId:          "image",
			ZoneId:           zoneID,
			UseBaseDiskAsSrc: true,
			UseImageSize:     imageSize,
		},
	).Return(mock.Anything, nil).Once()

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, scheduler, storage, poolService, execCtx)

	var m *resources.ImageMeta
	storage.On(
		"DeleteImage",
		ctx,
		"image",
		"delete_image_task", // taskID
		mock.Anything,       // deletingAt
	).Unset().On(
		"DeleteImage",
		ctx,
		"image",
		"delete_image_task", // taskID
		mock.Anything,       // deletingAt
	).Return(m, nil)

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, scheduler, storage, poolService, execCtx)
}

func TestDeleteImagePausesDataplaneWhenZoneIsLost(t *testing.T) {
	zoneID := "zone-a"

	ctx := logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
	scheduler := tasks_mocks.NewSchedulerMock()
	storage := resources_storage_mocks.NewStorageMock()
	poolService := pools_service_mocks.NewServiceMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	config := &config.ImagesConfig{}

	task := &deleteImageTask{
		config:      config,
		scheduler:   scheduler,
		storage:     storage,
		poolService: poolService,
		nbsFactory:  nbsFactory,
		request: &protos.DeleteImageRequest{
			ImageId: "image",
		},
		state: &protos.DeleteImageTaskState{},
	}

	execCtx.On("GetTaskID").Return("delete_image_task")
	poolService.On("ImageDeleting", mock.Anything, mock.Anything).Return("pool-task", nil)
	scheduler.On("WaitTask", mock.Anything, execCtx, "pool-task").Return(mock.Anything, nil)
	storage.On(
		"DeleteImage",
		ctx,
		"image",
		"delete_image_task",
		mock.Anything,
	).Return(&resources.ImageMeta{
		ID:           "image",
		SrcDiskID:    "disk",
		CheckpointID: "checkpoint",
		DeleteTaskID: "delete_image_task",
	}, nil)
	storage.On("GetDiskMeta", ctx, "disk").Return(&resources.DiskMeta{
		ID:     "disk",
		ZoneID: zoneID,
	}, nil)
	nbsFactory.On("IsZoneLost", zoneID).Return(true)
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"dataplane.DeleteSnapshot",
		mock.Anything,
		mock.Anything,
	).Return("dataplane-task", nil)
	scheduler.On("PauseTask", ctx, "dataplane-task").Return(nil)
	storage.On("ImageDeleted", ctx, "image", mock.Anything).Return(nil)

	require.NoError(t, task.Run(ctx, execCtx))
	scheduler.AssertNotCalled(
		t,
		"WaitTask",
		mock.Anything,
		mock.Anything,
		"dataplane-task",
	)
	mock.AssertExpectationsForObjects(t, scheduler, storage, poolService, nbsFactory, execCtx)
}
