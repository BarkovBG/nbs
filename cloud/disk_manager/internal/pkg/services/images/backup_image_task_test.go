package images

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

const backupTestBucket = "images-backup"

func TestBackupImageTask(t *testing.T) {
	ctx := test.NewContext()

	creatingAt := time.Date(2026, 8, 31, 10, 0, 0, 0, time.UTC)
	image := &resources.ImageMeta{
		ID:            "image1",
		FolderID:      "folder",
		SrcSnapshotID: "snap1",
		CreateTaskID:  "task1",
		CreatingAt:    creatingAt,
		Size:          8192,
		StorageSize:   4096,
		Ready:         true,
	}

	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	storage.On("GetImageMeta", mock.Anything, "image1").Return(image, nil)
	storage.On("ImageBackupScheduled", mock.Anything, "image1", "").Return(nil)
	execCtx.On("GetTaskID").Return("backup1")
	execCtx.On("SaveState", mock.Anything).Return(nil)
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"dataplane.BackupSnapshotData",
		"",
		mock.MatchedBy(func(request *dataplane_protos.BackupSnapshotDataRequest) bool {
			return request.SnapshotId == "image1"
		}),
	).Return("dataplane1", nil)
	scheduler.On(
		"WaitTask",
		mock.Anything,
		execCtx,
		"dataplane1",
	).Return(&empty.Empty{}, nil)

	task := &backupImageTask{
		scheduler: scheduler,
		storage:   storage,
		request:   &protos.BackupImageRequest{ImageId: "image1"},
		state:     &protos.BackupImageTaskState{},
	}

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	require.Equal(t, "dataplane1", task.state.DataplaneTaskID)
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)

	require.Equal(t, backup.ImageMetaKey("image1"), task.state.DataplaneRequest.MetaKey)

	var meta backup.ImageMeta
	require.NoError(t, json.Unmarshal(task.state.DataplaneRequest.Meta, &meta))
	require.Equal(
		t,
		backup.ImageMeta{
			ID:            "image1",
			FolderID:      "folder",
			SrcSnapshotID: "snap1",
			CreateTaskID:  "task1",
			CreatingAt:    creatingAt,
			Size:          8192,
			StorageSize:   4096,
		},
		meta,
	)
}

func TestBackupImageTaskCancellationUsesBackupID(t *testing.T) {
	ctx := test.NewContext()
	storage := resources_mocks.NewStorageMock()
	storage.On(
		"ImageBackupCancelled", mock.Anything, "image1", "attempt",
	).Return(nil).Once()

	task := &backupImageTask{
		storage: storage,
		state:   &protos.BackupImageTaskState{},
		request: &protos.BackupImageRequest{
			ImageId:  "image1",
			BackupId: "attempt",
		},
	}
	err := task.Cancel(ctx, tasks_mocks.NewExecutionContextMock())
	require.NoError(t, err)
	storage.AssertExpectations(t)
}

func TestBackupImageTaskDoesNotBackUpDeletingImage(t *testing.T) {
	ctx := test.NewContext()
	storage := resources_mocks.NewStorageMock()
	storage.On("GetImageMeta", mock.Anything, "image1").Return(
		&resources.ImageMeta{ID: "image1", Ready: false},
		nil,
	).Once()
	storage.On(
		"ImageBackupCancelled", mock.Anything, "image1", "attempt",
	).Return(nil).Once()

	task := &backupImageTask{
		storage: storage,
		state:   &protos.BackupImageTaskState{},
		request: &protos.BackupImageRequest{
			ImageId:  "image1",
			BackupId: "attempt",
		},
	}
	err := task.Run(ctx, tasks_mocks.NewExecutionContextMock())
	require.NoError(t, err)
	storage.AssertExpectations(t)
}

func TestBackupImageTaskCancelRecoversChildAndWaitsForDrain(t *testing.T) {
	ctx := test.NewContext()
	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("backup1")
	execCtx.On("SaveState", mock.Anything).Return(nil)
	request := &dataplane_protos.BackupSnapshotDataRequest{
		SnapshotId: "image1",
		MetaKey:    "meta-key",
		Meta:       []byte("metadata"),
	}
	// This is the persisted state if scheduling committed but saving its ID
	// did not. Recovery must use exactly the original request and task key.
	task := &backupImageTask{
		storage:   storage,
		scheduler: scheduler,
		request:   &protos.BackupImageRequest{ImageId: "image1", BackupId: "attempt"},
		state:     &protos.BackupImageTaskState{DataplaneRequest: request},
	}
	scheduler.On("ScheduleTask", mock.MatchedBy(func(ctx context.Context) bool {
		return headers.GetIdempotencyKey(ctx) == "backup1_image1_backup"
	}), "dataplane.BackupSnapshotData", "", request).Return("child", nil).Once()
	scheduler.On("CancelTask", mock.Anything, "child").Return(true, nil).Twice()
	scheduler.On("WaitTaskEnded", mock.Anything, "child").Return(
		errors.NewInterruptExecutionError(),
	).Once()
	err := task.Cancel(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	storage.AssertNotCalled(t, "ImageBackupCancelled", mock.Anything, "image1", "attempt")

	drained := false
	scheduler.On("WaitTaskEnded", mock.Anything, "child").Run(func(mock.Arguments) {
		drained = true
	}).Return(nil).Once()
	storage.On("ImageBackupCancelled", mock.Anything, "image1", "attempt").Run(func(mock.Arguments) {
		require.True(t, drained)
	}).Return(nil).Once()
	require.NoError(t, task.Cancel(ctx, execCtx))
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
}

func TestBackupImageTaskResumesSavedChildWithoutRewritingMetadata(t *testing.T) {
	ctx := test.NewContext()
	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	scheduler.On("WaitTask", mock.Anything, execCtx, "child").Return(&empty.Empty{}, nil).Once()
	storage.On("ImageBackupScheduled", mock.Anything, "image1", "attempt").Return(nil).Once()
	task := &backupImageTask{
		storage:   storage,
		scheduler: scheduler,
		request:   &protos.BackupImageRequest{ImageId: "image1", BackupId: "attempt"},
		state:     &protos.BackupImageTaskState{DataplaneTaskID: "child"},
	}
	require.NoError(t, task.Run(ctx, execCtx))
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
}
