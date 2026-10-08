package images

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestScheduleBackupImageTasks(t *testing.T) {
	ctx := logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)

	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	storage.On("ListImagesToBackup", mock.Anything, 2).Return(
		[]resources.ImageBackupRequest{
			{ImageID: "snap1"},
			{ImageID: "snap2", BackupID: "attempt"},
		},
		nil,
	)

	for _, backup := range []resources.ImageBackupRequest{
		{ImageID: "snap1"},
		{ImageID: "snap2", BackupID: "attempt"},
	} {
		id := backup.ImageID
		backupID := backup.BackupID
		key := "backup_image_" + id
		if backupID != "" {
			key += "_" + backupID
		}
		scheduler.On(
			"ScheduleTask",
			mock.MatchedBy(func(ctx context.Context) bool {
				return headers.GetIdempotencyKey(ctx) == key
			}),
			"images.BackupImage",
			"",
			mock.MatchedBy(func(request *protos.BackupImageRequest) bool {
				return request.ImageId == id && request.BackupId == backupID
			}),
		).Return(id+"_task", nil)
	}

	task := &scheduleBackupImageTasks{
		scheduler: scheduler,
		storage:   storage,
		limit:     2,
	}

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, storage, scheduler)
}
