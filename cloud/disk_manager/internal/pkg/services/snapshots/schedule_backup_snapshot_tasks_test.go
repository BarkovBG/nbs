package snapshots

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestScheduleBackupSnapshotTasks(t *testing.T) {
	ctx := logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)

	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	storage.On("ListSnapshotBackupFolders", mock.Anything).Return(
		[]resources.SnapshotBackupFolder{
			{FolderID: "folder1", Generation: "first"},
			{FolderID: "folder2", Generation: "generation"},
		},
		nil,
	)

	for _, backup := range []resources.SnapshotBackupFolder{
		{FolderID: "folder1", Generation: "first"},
		{FolderID: "folder2", Generation: "generation"},
	} {
		id := backup.FolderID
		backupID := backup.Generation
		key := "backup_snapshot_folder_" + id + "_" + backupID
		scheduler.On(
			"ScheduleTask",
			mock.MatchedBy(func(ctx context.Context) bool {
				return headers.GetIdempotencyKey(ctx) == key
			}),
			"snapshots.BackupFolder",
			"",
			mock.MatchedBy(func(request *protos.BackupFolderRequest) bool {
				return request.FolderId == id && request.Generation == backupID
			}),
		).Return(id+"_task", nil)
	}

	task := &scheduleBackupSnapshotTasks{
		scheduler: scheduler,
		storage:   storage,
	}

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, storage, scheduler)
}
