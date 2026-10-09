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
	storage.On("ListSnapshotBackupFolders", mock.Anything).Return([]resources.SnapshotBackupFolder{{FolderID: "folder", Generation: "cycle"}}, nil)
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	scheduler.On("ScheduleTask", mock.MatchedBy(func(ctx context.Context) bool {
		return headers.GetIdempotencyKey(ctx) == "backup_snapshot_folder_folder_cycle"
	}), "snapshots.BackupFolder", "", &protos.BackupFolderRequest{FolderId: "folder", Generation: "cycle"}).Return("producer", nil).Once()

	storage.On("ListSnapshotsToBackup", mock.Anything, 2).Return(
		[]resources.SnapshotBackupRequest{{SnapshotID: "snap1"}, {SnapshotID: "snap2", BackupID: "attempt"}},
		nil,
	)

	for _, snapshotID := range []string{"snap1", "snap2"} {
		id := snapshotID
		scheduler.On(
			"ScheduleTask",
			mock.MatchedBy(func(ctx context.Context) bool {
				key := "backup_snapshot_" + id
				if id == "snap2" {
					key += "_attempt"
				}
				return headers.GetIdempotencyKey(ctx) == key
			}),
			"snapshots.BackupSnapshot",
			"",
			mock.MatchedBy(func(request *protos.BackupSnapshotRequest) bool {
				attempt := ""
				if id == "snap2" {
					attempt = "attempt"
				}
				return request.SnapshotId == id && request.BackupId == attempt
			}),
		).Return(id+"_task", nil)
	}

	task := &scheduleBackupSnapshotTasks{
		scheduler: scheduler,
		storage:   storage,
		limit:     2,
	}

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, storage, scheduler)
}
