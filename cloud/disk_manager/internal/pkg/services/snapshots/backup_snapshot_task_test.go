package snapshots

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
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

const backupTestBucket = "snapshots-backup"

func TestBackupSnapshotTask(t *testing.T) {
	for _, backupID := range []string{"", "manual"} {
		t.Run("backupID="+backupID, func(t *testing.T) {
			testBackupSnapshotTask(t, backupID)
		})
	}
}

func testBackupSnapshotTask(t *testing.T, backupID string) {
	ctx := test.NewContext()

	creatingAt := time.Date(2026, 8, 31, 10, 0, 0, 0, time.UTC)
	snapshot := &resources.SnapshotMeta{
		ID:           "snap1",
		FolderID:     "folder",
		Disk:         &types.Disk{ZoneId: "zone", DiskId: "disk1"},
		CheckpointID: "cp1",
		CreateTaskID: "task1",
		CreatingAt:   creatingAt,
		CreatedBy:    "user",
		Size:         8192,
		StorageSize:  4096,
		Encryption: &types.EncryptionDesc{
			Mode: types.EncryptionMode_ENCRYPTION_AES_XTS,
			Key:  &types.EncryptionDesc_KeyHash{KeyHash: []byte("hash")},
		},
		Ready: true,
	}

	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	storage.On("GetSnapshotMeta", mock.Anything, "snap1").Return(snapshot, nil)
	storage.On(
		"SnapshotBackupScheduled", mock.Anything, "snap1", backupID,
	).Return(nil)
	execCtx.On("GetTaskID").Return("backup1")
	execCtx.On("SaveState", mock.Anything).Return(nil)
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"dataplane.BackupSnapshotData",
		"",
		mock.MatchedBy(func(request *dataplane_protos.BackupSnapshotDataRequest) bool {
			return request.SnapshotId == "snap1"
		}),
	).Return("dataplane1", nil)
	scheduler.On(
		"WaitTask",
		mock.Anything,
		execCtx,
		"dataplane1",
	).Return(&empty.Empty{}, nil)

	task := &backupSnapshotTask{
		scheduler: scheduler,
		storage:   storage,
		request: &protos.BackupSnapshotRequest{
			SnapshotId: "snap1",
			BackupId:   backupID,
		},
		state: &protos.BackupSnapshotTaskState{},
	}

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	require.Equal(t, "dataplane1", task.state.DataplaneTaskID)
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)

	require.Equal(t, backup.SnapshotMetaKey("disk1", "snap1"), task.state.DataplaneRequest.MetaKey)

	var meta backup.SnapshotMeta
	require.NoError(t, json.Unmarshal(task.state.DataplaneRequest.Meta, &meta))
	require.Equal(
		t,
		backup.SnapshotMeta{
			ID:                "snap1",
			FolderID:          "folder",
			ZoneID:            "zone",
			DiskID:            "disk1",
			CheckpointID:      "cp1",
			CreateTaskID:      "task1",
			CreatingAt:        creatingAt,
			CreatedBy:         "user",
			Size:              8192,
			StorageSize:       4096,
			EncryptionMode:    uint32(types.EncryptionMode_ENCRYPTION_AES_XTS),
			EncryptionKeyHash: []byte("hash"),
		},
		meta,
	)
}

func TestBackupSnapshotTaskCancellationUsesBackupID(t *testing.T) {
	ctx := test.NewContext()
	storage := resources_mocks.NewStorageMock()
	storage.On(
		"SnapshotBackupCancelled", mock.Anything, "snap1", "attempt",
	).Return(nil).Once()

	task := &backupSnapshotTask{
		storage: storage,
		state:   &protos.BackupSnapshotTaskState{},
		request: &protos.BackupSnapshotRequest{
			SnapshotId: "snap1",
			BackupId:   "attempt",
		},
	}
	err := task.Cancel(ctx, tasks_mocks.NewExecutionContextMock())
	require.NoError(t, err)
	storage.AssertExpectations(t)
}

func TestBackupSnapshotTaskDoesNotBackUpDeletingSnapshot(t *testing.T) {
	ctx := test.NewContext()
	storage := resources_mocks.NewStorageMock()
	storage.On("GetSnapshotMeta", mock.Anything, "snap1").Return(
		&resources.SnapshotMeta{ID: "snap1", Ready: false},
		nil,
	).Once()
	storage.On(
		"SnapshotBackupCancelled", mock.Anything, "snap1", "attempt",
	).Return(nil).Once()

	task := &backupSnapshotTask{
		storage: storage,
		state:   &protos.BackupSnapshotTaskState{},
		request: &protos.BackupSnapshotRequest{
			SnapshotId: "snap1",
			BackupId:   "attempt",
		},
	}
	err := task.Run(ctx, tasks_mocks.NewExecutionContextMock())
	require.NoError(t, err)
	storage.AssertExpectations(t)
}

func TestBackupSnapshotTaskCancelRecoversChildAndWaitsForDrain(t *testing.T) {
	ctx := test.NewContext()
	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("backup1")
	execCtx.On("SaveState", mock.Anything).Return(nil)
	request := &dataplane_protos.BackupSnapshotDataRequest{
		SnapshotId: "snap1",
		MetaKey:    "meta-key",
		Meta:       []byte("metadata"),
	}
	// This is the persisted state if scheduling committed but saving its ID
	// did not. Recovery must use exactly the original request and task key.
	task := &backupSnapshotTask{
		storage:   storage,
		scheduler: scheduler,
		request:   &protos.BackupSnapshotRequest{SnapshotId: "snap1", BackupId: "attempt"},
		state:     &protos.BackupSnapshotTaskState{DataplaneRequest: request},
	}
	scheduler.On("ScheduleTask", mock.MatchedBy(func(ctx context.Context) bool {
		return headers.GetIdempotencyKey(ctx) == "backup1_snap1_backup"
	}), "dataplane.BackupSnapshotData", "", request).Return("child", nil).Once()
	scheduler.On("CancelTask", mock.Anything, "child").Return(true, nil).Twice()
	scheduler.On("WaitTaskEnded", mock.Anything, "child").Return(
		errors.NewInterruptExecutionError(),
	).Once()
	err := task.Cancel(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	storage.AssertNotCalled(t, "SnapshotBackupCancelled", mock.Anything, "snap1", "attempt")

	drained := false
	scheduler.On("WaitTaskEnded", mock.Anything, "child").Run(func(mock.Arguments) {
		drained = true
	}).Return(nil).Once()
	storage.On("SnapshotBackupCancelled", mock.Anything, "snap1", "attempt").Run(func(mock.Arguments) {
		require.True(t, drained)
	}).Return(nil).Once()
	require.NoError(t, task.Cancel(ctx, execCtx))
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
}

func TestBackupSnapshotTaskResumesSavedChildWithoutRewritingMetadata(t *testing.T) {
	ctx := test.NewContext()
	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	scheduler.On("WaitTask", mock.Anything, execCtx, "child").Return(&empty.Empty{}, nil).Once()
	storage.On("SnapshotBackupScheduled", mock.Anything, "snap1", "attempt").Return(nil).Once()
	task := &backupSnapshotTask{
		storage:   storage,
		scheduler: scheduler,
		request:   &protos.BackupSnapshotRequest{SnapshotId: "snap1", BackupId: "attempt"},
		state:     &protos.BackupSnapshotTaskState{DataplaneTaskID: "child"},
	}
	require.NoError(t, task.Run(ctx, execCtx))
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
}
