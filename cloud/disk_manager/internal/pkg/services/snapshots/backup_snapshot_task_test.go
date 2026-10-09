package snapshots

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
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
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

func TestBackupSnapshotTaskKeepsAcceptedMetadataAndDEK(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		for _, deleting := range []bool{false, true} {
			t.Run(fmt.Sprintf("encrypted=%v/deleting=%v", encrypted, deleting), func(t *testing.T) {
				ctx := test.NewContext()
				kekID := ""
				var kek []byte
				if encrypted {
					kekID = "key"
					kek = make([]byte, 32)
				}
				follower, err := backup.NewS3(nil, "bucket", "prefix", kekID, kek)
				require.NoError(t, err)
				storage := resources_mocks.NewStorageMock()
				scheduler := tasks_mocks.NewSchedulerMock()
				execCtx := tasks_mocks.NewExecutionContextMock()
				execCtx.On("SaveState", mock.Anything).Return(nil)
				execCtx.On("GetTaskID").Return("owner")
				meta := &resources.SnapshotMeta{ID: "snapshot", FolderID: "folder", Disk: &types.Disk{DiskId: "disk", ZoneId: "zone"}, Ready: !deleting, BackupTaskID: "owner"}
				storage.On("ReserveSnapshotBackup", mock.Anything, "snapshot", "owner").Return(meta, nil).Once()
				storage.On("ReleaseSnapshotBackup", mock.Anything, "snapshot", "owner", true).Return(nil)
				var request *dataplane_protos.BackupSnapshotDataRequest
				scheduler.On("ScheduleTask", mock.Anything, "dataplane.BackupSnapshotData", "", mock.Anything).Run(func(a mock.Arguments) {
					request = proto.Clone(a.Get(3).(*dataplane_protos.BackupSnapshotDataRequest)).(*dataplane_protos.BackupSnapshotDataRequest)
				}).Return("child", nil).Once()
				scheduler.On("WaitTask", mock.Anything, execCtx, "child").Return(&empty.Empty{}, nil).Once()
				task := &backupSnapshotTask{scheduler: scheduler, storage: storage, backupS3: follower, request: &protos.BackupSnapshotRequest{SnapshotId: "snapshot"}, state: &protos.BackupSnapshotTaskState{}}
				require.NoError(t, task.Run(ctx, execCtx))
				require.Equal(t, "owner", request.OwnerId)
				require.Equal(t, backup.SnapshotMetaKey("disk", "snapshot"), request.MetaKey)
				require.Equal(t, encrypted, len(request.EncryptedDek) != 0)
				require.NoError(t, follower.CheckEncryptedDEK(request.EncryptedDek))
				var savedMeta backup.SnapshotMeta
				require.NoError(t, json.Unmarshal(request.Meta, &savedMeta))
				require.Equal(t, "snapshot", savedMeta.ID)
				require.Equal(t, "folder", savedMeta.FolderID)
				saved, err := task.Save()
				require.NoError(t, err)
				serialized, err := proto.Marshal(task.request)
				require.NoError(t, err)
				resumed := &backupSnapshotTask{scheduler: scheduler, storage: storage, backupS3: follower}
				require.NoError(t, resumed.Load(serialized, saved))
				require.NoError(t, resumed.Run(ctx, execCtx))
				require.True(t, resumed.GetResponse().(*protos.BackupSnapshotResponse).Completed)
				mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
			})
		}
	}
}

func TestBackupSnapshotTaskRestoresExactRequestAfterSchedulingCrash(t *testing.T) {
	ctx := test.NewContext()
	follower, err := backup.NewS3(nil, "bucket", "prefix", "key", make([]byte, 32))
	require.NoError(t, err)
	dek, err := follower.NewEncryptedDEK()
	require.NoError(t, err)
	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("owner")
	execCtx.On("SaveState", mock.Anything).Return(nil)
	storage.On("ReleaseSnapshotBackup", mock.Anything, "snapshot", "owner", true).Return(nil)
	request := &dataplane_protos.BackupSnapshotDataRequest{SnapshotId: "snapshot", OwnerId: "owner", EncryptedDek: dek, MetaKey: "meta", Meta: []byte("saved")}
	scheduler.On("ScheduleTask", mock.Anything, "dataplane.BackupSnapshotData", "", mock.MatchedBy(func(r *dataplane_protos.BackupSnapshotDataRequest) bool {
		return bytes.Equal(r.EncryptedDek, dek) && bytes.Equal(r.Meta, []byte("saved"))
	})).Return("same-child", nil)
	scheduler.On("WaitTask", mock.Anything, execCtx, "same-child").Return(&empty.Empty{}, nil)
	task := &backupSnapshotTask{scheduler: scheduler, storage: storage, backupS3: follower, request: &protos.BackupSnapshotRequest{SnapshotId: "snapshot"}, state: &protos.BackupSnapshotTaskState{DataplaneRequest: request, EncryptedDek: dek}}
	require.NoError(t, task.Run(ctx, execCtx))
	storage.AssertNotCalled(t, "ReserveSnapshotBackup", mock.Anything, mock.Anything, mock.Anything)
	mock.AssertExpectationsForObjects(t, storage, scheduler)
}

func TestBackupSnapshotTaskDoesNotCopyDeletingCandidate(t *testing.T) {
	ctx := test.NewContext()
	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("owner")
	storage.On("ReserveSnapshotBackup", mock.Anything, "snapshot", "owner").Return((*resources.SnapshotMeta)(nil), nil)
	storage.On("SnapshotBackupCancelled", mock.Anything, "snapshot").Return(nil)
	task := &backupSnapshotTask{scheduler: scheduler, storage: storage, request: &protos.BackupSnapshotRequest{SnapshotId: "snapshot"}, state: &protos.BackupSnapshotTaskState{}}
	require.NoError(t, task.Run(ctx, execCtx))
	require.False(t, task.GetResponse().(*protos.BackupSnapshotResponse).Completed)
	scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	storage.AssertExpectations(t)
}

func TestBackupSnapshotTaskCancellationWaitsForUnknownChild(t *testing.T) {
	ctx := test.NewContext()
	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("owner")
	execCtx.On("SaveState", mock.Anything).Return(nil)
	scheduler.On("ScheduleTask", mock.Anything, "dataplane.BackupSnapshotData", "", mock.Anything).Return("child", nil)
	scheduler.On("CancelTask", mock.Anything, "child").Return("", nil)
	scheduler.On("WaitTaskEnded", mock.Anything, "child").Return(errors.NewInterruptExecutionError()).Once()
	scheduler.On("WaitTaskEnded", mock.Anything, "child").Return(nil).Once()
	scheduler.On("ScheduleTask", mock.Anything, "dataplane.ReleaseSnapshotBackup", "", mock.Anything).Return("release", nil)
	scheduler.On("WaitTask", mock.Anything, execCtx, "release").Return(&empty.Empty{}, nil)
	storage.On("ReleaseSnapshotBackup", mock.Anything, "snapshot", "owner", false).Return(nil).Once()
	task := &backupSnapshotTask{scheduler: scheduler, storage: storage, request: &protos.BackupSnapshotRequest{SnapshotId: "snapshot"}, state: &protos.BackupSnapshotTaskState{DataplaneRequest: &dataplane_protos.BackupSnapshotDataRequest{SnapshotId: "snapshot", OwnerId: "owner"}}}
	err := task.Cancel(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	storage.AssertNotCalled(t, "ReleaseSnapshotBackup", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	require.NoError(t, task.Cancel(ctx, execCtx))
	mock.AssertExpectationsForObjects(t, storage, scheduler)
}

func TestBackupSnapshotTaskDoesNotCertifyLegacyChild(t *testing.T) {
	ctx := test.NewContext()
	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("legacy")
	scheduler.On("WaitTask", ctx, execCtx, "legacy-child").Return(&empty.Empty{}, nil)
	storage.On("SnapshotBackupCancelled", ctx, "snapshot").Return(nil)
	task := &backupSnapshotTask{storage: storage, scheduler: scheduler, request: &protos.BackupSnapshotRequest{SnapshotId: "snapshot"}, state: &protos.BackupSnapshotTaskState{DataplaneTaskID: "legacy-child"}}
	require.NoError(t, task.Run(ctx, execCtx))
	require.False(t, task.GetResponse().(*protos.BackupSnapshotResponse).Completed)
	storage.AssertNotCalled(t, "ReleaseSnapshotBackup", mock.Anything, mock.Anything, mock.Anything, true)
	mock.AssertExpectationsForObjects(t, storage, scheduler)
}
