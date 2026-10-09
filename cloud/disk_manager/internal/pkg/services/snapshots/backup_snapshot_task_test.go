package snapshots

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
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

////////////////////////////////////////////////////////////////////////////////

type backupSnapshotFixture struct {
	ctx       context.Context
	storage   *resources_mocks.StorageMock
	scheduler *tasks_mocks.SchedulerMock
	execCtx   *tasks_mocks.ExecutionContextMock
	task      *backupSnapshotTask
	meta      *resources.SnapshotMeta
}

func newBackupSnapshotFixture(t *testing.T) *backupSnapshotFixture {
	t.Helper()
	f := &backupSnapshotFixture{
		ctx:       test.NewContext(),
		storage:   resources_mocks.NewStorageMock(),
		scheduler: tasks_mocks.NewSchedulerMock(),
		execCtx:   tasks_mocks.NewExecutionContextMock(),
		meta: &resources.SnapshotMeta{
			ID:       "snapshot",
			FolderID: "folder",
			Ready:    true,
			Disk:     &types.Disk{DiskId: "disk", ZoneId: "zone"},
		},
	}
	f.execCtx.On("GetTaskID").Return("task")
	f.execCtx.On("SaveState", mock.Anything).Return(nil)

	follower, err := backup.NewS3(nil, "bucket", "prefix", "", nil)
	require.NoError(t, err)
	f.task = &backupSnapshotTask{
		storage:   f.storage,
		scheduler: f.scheduler,
		backupS3:  follower,
		request: &protos.BackupSnapshotRequest{
			SnapshotId: "snapshot",
			BackupId:   "attempt",
		},
		state: &protos.BackupSnapshotTaskState{},
	}

	f.storage.On("GetSnapshotMeta", mock.Anything, "snapshot").Return(f.meta, nil).Maybe()
	f.storage.On("SnapshotBackupCompleted", mock.Anything, "snapshot").Run(
		func(mock.Arguments) {
			f.meta.BackupCompleted = true
		},
	).Return(nil).Maybe()
	f.storage.On("RemoveSnapshotFromBackupQueue", mock.Anything, "snapshot", "attempt").Return(nil).Maybe()
	return f
}

func (f *backupSnapshotFixture) copy(skipped bool) {
	f.scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"dataplane.BackupSnapshotData",
		"",
		mock.Anything,
	).Return("copy", nil).Once()
	f.scheduler.On("WaitTask", mock.Anything, f.execCtx, "copy").Return(
		&dataplane_protos.BackupSnapshotDataResponse{Skipped: skipped},
		nil,
	).Once()
}

////////////////////////////////////////////////////////////////////////////////

func TestBackupSnapshotTask(t *testing.T) {
	f := newBackupSnapshotFixture(t)
	f.copy(false)

	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.True(t, f.meta.BackupCompleted)
	f.storage.AssertCalled(t, "RemoveSnapshotFromBackupQueue", mock.Anything, "snapshot", "attempt")
	f.scheduler.AssertExpectations(t)
}

func TestBackupSnapshotTaskAlreadyCompletedNeedsNoCopy(t *testing.T) {
	f := newBackupSnapshotFixture(t)
	f.meta.BackupCompleted = true

	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	f.scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	f.storage.AssertCalled(t, "RemoveSnapshotFromBackupQueue", mock.Anything, "snapshot", "attempt")
}

func TestBackupSnapshotTaskSkipsDeletingSnapshot(t *testing.T) {
	f := newBackupSnapshotFixture(t)
	f.meta.Ready = false

	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.False(t, f.meta.BackupCompleted)
	f.scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	f.storage.AssertCalled(t, "RemoveSnapshotFromBackupQueue", mock.Anything, "snapshot", "attempt")
}

func TestBackupSnapshotTaskSkipsSnapshotWhoseDeletionWonTheHold(t *testing.T) {
	f := newBackupSnapshotFixture(t)
	f.copy(true)

	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.False(t, f.meta.BackupCompleted)
}

func TestBackupSnapshotTaskFinishesAcceptedCopyOfDeletingSnapshot(t *testing.T) {
	f := newBackupSnapshotFixture(t)
	f.scheduler.On("WaitTask", mock.Anything, f.execCtx, "copy").Return(
		nil,
		errors.NewInterruptExecutionError(),
	).Once()
	f.copy(false)

	err := f.task.Run(f.ctx, f.execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	f.meta.Ready = false
	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.True(t, f.meta.BackupCompleted)
	f.scheduler.AssertExpectations(t)
}

func TestBackupSnapshotTaskRestoresAcceptedRequestAndDEK(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprint(encrypted), func(t *testing.T) {
			f := newBackupSnapshotFixture(t)

			var dek []byte
			if encrypted {
				var err error
				f.task.backupS3, err = backup.NewS3(nil, "bucket", "prefix", "key", make([]byte, 32))
				require.NoError(t, err)
				dek, err = f.task.backupS3.NewEncryptedDEK()
				require.NoError(t, err)
			}
			data, err := json.Marshal(backup.SnapshotMeta{ID: "snapshot", FolderID: "folder"})
			require.NoError(t, err)
			f.task.state.DataplaneRequest = &dataplane_protos.BackupSnapshotDataRequest{
				SnapshotId:   "snapshot",
				EncryptedDek: dek,
				Meta:         data,
			}
			f.meta.Ready = false

			f.scheduler.On(
				"ScheduleTask",
				mock.Anything,
				"dataplane.BackupSnapshotData",
				"",
				mock.MatchedBy(func(r *dataplane_protos.BackupSnapshotDataRequest) bool {
					return bytes.Equal(r.EncryptedDek, dek) && bytes.Equal(r.Meta, data)
				}),
			).Return("copy", nil).Once()
			f.scheduler.On("WaitTask", mock.Anything, f.execCtx, "copy").Return(
				&dataplane_protos.BackupSnapshotDataResponse{},
				nil,
			).Once()

			require.NoError(t, f.task.Run(f.ctx, f.execCtx))
			require.True(t, f.meta.BackupCompleted)
			f.scheduler.AssertExpectations(t)
		})
	}
}

func TestBackupSnapshotTaskSavesMetadataAndDEKBeforeScheduling(t *testing.T) {
	f := newBackupSnapshotFixture(t)
	var err error
	f.task.backupS3, err = backup.NewS3(nil, "bucket", "prefix", "key", make([]byte, 32))
	require.NoError(t, err)

	var saved *dataplane_protos.BackupSnapshotDataRequest
	f.execCtx.ExpectedCalls = nil
	f.execCtx.On("GetTaskID").Return("task")
	f.execCtx.On("SaveState", mock.Anything).Run(func(mock.Arguments) {
		if f.task.state.DataplaneRequest != nil {
			saved = proto.Clone(f.task.state.DataplaneRequest).(*dataplane_protos.BackupSnapshotDataRequest)
		}
	}).Return(nil)
	f.scheduler.On("ScheduleTask", mock.Anything, "dataplane.BackupSnapshotData", "", mock.Anything).Run(
		func(a mock.Arguments) {
			request := a.Get(3).(*dataplane_protos.BackupSnapshotDataRequest)
			require.True(t, proto.Equal(saved, request))
			require.NoError(t, f.task.backupS3.CheckEncryptedDEK(request.EncryptedDek))
			require.Equal(t, backup.SnapshotMetaKey("disk", "snapshot"), request.MetaKey)

			var meta backup.SnapshotMeta
			require.NoError(t, json.Unmarshal(request.Meta, &meta))
			require.Equal(t, "snapshot", meta.ID)
			require.Equal(t, "folder", meta.FolderID)
		},
	).Return("copy", nil).Once()
	f.scheduler.On("WaitTask", mock.Anything, f.execCtx, "copy").Return(
		&dataplane_protos.BackupSnapshotDataResponse{},
		nil,
	).Once()

	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	f.scheduler.AssertExpectations(t)
}

func TestBackupSnapshotTaskCancellationStopsCopy(t *testing.T) {
	f := newBackupSnapshotFixture(t)
	f.task.state.DataplaneRequest = &dataplane_protos.BackupSnapshotDataRequest{SnapshotId: "snapshot"}
	// The copy was scheduled, but its ID was not saved.
	f.scheduler.On("ScheduleTask", mock.Anything, "dataplane.BackupSnapshotData", "", mock.Anything).Return("copy", nil).Once()
	f.scheduler.On("CancelTask", mock.Anything, "copy").Return(true, nil).Once()

	require.NoError(t, f.task.Cancel(f.ctx, f.execCtx))
	f.storage.AssertCalled(t, "RemoveSnapshotFromBackupQueue", mock.Anything, "snapshot", "attempt")
	f.scheduler.AssertNotCalled(t, "WaitTaskEnded", mock.Anything, mock.Anything)
	f.scheduler.AssertExpectations(t)
}

func TestBackupSnapshotTaskCancellationWithoutCopyRemovesQueueRow(t *testing.T) {
	f := newBackupSnapshotFixture(t)

	require.NoError(t, f.task.Cancel(f.ctx, f.execCtx))
	f.scheduler.AssertNotCalled(t, "CancelTask", mock.Anything, mock.Anything)
	f.storage.AssertCalled(t, "RemoveSnapshotFromBackupQueue", mock.Anything, "snapshot", "attempt")
}
