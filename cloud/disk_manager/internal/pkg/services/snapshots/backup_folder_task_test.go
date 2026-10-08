package snapshots

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

func preparedBackupPlan(t *testing.T, id, base string) resources.SnapshotBackupPlan {
	state, err := proto.Marshal(&protos.SnapshotBackupPlanState{Prepared: true, PrepareRequested: true, PrepareTaskId: "prepare_" + id})
	require.NoError(t, err)
	return resources.SnapshotBackupPlan{FolderID: "folder", SnapshotID: id, OwnerID: "coordinator_0_" + id, CoordinatorID: "coordinator", BaseSnapshotID: base, Prepared: true, State: state}
}

func TestBackupFolderPreparesOldSetBeforeCopyingAndThenTakesNewReady(t *testing.T) {
	ctx := context.Background()
	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("coordinator")
	execCtx.On("SaveState", ctx).Return(nil)
	storage.On("ListSnapshotBackupFolders", ctx).Return([]resources.SnapshotBackupFolder{{FolderID: "folder", Generation: "generation"}}, nil)
	storage.On("ClaimSnapshotBackupFolder", ctx, "folder", "generation", "coordinator").Return(true, nil)
	storage.On("SaveSnapshotBackupPlan", ctx, mock.Anything).Return(nil)
	storage.On("SetSnapshotBackupFolderPrepared", ctx, "folder", "generation", false).Return(nil).Once()
	folderPrepared := false
	var events []string
	storage.On("SetSnapshotBackupFolderPrepared", ctx, "folder", "generation", true).Run(func(mock.Arguments) {
		folderPrepared = true
		events = append(events, "prepared-folder")
	}).Return(nil).Once()
	storage.On("ListSnapshotBackupCandidates", ctx, "folder", "", mock.Anything, snapshotBackfillPageSize).Return([]string{"a", "b"}, nil).Once()
	storage.On("ListSnapshotBackupCandidates", ctx, "folder", "b", mock.Anything, snapshotBackfillPageSize).Return([]string(nil), nil).Once()
	a := preparedBackupPlan(t, "a", "")
	b := preparedBackupPlan(t, "b", "a")
	newer := preparedBackupPlan(t, "new", "b")
	newer.OwnerID = "coordinator_1_new"
	storage.On("ListSnapshotBackupPlans", ctx, "folder", snapshotBackfillPageSize, false, "").Return([]resources.SnapshotBackupPlan{a, b}, nil).Once()
	storage.On("ListSnapshotBackupPlans", ctx, "folder", snapshotBackfillPageSize, false, "b").Return([]resources.SnapshotBackupPlan(nil), nil).Once()
	for _, p := range []resources.SnapshotBackupPlan{a, b} {
		storage.On("ListSnapshotBackupPlans", ctx, "folder", 1, true, "").Return([]resources.SnapshotBackupPlan{p}, nil).Once()
	}
	storage.On("ListSnapshotBackupPlans", ctx, "folder", 1, true, "").Return([]resources.SnapshotBackupPlan(nil), nil).Once()
	storage.On("ListSnapshotBackupPlans", ctx, "folder", 1, false, "").Return([]resources.SnapshotBackupPlan(nil), nil).Once()
	storage.On("ListSnapshotBackupCandidates", ctx, "folder", "", mock.Anything, 1).Run(func(mock.Arguments) {
		require.Equal(t, []string{"prepare-a", "prepare-b", "prepared-folder", "copy-a", "release-a", "copy-b", "release-b"}, events)
	}).Return([]string{"new"}, nil).Once()
	storage.On("ListSnapshotBackupPlans", ctx, "folder", 1, true, "").Return([]resources.SnapshotBackupPlan{newer}, nil).Once()
	storage.On("ListSnapshotBackupPlans", ctx, "folder", 1, true, "").Return([]resources.SnapshotBackupPlan(nil), nil).Once()
	storage.On("ListSnapshotBackupPlans", ctx, "folder", 1, false, "").Return([]resources.SnapshotBackupPlan(nil), nil).Once()
	storage.On("ListSnapshotBackupCandidates", ctx, "folder", "", mock.Anything, 1).Return([]string(nil), nil).Once()
	for _, p := range []resources.SnapshotBackupPlan{a, b, newer} {
		id, owner, base := p.SnapshotID, p.OwnerID, p.BaseSnapshotID
		storage.On("GetSnapshotBackupPlan", ctx, "folder", id).Return((*resources.SnapshotBackupPlan)(nil), nil).Once()
		storage.On("GetSnapshotMeta", ctx, id).Return(&resources.SnapshotMeta{ID: id, FolderID: "folder", Ready: true, CreatingAt: time.Now()}, nil).Once()
		storage.On("ReserveSnapshotBackup", ctx, id, owner, false).Return(&resources.SnapshotMeta{ID: id, Ready: true}, nil).Once()
		scheduler.On("ScheduleTask", mock.Anything, "dataplane.PrepareSnapshotBackup", "", mock.MatchedBy(func(r *dataplane_protos.PrepareSnapshotBackupRequest) bool {
			return r.SnapshotId == id && r.OwnerId == owner
		})).Return("prepare_"+id, nil).Once()
		scheduler.On("WaitTask", ctx, execCtx, "prepare_"+id).Run(func(mock.Arguments) {
			events = append(events, "prepare-"+id)
		}).Return(&dataplane_protos.PrepareSnapshotBackupResponse{BaseSnapshotId: base}, nil).Once()
		scheduler.On("ScheduleTask", mock.Anything, "snapshots.BackupSnapshot", "", mock.MatchedBy(func(r *protos.BackupSnapshotRequest) bool { return r.SnapshotId == id && r.BackupId == owner })).Run(func(mock.Arguments) {
			require.True(t, folderPrepared)
			events = append(events, "copy-"+id)
		}).Return("copy_"+id, nil).Once()
		scheduler.On("WaitTask", ctx, execCtx, "copy_"+id).Return(&protos.BackupSnapshotResponse{Completed: true}, nil).Once()
		scheduler.On("WaitTaskEnded", ctx, "copy_"+id).Return(nil).Once()
		scheduler.On("CancelTask", ctx, "prepare_"+id).Return("", nil).Once()
		scheduler.On("WaitTaskEnded", ctx, "prepare_"+id).Return(nil).Once()
		scheduler.On("ScheduleTask", mock.Anything, "dataplane.ReleaseSnapshotBackup", "", mock.MatchedBy(func(r *dataplane_protos.ReleaseSnapshotBackupRequest) bool {
			return r.SnapshotId == id && r.OwnerId == owner
		})).Return("release_"+id, nil).Once()
		scheduler.On("WaitTask", ctx, execCtx, "release_"+id).Run(func(mock.Arguments) {
			events = append(events, "release-"+id)
		}).Return(&empty.Empty{}, nil).Once()
		storage.On("ReleaseSnapshotBackup", ctx, id, owner, true).Return(nil).Once()
		storage.On("RemoveSnapshotBackupPlan", ctx, "folder", id, owner).Return(nil).Once()
	}
	task := &backupFolderTask{storage: storage, scheduler: scheduler, request: &protos.BackupFolderRequest{FolderId: "folder", Generation: "generation"}, state: &protos.BackupFolderState{}}
	err := task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()), fmt.Sprint(err))
	require.True(t, task.state.InitialPassComplete)
	require.Equal(t, []string{"prepare-a", "prepare-b", "prepared-folder", "copy-a", "release-a", "copy-b", "release-b", "prepare-new", "copy-new", "release-new"}, events)
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
}

func TestBackupFolderRemovalFinishesAcceptedCopyBeforeRelease(t *testing.T) {
	ctx := context.Background()
	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("coordinator")
	p := preparedBackupPlan(t, "a", "")
	state := &protos.SnapshotBackupPlanState{Prepared: true, PrepareTaskId: "prepare_a", CopyRequested: true}
	data, err := proto.Marshal(state)
	require.NoError(t, err)
	p.State = data
	storage.On("ListSnapshotBackupFolders", ctx).Return([]resources.SnapshotBackupFolder(nil), nil)
	storage.On("ListSnapshotBackupPlans", ctx, "folder", snapshotBackfillPageSize, false, "").Return([]resources.SnapshotBackupPlan{p}, nil).Once()
	storage.On("ListSnapshotBackupPlans", ctx, "folder", snapshotBackfillPageSize, false, "").Return([]resources.SnapshotBackupPlan(nil), nil).Once()
	storage.On("SaveSnapshotBackupPlan", ctx, mock.Anything).Return(nil)
	scheduler.On("ScheduleTask", mock.Anything, "snapshots.BackupSnapshot", "", mock.MatchedBy(func(r *protos.BackupSnapshotRequest) bool { return r.BackupId == p.OwnerID })).Return("copy_a", nil).Once()
	scheduler.On("WaitTask", ctx, execCtx, "copy_a").Return(&protos.BackupSnapshotResponse{Completed: true}, nil).Once()
	scheduler.On("WaitTaskEnded", ctx, "copy_a").Return(nil).Once()
	scheduler.On("CancelTask", ctx, "prepare_a").Return("", nil).Once()
	scheduler.On("WaitTaskEnded", ctx, "prepare_a").Return(nil).Once()
	scheduler.On("ScheduleTask", mock.Anything, "dataplane.ReleaseSnapshotBackup", "", mock.Anything).Return("release_a", nil).Once()
	scheduler.On("WaitTask", ctx, execCtx, "release_a").Return(&empty.Empty{}, nil).Once()
	storage.On("ReleaseSnapshotBackup", ctx, "a", p.OwnerID, true).Return(nil).Once()
	storage.On("RemoveSnapshotBackupPlan", ctx, "folder", "a", p.OwnerID).Return(nil).Once()
	storage.On("ReleaseSnapshotBackupFolder", ctx, "folder", "coordinator").Return(nil).Once()
	task := &backupFolderTask{storage: storage, scheduler: scheduler, request: &protos.BackupFolderRequest{FolderId: "folder", Generation: "generation"}, state: &protos.BackupFolderState{}}
	require.NoError(t, task.Run(ctx, execCtx))
	scheduler.AssertNotCalled(t, "CancelTask", ctx, "copy_a")
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
}
