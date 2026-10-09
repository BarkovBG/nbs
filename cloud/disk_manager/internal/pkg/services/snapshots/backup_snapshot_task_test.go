package snapshots

import (
	"bytes"
	"context"
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

type backupGraphFixture struct {
	ctx       context.Context
	storage   *resources_mocks.StorageMock
	scheduler *tasks_mocks.SchedulerMock
	execCtx   *tasks_mocks.ExecutionContextMock
	task      *backupSnapshotTask
	metas     map[string]*resources.SnapshotMeta
	copies    []string
}

func newBackupGraphFixture(t *testing.T, root string, ids ...string) *backupGraphFixture {
	t.Helper()
	f := &backupGraphFixture{
		ctx: test.NewContext(), storage: resources_mocks.NewStorageMock(),
		scheduler: tasks_mocks.NewSchedulerMock(), execCtx: tasks_mocks.NewExecutionContextMock(),
		metas: make(map[string]*resources.SnapshotMeta),
	}
	f.execCtx.On("GetTaskID").Return("task")
	f.execCtx.On("SaveState", mock.Anything).Return(nil)
	follower, err := backup.NewS3(nil, "bucket", "prefix", "", nil)
	require.NoError(t, err)
	f.task = &backupSnapshotTask{
		storage: f.storage, scheduler: f.scheduler, backupS3: follower,
		request: &protos.BackupSnapshotRequest{SnapshotId: root, BackupId: "attempt"},
		state:   &protos.BackupSnapshotTaskState{},
	}
	for _, id := range ids {
		meta := &resources.SnapshotMeta{ID: id, FolderID: "folder", Ready: true, Disk: &types.Disk{DiskId: "disk", ZoneId: "zone"}}
		f.metas[id] = meta
		f.storage.On("GetSnapshotMeta", mock.Anything, id).Return(meta, nil)
		f.storage.On("HoldSnapshotBackupPlan", mock.Anything, id, "task").Run(func(a mock.Arguments) {
			require.NotNil(t, f.task.node(a.String(1)), "hold intent must already be in the saved graph")
			f.metas[a.String(1)].BackupPlanID = "task"
		}).Return(meta, nil)
	}
	f.storage.On("ReleaseSnapshotBackup", mock.Anything, mock.Anything, "task", mock.Anything).Run(func(a mock.Arguments) {
		if meta := f.metas[a.String(1)]; meta != nil {
			meta.BackupPlanID = ""
			meta.BackupTaskID = ""
			meta.BackupCompleted = meta.BackupCompleted || a.Bool(3)
		}
	}).Return(nil)
	f.storage.On("FinishSnapshotBackupPlan", mock.Anything, "task").Return(nil)
	f.storage.On("ClaimSnapshotBackupPlan", mock.Anything, "task").Return(nil)
	f.storage.On("RemoveSnapshotFromBackupQueue", mock.Anything, root, "attempt").Return(nil)
	return f
}

func (f *backupGraphFixture) reader(id string, deps ...string) {
	f.scheduler.On("ScheduleTask", mock.Anything, "dataplane.GetSnapshotBackupDependencies", "", mock.MatchedBy(func(r *dataplane_protos.GetSnapshotBackupDependenciesRequest) bool {
		return r.SnapshotId == id
	})).Return("reader-"+id, nil)
	f.scheduler.On("WaitTask", mock.Anything, f.execCtx, "reader-"+id).Return(&dataplane_protos.GetSnapshotBackupDependenciesResponse{SnapshotIds: deps}, nil)
}

func (f *backupGraphFixture) copy(t *testing.T, id string, dependent bool) {
	t.Helper()
	f.storage.On("ReserveSnapshotBackup", mock.Anything, id, "task", dependent).Run(func(a mock.Arguments) {
		f.metas[a.String(1)].BackupTaskID = "task"
	}).Return(f.metas[id], nil).Once()
	f.scheduler.On("ScheduleTask", mock.Anything, "dataplane.BackupSnapshotData", "", mock.MatchedBy(func(r *dataplane_protos.BackupSnapshotDataRequest) bool {
		return r.SnapshotId == id
	})).Run(func(a mock.Arguments) {
		require.True(t, f.task.state.GraphPrepared)
		for _, node := range f.task.state.Nodes {
			if !node.Excluded && !node.Completed {
				require.Equal(t, "task", f.metas[node.SnapshotId].BackupPlanID)
			}
		}
		f.copies = append(f.copies, id)
	}).Return("copy-"+id, nil).Once()
}

func (f *backupGraphFixture) finishCopy(id string) {
	f.scheduler.On("WaitTask", mock.Anything, f.execCtx, "copy-"+id).Return(&empty.Empty{}, nil).Once()
}

func TestBackupSnapshotTaskHoldsEntireGraphBeforeCopyingAndResumesPreparation(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "A", "B", "C")
	f.reader("C", "A", "B")
	// Pause during dependency discovery: no data copy is allowed yet.
	f.scheduler.On("ScheduleTask", mock.Anything, "dataplane.GetSnapshotBackupDependencies", "", mock.MatchedBy(func(r *dataplane_protos.GetSnapshotBackupDependenciesRequest) bool { return r.SnapshotId == "A" })).Return("reader-A", nil).Once()
	f.scheduler.On("WaitTask", mock.Anything, f.execCtx, "reader-A").Return(nil, errors.NewInterruptExecutionError()).Once()
	f.scheduler.On("WaitTask", mock.Anything, f.execCtx, "reader-A").Return(&dataplane_protos.GetSnapshotBackupDependenciesResponse{}, nil).Once()
	f.reader("B", "A")
	for _, id := range []string{"A", "B", "C"} {
		f.copy(t, id, id != "C")
		f.finishCopy(id)
	}
	require.True(t, errors.Is(f.task.Run(f.ctx, f.execCtx), errors.NewInterruptExecutionError()))
	require.Equal(t, "task", f.metas["C"].BackupPlanID)
	require.Equal(t, "task", f.metas["A"].BackupPlanID)
	f.scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything, "dataplane.BackupSnapshotData", mock.Anything, mock.Anything)
	state, err := f.task.Save()
	require.NoError(t, err)
	request, err := proto.Marshal(f.task.request)
	require.NoError(t, err)
	resumed := &backupSnapshotTask{storage: f.storage, scheduler: f.scheduler, backupS3: f.task.backupS3}
	require.NoError(t, resumed.Load(request, state))
	f.task = resumed
	require.NoError(t, resumed.Run(f.ctx, f.execCtx))
	require.Equal(t, []string{"A", "B", "C"}, f.copies)
	require.Equal(t, []string{"A", "B", "C"}, resumed.GetResponse().(*protos.BackupSnapshotResponse).CompletedSnapshotIds)
	f.scheduler.AssertExpectations(t)
}

func TestBackupSnapshotTaskRepresentsMissingCreatorAndCopiesThroughDescendant(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "C")
	f.storage.On("GetSnapshotMeta", mock.Anything, "A").Return((*resources.SnapshotMeta)(nil), nil)
	f.reader("C", "A")
	f.copy(t, "C", false)
	f.finishCopy("C")
	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.True(t, f.task.node("A").Excluded)
	require.Equal(t, []string{"C"}, f.copies)
	call := f.scheduler.Calls
	for _, c := range call {
		if c.Method == "ScheduleTask" && c.Arguments.String(1) == "dataplane.BackupSnapshotData" {
			require.Empty(t, c.Arguments.Get(3).(*dataplane_protos.BackupSnapshotDataRequest).CompletedSnapshotIds)
		}
	}
}

func preparedBackupGraph(f *backupGraphFixture, nodes ...*protos.SnapshotBackupNode) {
	f.task.state.Nodes = nodes
	f.task.state.GraphPrepared = true
	for _, node := range nodes {
		node.Discovered = true
		f.metas[node.SnapshotId].BackupPlanID = "task"
	}
	var err error
	f.task.state.CopyOrder, err = snapshotBackupOrder(nodes)
	if err != nil {
		panic(err)
	}
}

func TestBackupSnapshotTaskPrunesDeletingLeafButKeepsLiveAncestors(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "A", "C")
	preparedBackupGraph(f, &protos.SnapshotBackupNode{SnapshotId: "C", Dependencies: []string{"A"}}, &protos.SnapshotBackupNode{SnapshotId: "A"})
	f.metas["C"].Ready = false
	f.copy(t, "A", false)
	f.finishCopy("A")
	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.Equal(t, []string{"A"}, f.copies)
	require.True(t, f.task.node("C").Excluded)
	require.Empty(t, f.metas["C"].BackupPlanID)
	require.False(t, f.task.GetResponse().(*protos.BackupSnapshotResponse).Completed)
}

func TestBackupSnapshotTaskKeepsDeletingAncestorForLiveDescendant(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "A", "C")
	preparedBackupGraph(f, &protos.SnapshotBackupNode{SnapshotId: "C", Dependencies: []string{"A"}}, &protos.SnapshotBackupNode{SnapshotId: "A"})
	f.metas["A"].Ready = false
	f.copy(t, "A", true)
	f.finishCopy("A")
	f.copy(t, "C", false)
	f.finishCopy("C")
	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.Equal(t, []string{"A", "C"}, f.copies)
	f.storage.AssertCalled(t, "ReserveSnapshotBackup", mock.Anything, "A", "task", true)
}

func TestBackupSnapshotTaskDeletionDuringCopyReleasesPendingLeafAndFinishesCopy(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "A", "C")
	preparedBackupGraph(f, &protos.SnapshotBackupNode{SnapshotId: "C", Dependencies: []string{"A"}}, &protos.SnapshotBackupNode{SnapshotId: "A"})
	f.copy(t, "A", true)
	f.scheduler.On("WaitTask", mock.Anything, f.execCtx, "copy-A").Return(nil, errors.NewInterruptExecutionError()).Once()
	f.finishCopy("A")
	require.True(t, errors.Is(f.task.Run(f.ctx, f.execCtx), errors.NewInterruptExecutionError()))
	f.metas["A"].Ready = false
	f.metas["C"].Ready = false
	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.Equal(t, []string{"A"}, f.copies)
	require.True(t, f.task.node("A").Completed)
	require.True(t, f.task.node("C").Excluded)
	f.storage.AssertNumberOfCalls(t, "ReserveSnapshotBackup", 1)
}

func TestBackupSnapshotTaskDeletingGraphWithoutLiveRootsReleasesAllHolds(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "A", "C")
	preparedBackupGraph(f, &protos.SnapshotBackupNode{SnapshotId: "C", Dependencies: []string{"A"}}, &protos.SnapshotBackupNode{SnapshotId: "A"})
	f.metas["A"].Ready = false
	f.metas["C"].Ready = false
	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.Empty(t, f.copies)
	require.Empty(t, f.metas["A"].BackupPlanID)
	require.Empty(t, f.metas["C"].BackupPlanID)
	f.storage.AssertNotCalled(t, "ReserveSnapshotBackup", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestBackupSnapshotTaskRestoresAcceptedExactRequestAndDEK(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprint(encrypted), func(t *testing.T) {
			f := newBackupGraphFixture(t, "C", "C")
			preparedBackupGraph(f, &protos.SnapshotBackupNode{SnapshotId: "C"})
			var dek []byte
			if encrypted {
				var err error
				f.task.backupS3, err = backup.NewS3(nil, "bucket", "prefix", "key", make([]byte, 32))
				require.NoError(t, err)
				dek, err = f.task.backupS3.NewEncryptedDEK()
				require.NoError(t, err)
			}
			data, err := json.Marshal(backup.SnapshotMeta{ID: "C", FolderID: "folder"})
			require.NoError(t, err)
			f.task.state.DataplaneRequest = &dataplane_protos.BackupSnapshotDataRequest{SnapshotId: "C", OwnerId: "task", EncryptedDek: dek, Meta: data}
			f.metas["C"].Ready = false
			f.metas["C"].BackupTaskID = "task"
			f.scheduler.On("ScheduleTask", mock.Anything, "dataplane.BackupSnapshotData", "", mock.MatchedBy(func(r *dataplane_protos.BackupSnapshotDataRequest) bool {
				return r.SnapshotId == "C" && bytes.Equal(r.EncryptedDek, dek) && bytes.Equal(r.Meta, data)
			})).Return("copy-C", nil).Once()
			f.finishCopy("C")
			require.NoError(t, f.task.Run(f.ctx, f.execCtx))
			require.True(t, f.task.node("C").Completed)
			f.storage.AssertNotCalled(t, "ReserveSnapshotBackup", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			f.scheduler.AssertExpectations(t)
		})
	}
}

func TestBackupSnapshotTaskCancellationRecoversReaderBeforeReleasingGraph(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "C")
	f.task.state.Nodes = []*protos.SnapshotBackupNode{{SnapshotId: "C"}}
	f.task.state.ReaderSnapshotId = "C"
	f.metas["C"].BackupPlanID = "task"
	f.scheduler.On("ScheduleTask", mock.Anything, "dataplane.GetSnapshotBackupDependencies", "", mock.Anything).Return("reader", nil).Once()
	f.scheduler.On("CancelTask", mock.Anything, "reader").Return("", nil)
	f.scheduler.On("WaitTaskEnded", mock.Anything, "reader").Return(errors.NewInterruptExecutionError()).Once()
	f.scheduler.On("WaitTaskEnded", mock.Anything, "reader").Return(nil).Once()
	require.True(t, errors.Is(f.task.Cancel(f.ctx, f.execCtx), errors.NewInterruptExecutionError()))
	require.Equal(t, "task", f.metas["C"].BackupPlanID)
	f.storage.AssertNotCalled(t, "ReleaseSnapshotBackup", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	require.NoError(t, f.task.Cancel(f.ctx, f.execCtx))
	require.Empty(t, f.metas["C"].BackupPlanID)
	f.scheduler.AssertExpectations(t)
}

func TestBackupSnapshotTaskCancellationWaitsForCopyAndDrainBeforeReleasingGraph(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "A", "C")
	preparedBackupGraph(f, &protos.SnapshotBackupNode{SnapshotId: "C", Dependencies: []string{"A"}}, &protos.SnapshotBackupNode{SnapshotId: "A"})
	f.task.state.DataplaneRequest = &dataplane_protos.BackupSnapshotDataRequest{SnapshotId: "A", OwnerId: "task"}
	f.scheduler.On("ScheduleTask", mock.Anything, "dataplane.BackupSnapshotData", "", mock.Anything).Return("copy-A", nil).Once()
	f.scheduler.On("CancelTask", mock.Anything, "copy-A").Return("", nil)
	f.scheduler.On("WaitTaskEnded", mock.Anything, "copy-A").Return(errors.NewInterruptExecutionError()).Once()
	f.scheduler.On("WaitTaskEnded", mock.Anything, "copy-A").Return(nil).Once()
	f.scheduler.On("ScheduleTask", mock.Anything, "dataplane.ReleaseSnapshotBackup", "", mock.MatchedBy(func(r *dataplane_protos.ReleaseSnapshotBackupRequest) bool {
		return r.SnapshotId == "A" && r.OwnerId == "task"
	})).Return("drain", nil).Once()
	f.scheduler.On("WaitTask", mock.Anything, f.execCtx, "drain").Return(&empty.Empty{}, nil).Once()
	require.True(t, errors.Is(f.task.Cancel(f.ctx, f.execCtx), errors.NewInterruptExecutionError()))
	f.storage.AssertNotCalled(t, "ReleaseSnapshotBackup", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	require.NoError(t, f.task.Cancel(f.ctx, f.execCtx))
	require.Empty(t, f.metas["A"].BackupPlanID)
	require.Empty(t, f.metas["C"].BackupPlanID)
	f.scheduler.AssertExpectations(t)
}

func TestSnapshotBackupOrderRejectsCycles(t *testing.T) {
	_, err := snapshotBackupOrder([]*protos.SnapshotBackupNode{
		{SnapshotId: "A", Dependencies: []string{"C"}},
		{SnapshotId: "C", Dependencies: []string{"A"}},
	})
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
}

func TestBackupSnapshotTaskCancellationRecoversHoldAfterFailedStateSave(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "C")
	f.execCtx.ExpectedCalls = nil
	f.execCtx.On("GetTaskID").Return("task")
	var durableState []byte
	// Initial node intent and the state immediately before acquisition succeed.
	f.execCtx.On("SaveState", mock.Anything).Run(func(mock.Arguments) {
		var err error
		durableState, err = f.task.Save()
		require.NoError(t, err)
	}).Return(nil).Twice()
	// Holding succeeded, but the reader intent has not reached storage.
	f.execCtx.On("SaveState", mock.Anything).Return(errors.NewRetriableErrorf("save failed")).Once()
	require.Error(t, f.task.Run(f.ctx, f.execCtx))
	require.Equal(t, "task", f.metas["C"].BackupPlanID)
	request, err := proto.Marshal(f.task.request)
	require.NoError(t, err)
	resumed := &backupSnapshotTask{storage: f.storage, scheduler: f.scheduler}
	require.NoError(t, resumed.Load(request, durableState))
	require.Empty(t, resumed.state.ReaderSnapshotId)
	require.NoError(t, resumed.Cancel(f.ctx, f.execCtx))
	require.Empty(t, f.metas["C"].BackupPlanID)
	f.scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestBackupSnapshotTaskAlreadyCompletedIsSuccessfulWithoutNewCopy(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "C")
	f.metas["C"].BackupCompleted = true
	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.True(t, f.task.GetResponse().(*protos.BackupSnapshotResponse).Completed)
	f.storage.AssertNotCalled(t, "HoldSnapshotBackupPlan", mock.Anything, mock.Anything, mock.Anything)
	f.scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestBackupSnapshotTaskSavesNewMetadataAndEncryptedDEKBeforeScheduling(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "C")
	preparedBackupGraph(f, &protos.SnapshotBackupNode{SnapshotId: "C"})
	var err error
	f.task.backupS3, err = backup.NewS3(nil, "bucket", "prefix", "key", make([]byte, 32))
	require.NoError(t, err)
	f.storage.On("ReserveSnapshotBackup", mock.Anything, "C", "task", false).Run(func(mock.Arguments) {
		f.metas["C"].BackupTaskID = "task"
	}).Return(f.metas["C"], nil).Once()
	var savedRequest *dataplane_protos.BackupSnapshotDataRequest
	f.execCtx.ExpectedCalls = nil
	f.execCtx.On("GetTaskID").Return("task")
	f.execCtx.On("SaveState", mock.Anything).Run(func(mock.Arguments) {
		if f.task.state.DataplaneRequest != nil {
			savedRequest = proto.Clone(f.task.state.DataplaneRequest).(*dataplane_protos.BackupSnapshotDataRequest)
		}
	}).Return(nil)
	f.scheduler.On("ScheduleTask", mock.Anything, "dataplane.BackupSnapshotData", "", mock.Anything).Run(func(a mock.Arguments) {
		request := a.Get(3).(*dataplane_protos.BackupSnapshotDataRequest)
		require.NotNil(t, savedRequest)
		require.True(t, proto.Equal(savedRequest, request))
		require.NotEmpty(t, request.EncryptedDek)
		require.NoError(t, f.task.backupS3.CheckEncryptedDEK(request.EncryptedDek))
		require.Equal(t, backup.SnapshotMetaKey("disk", "C"), request.MetaKey)
		var meta backup.SnapshotMeta
		require.NoError(t, json.Unmarshal(request.Meta, &meta))
		require.Equal(t, "C", meta.ID)
		require.Equal(t, "folder", meta.FolderID)
	}).Return("copy-C", nil).Once()
	f.finishCopy("C")
	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	f.scheduler.AssertExpectations(t)
}

func TestBackupSnapshotTaskWaitsForQueueSlotBeforeTakingSourceHolds(t *testing.T) {
	ctx := test.NewContext()
	storage := resources_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("second")
	storage.On("ClaimSnapshotBackupPlan", ctx, "second").Return(errors.NewInterruptExecutionError()).Once()
	task := &backupSnapshotTask{storage: storage, request: &protos.BackupSnapshotRequest{SnapshotId: "C"}, state: &protos.BackupSnapshotTaskState{}}
	require.True(t, errors.Is(task.Run(ctx, execCtx), errors.NewInterruptExecutionError()))
	require.Empty(t, task.state.Nodes)
	storage.AssertExpectations(t)
}
