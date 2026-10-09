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
	}
	f.storage.On("SnapshotBackupCompleted", mock.Anything, mock.Anything).Run(func(a mock.Arguments) {
		if meta := f.metas[a.String(1)]; meta != nil {
			meta.BackupCompleted = true
		}
	}).Return(nil)
	f.scheduler.On("SendEvent", mock.Anything, mock.Anything, int64(dataplane_protos.SnapshotBackupLockEvent_RELEASE)).Return(nil).Maybe()
	f.scheduler.On("WaitTaskEnded", mock.Anything, mock.MatchedBy(func(id string) bool { return len(id) > 5 && id[:5] == "lock-" })).Return(nil).Maybe()
	f.storage.On("RemoveSnapshotFromBackupQueue", mock.Anything, root, "attempt").Return(nil)
	return f
}

func (f *backupGraphFixture) lock(id string, deps ...string) {
	f.scheduler.On("ScheduleTask", mock.Anything, "dataplane.LockSnapshotForBackup", "", mock.MatchedBy(func(r *dataplane_protos.LockSnapshotForBackupRequest) bool {
		return r.SnapshotId == id
	})).Return("lock-"+id, nil)
	f.scheduler.On("GetTaskMetadata", mock.Anything, "lock-"+id).Return(&dataplane_protos.LockSnapshotForBackupState{SnapshotIds: deps, Ready: true, Locked: true}, nil)
}

func (f *backupGraphFixture) copy(t *testing.T, id string) {
	t.Helper()
	f.scheduler.On("ScheduleTask", mock.Anything, "dataplane.BackupSnapshotData", "", mock.MatchedBy(func(r *dataplane_protos.BackupSnapshotDataRequest) bool {
		return r.SnapshotId == id
	})).Run(func(a mock.Arguments) {
		require.Nil(t, f.task.nextUndiscoveredNode())
		for _, node := range f.task.state.Nodes {
			if !node.Excluded && !node.Completed {
				require.True(t, node.LockRequested)
				require.NotEmpty(t, node.LockTaskId)
			}
		}
		f.copies = append(f.copies, id)
	}).Return("copy-"+id, nil).Once()
}

func (f *backupGraphFixture) finishCopy(id string) {
	f.scheduler.On("WaitTask", mock.Anything, f.execCtx, "copy-"+id).Return(&empty.Empty{}, nil).Once()
}

func (f *backupGraphFixture) released(t *testing.T, id string) {
	t.Helper()
	f.scheduler.AssertCalled(t, "SendEvent", mock.Anything, "lock-"+id, int64(dataplane_protos.SnapshotBackupLockEvent_RELEASE))
}

func TestBackupSnapshotTaskLocksEntireGraphBeforeCopyingAndResumesPreparation(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "A", "B", "C")
	f.lock("C", "A", "B")
	// Pause during dependency discovery: no data copy is allowed yet.
	f.scheduler.On("ScheduleTask", mock.Anything, "dataplane.LockSnapshotForBackup", "", mock.MatchedBy(func(r *dataplane_protos.LockSnapshotForBackupRequest) bool { return r.SnapshotId == "A" })).Return("lock-A", nil).Once()
	f.scheduler.On("GetTaskMetadata", mock.Anything, "lock-A").Return(&dataplane_protos.LockSnapshotForBackupState{}, nil).Once()
	f.scheduler.On("GetTaskMetadata", mock.Anything, "lock-A").Return(&dataplane_protos.LockSnapshotForBackupState{Ready: true, Locked: true}, nil).Once()
	f.lock("B", "A")
	for _, id := range []string{"A", "B", "C"} {
		f.copy(t, id)
		f.finishCopy(id)
	}
	require.True(t, errors.Is(f.task.Run(f.ctx, f.execCtx), errors.NewInterruptExecutionError()))
	require.Equal(t, "lock-C", f.task.node("C").LockTaskId)
	require.Equal(t, "lock-A", f.task.node("A").LockTaskId)
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
	for _, id := range []string{"A", "B", "C"} {
		f.released(t, id)
		require.True(t, f.metas[id].BackupCompleted)
	}
	require.Equal(t, []string{"A", "B", "C"}, resumed.GetResponse().(*protos.BackupSnapshotResponse).CompletedSnapshotIds)
	f.scheduler.AssertExpectations(t)
}

func TestBackupSnapshotTaskRepresentsMissingCreatorAndCopiesThroughDescendant(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "C")
	f.storage.On("GetSnapshotMeta", mock.Anything, "A").Return((*resources.SnapshotMeta)(nil), nil)
	f.lock("C", "A")
	f.copy(t, "C")
	f.finishCopy("C")
	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.True(t, f.task.node("A").Excluded)
	require.Equal(t, []string{"C"}, f.copies)
	for _, c := range f.scheduler.Calls {
		if c.Method == "ScheduleTask" && c.Arguments.String(1) == "dataplane.BackupSnapshotData" {
			require.Empty(t, c.Arguments.Get(3).(*dataplane_protos.BackupSnapshotDataRequest).CompletedSnapshotIds)
		}
	}
}

func preparedBackupGraph(f *backupGraphFixture, nodes ...*protos.SnapshotBackupNode) {
	f.task.state.Nodes = nodes
	for _, node := range nodes {
		node.Discovered = true
		node.LockRequested = true
		node.LockTaskId = "lock-" + node.SnapshotId
	}
	if _, err := snapshotBackupOrder(nodes); err != nil {
		panic(err)
	}
}

func TestBackupSnapshotTaskPrunesDeletingLeafButKeepsLiveAncestors(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "A", "C")
	preparedBackupGraph(f, &protos.SnapshotBackupNode{SnapshotId: "C", Dependencies: []string{"A"}}, &protos.SnapshotBackupNode{SnapshotId: "A"})
	f.metas["C"].Ready = false
	f.copy(t, "A")
	f.finishCopy("A")
	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.Equal(t, []string{"A"}, f.copies)
	require.True(t, f.task.node("C").Excluded)
	f.released(t, "C")
	require.False(t, f.metas["C"].BackupCompleted)
	require.True(t, f.metas["A"].BackupCompleted)
	require.False(t, f.task.GetResponse().(*protos.BackupSnapshotResponse).Completed)
}

func TestBackupSnapshotTaskKeepsDeletingAncestorForLiveDescendant(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "A", "C")
	preparedBackupGraph(f, &protos.SnapshotBackupNode{SnapshotId: "C", Dependencies: []string{"A"}}, &protos.SnapshotBackupNode{SnapshotId: "A"})
	f.metas["A"].Ready = false
	f.copy(t, "A")
	f.finishCopy("A")
	f.copy(t, "C")
	f.finishCopy("C")
	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.Equal(t, []string{"A", "C"}, f.copies)
}

func TestBackupSnapshotTaskDeletionDuringCopyReleasesPendingLeafAndFinishesCopy(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "A", "C")
	preparedBackupGraph(f, &protos.SnapshotBackupNode{SnapshotId: "C", Dependencies: []string{"A"}}, &protos.SnapshotBackupNode{SnapshotId: "A"})
	f.copy(t, "A")
	f.scheduler.On("WaitTask", mock.Anything, f.execCtx, "copy-A").Return(nil, errors.NewInterruptExecutionError()).Once()
	f.finishCopy("A")
	require.True(t, errors.Is(f.task.Run(f.ctx, f.execCtx), errors.NewInterruptExecutionError()))
	f.metas["A"].Ready = false
	f.metas["C"].Ready = false
	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.Equal(t, []string{"A"}, f.copies)
	require.True(t, f.task.node("A").Completed)
	require.True(t, f.task.node("C").Excluded)
	f.scheduler.AssertExpectations(t)
}

func TestBackupSnapshotTaskDeletingGraphWithoutLiveRootsReleasesAllLocks(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "A", "C")
	preparedBackupGraph(f, &protos.SnapshotBackupNode{SnapshotId: "C", Dependencies: []string{"A"}}, &protos.SnapshotBackupNode{SnapshotId: "A"})
	f.metas["A"].Ready = false
	f.metas["C"].Ready = false
	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.Empty(t, f.copies)
	f.released(t, "A")
	f.released(t, "C")
	f.scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything, "dataplane.BackupSnapshotData", mock.Anything, mock.Anything)
	f.storage.AssertNotCalled(t, "SnapshotBackupCompleted", mock.Anything, mock.Anything)
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
			// The accepted copy survives deletion of its source.
			f.task.state.DataplaneRequest = &dataplane_protos.BackupSnapshotDataRequest{SnapshotId: "C", EncryptedDek: dek, Meta: data}
			f.metas["C"].Ready = false
			f.scheduler.On("ScheduleTask", mock.Anything, "dataplane.BackupSnapshotData", "", mock.MatchedBy(func(r *dataplane_protos.BackupSnapshotDataRequest) bool {
				return r.SnapshotId == "C" && bytes.Equal(r.EncryptedDek, dek) && bytes.Equal(r.Meta, data)
			})).Return("copy-C", nil).Once()
			f.finishCopy("C")
			require.NoError(t, f.task.Run(f.ctx, f.execCtx))
			require.True(t, f.task.node("C").Completed)
			require.True(t, f.metas["C"].BackupCompleted)
			f.scheduler.AssertExpectations(t)
		})
	}
}

func TestBackupSnapshotTaskCancellationRecoversLockBeforeReleasingGraph(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "C")
	f.task.state.Nodes = []*protos.SnapshotBackupNode{{SnapshotId: "C", LockRequested: true}}
	f.scheduler.On("ScheduleTask", mock.Anything, "dataplane.LockSnapshotForBackup", "", mock.Anything).Return("lock", nil).Once()
	f.scheduler.On("CancelTask", mock.Anything, "lock").Return("", nil)
	f.scheduler.On("WaitTaskEnded", mock.Anything, "lock").Return(errors.NewInterruptExecutionError()).Once()
	f.scheduler.On("WaitTaskEnded", mock.Anything, "lock").Return(nil).Once()
	require.True(t, errors.Is(f.task.Cancel(f.ctx, f.execCtx), errors.NewInterruptExecutionError()))
	f.storage.AssertNotCalled(t, "RemoveSnapshotFromBackupQueue", mock.Anything, mock.Anything, mock.Anything)
	require.NoError(t, f.task.Cancel(f.ctx, f.execCtx))
	f.storage.AssertCalled(t, "RemoveSnapshotFromBackupQueue", mock.Anything, "C", "attempt")
	f.scheduler.AssertExpectations(t)
}

func TestBackupSnapshotTaskCancellationWaitsForCopyCleanupBeforeReleasingGraph(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "A", "C")
	preparedBackupGraph(f, &protos.SnapshotBackupNode{SnapshotId: "C", Dependencies: []string{"A"}}, &protos.SnapshotBackupNode{SnapshotId: "A"})
	f.task.state.DataplaneRequest = &dataplane_protos.BackupSnapshotDataRequest{SnapshotId: "A"}
	f.scheduler.On("ScheduleTask", mock.Anything, "dataplane.BackupSnapshotData", "", mock.Anything).Return("copy-A", nil).Once()
	f.scheduler.On("CancelTask", mock.Anything, "copy-A").Return("", nil)
	f.scheduler.On("WaitTaskEnded", mock.Anything, "copy-A").Return(errors.NewInterruptExecutionError()).Once()
	f.scheduler.On("WaitTaskEnded", mock.Anything, "copy-A").Return(nil).Once()
	f.scheduler.On("CancelTask", mock.Anything, "lock-A").Return("", nil)
	f.scheduler.On("CancelTask", mock.Anything, "lock-C").Return("", nil)
	require.True(t, errors.Is(f.task.Cancel(f.ctx, f.execCtx), errors.NewInterruptExecutionError()))
	f.scheduler.AssertNotCalled(t, "CancelTask", mock.Anything, "lock-A")
	f.scheduler.AssertNotCalled(t, "CancelTask", mock.Anything, "lock-C")
	f.storage.AssertNotCalled(t, "RemoveSnapshotFromBackupQueue", mock.Anything, mock.Anything, mock.Anything)
	require.NoError(t, f.task.Cancel(f.ctx, f.execCtx))
	f.storage.AssertCalled(t, "RemoveSnapshotFromBackupQueue", mock.Anything, "C", "attempt")
	f.scheduler.AssertExpectations(t)
}

func TestSnapshotBackupOrderRejectsCycles(t *testing.T) {
	_, err := snapshotBackupOrder([]*protos.SnapshotBackupNode{
		{SnapshotId: "A", Dependencies: []string{"C"}},
		{SnapshotId: "C", Dependencies: []string{"A"}},
	})
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
}

func TestBackupSnapshotTaskCancellationAfterFailedStateSaveSchedulesNoLock(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "C")
	f.execCtx.ExpectedCalls = nil
	f.execCtx.On("GetTaskID").Return("task")
	var durableState []byte
	// The node intent is durable; the lock intent does not reach storage.
	f.execCtx.On("SaveState", mock.Anything).Run(func(mock.Arguments) {
		var err error
		durableState, err = f.task.Save()
		require.NoError(t, err)
	}).Return(nil).Once()
	f.execCtx.On("SaveState", mock.Anything).Return(errors.NewRetriableErrorf("save failed")).Once()
	require.Error(t, f.task.Run(f.ctx, f.execCtx))
	request, err := proto.Marshal(f.task.request)
	require.NoError(t, err)
	resumed := &backupSnapshotTask{storage: f.storage, scheduler: f.scheduler}
	require.NoError(t, resumed.Load(request, durableState))
	require.False(t, resumed.node("C").LockRequested)
	require.NoError(t, resumed.Cancel(f.ctx, f.execCtx))
	f.scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	f.storage.AssertCalled(t, "RemoveSnapshotFromBackupQueue", mock.Anything, "C", "attempt")
}

func TestBackupSnapshotTaskAlreadyCompletedIsSuccessfulWithoutNewCopy(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "C")
	f.metas["C"].BackupCompleted = true
	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.True(t, f.task.GetResponse().(*protos.BackupSnapshotResponse).Completed)
	f.scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestBackupSnapshotTaskSavesNewMetadataAndEncryptedDEKBeforeScheduling(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "C")
	preparedBackupGraph(f, &protos.SnapshotBackupNode{SnapshotId: "C"})
	var err error
	f.task.backupS3, err = backup.NewS3(nil, "bucket", "prefix", "key", make([]byte, 32))
	require.NoError(t, err)
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

func TestSnapshotBackupOrderDoesNotChangeAfterCompletionOrPruning(t *testing.T) {
	nodes := []*protos.SnapshotBackupNode{
		{SnapshotId: "C", Dependencies: []string{"B"}},
		{SnapshotId: "B", Dependencies: []string{"A"}},
		{SnapshotId: "A"},
	}
	order, err := snapshotBackupOrder(nodes)
	require.NoError(t, err)
	require.Equal(t, []string{"A", "B", "C"}, order)
	nodes[2].Completed = true
	nodes[1].Excluded = true
	resumed, err := snapshotBackupOrder(nodes)
	require.NoError(t, err)
	require.Equal(t, order, resumed)
}

func TestBackupSnapshotTaskResumesCopyCursorFromSavedGraph(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "A", "B", "C")
	preparedBackupGraph(f,
		&protos.SnapshotBackupNode{SnapshotId: "C", Dependencies: []string{"B"}},
		&protos.SnapshotBackupNode{SnapshotId: "B", Dependencies: []string{"A"}},
		&protos.SnapshotBackupNode{SnapshotId: "A", Completed: true},
	)
	// A is copied and unlocked; the restart resumes with B.
	f.task.node("A").LockRequested = false
	f.task.node("A").LockTaskId = ""
	f.metas["A"].BackupCompleted = true
	f.copy(t, "B")
	f.finishCopy("B")
	f.copy(t, "C")
	f.finishCopy("C")
	request, err := proto.Marshal(f.task.request)
	require.NoError(t, err)
	state, err := f.task.Save()
	require.NoError(t, err)
	require.NoError(t, f.task.Load(request, state))
	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.Equal(t, []string{"B", "C"}, f.copies)
	require.Equal(t, []string{"A", "B", "C"}, f.task.completedSnapshotIDs())
	f.scheduler.AssertExpectations(t)
}

func TestBackupSnapshotTaskDoesNotCopySourceRejectedByDataplaneLock(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "C")
	f.scheduler.On("ScheduleTask", mock.Anything, "dataplane.LockSnapshotForBackup", "", mock.Anything).Return("lock-C", nil)
	f.scheduler.On("GetTaskMetadata", mock.Anything, "lock-C").Return(&dataplane_protos.LockSnapshotForBackupState{Ready: true}, nil)
	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.True(t, f.task.node("C").Excluded)
	require.False(t, f.task.GetResponse().(*protos.BackupSnapshotResponse).Completed)
	f.scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything, "dataplane.BackupSnapshotData", mock.Anything, mock.Anything)
}

func TestBackupSnapshotTaskWaitsForLockReleaseBeforeFinishing(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "C")
	preparedBackupGraph(f, &protos.SnapshotBackupNode{SnapshotId: "C"})
	// Override the fixture's successful wait: the event alone is not release.
	f.scheduler.ExpectedCalls = nil
	f.scheduler.On("SendEvent", mock.Anything, "lock-C", int64(dataplane_protos.SnapshotBackupLockEvent_RELEASE)).Return(nil)
	f.scheduler.On("WaitTaskEnded", mock.Anything, "lock-C").Return(errors.NewInterruptExecutionError()).Once()
	f.scheduler.On("WaitTaskEnded", mock.Anything, "lock-C").Return(nil).Once()
	f.metas["C"].Ready = false
	require.True(t, errors.Is(f.task.Run(f.ctx, f.execCtx), errors.NewInterruptExecutionError()))
	f.storage.AssertNotCalled(t, "RemoveSnapshotFromBackupQueue", mock.Anything, mock.Anything, mock.Anything)
	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	f.storage.AssertCalled(t, "RemoveSnapshotFromBackupQueue", mock.Anything, "C", "attempt")
	f.scheduler.AssertExpectations(t)
}

func TestBackupSnapshotTaskRepeatsReleaseOfCompletedNodeAfterRestart(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "A", "C")
	preparedBackupGraph(f,
		&protos.SnapshotBackupNode{SnapshotId: "C", Dependencies: []string{"A"}},
		&protos.SnapshotBackupNode{SnapshotId: "A", Completed: true},
	)
	// A's completion was saved, but its lock was not released before restart.
	f.copy(t, "C")
	f.finishCopy("C")
	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.Equal(t, []string{"C"}, f.copies)
	f.released(t, "A")
	require.True(t, f.metas["A"].BackupCompleted)
	f.scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything, "dataplane.BackupSnapshotData", "", mock.MatchedBy(func(r *dataplane_protos.BackupSnapshotDataRequest) bool {
		return r.SnapshotId == "A"
	}))
}
