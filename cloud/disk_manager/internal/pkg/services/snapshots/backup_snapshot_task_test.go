package snapshots

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
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

////////////////////////////////////////////////////////////////////////////////

type backupGraphFixture struct {
	ctx       context.Context
	storage   *resources_mocks.StorageMock
	scheduler *tasks_mocks.SchedulerMock
	execCtx   *tasks_mocks.ExecutionContextMock
	task      *backupSnapshotTask
	metas     map[string]*resources.SnapshotMeta
	copies    []string
}

func isLockTask(id string) bool {
	return strings.HasPrefix(id, "lock-")
}

func newBackupGraphFixture(
	t *testing.T,
	root string,
	ids ...string,
) *backupGraphFixture {

	t.Helper()
	f := &backupGraphFixture{
		ctx:       test.NewContext(),
		storage:   resources_mocks.NewStorageMock(),
		scheduler: tasks_mocks.NewSchedulerMock(),
		execCtx:   tasks_mocks.NewExecutionContextMock(),
		metas:     make(map[string]*resources.SnapshotMeta),
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
			SnapshotId: root,
			BackupId:   "attempt",
		},
		state: &protos.BackupSnapshotTaskState{},
	}

	for _, id := range ids {
		id := id
		meta := &resources.SnapshotMeta{
			ID:       id,
			FolderID: "folder",
			Ready:    true,
			Disk:     &types.Disk{DiskId: "disk", ZoneId: "zone"},
		}
		f.metas[id] = meta
		f.storage.On("GetSnapshotMeta", mock.Anything, id).Return(meta, nil)
		f.scheduler.On(
			"ScheduleTask",
			mock.Anything,
			"dataplane.LockSnapshotForBackup",
			"",
			mock.MatchedBy(func(r *dataplane_protos.LockSnapshotForBackupRequest) bool {
				return r.SnapshotId == id
			}),
		).Return("lock-"+id, nil).Maybe()
	}

	f.storage.On("SnapshotBackupCompleted", mock.Anything, mock.Anything).Run(
		func(a mock.Arguments) {
			if meta := f.metas[a.String(1)]; meta != nil {
				meta.BackupCompleted = true
			}
		},
	).Return(nil).Maybe()
	f.scheduler.On("CancelTask", mock.Anything, mock.MatchedBy(isLockTask)).Return(true, nil).Maybe()
	f.scheduler.On("WaitTaskEnded", mock.Anything, mock.MatchedBy(isLockTask)).Return(nil).Maybe()
	f.storage.On("RemoveSnapshotFromBackupQueue", mock.Anything, root, "attempt").Return(nil).Maybe()
	return f
}

func (f *backupGraphFixture) lock(id string, deps ...string) {
	f.scheduler.On("GetTaskMetadata", mock.Anything, "lock-"+id).Return(
		&dataplane_protos.LockSnapshotForBackupState{
			SnapshotIds: deps,
			Ready:       true,
			Locked:      true,
		},
		nil,
	)
}

func (f *backupGraphFixture) copy(t *testing.T, id string) {
	t.Helper()
	f.scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"dataplane.BackupSnapshotData",
		"",
		mock.MatchedBy(func(r *dataplane_protos.BackupSnapshotDataRequest) bool {
			return r.SnapshotId == id
		}),
	).Run(func(a mock.Arguments) {
		for _, node := range f.task.state.Nodes {
			require.True(t, node.Discovered, "graph is locked before copying")
			if !node.Excluded && !node.Completed {
				require.True(t, node.Locked)
			}
		}
		f.copies = append(f.copies, id)
	}).Return("copy-"+id, nil).Once()
	f.scheduler.On("WaitTask", mock.Anything, f.execCtx, "copy-"+id).Return(&empty.Empty{}, nil).Once()
}

func (f *backupGraphFixture) prepared(nodes ...*protos.SnapshotBackupNode) {
	for _, node := range nodes {
		node.Discovered = true
		node.Locked = !node.Excluded
	}
	f.task.state.Nodes = nodes
}

func (f *backupGraphFixture) unlocked(t *testing.T, id string) {
	t.Helper()
	f.scheduler.AssertCalled(t, "CancelTask", mock.Anything, "lock-"+id)
	require.False(t, f.task.node(id).Locked)
}

func (f *backupGraphFixture) resume(t *testing.T) {
	t.Helper()
	request, err := proto.Marshal(f.task.request)
	require.NoError(t, err)
	state, err := f.task.Save()
	require.NoError(t, err)
	resumed := &backupSnapshotTask{
		storage:   f.storage,
		scheduler: f.scheduler,
		backupS3:  f.task.backupS3,
	}
	require.NoError(t, resumed.Load(request, state))
	f.task = resumed
}

func copyRequests(f *backupGraphFixture) []*dataplane_protos.BackupSnapshotDataRequest {
	var requests []*dataplane_protos.BackupSnapshotDataRequest
	for _, call := range f.scheduler.Calls {
		if call.Method == "ScheduleTask" && call.Arguments.String(1) == "dataplane.BackupSnapshotData" {
			requests = append(requests, call.Arguments.Get(3).(*dataplane_protos.BackupSnapshotDataRequest))
		}
	}
	return requests
}

////////////////////////////////////////////////////////////////////////////////

func TestBackupSnapshotTaskLocksGraphBeforeCopyingCreatorsFirst(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "A", "B", "C")
	f.lock("C", "A", "B")
	f.lock("B", "A")
	// The lock of A is not decided yet: preparation pauses.
	f.scheduler.On("GetTaskMetadata", mock.Anything, "lock-A").Return(
		&dataplane_protos.LockSnapshotForBackupState{},
		nil,
	).Once()
	f.lock("A")
	for _, id := range []string{"A", "B", "C"} {
		f.copy(t, id)
	}

	err := f.task.Run(f.ctx, f.execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	require.Empty(t, f.copies)

	f.resume(t)
	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.Equal(t, []string{"A", "B", "C"}, f.copies)
	for _, id := range []string{"A", "B", "C"} {
		f.unlocked(t, id)
		require.True(t, f.metas[id].BackupCompleted)
	}
	requests := copyRequests(f)
	require.Empty(t, requests[0].CompletedSnapshotIds)
	require.Equal(t, []string{"A", "B"}, requests[2].CompletedSnapshotIds)
	require.Equal(
		t,
		[]string{"A", "B", "C"},
		f.task.GetResponse().(*protos.BackupSnapshotResponse).CompletedSnapshotIds,
	)
	f.storage.AssertCalled(t, "RemoveSnapshotFromBackupQueue", mock.Anything, "C", "attempt")
}

func TestBackupSnapshotTaskSkipsMissingCreator(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "C")
	f.storage.On("GetSnapshotMeta", mock.Anything, "A").Return((*resources.SnapshotMeta)(nil), nil)
	f.lock("C", "A")
	f.copy(t, "C")

	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.True(t, f.task.node("A").Excluded)
	require.False(t, f.task.node("A").Locked)
	require.Equal(t, []string{"C"}, f.copies)
	// The chunks of A that C uses are copied with C.
	require.Empty(t, copyRequests(f)[0].CompletedSnapshotIds)
}

func TestBackupSnapshotTaskSkipsDeletingLeaf(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "A", "C")
	f.prepared(
		&protos.SnapshotBackupNode{SnapshotId: "C", Dependencies: []string{"A"}},
		&protos.SnapshotBackupNode{SnapshotId: "A"},
	)
	f.metas["C"].Ready = false
	f.copy(t, "A")

	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.Equal(t, []string{"A"}, f.copies)
	require.True(t, f.task.node("C").Excluded)
	f.unlocked(t, "C")
	require.False(t, f.metas["C"].BackupCompleted)
	require.False(t, f.task.GetResponse().(*protos.BackupSnapshotResponse).Completed)
}

func TestBackupSnapshotTaskSkipsDeletingAncestor(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "A", "C")
	f.prepared(
		&protos.SnapshotBackupNode{SnapshotId: "C", Dependencies: []string{"A"}},
		&protos.SnapshotBackupNode{SnapshotId: "A"},
	)
	f.metas["A"].Ready = false
	f.copy(t, "C")

	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.Equal(t, []string{"C"}, f.copies)
	require.True(t, f.task.node("A").Excluded)
	f.unlocked(t, "A")
	require.Empty(t, copyRequests(f)[0].CompletedSnapshotIds)
}

func TestBackupSnapshotTaskFinishesAcceptedCopyOfDeletingSnapshot(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "A", "C")
	f.prepared(
		&protos.SnapshotBackupNode{SnapshotId: "C", Dependencies: []string{"A"}},
		&protos.SnapshotBackupNode{SnapshotId: "A"},
	)
	f.scheduler.On("WaitTask", mock.Anything, f.execCtx, "copy-A").Return(
		nil,
		errors.NewInterruptExecutionError(),
	).Once()
	f.copy(t, "A")

	err := f.task.Run(f.ctx, f.execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	f.metas["A"].Ready = false
	f.metas["C"].Ready = false
	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.Equal(t, []string{"A"}, f.copies)
	require.True(t, f.task.node("A").Completed)
	require.True(t, f.task.node("C").Excluded)
	f.scheduler.AssertExpectations(t)
}

func TestBackupSnapshotTaskRestoresAcceptedRequestAndDEK(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprint(encrypted), func(t *testing.T) {
			f := newBackupGraphFixture(t, "C", "C")
			f.prepared(&protos.SnapshotBackupNode{SnapshotId: "C"})

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
			f.task.state.DataplaneRequest = &dataplane_protos.BackupSnapshotDataRequest{
				SnapshotId:   "C",
				EncryptedDek: dek,
				Meta:         data,
			}
			f.metas["C"].Ready = false

			f.scheduler.On(
				"ScheduleTask",
				mock.Anything,
				"dataplane.BackupSnapshotData",
				"",
				mock.MatchedBy(func(r *dataplane_protos.BackupSnapshotDataRequest) bool {
					return r.SnapshotId == "C" &&
						bytes.Equal(r.EncryptedDek, dek) &&
						bytes.Equal(r.Meta, data)
				}),
			).Return("copy-C", nil).Once()
			f.scheduler.On("WaitTask", mock.Anything, f.execCtx, "copy-C").Return(&empty.Empty{}, nil).Once()

			require.NoError(t, f.task.Run(f.ctx, f.execCtx))
			require.True(t, f.task.node("C").Completed)
			require.True(t, f.metas["C"].BackupCompleted)
			f.scheduler.AssertExpectations(t)
		})
	}
}

func TestBackupSnapshotTaskSavesMetadataAndDEKBeforeScheduling(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "C")
	f.prepared(&protos.SnapshotBackupNode{SnapshotId: "C"})
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
			require.Equal(t, backup.SnapshotMetaKey("disk", "C"), request.MetaKey)

			var meta backup.SnapshotMeta
			require.NoError(t, json.Unmarshal(request.Meta, &meta))
			require.Equal(t, "C", meta.ID)
			require.Equal(t, "folder", meta.FolderID)
		},
	).Return("copy-C", nil).Once()
	f.scheduler.On("WaitTask", mock.Anything, f.execCtx, "copy-C").Return(&empty.Empty{}, nil).Once()

	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	f.scheduler.AssertExpectations(t)
}

func TestBackupSnapshotTaskAlreadyCompletedNeedsNoLock(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "C")
	f.metas["C"].BackupCompleted = true

	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.True(t, f.task.GetResponse().(*protos.BackupSnapshotResponse).Completed)
	f.scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestBackupSnapshotTaskSkipsSnapshotWhoseDeletionWonTheLock(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "C")
	f.scheduler.On("GetTaskMetadata", mock.Anything, "lock-C").Return(
		&dataplane_protos.LockSnapshotForBackupState{Ready: true},
		nil,
	)

	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.True(t, f.task.node("C").Excluded)
	f.unlocked(t, "C")
	require.Empty(t, f.copies)
}

func TestBackupSnapshotTaskResumesAfterCompletedNodes(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "A", "B", "C")
	f.prepared(
		&protos.SnapshotBackupNode{SnapshotId: "C", Dependencies: []string{"B"}},
		&protos.SnapshotBackupNode{SnapshotId: "B", Dependencies: []string{"A"}},
		&protos.SnapshotBackupNode{SnapshotId: "A"},
	)
	// A is copied and unlocked; its completion was saved before the restart.
	f.task.node("A").Completed = true
	f.task.node("A").Locked = false
	f.metas["A"].BackupCompleted = true
	f.copy(t, "B")
	f.copy(t, "C")

	f.resume(t)
	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.Equal(t, []string{"B", "C"}, f.copies)
	f.scheduler.AssertNotCalled(t, "CancelTask", mock.Anything, "lock-A")
}

func TestBackupSnapshotTaskRepeatsRecordAndUnlockOfCompletedNode(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "A", "C")
	f.prepared(
		&protos.SnapshotBackupNode{SnapshotId: "C", Dependencies: []string{"A"}},
		&protos.SnapshotBackupNode{SnapshotId: "A"},
	)
	// The completion of A was saved, but A was not recorded and unlocked.
	f.task.node("A").Completed = true
	f.copy(t, "C")

	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.Equal(t, []string{"C"}, f.copies)
	require.True(t, f.metas["A"].BackupCompleted)
	f.unlocked(t, "A")
}

func TestBackupSnapshotTaskWaitsForLockTaskToEnd(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "C")
	f.prepared(&protos.SnapshotBackupNode{SnapshotId: "C"})
	f.metas["C"].Ready = false
	f.scheduler.ExpectedCalls = nil
	f.scheduler.On("ScheduleTask", mock.Anything, "dataplane.LockSnapshotForBackup", "", mock.Anything).Return("lock-C", nil)
	f.scheduler.On("CancelTask", mock.Anything, "lock-C").Return(true, nil)
	f.scheduler.On("WaitTaskEnded", mock.Anything, "lock-C").Return(errors.NewInterruptExecutionError()).Once()
	f.scheduler.On("WaitTaskEnded", mock.Anything, "lock-C").Return(nil).Once()

	err := f.task.Run(f.ctx, f.execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	require.True(t, f.task.node("C").Locked)
	f.storage.AssertNotCalled(t, "RemoveSnapshotFromBackupQueue", mock.Anything, mock.Anything, mock.Anything)

	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.False(t, f.task.node("C").Locked)
	f.storage.AssertCalled(t, "RemoveSnapshotFromBackupQueue", mock.Anything, "C", "attempt")
}

func TestBackupSnapshotTaskCancellationStopsCopyBeforeUnlocking(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "A", "C")
	f.prepared(
		&protos.SnapshotBackupNode{SnapshotId: "C", Dependencies: []string{"A"}},
		&protos.SnapshotBackupNode{SnapshotId: "A"},
	)
	f.task.state.DataplaneRequest = &dataplane_protos.BackupSnapshotDataRequest{SnapshotId: "A"}
	// The copy was scheduled, but its ID was not saved.
	f.scheduler.On("ScheduleTask", mock.Anything, "dataplane.BackupSnapshotData", "", mock.Anything).Return("copy-A", nil).Once()
	f.scheduler.On("CancelTask", mock.Anything, "copy-A").Return(true, nil)
	f.scheduler.On("WaitTaskEnded", mock.Anything, "copy-A").Return(errors.NewInterruptExecutionError()).Once()
	f.scheduler.On("WaitTaskEnded", mock.Anything, "copy-A").Return(nil).Once()

	err := f.task.Cancel(f.ctx, f.execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	f.scheduler.AssertNotCalled(t, "CancelTask", mock.Anything, "lock-A")
	f.scheduler.AssertNotCalled(t, "CancelTask", mock.Anything, "lock-C")

	require.NoError(t, f.task.Cancel(f.ctx, f.execCtx))
	f.unlocked(t, "A")
	f.unlocked(t, "C")
	f.storage.AssertCalled(t, "RemoveSnapshotFromBackupQueue", mock.Anything, "C", "attempt")
	f.scheduler.AssertExpectations(t)
}

func TestBackupSnapshotTaskCancellationFindsLockWithUnsavedID(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "C")
	f.task.state.Nodes = []*protos.SnapshotBackupNode{{SnapshotId: "C", Locked: true}}

	require.NoError(t, f.task.Cancel(f.ctx, f.execCtx))
	f.unlocked(t, "C")
}

func TestBackupSnapshotTaskSchedulesNoLockWhoseIntentWasNotSaved(t *testing.T) {
	f := newBackupGraphFixture(t, "C", "C")
	f.execCtx.ExpectedCalls = nil
	f.execCtx.On("GetTaskID").Return("task")
	f.execCtx.On("SaveState", mock.Anything).Return(errors.NewRetriableErrorf("save failed")).Once()

	require.Error(t, f.task.Run(f.ctx, f.execCtx))
	f.scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything)

	// Nothing was saved, so cancellation has no lock to cancel.
	f.task.state = &protos.BackupSnapshotTaskState{}
	require.NoError(t, f.task.Cancel(f.ctx, f.execCtx))
	f.scheduler.AssertNotCalled(t, "CancelTask", mock.Anything, mock.Anything)
}

func TestSnapshotBackupOrderPutsCreatorsFirst(t *testing.T) {
	nodes := []*protos.SnapshotBackupNode{
		{SnapshotId: "D", Dependencies: []string{"C", "A"}},
		{SnapshotId: "C", Dependencies: []string{"B", "A"}},
		{SnapshotId: "A"},
		{SnapshotId: "B", Dependencies: []string{"A"}},
	}

	var order []string
	for _, node := range snapshotBackupOrder(nodes) {
		order = append(order, node.SnapshotId)
	}
	require.Equal(t, []string{"A", "B", "C", "D"}, order)
}
