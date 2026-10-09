package snapshots

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

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
	metas     map[string]*resources.SnapshotMeta
	copies    []string
}

// Snapshots are created in the order of ids.
func newBackupSnapshotFixture(
	t *testing.T,
	root string,
	ids ...string,
) *backupSnapshotFixture {

	t.Helper()
	f := &backupSnapshotFixture{
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

	createdAt := time.Now()
	for i, id := range ids {
		meta := &resources.SnapshotMeta{
			ID:         id,
			FolderID:   "folder",
			Ready:      true,
			Disk:       &types.Disk{DiskId: "disk", ZoneId: "zone"},
			CreatingAt: createdAt.Add(time.Duration(i) * time.Minute),
		}
		f.metas[id] = meta
		f.storage.On("GetSnapshotMeta", mock.Anything, id).Return(meta, nil)
	}

	f.storage.On("SnapshotBackupCompleted", mock.Anything, mock.Anything).Run(
		func(a mock.Arguments) {
			f.metas[a.String(1)].BackupCompleted = true
		},
	).Return(nil).Maybe()
	f.storage.On("RemoveSnapshotFromBackupQueue", mock.Anything, root, "attempt").Return(nil).Maybe()
	return f
}

func (f *backupSnapshotFixture) chunkCreators(ids ...string) {
	f.scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"dataplane.ListSnapshotChunkCreators",
		"",
		mock.Anything,
	).Return("creators", nil)
	f.scheduler.On("WaitTask", mock.Anything, f.execCtx, "creators").Return(
		&dataplane_protos.ListSnapshotChunkCreatorsResponse{SnapshotIds: ids},
		nil,
	)
}

func (f *backupSnapshotFixture) copy(t *testing.T, id string, skipped bool) {
	t.Helper()
	f.scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"dataplane.BackupSnapshotData",
		"",
		mock.MatchedBy(func(r *dataplane_protos.BackupSnapshotDataRequest) bool {
			return r.SnapshotId == id
		}),
	).Run(func(mock.Arguments) {
		f.copies = append(f.copies, id)
	}).Return("copy-"+id, nil).Once()
	f.scheduler.On("WaitTask", mock.Anything, f.execCtx, "copy-"+id).Return(
		&dataplane_protos.BackupSnapshotDataResponse{Skipped: skipped},
		nil,
	).Once()
}

func (f *backupSnapshotFixture) nodes(nodes ...*protos.SnapshotBackupNode) {
	f.task.state.Nodes = nodes
}

func (f *backupSnapshotFixture) node(id string) *protos.SnapshotBackupNode {
	for _, node := range f.task.state.Nodes {
		if node.SnapshotId == id {
			return node
		}
	}
	return nil
}

func (f *backupSnapshotFixture) copyRequests() []*dataplane_protos.BackupSnapshotDataRequest {
	var requests []*dataplane_protos.BackupSnapshotDataRequest
	for _, call := range f.scheduler.Calls {
		if call.Method == "ScheduleTask" && call.Arguments.String(1) == "dataplane.BackupSnapshotData" {
			requests = append(requests, call.Arguments.Get(3).(*dataplane_protos.BackupSnapshotDataRequest))
		}
	}
	return requests
}

////////////////////////////////////////////////////////////////////////////////

func TestBackupSnapshotTaskCopiesCreatorsFirst(t *testing.T) {
	f := newBackupSnapshotFixture(t, "C", "A", "B", "C")
	f.chunkCreators("B", "A")
	for _, id := range []string{"A", "B", "C"} {
		f.copy(t, id, false)
	}

	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.Equal(t, []string{"A", "B", "C"}, f.copies)
	for _, id := range []string{"A", "B", "C"} {
		require.True(t, f.metas[id].BackupCompleted)
	}

	requests := f.copyRequests()
	require.Empty(t, requests[0].CompletedSnapshotIds)
	require.Equal(t, []string{"A", "B"}, requests[2].CompletedSnapshotIds)

	response := f.task.GetResponse().(*protos.BackupSnapshotResponse)
	require.True(t, response.Completed)
	require.Equal(t, []string{"A", "B", "C"}, response.CompletedSnapshotIds)
	f.storage.AssertCalled(t, "RemoveSnapshotFromBackupQueue", mock.Anything, "C", "attempt")
}

func TestBackupSnapshotTaskReusesCompletedCreator(t *testing.T) {
	f := newBackupSnapshotFixture(t, "C", "A", "C")
	f.metas["A"].BackupCompleted = true
	f.chunkCreators("A")
	f.copy(t, "C", false)

	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.Equal(t, []string{"C"}, f.copies)
	require.Equal(t, []string{"A"}, f.copyRequests()[0].CompletedSnapshotIds)
}

func TestBackupSnapshotTaskDoesNotListDeletedCreator(t *testing.T) {
	f := newBackupSnapshotFixture(t, "C", "C")
	f.storage.On("GetSnapshotMeta", mock.Anything, "A").Return((*resources.SnapshotMeta)(nil), nil)
	f.chunkCreators("A")
	f.copy(t, "C", false)

	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.Nil(t, f.node("A"))
	require.Equal(t, []string{"C"}, f.copies)
	// The chunks of A that C uses are copied with C.
	require.Empty(t, f.copyRequests()[0].CompletedSnapshotIds)
}

func TestBackupSnapshotTaskSkipsCreatorDeletedBeforeItsTurn(t *testing.T) {
	f := newBackupSnapshotFixture(t, "C", "A", "C")
	f.nodes(
		&protos.SnapshotBackupNode{SnapshotId: "A"},
		&protos.SnapshotBackupNode{SnapshotId: "C"},
	)
	f.metas["A"].Ready = false
	f.copy(t, "C", false)

	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.True(t, f.node("A").Skipped)
	require.Equal(t, []string{"C"}, f.copies)
	require.Empty(t, f.copyRequests()[0].CompletedSnapshotIds)
}

func TestBackupSnapshotTaskSkipsSnapshotWhoseDeletionWonTheHold(t *testing.T) {
	f := newBackupSnapshotFixture(t, "C", "A", "C")
	f.nodes(
		&protos.SnapshotBackupNode{SnapshotId: "A"},
		&protos.SnapshotBackupNode{SnapshotId: "C"},
	)
	f.copy(t, "A", true)
	f.copy(t, "C", false)

	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.True(t, f.node("A").Skipped)
	require.False(t, f.metas["A"].BackupCompleted)
	require.Empty(t, f.copyRequests()[1].CompletedSnapshotIds)
}

func TestBackupSnapshotTaskSkipsDeletingRootWithoutListingCreators(t *testing.T) {
	f := newBackupSnapshotFixture(t, "C", "C")
	f.metas["C"].Ready = false

	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.True(t, f.node("C").Skipped)
	require.False(t, f.task.GetResponse().(*protos.BackupSnapshotResponse).Completed)
	f.scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestBackupSnapshotTaskAlreadyCompletedRootNeedsNoCopy(t *testing.T) {
	f := newBackupSnapshotFixture(t, "C", "C")
	f.metas["C"].BackupCompleted = true

	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.True(t, f.task.GetResponse().(*protos.BackupSnapshotResponse).Completed)
	f.scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestBackupSnapshotTaskWaitsForChunkCreators(t *testing.T) {
	f := newBackupSnapshotFixture(t, "C", "A", "C")
	f.scheduler.On("ScheduleTask", mock.Anything, "dataplane.ListSnapshotChunkCreators", "", mock.Anything).Return("creators", nil)
	f.scheduler.On("WaitTask", mock.Anything, f.execCtx, "creators").Return(
		nil,
		errors.NewInterruptExecutionError(),
	).Once()
	f.chunkCreators("A")
	f.copy(t, "A", false)
	f.copy(t, "C", false)

	err := f.task.Run(f.ctx, f.execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	require.Empty(t, f.task.state.Nodes)

	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.Equal(t, []string{"A", "C"}, f.copies)
}

func TestBackupSnapshotTaskFinishesAcceptedCopyOfDeletingSnapshot(t *testing.T) {
	f := newBackupSnapshotFixture(t, "C", "A", "C")
	f.nodes(
		&protos.SnapshotBackupNode{SnapshotId: "A"},
		&protos.SnapshotBackupNode{SnapshotId: "C"},
	)
	f.scheduler.On("WaitTask", mock.Anything, f.execCtx, "copy-A").Return(
		nil,
		errors.NewInterruptExecutionError(),
	).Once()
	f.copy(t, "A", false)

	err := f.task.Run(f.ctx, f.execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	f.metas["A"].Ready = false
	f.metas["C"].Ready = false
	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.Equal(t, []string{"A"}, f.copies)
	require.True(t, f.node("A").Completed)
	require.True(t, f.node("C").Skipped)
	f.scheduler.AssertExpectations(t)
}

func TestBackupSnapshotTaskRestoresAcceptedRequestAndDEK(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprint(encrypted), func(t *testing.T) {
			f := newBackupSnapshotFixture(t, "C", "C")
			f.nodes(&protos.SnapshotBackupNode{SnapshotId: "C"})

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
			f.scheduler.On("WaitTask", mock.Anything, f.execCtx, "copy-C").Return(
				&dataplane_protos.BackupSnapshotDataResponse{},
				nil,
			).Once()

			require.NoError(t, f.task.Run(f.ctx, f.execCtx))
			require.True(t, f.node("C").Completed)
			require.True(t, f.metas["C"].BackupCompleted)
			f.scheduler.AssertExpectations(t)
		})
	}
}

func TestBackupSnapshotTaskSavesMetadataAndDEKBeforeScheduling(t *testing.T) {
	f := newBackupSnapshotFixture(t, "C", "C")
	f.nodes(&protos.SnapshotBackupNode{SnapshotId: "C"})
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
	f.scheduler.On("WaitTask", mock.Anything, f.execCtx, "copy-C").Return(
		&dataplane_protos.BackupSnapshotDataResponse{},
		nil,
	).Once()

	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	f.scheduler.AssertExpectations(t)
}

func TestBackupSnapshotTaskResumesAfterCompletedNodes(t *testing.T) {
	f := newBackupSnapshotFixture(t, "C", "A", "B", "C")
	f.nodes(
		&protos.SnapshotBackupNode{SnapshotId: "A", Completed: true},
		&protos.SnapshotBackupNode{SnapshotId: "B"},
		&protos.SnapshotBackupNode{SnapshotId: "C"},
	)
	f.copy(t, "B", false)
	f.copy(t, "C", false)

	request, err := proto.Marshal(f.task.request)
	require.NoError(t, err)
	state, err := f.task.Save()
	require.NoError(t, err)
	require.NoError(t, f.task.Load(request, state))

	require.NoError(t, f.task.Run(f.ctx, f.execCtx))
	require.Equal(t, []string{"B", "C"}, f.copies)
	require.Equal(t, []string{"A"}, f.copyRequests()[0].CompletedSnapshotIds)
}

func TestBackupSnapshotTaskCancellationStopsCopy(t *testing.T) {
	f := newBackupSnapshotFixture(t, "C", "A", "C")
	f.nodes(
		&protos.SnapshotBackupNode{SnapshotId: "A"},
		&protos.SnapshotBackupNode{SnapshotId: "C"},
	)
	f.task.state.DataplaneRequest = &dataplane_protos.BackupSnapshotDataRequest{SnapshotId: "A"}
	// The copy was scheduled, but its ID was not saved.
	f.scheduler.On("ScheduleTask", mock.Anything, "dataplane.BackupSnapshotData", "", mock.Anything).Return("copy-A", nil).Once()
	f.scheduler.On("CancelTask", mock.Anything, "copy-A").Return(true, nil)
	f.scheduler.On("WaitTaskEnded", mock.Anything, "copy-A").Return(errors.NewInterruptExecutionError()).Once()
	f.scheduler.On("WaitTaskEnded", mock.Anything, "copy-A").Return(nil).Once()

	err := f.task.Cancel(f.ctx, f.execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	f.storage.AssertNotCalled(t, "RemoveSnapshotFromBackupQueue", mock.Anything, mock.Anything, mock.Anything)

	require.NoError(t, f.task.Cancel(f.ctx, f.execCtx))
	f.storage.AssertCalled(t, "RemoveSnapshotFromBackupQueue", mock.Anything, "C", "attempt")
	f.scheduler.AssertExpectations(t)
}

func TestBackupSnapshotTaskCancellationWithoutCopyRemovesQueueRow(t *testing.T) {
	f := newBackupSnapshotFixture(t, "C", "C")

	require.NoError(t, f.task.Cancel(f.ctx, f.execCtx))
	f.scheduler.AssertNotCalled(t, "CancelTask", mock.Anything, mock.Anything)
	f.storage.AssertCalled(t, "RemoveSnapshotFromBackupQueue", mock.Anything, "C", "attempt")
}
