package backups

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/backups/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

func expectStatus(storage *resources_mocks.StorageMock, kind, id string, ready, completed bool) {
	if kind == "snapshots" {
		storage.On("GetSnapshotMeta", mock.Anything, id).Return(&resources.SnapshotMeta{
			ID: id, Ready: ready, BackupCompleted: completed,
		}, nil).Once()
	} else {
		storage.On("GetImageMeta", mock.Anything, id).Return(&resources.ImageMeta{
			ID: id, Ready: ready, BackupCompleted: completed,
		}, nil).Once()
	}
}

func TestBackfillResumesAndWaitsForPage(t *testing.T) {
	for _, kind := range []string{"snapshots", "images"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			storage := resources_mocks.NewStorageMock()
			scheduler := tasks_mocks.NewSchedulerMock()
			execCtx := tasks_mocks.NewExecutionContextMock()
			execCtx.On("GetTaskID").Return("run")
			execCtx.On("SaveState", mock.Anything).Return(nil)
			cutoff := time.Unix(1700000000, 0)
			request := &protos.BackfillRequest{ResourceKind: kind, BatchSize: 2, CreatedBeforeMicros: cutoff.UnixMicro()}
			task := &backfillTask{storage: storage, scheduler: scheduler, request: request, state: &protos.BackfillState{}}
			storage.On("EnqueueBackupBatch", mock.Anything, kind, "", cutoff, 2, "run").Return([]resources.BackupRequest{
				{ResourceID: "a"}, {ResourceID: "b", BackupID: "run"},
			}, nil).Once()
			taskType, prefix := "snapshots.BackupSnapshot", "backup_snapshot_"
			if kind == "images" {
				taskType, prefix = "images.BackupImage", "backup_image_"
			}
			for _, id := range []string{"a", "b"} {
				key := prefix + id
				if id == "b" {
					key += "_run"
				}
				scheduler.On("ScheduleTask", mock.MatchedBy(func(ctx context.Context) bool {
					return headers.GetIdempotencyKey(ctx) == key
				}), taskType, "", mock.Anything).Return("task-"+id, nil).Once()
			}
			scheduler.On("WaitTask", mock.Anything, execCtx, "task-a").Return(&empty.Empty{}, errors.NewInterruptExecutionError()).Once()
			expectStatus(storage, kind, "a", true, false)
			err := task.Run(ctx, execCtx)
			require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
			require.Empty(t, task.state.AfterId)
			require.Len(t, task.state.Pending, 2)
			// Recover with exactly the stored task IDs; do not enqueue or schedule
			// another page until both backups have completed.
			state, err := task.Save()
			require.NoError(t, err)
			req, err := proto.Marshal(request)
			require.NoError(t, err)
			resumed := &backfillTask{storage: storage, scheduler: scheduler}
			require.NoError(t, resumed.Load(req, state))
			for _, id := range []string{"a", "b"} {
				scheduler.On("WaitTask", mock.Anything, execCtx, "task-"+id).Return(&empty.Empty{}, nil).Once()
				expectStatus(storage, kind, id, true, true)
			}
			storage.On("EnqueueBackupBatch", mock.Anything, kind, "b", cutoff, 2, "run").Return([]resources.BackupRequest(nil), nil).Once()
			require.NoError(t, resumed.Run(ctx, execCtx))
			require.EqualValues(t, 2, resumed.state.Processed)
			require.Equal(t, "b", resumed.state.AfterId)
			mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
		})
	}
}

func TestBackfillDoesNotReportMissingSuccessOrFailedCopyAsCompleted(t *testing.T) {
	for _, childErr := range []error{nil, errors.NewNonRetriableErrorf("copy failed")} {
		t.Run("childError="+fmt.Sprint(childErr), func(t *testing.T) {
			ctx := context.Background()
			storage := resources_mocks.NewStorageMock()
			scheduler := tasks_mocks.NewSchedulerMock()
			execCtx := tasks_mocks.NewExecutionContextMock()
			execCtx.On("SaveState", mock.Anything).Return(nil)
			task := &backfillTask{storage: storage, scheduler: scheduler,
				request: &protos.BackfillRequest{ResourceKind: "snapshots", BatchSize: 1, CreatedBeforeMicros: 1},
				state:   &protos.BackfillState{Pending: []*protos.BackfillEntry{{ResourceId: "a", TaskId: "child"}}},
			}
			scheduler.On("WaitTask", mock.Anything, execCtx, "child").Return(&empty.Empty{}, childErr).Once()
			expectStatus(storage, "snapshots", "a", true, false)
			require.Error(t, task.Run(ctx, execCtx))
			require.Zero(t, task.state.Processed)
			require.Empty(t, task.state.AfterId)
			storage.AssertNotCalled(t, "EnqueueBackupBatch", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
		})
	}
}

func TestBackfillSkipsDeletedResourceAfterChildCleanup(t *testing.T) {
	ctx := context.Background()
	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("SaveState", mock.Anything).Return(nil)
	execCtx.On("GetTaskID").Return("run")
	task := &backfillTask{storage: storage, scheduler: scheduler,
		request: &protos.BackfillRequest{ResourceKind: "snapshots", BatchSize: 1, CreatedBeforeMicros: 1},
		state:   &protos.BackfillState{Pending: []*protos.BackfillEntry{{ResourceId: "a", TaskId: "child"}}},
	}
	scheduler.On("WaitTask", mock.Anything, execCtx, "child").Return(&empty.Empty{}, errors.NewInterruptExecutionError()).Twice()
	expectStatus(storage, "snapshots", "a", false, false)
	expectStatus(storage, "snapshots", "a", false, false)
	scheduler.On("WaitTaskEnded", mock.Anything, "child").Return(errors.NewInterruptExecutionError()).Once()
	require.Error(t, task.Run(ctx, execCtx))
	require.Zero(t, task.state.Skipped)
	scheduler.On("WaitTaskEnded", mock.Anything, "child").Return(nil).Once()
	storage.On("EnqueueBackupBatch", mock.Anything, "snapshots", "a", time.UnixMicro(1), 1, "run").Return([]resources.BackupRequest(nil), nil).Once()
	require.NoError(t, task.Run(ctx, execCtx))
	require.EqualValues(t, 1, task.state.Skipped)
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
}
