package snapshots

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type backupSnapshotTask struct {
	scheduler tasks.Scheduler
	storage   resources.Storage
	request   *protos.BackupSnapshotRequest
	state     *protos.BackupSnapshotTaskState
}

func (t *backupSnapshotTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *backupSnapshotTask) Load(request, state []byte) error {
	t.request = &protos.BackupSnapshotRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.BackupSnapshotTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *backupSnapshotTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if len(t.state.DataplaneTaskID) == 0 {
		if t.state.DataplaneRequest == nil {
			meta, err := t.storage.GetSnapshotMeta(ctx, t.request.SnapshotId)
			if err != nil {
				return err
			}
			if meta == nil || !meta.Ready {
				return t.storage.SnapshotBackupCancelled(
					ctx,
					t.request.SnapshotId,
					t.request.BackupId,
				)
			}
			if meta.Disk == nil {
				return errors.NewNonRetriableErrorf(
					"snapshot %v has no disk",
					t.request.SnapshotId,
				)
			}
			snapshotMeta, err := backup.NewSnapshotMeta(*meta)
			if err != nil {
				return err
			}
			data, err := json.Marshal(snapshotMeta)
			if err != nil {
				return errors.NewNonRetriableError(err)
			}
			t.state.DataplaneRequest = &dataplane_protos.BackupSnapshotDataRequest{
				SnapshotId: t.request.SnapshotId,
				MetaKey:    backup.SnapshotMetaKey(meta.Disk.DiskId, t.request.SnapshotId),
				Meta:       data,
			}
			// Persist the exact request before scheduling. Cancel can then recover
			// the child even if scheduling succeeds but saving its ID fails.
			err = execCtx.SaveState(ctx)
			if err != nil {
				return err
			}
		}
		err := t.scheduleDataplaneTask(ctx, execCtx)
		if err != nil {
			return err
		}
	}

	_, err := t.scheduler.WaitTask(ctx, execCtx, t.state.DataplaneTaskID)
	if err != nil {
		return err
	}
	return t.storage.SnapshotBackupScheduled(
		ctx,
		t.request.SnapshotId,
		t.request.BackupId,
	)
}

func (t *backupSnapshotTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) (err error) {

	// Cancellation must not be marked complete while cleanup has failed.
	defer func() {
		if err != nil && !errors.Is(err, errors.NewInterruptExecutionError()) {
			err = errors.NewRetriableErrorf("failed to cancel snapshot backup: %v", err)
		}
	}()

	if len(t.state.DataplaneTaskID) == 0 && t.state.DataplaneRequest != nil {
		err := t.scheduleDataplaneTask(ctx, execCtx)
		if err != nil {
			return err
		}
	}
	if len(t.state.DataplaneTaskID) != 0 {
		_, err := t.scheduler.CancelTask(ctx, t.state.DataplaneTaskID)
		if err != nil {
			return err
		}
		err = t.scheduler.WaitTaskEnded(ctx, t.state.DataplaneTaskID)
		if err != nil {
			return err
		}
	}

	// DP preserves a pending chunk map for deletion to clean up interrupted
	// copies. Shared chunks remain available to live source resources.
	return t.storage.SnapshotBackupCancelled(
		ctx,
		t.request.SnapshotId,
		t.request.BackupId,
	)
}

func (t *backupSnapshotTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *backupSnapshotTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *backupSnapshotTask) scheduleDataplaneTask(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	key := fmt.Sprintf("%v_%v_backup", execCtx.GetTaskID(), t.request.SnapshotId)
	taskID, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, key),
		"dataplane.BackupSnapshotData",
		"",
		t.state.DataplaneRequest,
	)
	if err != nil {
		return err
	}
	t.state.DataplaneTaskID = taskID
	return execCtx.SaveState(ctx)
}
