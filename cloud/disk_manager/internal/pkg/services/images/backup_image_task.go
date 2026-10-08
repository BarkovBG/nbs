package images

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type backupImageTask struct {
	scheduler tasks.Scheduler
	storage   resources.Storage
	request   *protos.BackupImageRequest
	state     *protos.BackupImageTaskState
}

func (t *backupImageTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *backupImageTask) Load(request, state []byte) error {
	t.request = &protos.BackupImageRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.BackupImageTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *backupImageTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if len(t.state.DataplaneTaskID) == 0 {
		if t.state.DataplaneRequest == nil {
			meta, err := t.storage.GetImageMeta(ctx, t.request.ImageId)
			if err != nil {
				return err
			}
			if meta == nil || !meta.Ready {
				return t.storage.ImageBackupCancelled(
					ctx,
					t.request.ImageId,
					t.request.BackupId,
				)
			}
			imageMeta, err := backup.NewImageMeta(*meta)
			if err != nil {
				return err
			}
			data, err := json.Marshal(imageMeta)
			if err != nil {
				return errors.NewNonRetriableError(err)
			}
			t.state.DataplaneRequest = &dataplane_protos.BackupSnapshotDataRequest{
				SnapshotId: t.request.ImageId,
				MetaKey:    backup.ImageMetaKey(t.request.ImageId),
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
	return t.storage.ImageBackupScheduled(
		ctx,
		t.request.ImageId,
		t.request.BackupId,
	)
}

func (t *backupImageTask) Cancel(
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
	return t.storage.ImageBackupCancelled(
		ctx,
		t.request.ImageId,
		t.request.BackupId,
	)
}

func (t *backupImageTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *backupImageTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *backupImageTask) scheduleDataplaneTask(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	key := fmt.Sprintf("%v_%v_backup", execCtx.GetTaskID(), t.request.ImageId)
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
