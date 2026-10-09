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

// Backs up one snapshot. dataplane.BackupSnapshotData holds the snapshot for
// the whole copy and copies every chunk of its map that is not in the follower
// yet, so chunks shared with other snapshots are copied once.
type backupSnapshotTask struct {
	scheduler tasks.Scheduler
	storage   resources.Storage
	backupS3  *backup.S3
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

	return retrySnapshotBackupError(t.run(ctx, execCtx))
}

func (t *backupSnapshotTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return retrySnapshotBackupError(t.cancel(ctx, execCtx))
}

func (t *backupSnapshotTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *backupSnapshotTask) GetResponse() proto.Message {
	return &protos.BackupSnapshotResponse{Completed: t.state.Completed}
}

////////////////////////////////////////////////////////////////////////////////

func (t *backupSnapshotTask) run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	completed, err := t.backup(ctx, execCtx)
	if err != nil {
		return err
	}

	t.state.Completed = completed
	err = execCtx.SaveState(ctx)
	if err != nil {
		return err
	}

	return t.storage.RemoveSnapshotFromBackupQueue(
		ctx,
		t.request.SnapshotId,
		t.request.BackupId,
	)
}

func (t *backupSnapshotTask) cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if t.state.DataplaneRequest != nil {
		taskID, err := t.scheduleCopy(ctx, execCtx)
		if err != nil {
			return err
		}

		_, err = t.scheduler.CancelTask(ctx, taskID)
		if err != nil {
			return err
		}

		err = t.scheduler.WaitTaskEnded(ctx, taskID)
		if err != nil {
			return err
		}
	}

	return t.storage.RemoveSnapshotFromBackupQueue(
		ctx,
		t.request.SnapshotId,
		t.request.BackupId,
	)
}

// Returns whether the snapshot has a complete copy in the follower. Saving the
// request accepts the copy: from then on it finishes even if the snapshot is
// being deleted, unless deletion won before the copy took its hold.
func (t *backupSnapshotTask) backup(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) (bool, error) {

	if t.state.DataplaneRequest == nil {
		meta, err := t.storage.GetSnapshotMeta(ctx, t.request.SnapshotId)
		if err != nil {
			return false, err
		}

		if meta == nil || !meta.Ready || meta.BackupCompleted {
			return meta != nil && meta.BackupCompleted, nil
		}

		err = t.prepareCopy(ctx, execCtx, meta)
		if err != nil {
			return false, err
		}
	}

	taskID, err := t.scheduleCopy(ctx, execCtx)
	if err != nil {
		return false, err
	}

	response, err := t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return false, err
	}

	typedResponse, ok := response.(*dataplane_protos.BackupSnapshotDataResponse)
	if !ok {
		return false, errors.NewNonRetriableErrorf(
			"invalid backup snapshot data response type %T",
			response,
		)
	}

	if typedResponse.Skipped {
		return false, nil
	}

	err = t.storage.SnapshotBackupCompleted(ctx, t.request.SnapshotId)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (t *backupSnapshotTask) prepareCopy(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	meta *resources.SnapshotMeta,
) error {

	if meta.Disk == nil {
		return errors.NewNonRetriableErrorf("snapshot %v has no disk", meta.ID)
	}

	snapshotMeta, err := backup.NewSnapshotMeta(*meta)
	if err != nil {
		return err
	}

	data, err := json.Marshal(snapshotMeta)
	if err != nil {
		return errors.NewNonRetriableError(err)
	}

	encryptedDEK, err := t.backupS3.EnsureEncryptedDEK(nil)
	if err != nil {
		return err
	}

	t.state.DataplaneRequest = &dataplane_protos.BackupSnapshotDataRequest{
		SnapshotId:   meta.ID,
		EncryptedDek: encryptedDEK,
		MetaKey:      backup.SnapshotMetaKey(meta.Disk.DiskId, meta.ID),
		Meta:         data,
	}
	return execCtx.SaveState(ctx)
}

func (t *backupSnapshotTask) scheduleCopy(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) (string, error) {

	if t.state.DataplaneTaskID != "" {
		return t.state.DataplaneTaskID, nil
	}

	taskID, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(
			ctx,
			fmt.Sprintf("%v_backup", execCtx.GetTaskID()),
		),
		"dataplane.BackupSnapshotData",
		"",
		t.state.DataplaneRequest,
	)
	if err != nil {
		return "", err
	}

	t.state.DataplaneTaskID = taskID
	return taskID, execCtx.SaveState(ctx)
}

func retrySnapshotBackupError(err error) error {
	if err == nil ||
		errors.Is(err, errors.NewInterruptExecutionError()) ||
		errors.Is(err, errors.NewWrongGenerationError()) ||
		errors.Is(err, context.Canceled) {

		return err
	}

	return errors.NewRetriableErrorWithIgnoreRetryLimitf(
		"snapshot backup waits for recovery: %v",
		err,
	)
}
