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
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *backupSnapshotTask) run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	// A saved copy ID accepts the copy. The copy holds the snapshot, so it
	// finishes even if the snapshot is being deleted.
	if len(t.state.DataplaneTaskID) == 0 {
		meta, err := t.storage.GetSnapshotMeta(ctx, t.request.SnapshotId)
		if err != nil {
			return err
		}

		// Nothing to copy: the snapshot is gone or already backed up.
		if meta == nil || !meta.Ready || meta.BackupCompleted {
			return t.removeFromQueue(ctx)
		}

		err = t.scheduleCopy(ctx, execCtx, meta)
		if err != nil {
			return err
		}
	}

	_, err := t.scheduler.WaitTask(ctx, execCtx, t.state.DataplaneTaskID)
	if err != nil {
		return err
	}

	// Marks only a ready snapshot: if deletion won before the copy took its
	// hold, the snapshot is already deleting and the copy copied nothing.
	err = t.storage.SnapshotBackupCompleted(ctx, t.request.SnapshotId)
	if err != nil {
		return err
	}

	return t.removeFromQueue(ctx)
}

func (t *backupSnapshotTask) cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	// The copy clears its chunks and releases the snapshot in its own Cancel.
	// A copy scheduled without a saved ID runs to the end and releases the
	// snapshot itself.
	if len(t.state.DataplaneTaskID) != 0 {
		_, err := t.scheduler.CancelTask(ctx, t.state.DataplaneTaskID)
		if err != nil {
			return err
		}
	}

	return t.removeFromQueue(ctx)
}

func (t *backupSnapshotTask) removeFromQueue(ctx context.Context) error {
	return t.storage.RemoveSnapshotFromBackupQueue(
		ctx,
		t.request.SnapshotId,
		t.request.BackupId,
	)
}

func (t *backupSnapshotTask) scheduleCopy(
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

	generated := len(t.state.EncryptedDek) == 0
	t.state.EncryptedDek, err = t.backupS3.EnsureEncryptedDEK(
		t.state.EncryptedDek,
	)
	if err != nil {
		return err
	}

	if generated && len(t.state.EncryptedDek) != 0 {
		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}
	}

	taskID, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(
			ctx,
			fmt.Sprintf("%v_backup", execCtx.GetTaskID()),
		),
		"dataplane.BackupSnapshotData",
		"",
		&dataplane_protos.BackupSnapshotDataRequest{
			SnapshotId:   meta.ID,
			EncryptedDek: t.state.EncryptedDek,
			MetaKey:      backup.SnapshotMetaKey(meta.Disk.DiskId, meta.ID),
			Meta:         data,
		},
	)
	if err != nil {
		return err
	}

	t.state.DataplaneTaskID = taskID
	return execCtx.SaveState(ctx)
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
