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
	backupS3  *backup.S3
	request   *protos.BackupSnapshotRequest
	state     *protos.BackupSnapshotTaskState
}

func (t *backupSnapshotTask) Save() ([]byte, error) { return proto.Marshal(t.state) }

func (t *backupSnapshotTask) Load(request, state []byte) error {
	t.request = &protos.BackupSnapshotRequest{}
	if err := proto.Unmarshal(request, t.request); err != nil {
		return err
	}
	t.state = &protos.BackupSnapshotTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *backupSnapshotTask) ownerID(execCtx tasks.ExecutionContext) string {
	if t.request.BackupId != "" {
		return t.request.BackupId
	}
	return execCtx.GetTaskID()
}

func (t *backupSnapshotTask) scheduleDataplaneTask(ctx context.Context, execCtx tasks.ExecutionContext) error {
	id, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, fmt.Sprintf("%v_%v_backup", execCtx.GetTaskID(), t.request.SnapshotId)),
		"dataplane.BackupSnapshotData", "", t.state.DataplaneRequest,
	)
	if err != nil {
		return err
	}
	t.state.DataplaneTaskID = id
	return execCtx.SaveState(ctx)
}

func (t *backupSnapshotTask) Run(ctx context.Context, execCtx tasks.ExecutionContext) (err error) {
	defer func() { err = retrySnapshotBackupError(err) }()
	owner := t.ownerID(execCtx)
	if t.state.DataplaneTaskID != "" && t.state.DataplaneRequest == nil {
		// Finish a child scheduled by the previous implementation. Its result
		// predates full-map verification and must not set backup_completed.
		if _, err := t.scheduler.WaitTask(ctx, execCtx, t.state.DataplaneTaskID); err != nil {
			return err
		}
		return t.storage.SnapshotBackupCancelled(ctx, t.request.SnapshotId, t.request.BackupId)
	}
	if t.state.Finished {
		return t.storage.ReleaseSnapshotBackup(ctx, t.request.SnapshotId, owner, true)
	}
	if t.state.DataplaneRequest == nil {
		// The reservation is durable before scheduling any DP work. A retry of
		// an accepted owner is allowed when Delete has since marked deleting.
		meta, err := t.storage.ReserveSnapshotBackup(ctx, t.request.SnapshotId, owner, true)
		if err != nil {
			return err
		}
		if meta == nil {
			return t.storage.SnapshotBackupCancelled(ctx, t.request.SnapshotId, t.request.BackupId)
		}
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
		if len(t.state.EncryptedDek) == 0 {
			t.state.EncryptedDek = t.request.EncryptedDek
		}
		t.state.EncryptedDek, err = t.backupS3.EnsureEncryptedDEK(t.state.EncryptedDek)
		if err != nil {
			return err
		}
		t.state.DataplaneRequest = &dataplane_protos.BackupSnapshotDataRequest{
			SnapshotId: meta.ID, OwnerId: owner, EncryptedDek: t.state.EncryptedDek,
			MetaKey: backup.SnapshotMetaKey(meta.Disk.DiskId, meta.ID), Meta: data,
		}
		// Exact metadata and DEK survive a crash after scheduling but before
		// saving the child ID. Cancellation recovers that same child.
		if err := execCtx.SaveState(ctx); err != nil {
			return err
		}
	}
	if t.state.DataplaneTaskID == "" {
		if err := t.scheduleDataplaneTask(ctx, execCtx); err != nil {
			return err
		}
	}
	if _, err := t.scheduler.WaitTask(ctx, execCtx, t.state.DataplaneTaskID); err != nil {
		return err
	}
	t.state.Finished = true
	if err := execCtx.SaveState(ctx); err != nil {
		return err
	}
	return t.storage.ReleaseSnapshotBackup(ctx, t.request.SnapshotId, owner, true)
}

func (t *backupSnapshotTask) Cancel(ctx context.Context, execCtx tasks.ExecutionContext) (err error) {
	defer func() {
		if err != nil && !errors.Is(err, errors.NewInterruptExecutionError()) {
			err = errors.NewRetriableErrorf("failed to cancel snapshot backup: %v", err)
		}
	}()
	if t.state.DataplaneTaskID == "" && t.state.DataplaneRequest != nil {
		if err := t.scheduleDataplaneTask(ctx, execCtx); err != nil {
			return err
		}
	}
	if t.state.DataplaneTaskID != "" {
		if _, err := t.scheduler.CancelTask(ctx, t.state.DataplaneTaskID); err != nil {
			return err
		}
		if err := t.scheduler.WaitTaskEnded(ctx, t.state.DataplaneTaskID); err != nil {
			return err
		}
	}
	// A preparation may have installed the same hold before this task started.
	id, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, t.ownerID(execCtx)+"_release"),
		"dataplane.ReleaseSnapshotBackup", "", &dataplane_protos.ReleaseSnapshotBackupRequest{SnapshotId: t.request.SnapshotId, OwnerId: t.ownerID(execCtx)},
	)
	if err != nil {
		return err
	}
	if _, err := t.scheduler.WaitTask(ctx, execCtx, id); err != nil {
		return err
	}
	return t.storage.ReleaseSnapshotBackup(ctx, t.request.SnapshotId, t.ownerID(execCtx), t.state.Finished)
}

func (t *backupSnapshotTask) GetMetadata(ctx context.Context) (proto.Message, error) {
	return &empty.Empty{}, nil
}

func (t *backupSnapshotTask) GetResponse() proto.Message {
	return &protos.BackupSnapshotResponse{Completed: t.state.Finished}
}

func retrySnapshotBackupError(err error) error {
	if err == nil || errors.Is(err, errors.NewInterruptExecutionError()) || errors.Is(err, errors.NewWrongGenerationError()) || errors.Is(err, context.Canceled) {
		return err
	}
	return errors.NewRetriableErrorWithIgnoreRetryLimitf("snapshot backup waits for recovery: %v", err)
}
