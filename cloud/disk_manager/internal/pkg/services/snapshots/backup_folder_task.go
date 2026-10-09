package snapshots

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type backupFolderTask struct {
	scheduler tasks.Scheduler
	storage   resources.Storage
	request   *protos.BackupFolderRequest
	state     *protos.BackupFolderState
}

func (t *backupFolderTask) selected(ctx context.Context) (bool, error) {
	folders, err := t.storage.ListSnapshotBackupFolders(ctx)
	if err != nil {
		return false, err
	}
	for _, folder := range folders {
		if folder.FolderID == t.request.FolderId && folder.Generation == t.request.Generation {
			return true, nil
		}
	}
	return false, nil
}

func (t *backupFolderTask) scheduleCopy(ctx context.Context, execCtx tasks.ExecutionContext) error {
	if t.state.BackupTaskId != "" {
		return nil
	}
	id, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, "backup_snapshot_"+t.state.SnapshotId+"_"+t.state.OwnerId),
		"snapshots.BackupSnapshot", "", &protos.BackupSnapshotRequest{
			SnapshotId:    t.state.SnapshotId,
			BackupId:      t.state.OwnerId,
			CoordinatorId: execCtx.GetTaskID(),
		},
	)
	if err != nil {
		return err
	}
	t.state.BackupTaskId = id
	return execCtx.SaveState(ctx)
}

func (t *backupFolderTask) finishCopy(ctx context.Context, execCtx tasks.ExecutionContext) error {
	if err := t.scheduleCopy(ctx, execCtx); err != nil {
		return err
	}
	response, err := t.scheduler.WaitTask(ctx, execCtx, t.state.BackupTaskId)
	if err != nil {
		return err
	}
	if _, ok := response.(*protos.BackupSnapshotResponse); !ok {
		return errors.NewNonRetriableErrorf("unexpected snapshot backup response")
	}
	// The child has either completed its verified copy and cleanup, or was
	// excluded before admission. Advance only after that outcome is durable.
	t.state.AfterId = t.state.SnapshotId
	t.state.SnapshotId = ""
	t.state.OwnerId = ""
	t.state.BackupTaskId = ""
	return execCtx.SaveState(ctx)
}

////////////////////////////////////////////////////////////////////////////////

func (t *backupFolderTask) Save() ([]byte, error) { return proto.Marshal(t.state) }

func (t *backupFolderTask) Load(request, state []byte) error {
	t.request = &protos.BackupFolderRequest{}
	if err := proto.Unmarshal(request, t.request); err != nil {
		return err
	}
	t.state = &protos.BackupFolderState{}
	return proto.Unmarshal(state, t.state)
}

func (t *backupFolderTask) Run(ctx context.Context, execCtx tasks.ExecutionContext) (err error) {
	defer func() { err = retrySnapshotBackupError(err) }()
	if t.request.FolderId == "" || t.request.Generation == "" {
		return errors.NewNonRetriableErrorf("invalid snapshot backup folder")
	}
	for {
		// Saved intent is recovered even after disabling a folder. An already
		// accepted child finishes; an unaccepted child rejects admission.
		if t.state.SnapshotId != "" {
			if err := t.finishCopy(ctx, execCtx); err != nil {
				return err
			}
		}
		selected, err := t.selected(ctx)
		if err != nil {
			return err
		}
		if !selected {
			return t.storage.ReleaseSnapshotBackupFolder(ctx, t.request.FolderId, execCtx.GetTaskID())
		}
		claimed, err := t.storage.ClaimSnapshotBackupFolder(ctx, t.request.FolderId, t.request.Generation, execCtx.GetTaskID())
		if err != nil {
			return err
		}
		if !claimed {
			continue
		}
		ids, err := t.storage.ListSnapshotBackupCandidates(ctx, t.request.FolderId, t.state.AfterId, 1)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			if t.state.AfterId == "" {
				return errors.NewInterruptExecutionError()
			}
			// A ready event behind the cursor is picked up on the next pass.
			t.state.AfterId = ""
			if err := execCtx.SaveState(ctx); err != nil {
				return err
			}
			continue
		}
		t.state.Round++
		t.state.SnapshotId = ids[0]
		t.state.OwnerId = fmt.Sprintf("%v_%v_%v", execCtx.GetTaskID(), t.state.Round, ids[0])
		// Persist the exact child key before sending it. No other snapshot is
		// held or scheduled while this one is in progress.
		if err := execCtx.SaveState(ctx); err != nil {
			return err
		}
	}
}

func (t *backupFolderTask) Cancel(ctx context.Context, execCtx tasks.ExecutionContext) (err error) {
	defer func() { err = retrySnapshotBackupError(err) }()
	if err := t.storage.SetSnapshotBackupFolder(ctx, t.request.FolderId, t.request.Generation, false); err != nil {
		return err
	}
	if t.state.SnapshotId != "" {
		if err := t.scheduleCopy(ctx, execCtx); err != nil {
			return err
		}
		if _, err := t.scheduler.CancelTask(ctx, t.state.BackupTaskId); err != nil {
			return err
		}
		if err := t.scheduler.WaitTaskEnded(ctx, t.state.BackupTaskId); err != nil {
			return err
		}
	}
	return t.storage.ReleaseSnapshotBackupFolder(ctx, t.request.FolderId, execCtx.GetTaskID())
}

func (t *backupFolderTask) GetMetadata(ctx context.Context) (proto.Message, error) {
	return t.state, nil
}

func (t *backupFolderTask) GetResponse() proto.Message { return t.state }
