package snapshots

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

const backupFolderPageSize = 100

////////////////////////////////////////////////////////////////////////////////

// The folder task only fills the shared queue. It owns no source locks or
// copy children; the single-ID graph task performs all protection and copying.
type backupFolderTask struct {
	storage resources.Storage
	request *protos.BackupFolderRequest
	state   *protos.BackupFolderState
}

func (t *backupFolderTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *backupFolderTask) Load(request, state []byte) error {
	t.request = &protos.BackupFolderRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.BackupFolderState{}
	return proto.Unmarshal(state, t.state)
}

func (t *backupFolderTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if t.request.FolderId == "" || t.request.Generation == "" {
		return errors.NewNonRetriableErrorf("invalid snapshot backup folder")
	}

	selected, err := t.isSelected(ctx)
	if err != nil {
		return err
	}
	if !selected {
		return nil
	}

	if t.state.Round == 0 {
		t.state.Round = 1
		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}
	}

	ids, err := t.storage.ListSnapshotBackupCandidates(
		ctx,
		t.request.FolderId,
		t.state.AfterId,
		backupFolderPageSize,
	)
	if err != nil {
		return err
	}

	for _, id := range ids {
		selected, err = t.storage.EnqueueSnapshotBackupForFolder(
			ctx,
			id,
			t.backupID(execCtx),
			t.request.FolderId,
			t.request.Generation,
		)
		if err != nil {
			return err
		}
		if !selected {
			return nil
		}

		t.state.AfterId = id
		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}
	}

	if len(ids) == 0 {
		// Resetting catches snapshots that became ready behind the cursor, and a
		// fresh round gives explicitly cancelled copies a new scheduling identity.
		t.state.AfterId = ""
		t.state.Round++
		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}
	}

	return errors.NewInterruptExecutionError()
}

func (t *backupFolderTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if t.request.FolderId == "" || t.request.Generation == "" {
		return nil
	}

	return retrySnapshotBackupError(t.storage.SetSnapshotBackupFolder(
		ctx,
		t.request.FolderId,
		t.request.Generation,
		false, // enabled
	))
}

func (t *backupFolderTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return t.state, nil
}

func (t *backupFolderTask) GetResponse() proto.Message {
	return t.state
}

////////////////////////////////////////////////////////////////////////////////

func (t *backupFolderTask) isSelected(ctx context.Context) (bool, error) {
	folders, err := t.storage.ListSnapshotBackupFolders(ctx)
	if err != nil {
		return false, err
	}

	for _, folder := range folders {
		if folder.FolderID == t.request.FolderId &&
			folder.Generation == t.request.Generation {

			return true, nil
		}
	}

	return false, nil
}

func (t *backupFolderTask) backupID(execCtx tasks.ExecutionContext) string {
	return fmt.Sprintf(
		"%v_%v_%v",
		execCtx.GetTaskID(),
		t.request.Generation,
		t.state.Round,
	)
}
