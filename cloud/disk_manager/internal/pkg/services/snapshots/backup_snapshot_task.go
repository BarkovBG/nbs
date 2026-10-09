package snapshots

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

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

// Backs up a snapshot together with the snapshots that created its chunks,
// creators first, so that their chunks are copied once. Each copy holds only
// its own snapshot: the chunks a snapshot uses stay while it exists, so a
// creator deleted before its turn is skipped and its chunks are copied with
// the snapshots that use them.
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
	var completed bool
	if len(t.state.Nodes) != 0 {
		completed = t.state.Nodes[len(t.state.Nodes)-1].Completed
	}

	return &protos.BackupSnapshotResponse{
		Completed:            completed,
		CompletedSnapshotIds: t.completedSnapshotIDs(),
	}
}

////////////////////////////////////////////////////////////////////////////////

func (t *backupSnapshotTask) run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if len(t.state.Nodes) == 0 {
		err := t.prepareNodes(ctx, execCtx)
		if err != nil {
			return err
		}
	}

	for _, node := range t.state.Nodes {
		if node.Completed || node.Skipped {
			continue
		}

		err := t.copyNode(ctx, execCtx, node)
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

// Orders the snapshots by creation time: a chunk is created before the
// snapshots that reference it, so creators come before their users.
func (t *backupSnapshotTask) prepareNodes(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	root, err := t.storage.GetSnapshotMeta(ctx, t.request.SnapshotId)
	if err != nil {
		return err
	}

	metas := make([]*resources.SnapshotMeta, 0)
	if root != nil && root.Ready && !root.BackupCompleted {
		creators, err := t.listChunkCreators(ctx, execCtx)
		if err != nil {
			return err
		}

		for _, id := range creators {
			meta, err := t.storage.GetSnapshotMeta(ctx, id)
			if err != nil {
				return err
			}

			// A deleted creator is not copied; the chunks of it that are
			// still used are copied with the snapshots that use them.
			if meta != nil {
				metas = append(metas, meta)
			}
		}

		sort.Slice(metas, func(i, j int) bool {
			return metas[i].CreatingAt.Before(metas[j].CreatingAt)
		})
	}

	for _, meta := range metas {
		t.state.Nodes = append(t.state.Nodes, &protos.SnapshotBackupNode{
			SnapshotId: meta.ID,
			Completed:  meta.BackupCompleted,
		})
	}

	t.state.Nodes = append(t.state.Nodes, &protos.SnapshotBackupNode{
		SnapshotId: t.request.SnapshotId,
		Completed:  root != nil && root.BackupCompleted,
		Skipped:    root == nil || !root.Ready && !root.BackupCompleted,
	})
	return execCtx.SaveState(ctx)
}

func (t *backupSnapshotTask) listChunkCreators(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) ([]string, error) {

	taskID, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(
			ctx,
			execCtx.GetTaskID()+"_chunk_creators",
		),
		"dataplane.ListSnapshotChunkCreators",
		"",
		&dataplane_protos.ListSnapshotChunkCreatorsRequest{
			SnapshotId: t.request.SnapshotId,
		},
	)
	if err != nil {
		return nil, err
	}

	response, err := t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return nil, err
	}

	typedResponse, ok := response.(*dataplane_protos.ListSnapshotChunkCreatorsResponse)
	if !ok {
		return nil, errors.NewNonRetriableErrorf(
			"invalid list snapshot chunk creators response type %T",
			response,
		)
	}

	return typedResponse.SnapshotIds, nil
}

// Saving the request accepts the copy. The copy then holds the snapshot, so
// it finishes even if the snapshot is being deleted, unless deletion won
// before the hold.
func (t *backupSnapshotTask) copyNode(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	node *protos.SnapshotBackupNode,
) error {

	if t.state.DataplaneRequest == nil {
		meta, err := t.storage.GetSnapshotMeta(ctx, node.SnapshotId)
		if err != nil {
			return err
		}

		if meta == nil || !meta.Ready {
			node.Skipped = true
			return execCtx.SaveState(ctx)
		}

		err = t.prepareCopy(ctx, execCtx, meta)
		if err != nil {
			return err
		}
	}

	taskID, err := t.scheduleCopy(ctx, execCtx)
	if err != nil {
		return err
	}

	response, err := t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return err
	}

	typedResponse, ok := response.(*dataplane_protos.BackupSnapshotDataResponse)
	if !ok {
		return errors.NewNonRetriableErrorf(
			"invalid backup snapshot data response type %T",
			response,
		)
	}

	if typedResponse.Skipped {
		node.Skipped = true
	} else {
		err = t.storage.SnapshotBackupCompleted(ctx, node.SnapshotId)
		if err != nil {
			return err
		}

		node.Completed = true
	}

	t.state.DataplaneRequest = nil
	t.state.DataplaneTaskID = ""
	return execCtx.SaveState(ctx)
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
		SnapshotId:           meta.ID,
		EncryptedDek:         encryptedDEK,
		MetaKey:              backup.SnapshotMetaKey(meta.Disk.DiskId, meta.ID),
		Meta:                 data,
		CompletedSnapshotIds: t.completedSnapshotIDs(),
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
			fmt.Sprintf(
				"%v_%v_backup",
				execCtx.GetTaskID(),
				t.state.DataplaneRequest.SnapshotId,
			),
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

func (t *backupSnapshotTask) completedSnapshotIDs() []string {
	var ids []string
	for _, node := range t.state.Nodes {
		if node.Completed {
			ids = append(ids, node.SnapshotId)
		}
	}

	sort.Strings(ids)
	return ids
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
