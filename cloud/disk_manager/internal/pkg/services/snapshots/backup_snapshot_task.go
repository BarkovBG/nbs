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

func (t *backupSnapshotTask) scheduleDataplaneTask(ctx context.Context, execCtx tasks.ExecutionContext) error {
	if err := execCtx.SaveState(ctx); err != nil {
		return err
	}
	id, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, fmt.Sprintf("%v_%v_backup", execCtx.GetTaskID(), t.state.DataplaneRequest.SnapshotId)),
		"dataplane.BackupSnapshotData", "", t.state.DataplaneRequest,
	)
	if err != nil {
		return err
	}
	t.state.DataplaneTaskID = id
	return execCtx.SaveState(ctx)
}

func (t *backupSnapshotTask) Run(ctx context.Context, execCtx tasks.ExecutionContext) error {
	return retrySnapshotBackupError(t.run(ctx, execCtx))
}

func (t *backupSnapshotTask) run(ctx context.Context, execCtx tasks.ExecutionContext) error {
	if err := t.prepareGraph(ctx, execCtx); err != nil {
		return err
	}
	order, err := snapshotBackupOrder(t.state.Nodes)
	if err != nil {
		return err
	}
	for _, id := range order {
		node := t.node(id)
		if nodeDone(node) {
			continue
		}
		metas, err := t.pruneGraph(ctx, execCtx)
		if err != nil {
			return err
		}
		if meta := metas[node.SnapshotId]; meta != nil {
			if err := t.copyNode(ctx, execCtx, node, meta); err != nil {
				return err
			}
		}
		if err := t.releaseNode(ctx, execCtx, node); err != nil {
			return err
		}
	}
	return t.releasePlan(ctx, execCtx)
}

// A node is done once it is copied or excluded and its lock is released.
// Completion is saved before the release, so a restart repeats only the
// release of the current node.
func nodeDone(node *protos.SnapshotBackupNode) bool {
	return (node.Completed || node.Excluded) && !node.LockRequested
}

// Saving the exact request accepts the copy: from then on it is finished
// even if the source gets deleted meanwhile.
func (t *backupSnapshotTask) copyNode(ctx context.Context, execCtx tasks.ExecutionContext, node *protos.SnapshotBackupNode, meta *resources.SnapshotMeta) error {
	if !t.copyAccepted(node) {
		if err := t.prepareCopy(ctx, execCtx, meta); err != nil {
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
	node.Completed = true
	t.state.DataplaneRequest = nil
	t.state.DataplaneTaskID = ""
	// Persist completion before releasing the source lock.
	return execCtx.SaveState(ctx)
}

// Record completion and unlock the node.
func (t *backupSnapshotTask) releaseNode(ctx context.Context, execCtx tasks.ExecutionContext, node *protos.SnapshotBackupNode) error {
	if node.Completed {
		if err := t.storage.SnapshotBackupCompleted(ctx, node.SnapshotId); err != nil {
			return err
		}
	}
	return t.releaseSnapshotLock(ctx, execCtx, node)
}

func (t *backupSnapshotTask) prepareCopy(ctx context.Context, execCtx tasks.ExecutionContext, meta *resources.SnapshotMeta) error {
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
		SnapshotId: meta.ID, EncryptedDek: encryptedDEK,
		CompletedSnapshotIds: t.completedSnapshotIDs(),
		MetaKey:              backup.SnapshotMetaKey(meta.Disk.DiskId, meta.ID), Meta: data,
	}
	// Cancellation recovers this exact request if the child ID was not saved.
	return execCtx.SaveState(ctx)
}

func (t *backupSnapshotTask) Cancel(ctx context.Context, execCtx tasks.ExecutionContext) error {
	return retrySnapshotBackupError(t.cancel(ctx, execCtx))
}

// Stop the copy and wait for its queue cleanup before the lock tasks are
// cancelled.
func (t *backupSnapshotTask) cancel(ctx context.Context, execCtx tasks.ExecutionContext) error {
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
	for _, node := range t.state.Nodes {
		if err := t.cancelSnapshotLock(ctx, execCtx, node); err != nil {
			return err
		}
	}
	return t.releasePlan(ctx, execCtx)
}

// Includes locks requested immediately before a failed SaveState: every
// node's intent was durable before its lock task was scheduled.
func (t *backupSnapshotTask) releasePlan(ctx context.Context, execCtx tasks.ExecutionContext) error {
	for _, node := range t.state.Nodes {
		if err := t.releaseSnapshotLock(ctx, execCtx, node); err != nil {
			return err
		}
	}
	return t.storage.RemoveSnapshotFromBackupQueue(ctx, t.request.SnapshotId, t.request.BackupId)
}

func (t *backupSnapshotTask) GetMetadata(ctx context.Context) (proto.Message, error) {
	return &empty.Empty{}, nil
}

func (t *backupSnapshotTask) GetResponse() proto.Message {
	root := t.node(t.request.SnapshotId)
	return &protos.BackupSnapshotResponse{
		Completed:            root != nil && root.Completed,
		CompletedSnapshotIds: t.completedSnapshotIDs(),
	}
}

func retrySnapshotBackupError(err error) error {
	if err == nil || errors.Is(err, errors.NewInterruptExecutionError()) || errors.Is(err, errors.NewWrongGenerationError()) || errors.Is(err, context.Canceled) {
		return err
	}
	return errors.NewRetriableErrorWithIgnoreRetryLimitf("snapshot backup waits for recovery: %v", err)
}
