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

// Backs up a snapshot together with the snapshots whose chunks it uses.
// First every snapshot of the graph is locked by dataplane.LockSnapshotForBackup,
// then dataplane.BackupSnapshotData copies them, creators of chunks first.
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
	root := t.node(t.request.SnapshotId)
	return &protos.BackupSnapshotResponse{
		Completed:            root != nil && root.Completed,
		CompletedSnapshotIds: t.completedSnapshotIDs(),
	}
}

////////////////////////////////////////////////////////////////////////////////

func (t *backupSnapshotTask) run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.prepareGraph(ctx, execCtx)
	if err != nil {
		return err
	}

	for _, node := range snapshotBackupOrder(t.state.Nodes) {
		if !node.Completed && !node.Excluded {
			err = t.copyNode(ctx, execCtx, node)
			if err != nil {
				return err
			}
		}

		// Completion is saved before the unlock, so a restart repeats only
		// the record and the unlock.
		if node.Completed && node.Locked {
			err = t.storage.SnapshotBackupCompleted(ctx, node.SnapshotId)
			if err != nil {
				return err
			}
		}

		err = t.unlockSnapshot(ctx, execCtx, node)
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

// Stops the copy before the locks are cancelled: the copy reads the snapshot
// that the lock keeps.
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

	for _, node := range t.state.Nodes {
		err := t.unlockSnapshot(ctx, execCtx, node)
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

// Starts from the requested snapshot and adds the creators of its chunks
// until the whole graph is locked. No copy starts before that.
func (t *backupSnapshotTask) prepareGraph(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if len(t.state.Nodes) == 0 {
		t.state.Nodes = []*protos.SnapshotBackupNode{
			{SnapshotId: t.request.SnapshotId},
		}
	}

	// Discovery appends new nodes to the slice.
	for i := 0; i < len(t.state.Nodes); i++ {
		node := t.state.Nodes[i]
		if node.Discovered {
			continue
		}

		err := t.discoverNode(ctx, execCtx, node)
		if err != nil {
			return err
		}

		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *backupSnapshotTask) discoverNode(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	node *protos.SnapshotBackupNode,
) error {

	meta, err := t.storage.GetSnapshotMeta(ctx, node.SnapshotId)
	if err != nil {
		return err
	}

	switch {
	case meta != nil && meta.BackupCompleted:
		node.Completed = true
	case meta == nil || !meta.Ready:
		// Missing or deleting creators are not copied. Descendants keep
		// references to the chunks they use and copy those chunks themselves.
		node.Excluded = true
	default:
		lock, err := t.lockSnapshot(ctx, execCtx, node)
		if err != nil {
			return err
		}

		if !lock.Ready {
			return errors.NewInterruptExecutionError()
		}

		if !lock.Locked {
			// Deletion won the lock.
			node.Excluded = true
		}

		node.Dependencies = lock.SnapshotIds
		for _, id := range node.Dependencies {
			if t.node(id) == nil {
				t.state.Nodes = append(
					t.state.Nodes,
					&protos.SnapshotBackupNode{SnapshotId: id},
				)
			}
		}
	}

	node.Discovered = true
	return nil
}

// Saving the request accepts the copy: from then on it is finished even if
// the snapshot is being deleted. Before that a deleting snapshot is skipped;
// the chunks of it that descendants use are copied with the descendants.
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
			node.Excluded = true
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

	_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return err
	}

	node.Completed = true
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

// Returns the state of the task that locks the snapshot of the node.
func (t *backupSnapshotTask) lockSnapshot(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	node *protos.SnapshotBackupNode,
) (*dataplane_protos.LockSnapshotForBackupState, error) {

	taskID, err := t.scheduleLock(ctx, execCtx, node)
	if err != nil {
		return nil, err
	}

	metadata, err := t.scheduler.GetTaskMetadata(ctx, taskID)
	if err != nil {
		return nil, err
	}

	lock, ok := metadata.(*dataplane_protos.LockSnapshotForBackupState)
	if !ok {
		return nil, errors.NewNonRetriableErrorf(
			"unexpected snapshot backup lock metadata %T",
			metadata,
		)
	}

	return lock, nil
}

// Cancelling the lock task drops its dataplane reference to the snapshot.
func (t *backupSnapshotTask) unlockSnapshot(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	node *protos.SnapshotBackupNode,
) error {

	if !node.Locked {
		return nil
	}

	taskID, err := t.scheduleLock(ctx, execCtx, node)
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

	node.Locked = false
	return execCtx.SaveState(ctx)
}

// Locked is saved before scheduling, so cancellation finds every lock task.
// Scheduling with the same idempotency key returns the same task, so its ID
// is not saved.
func (t *backupSnapshotTask) scheduleLock(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	node *protos.SnapshotBackupNode,
) (string, error) {

	if !node.Locked {
		node.Locked = true
		err := execCtx.SaveState(ctx)
		if err != nil {
			return "", err
		}
	}

	return t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(
			ctx,
			execCtx.GetTaskID()+"_lock_"+node.SnapshotId,
		),
		"dataplane.LockSnapshotForBackup",
		"",
		&dataplane_protos.LockSnapshotForBackupRequest{
			SnapshotId: node.SnapshotId,
		},
	)
}

func (t *backupSnapshotTask) node(id string) *protos.SnapshotBackupNode {
	for _, node := range t.state.Nodes {
		if node.SnapshotId == id {
			return node
		}
	}

	return nil
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

// Puts the creators of chunks before the snapshots that use them. A creator
// existed before the snapshot that references its chunks, so there are no
// cycles.
func snapshotBackupOrder(
	nodes []*protos.SnapshotBackupNode,
) []*protos.SnapshotBackupNode {

	byID := make(map[string]*protos.SnapshotBackupNode)
	for _, node := range nodes {
		byID[node.SnapshotId] = node
	}

	visited := make(map[string]bool)
	order := make([]*protos.SnapshotBackupNode, 0, len(nodes))

	var visit func(node *protos.SnapshotBackupNode)
	visit = func(node *protos.SnapshotBackupNode) {
		if visited[node.SnapshotId] {
			return
		}
		visited[node.SnapshotId] = true

		for _, id := range node.Dependencies {
			visit(byID[id])
		}
		order = append(order, node)
	}

	for _, node := range nodes {
		visit(node)
	}
	return order
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
