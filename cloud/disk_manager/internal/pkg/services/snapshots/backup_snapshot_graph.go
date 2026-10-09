package snapshots

import (
	"context"
	"sort"

	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

func (t *backupSnapshotTask) node(id string) *protos.SnapshotBackupNode {
	for _, node := range t.state.Nodes {
		if node.SnapshotId == id {
			return node
		}
	}
	return nil
}

func (t *backupSnapshotTask) markCompleted(node *protos.SnapshotBackupNode) {
	node.Completed = true
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

func (t *backupSnapshotTask) nextUndiscoveredNode() *protos.SnapshotBackupNode {
	for _, node := range t.state.Nodes {
		if !node.Discovered {
			return node
		}
	}
	return nil
}

func (t *backupSnapshotTask) prepareGraph(ctx context.Context, execCtx tasks.ExecutionContext) error {
	if len(t.state.Nodes) == 0 {
		t.state.Nodes = []*protos.SnapshotBackupNode{{SnapshotId: t.request.SnapshotId}}
		// Persist every possible hold's owner and source before acquiring it.
		if err := execCtx.SaveState(ctx); err != nil {
			return err
		}
	}
	for node := t.nextUndiscoveredNode(); node != nil; node = t.nextUndiscoveredNode() {
		if !node.Discovered {
			meta, err := t.storage.GetSnapshotMeta(ctx, node.SnapshotId)
			if err != nil {
				return err
			}
			if meta == nil {
				// A removed creator is represented by its edge;
				// surviving chunks are copied through the held descendant.
				node.Excluded = true
				node.Discovered = true
			} else if meta.BackupCompleted {
				t.markCompleted(node)
				node.Discovered = true
			} else {
				if err := execCtx.SaveState(ctx); err != nil {
					return err
				}
				meta, err = t.storage.HoldSnapshotBackupPlan(ctx, node.SnapshotId, t.ownerID(execCtx))
				if err != nil {
					return err
				}
				if meta == nil {
					node.Excluded = true
					node.Discovered = true
				} else {
					if !node.LockRequested {
						node.LockRequested = true
						if err := execCtx.SaveState(ctx); err != nil {
							return err
						}
					}
					if node.LockTaskId == "" {
						if err := t.scheduleSnapshotLock(ctx, execCtx, node); err != nil {
							return err
						}
					}
					message, err := t.scheduler.GetTaskMetadata(ctx, node.LockTaskId)
					if err != nil {
						return err
					}
					lock, ok := message.(*dataplane_protos.LockSnapshotForBackupState)
					if !ok {
						return errors.NewNonRetriableErrorf("unexpected snapshot backup lock metadata %T", message)
					}
					if !lock.Ready {
						return errors.NewInterruptExecutionError()
					}
					if !lock.Locked {
						node.Excluded = true
					} else {
						node.Dependencies = append([]string(nil), lock.SnapshotIds...)
						for _, id := range node.Dependencies {
							if t.node(id) == nil {
								t.state.Nodes = append(t.state.Nodes, &protos.SnapshotBackupNode{SnapshotId: id})
							}
						}
					}
					node.Discovered = true
				}
			}
		}
		if err := execCtx.SaveState(ctx); err != nil {
			return err
		}
	}
	if _, err := snapshotBackupOrder(t.state.Nodes); err != nil {
		return err
	}
	t.state.GraphPrepared = true
	// No copy may start until the full dependency closure is durable.
	return execCtx.SaveState(ctx)
}

func (t *backupSnapshotTask) scheduleSnapshotLock(ctx context.Context, execCtx tasks.ExecutionContext, node *protos.SnapshotBackupNode) error {
	if err := execCtx.SaveState(ctx); err != nil {
		return err
	}
	id, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, execCtx.GetTaskID()+"_lock_"+node.SnapshotId),
		"dataplane.LockSnapshotForBackup", "",
		&dataplane_protos.LockSnapshotForBackupRequest{SnapshotId: node.SnapshotId, OwnerId: t.ownerID(execCtx)},
	)
	if err != nil {
		return err
	}
	node.LockTaskId = id
	return execCtx.SaveState(ctx)
}

func (t *backupSnapshotTask) releaseSnapshotLock(ctx context.Context, execCtx tasks.ExecutionContext, node *protos.SnapshotBackupNode) error {
	if !node.LockRequested {
		return nil
	}
	if node.LockTaskId == "" {
		if err := t.scheduleSnapshotLock(ctx, execCtx, node); err != nil {
			return err
		}
	}
	if err := t.scheduler.SendEvent(ctx, node.LockTaskId, int64(dataplane_protos.SnapshotBackupLockEvent_RELEASE)); err != nil {
		return err
	}
	if err := t.scheduler.WaitTaskEnded(ctx, node.LockTaskId); err != nil {
		return err
	}
	node.LockRequested = false
	node.LockTaskId = ""
	return execCtx.SaveState(ctx)
}

// Pending live snapshots are roots themselves. A deleting ancestor stays needed
// while any remaining live root uses it; a deleting leaf is dropped. The
// queue permits only one graph at a time, avoiding conflicting graph holds.
func (t *backupSnapshotTask) pruneGraph(ctx context.Context, execCtx tasks.ExecutionContext) (map[string]bool, error) {
	nodes := make(map[string]*protos.SnapshotBackupNode)
	roots := make([]string, 0)
	for _, node := range t.state.Nodes {
		if node.Excluded || node.Completed {
			continue
		}
		nodes[node.SnapshotId] = node
		meta, err := t.storage.GetSnapshotMeta(ctx, node.SnapshotId)
		if err != nil {
			return nil, err
		}
		if meta == nil || meta.BackupPlanID != t.ownerID(execCtx) {
			return nil, errors.NewNonRetriableErrorf("lost snapshot backup graph hold for %v", node.SnapshotId)
		}
		if meta.Ready || meta.BackupTaskID == t.ownerID(execCtx) {
			roots = append(roots, node.SnapshotId)
		}
	}
	needed := make(map[string]bool)
	var visit func(string)
	visit = func(id string) {
		node := nodes[id]
		if node == nil || needed[id] {
			return
		}
		needed[id] = true
		for _, parent := range node.Dependencies {
			visit(parent)
		}
	}
	for _, id := range roots {
		visit(id)
	}
	for _, node := range t.state.Nodes {
		if !node.Completed && !node.Excluded && !needed[node.SnapshotId] {
			node.Excluded = true
		}
	}
	if err := execCtx.SaveState(ctx); err != nil {
		return nil, err
	}
	// Drop holds only after pruning is saved. Ordinary deletion removes the
	// snapshot's own chunk references, leaving references of other sources.
	for _, node := range t.state.Nodes {
		if node.Excluded {
			if err := t.releaseSnapshotLock(ctx, execCtx, node); err != nil {
				return nil, err
			}
			if err := t.storage.ReleaseSnapshotBackup(ctx, node.SnapshotId, t.ownerID(execCtx), false); err != nil {
				return nil, err
			}
		}
	}
	return needed, nil
}

func (t *backupSnapshotTask) hasDependent(id string, needed map[string]bool) bool {
	for _, node := range t.state.Nodes {
		if !needed[node.SnapshotId] {
			continue
		}
		for _, parent := range node.Dependencies {
			if parent == id {
				return true
			}
		}
	}
	return false
}

// Order depends only on saved edges, so completing/pruning nodes cannot shift
// CopyIndex after a restart.
func snapshotBackupOrder(nodes []*protos.SnapshotBackupNode) ([]string, error) {
	byID := make(map[string]*protos.SnapshotBackupNode)
	for _, node := range nodes {
		byID[node.SnapshotId] = node
	}
	visited := make(map[string]bool)
	visiting := make(map[string]bool)
	order := make([]string, 0, len(nodes))
	var visit func(string) error
	visit = func(id string) error {
		if visited[id] {
			return nil
		}
		if visiting[id] {
			return errors.NewNonRetriableErrorf("snapshot backup dependency cycle at %v", id)
		}
		node := byID[id]
		if node == nil {
			return errors.NewNonRetriableErrorf("snapshot backup dependency %v is absent from graph", id)
		}
		visiting[id] = true
		for _, parent := range node.Dependencies {
			if err := visit(parent); err != nil {
				return err
			}
		}
		visited[id] = true
		visiting[id] = false
		order = append(order, id)
		return nil
	}
	for _, node := range nodes {
		if err := visit(node.SnapshotId); err != nil {
			return nil, err
		}
	}
	return order, nil
}
