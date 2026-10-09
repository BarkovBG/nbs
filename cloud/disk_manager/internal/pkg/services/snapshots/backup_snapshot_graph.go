package snapshots

import (
	"context"

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
	for _, id := range t.state.CompletedSnapshotIds {
		if id == node.SnapshotId {
			return
		}
	}
	t.state.CompletedSnapshotIds = append(t.state.CompletedSnapshotIds, node.SnapshotId)
}

func (t *backupSnapshotTask) prepareGraph(ctx context.Context, execCtx tasks.ExecutionContext) error {
	if len(t.state.Nodes) == 0 {
		meta, err := t.storage.GetSnapshotMeta(ctx, t.request.SnapshotId)
		if err != nil {
			return err
		}
		if meta != nil {
			t.state.FolderId = meta.FolderID
		}
		t.state.Nodes = []*protos.SnapshotBackupNode{{SnapshotId: t.request.SnapshotId}}
		// Persist every possible hold's owner and source before acquiring it.
		if err := execCtx.SaveState(ctx); err != nil {
			return err
		}
	}
	for t.state.DiscoveryIndex < uint32(len(t.state.Nodes)) {
		node := t.state.Nodes[t.state.DiscoveryIndex]
		if !node.Discovered {
			meta, err := t.storage.GetSnapshotMeta(ctx, node.SnapshotId)
			if err != nil {
				return err
			}
			if meta == nil || meta.FolderID != t.state.FolderId {
				// A removed or unselected creator is represented by its edge;
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
				meta, err = t.storage.HoldSnapshotBackupPlan(ctx, node.SnapshotId, t.ownerID(execCtx), t.request.CoordinatorId)
				if err != nil {
					return err
				}
				if meta == nil {
					node.Excluded = true
					node.Discovered = true
				} else {
					if t.state.ReaderSnapshotId == "" {
						t.state.ReaderSnapshotId = node.SnapshotId
						if err := execCtx.SaveState(ctx); err != nil {
							return err
						}
					}
					if t.state.ReaderTaskId == "" {
						if err := t.scheduleDependencyReader(ctx, execCtx); err != nil {
							return err
						}
					}
					response, err := t.scheduler.WaitTask(ctx, execCtx, t.state.ReaderTaskId)
					if err != nil {
						return err
					}
					deps, ok := response.(*dataplane_protos.GetSnapshotBackupDependenciesResponse)
					if !ok {
						return errors.NewNonRetriableErrorf("unexpected backup dependencies response")
					}
					node.Dependencies = append([]string(nil), deps.SnapshotIds...)
					for _, id := range node.Dependencies {
						if t.node(id) == nil {
							t.state.Nodes = append(t.state.Nodes, &protos.SnapshotBackupNode{SnapshotId: id})
						}
					}
					node.Discovered = true
				}
			}
		}
		t.state.DiscoveryIndex++
		t.state.ReaderSnapshotId = ""
		t.state.ReaderTaskId = ""
		if err := execCtx.SaveState(ctx); err != nil {
			return err
		}
	}
	order, err := snapshotBackupOrder(t.state.Nodes)
	if err != nil {
		return err
	}
	t.state.CopyOrder = order
	t.state.GraphPrepared = true
	// No copy may start until the full dependency closure is durable.
	return execCtx.SaveState(ctx)
}

func (t *backupSnapshotTask) scheduleDependencyReader(ctx context.Context, execCtx tasks.ExecutionContext) error {
	if err := execCtx.SaveState(ctx); err != nil {
		return err
	}
	id, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, execCtx.GetTaskID()+"_dependencies_"+t.state.ReaderSnapshotId),
		"dataplane.GetSnapshotBackupDependencies", "",
		&dataplane_protos.GetSnapshotBackupDependenciesRequest{SnapshotId: t.state.ReaderSnapshotId},
	)
	if err != nil {
		return err
	}
	t.state.ReaderTaskId = id
	return execCtx.SaveState(ctx)
}

// Pending live snapshots are roots themselves. A deleting ancestor stays needed
// while any remaining live root uses it; a deleting leaf is dropped. The folder
// coordinator permits only one graph, so holds cannot belong to another plan.
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
		if !node.Excluded && !node.Completed {
			for _, parent := range node.Dependencies {
				if err := visit(parent); err != nil {
					return err
				}
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
