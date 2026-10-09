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
	return execCtx.GetTaskID()
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
	if t.state.Finished {
		if err := execCtx.SaveState(ctx); err != nil {
			return err
		}
		return t.releasePlan(ctx, execCtx)
	}
	if err := t.storage.ClaimSnapshotBackupPlan(ctx, t.ownerID(execCtx)); err != nil {
		return err
	}
	if !t.state.GraphPrepared {
		if err := t.prepareGraph(ctx, execCtx); err != nil {
			return err
		}
	}
	order, err := snapshotBackupOrder(t.state.Nodes)
	if err != nil {
		return err
	}
	for t.state.CopyIndex < uint32(len(order)) {
		node := t.node(order[t.state.CopyIndex])
		needed, err := t.pruneGraph(ctx, execCtx)
		if err != nil {
			return err
		}
		if !node.Completed && !node.Excluded {
			// An exact saved request identifies an already accepted copy.
			// Deletion never prunes that copy, including after a restart.
			if t.state.DataplaneRequest == nil {
				meta, err := t.storage.ReserveSnapshotBackup(ctx, node.SnapshotId,
					t.ownerID(execCtx), t.hasDependent(node.SnapshotId, needed))
				if err != nil {
					return err
				}
				if meta == nil {
					node.Excluded = true
					if err := execCtx.SaveState(ctx); err != nil {
						return err
					}
				} else if err := t.prepareCopy(ctx, execCtx, meta); err != nil {
					return err
				}
			}
			if t.state.DataplaneRequest != nil {
				if t.state.DataplaneTaskID == "" {
					if err := t.scheduleDataplaneTask(ctx, execCtx); err != nil {
						return err
					}
				}
				if _, err := t.scheduler.WaitTask(ctx, execCtx, t.state.DataplaneTaskID); err != nil {
					return err
				}
				t.markCompleted(node)
				// Persist completion before releasing any source reference.
				if err := execCtx.SaveState(ctx); err != nil {
					return err
				}
			}
		}
		if err := execCtx.SaveState(ctx); err != nil {
			return err
		}
		if err := t.storage.ReleaseSnapshotBackup(ctx, node.SnapshotId, t.ownerID(execCtx), node.Completed); err != nil {
			return err
		}
		t.state.DataplaneRequest = nil
		t.state.DataplaneTaskID = ""
		t.state.CopyIndex++
		if err := execCtx.SaveState(ctx); err != nil {
			return err
		}
	}
	t.state.Finished = true
	if err := execCtx.SaveState(ctx); err != nil {
		return err
	}
	return t.releasePlan(ctx, execCtx)
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
	encryptedDEK, err := t.backupS3.EnsureEncryptedDEK(t.request.EncryptedDek)
	if err != nil {
		return err
	}
	t.state.DataplaneRequest = &dataplane_protos.BackupSnapshotDataRequest{
		SnapshotId: meta.ID, OwnerId: t.ownerID(execCtx), EncryptedDek: encryptedDEK,
		CompletedSnapshotIds: t.completedSnapshotIDs(),
		MetaKey:              backup.SnapshotMetaKey(meta.Disk.DiskId, meta.ID), Meta: data,
	}
	// Cancellation recovers this exact request if the child ID was not saved.
	return execCtx.SaveState(ctx)
}

func (t *backupSnapshotTask) Cancel(ctx context.Context, execCtx tasks.ExecutionContext) error {
	return retrySnapshotBackupError(t.cancel(ctx, execCtx))
}

func (t *backupSnapshotTask) cancel(ctx context.Context, execCtx tasks.ExecutionContext) error {
	// Reader intent is saved before scheduling, just like the exact copy request.
	if t.state.ReaderSnapshotId != "" && t.state.ReaderTaskId == "" {
		if err := t.scheduleDependencyReader(ctx, execCtx); err != nil {
			return err
		}
	}
	if t.state.ReaderTaskId != "" {
		if _, err := t.scheduler.CancelTask(ctx, t.state.ReaderTaskId); err != nil {
			return err
		}
		if err := t.scheduler.WaitTaskEnded(ctx, t.state.ReaderTaskId); err != nil {
			return err
		}
	}
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
	if t.state.DataplaneRequest != nil {
		snapshotID := t.state.DataplaneRequest.SnapshotId
		// Recover a DP hold acquired before saving the child ID. Cleanup drains
		// chunk workers before any CP graph hold is released.
		id, err := t.scheduler.ScheduleTask(
			headers.SetIncomingIdempotencyKey(ctx, t.ownerID(execCtx)+"_release_"+snapshotID),
			"dataplane.ReleaseSnapshotBackup", "", &dataplane_protos.ReleaseSnapshotBackupRequest{SnapshotId: snapshotID, OwnerId: t.ownerID(execCtx)},
		)
		if err != nil {
			return err
		}
		if _, err := t.scheduler.WaitTask(ctx, execCtx, id); err != nil {
			return err
		}
	}
	return t.releasePlan(ctx, execCtx)
}

func (t *backupSnapshotTask) releasePlan(ctx context.Context, execCtx tasks.ExecutionContext) error {
	// Includes nodes whose Hold succeeded immediately before a failed SaveState:
	// every node's intent was durable before acquisition, even if not discovered.
	for _, node := range t.state.Nodes {
		if err := t.storage.ReleaseSnapshotBackup(ctx, node.SnapshotId, t.ownerID(execCtx), node.Completed); err != nil {
			return err
		}
	}
	if err := t.storage.RemoveSnapshotFromBackupQueue(ctx, t.request.SnapshotId, t.request.BackupId); err != nil {
		return err
	}
	return t.storage.FinishSnapshotBackupPlan(ctx, t.ownerID(execCtx))
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
