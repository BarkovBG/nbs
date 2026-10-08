package snapshots

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

const snapshotBackfillPageSize = 32

type backupFolderTask struct {
	scheduler tasks.Scheduler
	storage   resources.Storage
	request   *protos.BackupFolderRequest
	state     *protos.BackupFolderState
}

func (t *backupFolderTask) Save() ([]byte, error) { return proto.Marshal(t.state) }

func (t *backupFolderTask) Load(request, state []byte) error {
	t.request = &protos.BackupFolderRequest{}
	if err := proto.Unmarshal(request, t.request); err != nil {
		return err
	}
	t.state = &protos.BackupFolderState{}
	return proto.Unmarshal(state, t.state)
}

func (t *backupFolderTask) selected(ctx context.Context) (bool, error) {
	folders, err := t.storage.ListSnapshotBackupFolders(ctx)
	if err != nil {
		return false, err
	}
	for _, f := range folders {
		if f.FolderID == t.request.FolderId && f.Generation == t.request.Generation {
			return true, nil
		}
	}
	return false, nil
}

func (t *backupFolderTask) savePlan(ctx context.Context, p *resources.SnapshotBackupPlan, state *protos.SnapshotBackupPlanState) error {
	data, err := proto.Marshal(state)
	if err != nil {
		return errors.NewNonRetriableError(err)
	}
	p.State = data
	p.Prepared = state.Prepared && !state.Excluded
	return t.storage.SaveSnapshotBackupPlan(ctx, *p)
}

func loadSnapshotBackupPlan(p resources.SnapshotBackupPlan) (*protos.SnapshotBackupPlanState, error) {
	state := &protos.SnapshotBackupPlanState{}
	if err := proto.Unmarshal(p.State, state); err != nil {
		return nil, errors.NewNonRetriableError(err)
	}
	return state, nil
}

func (t *backupFolderTask) schedulePreparation(ctx context.Context, p resources.SnapshotBackupPlan) (string, error) {
	return t.scheduler.ScheduleTask(headers.SetIncomingIdempotencyKey(ctx, p.OwnerID+"_prepare"),
		"dataplane.PrepareSnapshotBackup", "", &dataplane_protos.PrepareSnapshotBackupRequest{SnapshotId: p.SnapshotID, OwnerId: p.OwnerID})
}

func (t *backupFolderTask) prepare(ctx context.Context, execCtx tasks.ExecutionContext, p *resources.SnapshotBackupPlan, state *protos.SnapshotBackupPlanState) error {
	if state.Excluded {
		return t.release(ctx, execCtx, p, state, false)
	}
	if state.Prepared {
		return nil
	}
	meta, err := t.storage.ReserveSnapshotBackup(ctx, p.SnapshotID, p.OwnerID, false)
	if err != nil {
		return err
	}
	if meta == nil {
		state.Excluded = true
		if err := t.savePlan(ctx, p, state); err != nil {
			return err
		}
		return t.release(ctx, execCtx, p, state, false)
	}
	if state.PrepareTaskId == "" {
		state.PrepareRequested = true
		if err := t.savePlan(ctx, p, state); err != nil {
			return err
		}
		id, err := t.schedulePreparation(ctx, *p)
		if err != nil {
			return err
		}
		state.PrepareTaskId = id
		if err := t.savePlan(ctx, p, state); err != nil {
			return err
		}
	}
	response, err := t.scheduler.WaitTask(ctx, execCtx, state.PrepareTaskId)
	if err != nil {
		return err
	}
	result, ok := response.(*dataplane_protos.PrepareSnapshotBackupResponse)
	if !ok {
		return errors.NewNonRetriableErrorf("unexpected snapshot preparation response")
	}
	p.BaseSnapshotID = result.BaseSnapshotId
	state.Prepared = true
	return t.savePlan(ctx, p, state)
}

func (t *backupFolderTask) addCandidate(ctx context.Context, execCtx tasks.ExecutionContext, id string) error {
	p, err := t.storage.GetSnapshotBackupPlan(ctx, t.request.FolderId, id)
	if err != nil {
		return err
	}
	if p == nil {
		meta, err := t.storage.GetSnapshotMeta(ctx, id)
		if err != nil {
			return err
		}
		if meta == nil || !meta.Ready || meta.BackupCompleted {
			return nil
		}
		p = &resources.SnapshotBackupPlan{FolderID: t.request.FolderId, SnapshotID: id,
			OwnerID: fmt.Sprintf("%v_%v_%v", execCtx.GetTaskID(), t.state.Round, id), CreatingAt: meta.CreatingAt, CoordinatorID: execCtx.GetTaskID()}
		if err := t.savePlan(ctx, p, &protos.SnapshotBackupPlanState{}); err != nil {
			return err
		}
	}
	if p.CoordinatorID != execCtx.GetTaskID() {
		return errors.NewInterruptExecutionError()
	}
	state, err := loadSnapshotBackupPlan(*p)
	if err != nil {
		return err
	}
	return t.prepare(ctx, execCtx, p, state)
}

func (t *backupFolderTask) prepareRemaining(ctx context.Context, execCtx tasks.ExecutionContext) error {
	after := ""
	for {
		plans, err := t.storage.ListSnapshotBackupPlans(ctx, t.request.FolderId, snapshotBackfillPageSize, false, after)
		if err != nil {
			return err
		}
		if len(plans) == 0 {
			return nil
		}
		for i := range plans {
			if plans[i].CoordinatorID != execCtx.GetTaskID() {
				return errors.NewInterruptExecutionError()
			}
			state, err := loadSnapshotBackupPlan(plans[i])
			if err != nil {
				return err
			}
			if err := t.prepare(ctx, execCtx, &plans[i], state); err != nil {
				return err
			}
		}
		after = plans[len(plans)-1].SnapshotID
	}
}

func (t *backupFolderTask) Run(ctx context.Context, execCtx tasks.ExecutionContext) (err error) {
	defer func() { err = retrySnapshotBackupError(err) }()
	if t.request.FolderId == "" {
		return errors.NewNonRetriableErrorf("empty snapshot backup folder")
	}
	for {
		selected, err := t.selected(ctx)
		if err != nil {
			return err
		}
		// Removing a folder stops new admissions but lets an accepted copy end.
		if !selected {
			if err := t.stop(ctx, execCtx, false); err != nil {
				return err
			}
			return t.storage.ReleaseSnapshotBackupFolder(ctx, t.request.FolderId, execCtx.GetTaskID())
		}
		claimed, err := t.storage.ClaimSnapshotBackupFolder(ctx, t.request.FolderId, t.request.Generation, execCtx.GetTaskID())
		if err != nil {
			return err
		}
		if !claimed {
			continue
		}
		if t.state.CreatedBeforeMicros == 0 {
			t.state.CreatedBeforeMicros = time.Now().UnixMicro()
			if err := execCtx.SaveState(ctx); err != nil {
				return err
			}
			if err := t.storage.SetSnapshotBackupFolderPrepared(ctx, t.request.FolderId, t.request.Generation, false); err != nil {
				return err
			}
		}
		if !t.state.Prepared {
			ids, err := t.storage.ListSnapshotBackupCandidates(ctx, t.request.FolderId, t.state.AfterId,
				time.UnixMicro(t.state.CreatedBeforeMicros), snapshotBackfillPageSize)
			if err != nil {
				return err
			}
			for _, id := range ids {
				if err := t.addCandidate(ctx, execCtx, id); err != nil {
					return err
				}
			}
			if len(ids) != 0 {
				t.state.AfterId = ids[len(ids)-1]
				if err := execCtx.SaveState(ctx); err != nil {
					return err
				}
				continue
			}
			// A crash may have saved a plan before reservation, followed by
			// Delete or completion removing the resource from the CP page.
			if err := t.prepareRemaining(ctx, execCtx); err != nil {
				return err
			}
			if err := t.storage.SetSnapshotBackupFolderPrepared(ctx, t.request.FolderId, t.request.Generation, true); err != nil {
				return err
			}
			t.state.Prepared = true
			if err := execCtx.SaveState(ctx); err != nil {
				return err
			}
		}
		plans, err := t.storage.ListSnapshotBackupPlans(ctx, t.request.FolderId, 1, true, "")
		if err != nil {
			return err
		}
		if len(plans) != 0 {
			if err := t.copy(ctx, execCtx, &plans[0]); err != nil {
				return err
			}
			continue
		}
		remaining, err := t.storage.ListSnapshotBackupPlans(ctx, t.request.FolderId, 1, false, "")
		if err != nil {
			return err
		}
		if len(remaining) != 0 {
			return errors.NewNonRetriableErrorf("cycle in snapshot backup dependencies of folder %v", t.request.FolderId)
		}
		if !t.state.InitialPassComplete {
			t.state.InitialPassComplete = true
			t.state.AfterId = ""
			if err := execCtx.SaveState(ctx); err != nil {
				return err
			}
		}
		// After backfill, take one newly ready resource at a time. The ready
		// event queues it atomically; this repeated scan also repairs omissions.
		ids, err := t.storage.ListSnapshotBackupCandidates(ctx, t.request.FolderId, "", time.Now(), 1)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return errors.NewInterruptExecutionError()
		}
		t.state.Round++
		if err := execCtx.SaveState(ctx); err != nil {
			return err
		}
		if err := t.addCandidate(ctx, execCtx, ids[0]); err != nil {
			return err
		}
	}
}

func (t *backupFolderTask) copy(ctx context.Context, execCtx tasks.ExecutionContext, p *resources.SnapshotBackupPlan) error {
	if p.CoordinatorID != execCtx.GetTaskID() {
		return errors.NewInterruptExecutionError()
	}
	state, err := loadSnapshotBackupPlan(*p)
	if err != nil {
		return err
	}
	if state.Excluded || state.Complete {
		return t.release(ctx, execCtx, p, state, false)
	}
	if !state.Prepared {
		if err := t.prepare(ctx, execCtx, p, state); err != nil {
			return err
		}
		if state.Excluded {
			return nil
		}
	}
	if state.CopyTaskId == "" {
		state.CopyRequested = true
		if err := t.savePlan(ctx, p, state); err != nil {
			return err
		}
		id, err := t.scheduler.ScheduleTask(headers.SetIncomingIdempotencyKey(ctx, "backup_snapshot_"+p.SnapshotID+"_"+p.OwnerID),
			"snapshots.BackupSnapshot", "", &protos.BackupSnapshotRequest{SnapshotId: p.SnapshotID, BackupId: p.OwnerID})
		if err != nil {
			return err
		}
		state.CopyTaskId = id
		if err := t.savePlan(ctx, p, state); err != nil {
			return err
		}
	}
	response, err := t.scheduler.WaitTask(ctx, execCtx, state.CopyTaskId)
	if err != nil {
		return err
	}
	result, ok := response.(*protos.BackupSnapshotResponse)
	if !ok {
		return errors.NewNonRetriableErrorf("unexpected snapshot backup response")
	}
	state.Complete = result.Completed
	state.Excluded = !result.Completed
	if err := t.savePlan(ctx, p, state); err != nil {
		return err
	}
	return t.release(ctx, execCtx, p, state, false)
}

func (t *backupFolderTask) release(ctx context.Context, execCtx tasks.ExecutionContext, p *resources.SnapshotBackupPlan, state *protos.SnapshotBackupPlanState, cancelCopy bool) error {
	if state.CopyTaskId != "" {
		if cancelCopy {
			if _, err := t.scheduler.CancelTask(ctx, state.CopyTaskId); err != nil {
				return err
			}
		}
		if err := t.scheduler.WaitTaskEnded(ctx, state.CopyTaskId); err != nil {
			return err
		}
	} else {
		// Recover a scheduling success whose result was not saved. This exact
		// task key is shared with copy(), so no late child can start after release.
		meta, err := t.storage.GetSnapshotMeta(ctx, p.SnapshotID)
		if err != nil {
			return err
		}
		if state.CopyRequested || (meta != nil && meta.BackupStarted && meta.BackupTaskID == p.OwnerID) {
			id, err := t.scheduler.ScheduleTask(headers.SetIncomingIdempotencyKey(ctx, "backup_snapshot_"+p.SnapshotID+"_"+p.OwnerID),
				"snapshots.BackupSnapshot", "", &protos.BackupSnapshotRequest{SnapshotId: p.SnapshotID, BackupId: p.OwnerID})
			if err != nil {
				return err
			}
			if _, err := t.scheduler.CancelTask(ctx, id); err != nil {
				return err
			}
			if err := t.scheduler.WaitTaskEnded(ctx, id); err != nil {
				return err
			}
		}
	}
	if state.PrepareTaskId == "" {
		meta, err := t.storage.GetSnapshotMeta(ctx, p.SnapshotID)
		if err != nil {
			return err
		}
		if state.PrepareRequested || (meta != nil && meta.BackupTaskID == p.OwnerID) {
			id, err := t.schedulePreparation(ctx, *p)
			if err != nil {
				return err
			}
			state.PrepareTaskId = id
			if err := t.savePlan(ctx, p, state); err != nil {
				return err
			}
		}
	}
	if state.PrepareTaskId != "" {
		if _, err := t.scheduler.CancelTask(ctx, state.PrepareTaskId); err != nil {
			return err
		}
		if err := t.scheduler.WaitTaskEnded(ctx, state.PrepareTaskId); err != nil {
			return err
		}
	}
	id, err := t.scheduler.ScheduleTask(headers.SetIncomingIdempotencyKey(ctx, p.OwnerID+"_release"),
		"dataplane.ReleaseSnapshotBackup", "", &dataplane_protos.ReleaseSnapshotBackupRequest{SnapshotId: p.SnapshotID, OwnerId: p.OwnerID})
	if err != nil {
		return err
	}
	if _, err := t.scheduler.WaitTask(ctx, execCtx, id); err != nil {
		return err
	}
	if err := t.storage.ReleaseSnapshotBackup(ctx, p.SnapshotID, p.OwnerID, state.Complete); err != nil {
		return err
	}
	return t.storage.RemoveSnapshotBackupPlan(ctx, p.FolderID, p.SnapshotID, p.OwnerID)
}

func (t *backupFolderTask) stop(ctx context.Context, execCtx tasks.ExecutionContext, cancelCopies bool) error {
	for {
		plans, err := t.storage.ListSnapshotBackupPlans(ctx, t.request.FolderId, snapshotBackfillPageSize, false, "")
		if err != nil {
			return err
		}
		if len(plans) == 0 {
			return nil
		}
		for i := range plans {
			if plans[i].CoordinatorID != execCtx.GetTaskID() {
				return errors.NewInterruptExecutionError()
			}
			state, err := loadSnapshotBackupPlan(plans[i])
			if err != nil {
				return err
			}
			if state.CopyRequested && state.CopyTaskId == "" {
				id, err := t.scheduler.ScheduleTask(headers.SetIncomingIdempotencyKey(ctx, "backup_snapshot_"+plans[i].SnapshotID+"_"+plans[i].OwnerID),
					"snapshots.BackupSnapshot", "", &protos.BackupSnapshotRequest{SnapshotId: plans[i].SnapshotID, BackupId: plans[i].OwnerID})
				if err != nil {
					return err
				}
				state.CopyTaskId = id
				if err := t.savePlan(ctx, &plans[i], state); err != nil {
					return err
				}
			}
			if state.CopyTaskId != "" && !cancelCopies {
				response, err := t.scheduler.WaitTask(ctx, execCtx, state.CopyTaskId)
				if err != nil {
					return err
				}
				result, ok := response.(*protos.BackupSnapshotResponse)
				if !ok {
					return errors.NewNonRetriableErrorf("unexpected snapshot backup response")
				}
				state.Complete = result.Completed
			}
			state.Excluded = !state.Complete
			if err := t.savePlan(ctx, &plans[i], state); err != nil {
				return err
			}
			if err := t.release(ctx, execCtx, &plans[i], state, cancelCopies); err != nil {
				return err
			}
		}
	}
}

func (t *backupFolderTask) Cancel(ctx context.Context, execCtx tasks.ExecutionContext) error {
	if err := t.stop(ctx, execCtx, true); err != nil {
		return retrySnapshotBackupError(err)
	}
	return t.storage.ReleaseSnapshotBackupFolder(ctx, t.request.FolderId, execCtx.GetTaskID())
}

func (t *backupFolderTask) GetMetadata(ctx context.Context) (proto.Message, error) {
	return t.state, nil
}

func (t *backupFolderTask) GetResponse() proto.Message { return t.state }
