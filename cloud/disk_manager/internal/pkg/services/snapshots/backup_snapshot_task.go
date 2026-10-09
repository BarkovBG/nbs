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

func (t *backupSnapshotTask) Run(ctx context.Context, execCtx tasks.ExecutionContext) error {
	return retrySnapshotBackupError(t.run(ctx, execCtx))
}

func (t *backupSnapshotTask) run(ctx context.Context, execCtx tasks.ExecutionContext) error {
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
		// Old unscheduled tasks have no folder coordinator. Leave them for
		// the new folder pass instead of accepting an unowned attempt.
		if t.request.CoordinatorId == "" {
			return t.storage.SnapshotBackupCancelled(ctx, t.request.SnapshotId, t.request.BackupId)
		}
		if err := t.backupDependencies(ctx, execCtx); err != nil {
			return err
		}
		if t.state.AlreadyBackedUp {
			return nil
		}

		// The reservation is durable before scheduling any DP work. A retry of
		// an accepted owner is allowed when Delete has since marked deleting.
		meta, err := t.storage.ReserveSnapshotBackup(ctx, t.request.SnapshotId, owner, t.request.CoordinatorId)
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
			CompletedSnapshotIds: append([]string(nil), t.state.CompletedSnapshotIds...),
			MetaKey:              backup.SnapshotMetaKey(meta.Disk.DiskId, meta.ID), Meta: data,
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

func (t *backupSnapshotTask) Cancel(ctx context.Context, execCtx tasks.ExecutionContext) error {
	return retrySnapshotBackupError(t.cancel(ctx, execCtx))
}

func (t *backupSnapshotTask) cancel(ctx context.Context, execCtx tasks.ExecutionContext) error {
	// Recover a child whose scheduling succeeded before its ID was saved.
	if t.state.DataplaneRequest == nil && t.state.DataplaneTaskID == "" {
		if !t.state.DependenciesRead && t.state.DependencyReaderTaskId == "" {
			if err := t.scheduleDependencyReader(ctx, execCtx); err != nil {
				return err
			}
		}
		if t.state.DependencyIndex < uint32(len(t.state.DependencyIds)) && t.state.DependencyTaskId == "" {
			if err := t.scheduleDependency(ctx, execCtx); err != nil {
				return err
			}
		}
	}
	for _, id := range []string{t.state.DependencyTaskId, t.state.DependencyReaderTaskId} {
		if id == "" {
			continue
		}
		if _, err := t.scheduler.CancelTask(ctx, id); err != nil {
			return err
		}
		if err := t.scheduler.WaitTaskEnded(ctx, id); err != nil {
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
	// Recover cleanup even if the DP child ID was lost after scheduling.
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
	ids := append([]string(nil), t.state.CompletedSnapshotIds...)
	completed := t.state.Finished || t.state.AlreadyBackedUp
	if completed {
		ids = append(ids, t.request.SnapshotId)
	}
	return &protos.BackupSnapshotResponse{Completed: completed, CompletedSnapshotIds: ids}
}

func retrySnapshotBackupError(err error) error {
	if err == nil || errors.Is(err, errors.NewInterruptExecutionError()) || errors.Is(err, errors.NewWrongGenerationError()) || errors.Is(err, context.Canceled) {
		return err
	}
	return errors.NewRetriableErrorWithIgnoreRetryLimitf("snapshot backup waits for recovery: %v", err)
}

// Recursing through task dependencies gives a topological execution order:
// all available creators finish before the current snapshot is admitted.
func (t *backupSnapshotTask) backupDependencies(ctx context.Context, execCtx tasks.ExecutionContext) error {
	if !t.state.DependenciesRead {
		meta, err := t.storage.GetSnapshotMeta(ctx, t.request.SnapshotId)
		if err != nil {
			return err
		}
		if meta == nil || (!meta.Ready && !meta.BackupCompleted) {
			t.state.DependenciesRead = true
			return execCtx.SaveState(ctx)
		}
		if meta.BackupCompleted {
			t.state.AlreadyBackedUp = true
			t.state.DependenciesRead = true
			return execCtx.SaveState(ctx)
		}
		for _, id := range t.request.AncestorIds {
			if id == t.request.SnapshotId {
				return errors.NewNonRetriableErrorf("snapshot backup dependency cycle at %v", id)
			}
		}
		if t.state.DependencyReaderTaskId == "" {
			if err := t.scheduleDependencyReader(ctx, execCtx); err != nil {
				return err
			}
		}
		response, err := t.scheduler.WaitTask(ctx, execCtx, t.state.DependencyReaderTaskId)
		if err != nil {
			return err
		}
		deps, ok := response.(*dataplane_protos.GetSnapshotBackupDependenciesResponse)
		if !ok {
			return errors.NewNonRetriableErrorf("unexpected backup dependencies response")
		}
		t.state.DependencyIds = deps.SnapshotIds
		t.state.DependenciesRead = true
		if err := execCtx.SaveState(ctx); err != nil {
			return err
		}
	}
	for t.state.DependencyIndex < uint32(len(t.state.DependencyIds)) {
		dependency := t.state.DependencyIds[t.state.DependencyIndex]
		if t.state.DependencyTaskId == "" {
			// Do not enroll other folders or images implicitly. A missing,
			// deleting or unselected creator is copied from this source.
			current, err := t.storage.GetSnapshotMeta(ctx, t.request.SnapshotId)
			if err != nil {
				return err
			}
			meta, err := t.storage.GetSnapshotMeta(ctx, dependency)
			if err != nil {
				return err
			}
			if current == nil || meta == nil || meta.FolderID != current.FolderID || (!meta.Ready && !meta.BackupCompleted) {
				t.state.DependencyIndex++
				if err := execCtx.SaveState(ctx); err != nil {
					return err
				}
				continue
			}
			if err := t.scheduleDependency(ctx, execCtx); err != nil {
				return err
			}
		}
		response, err := t.scheduler.WaitTask(ctx, execCtx, t.state.DependencyTaskId)
		if err != nil {
			return err
		}
		completed, ok := response.(*protos.BackupSnapshotResponse)
		if !ok {
			return errors.NewNonRetriableErrorf("unexpected dependency backup response")
		}
		known := make(map[string]struct{})
		for _, id := range t.state.CompletedSnapshotIds {
			known[id] = struct{}{}
		}
		for _, id := range completed.CompletedSnapshotIds {
			if _, ok := known[id]; !ok {
				t.state.CompletedSnapshotIds = append(t.state.CompletedSnapshotIds, id)
				known[id] = struct{}{}
			}
		}
		t.state.DependencyIndex++
		t.state.DependencyTaskId = ""
		if err := execCtx.SaveState(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (t *backupSnapshotTask) scheduleDependencyReader(ctx context.Context, execCtx tasks.ExecutionContext) error {
	id, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, execCtx.GetTaskID()+"_dependencies"),
		"dataplane.GetSnapshotBackupDependencies", "",
		&dataplane_protos.GetSnapshotBackupDependenciesRequest{SnapshotId: t.request.SnapshotId},
	)
	if err != nil {
		return err
	}
	t.state.DependencyReaderTaskId = id
	return execCtx.SaveState(ctx)
}

func (t *backupSnapshotTask) scheduleDependency(ctx context.Context, execCtx tasks.ExecutionContext) error {
	dependency := t.state.DependencyIds[t.state.DependencyIndex]
	path := append(append([]string(nil), t.request.AncestorIds...), t.request.SnapshotId)
	id, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, execCtx.GetTaskID()+"_dependency_"+dependency),
		"snapshots.BackupSnapshot", "",
		&protos.BackupSnapshotRequest{SnapshotId: dependency, CoordinatorId: t.request.CoordinatorId, AncestorIds: path},
	)
	if err != nil {
		return err
	}
	t.state.DependencyTaskId = id
	return execCtx.SaveState(ctx)
}
