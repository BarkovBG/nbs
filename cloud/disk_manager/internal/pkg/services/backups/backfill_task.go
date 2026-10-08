package backups

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/backups/protos"
	image_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	snapshot_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type backfillTask struct {
	scheduler tasks.Scheduler
	storage   resources.Storage
	request   *protos.BackfillRequest
	state     *protos.BackfillState
}

func (t *backfillTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *backfillTask) Load(request, state []byte) error {
	t.request = &protos.BackfillRequest{}
	if err := proto.Unmarshal(request, t.request); err != nil {
		return err
	}
	t.state = &protos.BackfillState{}
	return proto.Unmarshal(state, t.state)
}

func (t *backfillTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if (t.request.ResourceKind != "snapshots" && t.request.ResourceKind != "images") ||
		t.request.BatchSize == 0 || t.request.BatchSize > 1000 || t.request.CreatedBeforeMicros <= 0 {
		return errors.NewNonRetriableErrorf("invalid backfill request")
	}
	for {
		if len(t.state.Pending) == 0 {
			page, err := t.storage.EnqueueBackupBatch(
				ctx,
				t.request.ResourceKind,
				t.state.AfterId,
				time.UnixMicro(t.request.CreatedBeforeMicros),
				int(t.request.BatchSize),
				execCtx.GetTaskID(),
			)
			if err != nil {
				return err
			}
			if len(page) == 0 {
				return nil
			}
			for _, entry := range page {
				t.state.Pending = append(t.state.Pending, &protos.BackfillEntry{
					ResourceId: entry.ResourceID, BackupId: entry.BackupID,
				})
			}
			if err := execCtx.SaveState(ctx); err != nil {
				return err
			}
		}

		// Schedule the entire page before waiting, bounding this run's copies
		// by BatchSize while preserving the regular scheduler's task keys.
		for _, entry := range t.state.Pending {
			if len(entry.TaskId) != 0 {
				continue
			}
			if err := t.schedule(ctx, entry); err != nil {
				return err
			}
		}
		if err := execCtx.SaveState(ctx); err != nil {
			return err
		}
		var skipped uint64
		for _, entry := range t.state.Pending {
			_, err := t.scheduler.WaitTask(ctx, execCtx, entry.TaskId)
			ready, completed, statusErr := t.resourceStatus(ctx, entry.ResourceId)
			if statusErr != nil {
				return statusErr
			}
			if !ready {
				// A resource deleted during the run is no longer a backfill target.
				// Wait for cleanup before counting it as skipped.
				if err := t.scheduler.WaitTaskEnded(ctx, entry.TaskId); err != nil {
					return err
				}
				skipped++
				continue
			}
			if err != nil {
				return err
			}
			if !completed {
				return errors.NewNonRetriableErrorf("backup of %v finished without a success marker", entry.ResourceId)
			}
		}
		t.state.Processed += uint64(len(t.state.Pending))
		t.state.Skipped += skipped
		t.state.AfterId = t.state.Pending[len(t.state.Pending)-1].ResourceId
		t.state.Pending = nil
		if err := execCtx.SaveState(ctx); err != nil {
			return err
		}
	}
}

func (t *backfillTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	// Stop scanning. Already enqueued resource backups keep their independent
	// lifecycle, including copies shared with another backfill or admin request.
	return nil
}

func (t *backfillTask) GetMetadata(ctx context.Context) (proto.Message, error) {
	return t.GetResponse(), nil
}

func (t *backfillTask) GetResponse() proto.Message {
	return &protos.BackfillProgress{
		AfterId: t.state.AfterId, Processed: t.state.Processed,
		Skipped: t.state.Skipped, Pending: uint32(len(t.state.Pending)),
	}
}

////////////////////////////////////////////////////////////////////////////////

func (t *backfillTask) schedule(ctx context.Context, entry *protos.BackfillEntry) error {
	var taskType, key string
	var request proto.Message
	if t.request.ResourceKind == "snapshots" {
		taskType = "snapshots.BackupSnapshot"
		key = fmt.Sprintf("backup_snapshot_%v", entry.ResourceId)
		request = &snapshot_protos.BackupSnapshotRequest{SnapshotId: entry.ResourceId, BackupId: entry.BackupId}
	} else {
		taskType = "images.BackupImage"
		key = fmt.Sprintf("backup_image_%v", entry.ResourceId)
		request = &image_protos.BackupImageRequest{ImageId: entry.ResourceId, BackupId: entry.BackupId}
	}
	if len(entry.BackupId) != 0 {
		key += "_" + entry.BackupId
	}
	id, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, key),
		taskType,
		"",
		request,
	)
	if err != nil {
		return err
	}
	entry.TaskId = id
	return nil
}

func (t *backfillTask) resourceStatus(
	ctx context.Context,
	id string,
) (ready, completed bool, err error) {

	if t.request.ResourceKind == "snapshots" {
		meta, err := t.storage.GetSnapshotMeta(ctx, id)
		if err != nil || meta == nil {
			return false, false, err
		}
		return meta.Ready, meta.BackupCompleted, nil
	}
	meta, err := t.storage.GetImageMeta(ctx, id)
	if err != nil || meta == nil {
		return false, false, err
	}
	return meta.Ready, meta.BackupCompleted, nil
}
