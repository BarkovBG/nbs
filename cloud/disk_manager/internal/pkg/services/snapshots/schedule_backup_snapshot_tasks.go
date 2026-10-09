package snapshots

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type scheduleBackupSnapshotTasks struct {
	scheduler tasks.Scheduler
	storage   resources.Storage
	limit     int
}

func (t *scheduleBackupSnapshotTasks) Save() ([]byte, error) {
	return nil, nil
}

func (t *scheduleBackupSnapshotTasks) Load(_, _ []byte) error {
	return nil
}

// Graphs run one at a time: the queue row of a finished backup is removed by
// its task, so each run resumes with the next request. Intersecting graphs
// of concurrent tasks would otherwise copy the same sources twice.
func (t *scheduleBackupSnapshotTasks) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	backups, err := t.storage.ListSnapshotsToBackup(ctx, t.limit)
	if err != nil {
		return err
	}

	for _, item := range backups {
		idempotencyKey := fmt.Sprintf(
			"backup_snapshot_%v_%v",
			item.SnapshotID,
			item.BackupID,
		)

		taskID, err := t.scheduler.ScheduleTask(
			headers.SetIncomingIdempotencyKey(ctx, idempotencyKey),
			"snapshots.BackupSnapshot",
			"",
			&protos.BackupSnapshotRequest{
				SnapshotId: item.SnapshotID,
				BackupId:   item.BackupID,
			},
		)
		if err != nil {
			return err
		}

		_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *scheduleBackupSnapshotTasks) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *scheduleBackupSnapshotTasks) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *scheduleBackupSnapshotTasks) GetResponse() proto.Message {
	return &empty.Empty{}
}
