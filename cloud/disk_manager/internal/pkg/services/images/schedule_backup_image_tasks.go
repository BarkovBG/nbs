package images

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type scheduleBackupImageTasks struct {
	scheduler tasks.Scheduler
	storage   resources.Storage
	limit     int
}

func (t *scheduleBackupImageTasks) Save() ([]byte, error) {
	return nil, nil
}

func (t *scheduleBackupImageTasks) Load(_, _ []byte) error {
	return nil
}

func (t *scheduleBackupImageTasks) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	backups, err := t.storage.ListImagesToBackup(ctx, t.limit)
	if err != nil {
		return err
	}

	for _, backup := range backups {
		// Preserve the task key for legacy queue entries. New attempts need a
		// distinct key so they do not reuse a previously cancelled task.
		idempotencyKey := fmt.Sprintf("backup_image_%v", backup.ImageID)
		if len(backup.BackupID) != 0 {
			idempotencyKey += "_" + backup.BackupID
		}

		_, err := t.scheduler.ScheduleTask(
			headers.SetIncomingIdempotencyKey(ctx, idempotencyKey),
			"images.BackupImage",
			"",
			&protos.BackupImageRequest{
				ImageId:  backup.ImageID,
				BackupId: backup.BackupID,
			},
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *scheduleBackupImageTasks) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *scheduleBackupImageTasks) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *scheduleBackupImageTasks) GetResponse() proto.Message {
	return &empty.Empty{}
}
