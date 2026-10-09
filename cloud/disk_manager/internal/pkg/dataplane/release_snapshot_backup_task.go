package dataplane

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

type releaseSnapshotBackupTask struct {
	storage storage.Storage
	request *protos.ReleaseSnapshotBackupRequest
}

func (t *releaseSnapshotBackupTask) Save() ([]byte, error) { return nil, nil }

func (t *releaseSnapshotBackupTask) Load(request, _ []byte) error {
	t.request = &protos.ReleaseSnapshotBackupRequest{}
	return proto.Unmarshal(request, t.request)
}

func (t *releaseSnapshotBackupTask) Run(ctx context.Context, execCtx tasks.ExecutionContext) error {
	if t.request.SnapshotId == "" || t.request.OwnerId == "" {
		return errors.NewNonRetriableErrorf("invalid snapshot backup release")
	}
	if err := t.storage.DrainSnapshotBackup(ctx, t.request.SnapshotId, t.request.OwnerId, 1000); err != nil {
		return err
	}
	return t.storage.ReleaseSnapshotForBackup(ctx, t.request.SnapshotId, t.request.OwnerId)
}

func (t *releaseSnapshotBackupTask) Cancel(ctx context.Context, execCtx tasks.ExecutionContext) error {
	if err := t.Run(ctx, execCtx); err != nil {
		return retrySnapshotBackupError(err)
	}
	return nil
}

func (t *releaseSnapshotBackupTask) GetMetadata(ctx context.Context) (proto.Message, error) {
	return &empty.Empty{}, nil
}

func (t *releaseSnapshotBackupTask) GetResponse() proto.Message { return &empty.Empty{} }
