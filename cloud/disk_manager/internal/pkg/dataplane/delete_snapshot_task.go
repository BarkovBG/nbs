package dataplane

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type deleteSnapshotTask struct {
	storage    storage.Storage
	nbsFactory nbs.Factory
	request    *protos.DeleteSnapshotRequest
	state      *protos.DeleteSnapshotTaskState
}

func (t *deleteSnapshotTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *deleteSnapshotTask) Load(request, state []byte) error {
	t.request = &protos.DeleteSnapshotRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.DeleteSnapshotTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *deleteSnapshotTask) deletingSnapshot(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	snapshotMeta, err := t.storage.DeletingSnapshot(ctx, t.request.SnapshotId, execCtx.GetTaskID())
	if err != nil {
		return err
	}

	if len(snapshotMeta.CheckpointID) != 0 {
		err = t.deleteSourceCheckpoint(
			ctx,
			snapshotMeta.Disk,
			snapshotMeta.CheckpointID,
		)
		if err != nil {
			return err
		}
	}

	if len(snapshotMeta.BaseSnapshotID) != 0 {
		err := t.storage.UnlockSnapshot(
			ctx,
			snapshotMeta.BaseSnapshotID,
			snapshotMeta.CreateTaskID,
		)
		if err != nil {
			return err
		}
		logging.Info(
			ctx,
			"Unlocked snapshot with id %v",
			snapshotMeta.BaseSnapshotID,
		)
	}

	return nil
}

func (t *deleteSnapshotTask) deleteSourceCheckpoint(
	ctx context.Context,
	disk *types.Disk,
	checkpointID string,
) error {

	nbsClient, err := t.nbsFactory.GetClient(ctx, disk.ZoneId)
	if err != nil {
		return err
	}

	err = nbsClient.DeleteCheckpoint(ctx, disk.DiskId, checkpointID)
	if err == nil || nbs.IsNotFoundError(err) {
		return nil
	}

	// Images are deleted through this same task. Checkpoint deletion is retried
	// without a limit, so a source disk that is gone or cannot accept the
	// request blocks snapshot and image deletion.
	gone, checkErr := sourceDiskGoneOrUnavailable(ctx, nbsClient, disk.DiskId)
	if checkErr != nil || !gone {
		return err
	}

	logging.Warn(
		ctx,
		"Skipping deletion of checkpoint %v on disk %v, disk is gone or unavailable: %v",
		checkpointID,
		disk.DiskId,
		err,
	)
	return nil
}

func sourceDiskGoneOrUnavailable(
	ctx context.Context,
	nbsClient nbs.Client,
	diskID string,
) (bool, error) {

	states, err := nbsClient.ListDiskStates(ctx)
	if err != nil {
		return false, err
	}

	for _, state := range states {
		if state.DiskID != diskID {
			continue
		}

		switch state.State {
		case types.DiskState_DISK_STATE_TEMPORARILY_UNAVAILABLE,
			types.DiskState_DISK_STATE_ERROR:
			return true, nil
		default:
			return false, nil
		}
	}

	_, err = nbsClient.Describe(ctx, diskID)
	if nbs.IsNotFoundError(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}

	return false, nil
}

func (t *deleteSnapshotTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.deletingSnapshot(ctx, execCtx)
	if err != nil {
		return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
	}

	return nil
}

func (t *deleteSnapshotTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.deletingSnapshot(ctx, execCtx)
}

func (t *deleteSnapshotTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *deleteSnapshotTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
