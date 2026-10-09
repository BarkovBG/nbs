package dataplane

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	nbs_client "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func newDeleteSnapshotFixture(
	t *testing.T,
	meta *storage.SnapshotMeta,
) (
	context.Context,
	*deleteSnapshotTask,
	*nbs_mocks.ClientMock,
	*storage_mocks.StorageMock,
	*tasks_mocks.ExecutionContextMock,
) {

	ctx := newContext()
	nbsClient := nbs_mocks.NewClientMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	snapshotStorage := storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	nbsFactory.On("GetClient", ctx, meta.Disk.ZoneId).Return(nbsClient, nil).Maybe()
	execCtx.On("GetTaskID").Return("task")
	snapshotStorage.On(
		"DeletingSnapshot",
		ctx,
		"snapshot",
		"task",
	).Return(meta, nil)

	task := &deleteSnapshotTask{
		storage:    snapshotStorage,
		nbsFactory: nbsFactory,
		request: &protos.DeleteSnapshotRequest{
			SnapshotId: "snapshot",
		},
		state: &protos.DeleteSnapshotTaskState{},
	}

	t.Cleanup(func() {
		nbsClient.AssertExpectations(t)
		nbsFactory.AssertExpectations(t)
		snapshotStorage.AssertExpectations(t)
	})

	return ctx, task, nbsClient, snapshotStorage, execCtx
}

func snapshotMeta() *storage.SnapshotMeta {
	return &storage.SnapshotMeta{
		ID:             "snapshot",
		Disk:           &types.Disk{ZoneId: "zone", DiskId: "disk"},
		CheckpointID:   "checkpoint",
		CreateTaskID:   "create-task",
		BaseSnapshotID: "base",
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestDeleteSnapshotDeletesCheckpoint(t *testing.T) {
	ctx, task, nbsClient, snapshotStorage, execCtx := newDeleteSnapshotFixture(
		t,
		snapshotMeta(),
	)

	nbsClient.On("DeleteCheckpoint", ctx, "disk", "checkpoint").Return(nil)
	snapshotStorage.On("UnlockSnapshot", ctx, "base", "create-task").Return(nil)

	require.NoError(t, task.deletingSnapshot(ctx, execCtx))
}

func TestDeleteSnapshotSkipsCheckpointWhenDiskIsUnavailable(t *testing.T) {
	ctx, task, nbsClient, snapshotStorage, execCtx := newDeleteSnapshotFixture(
		t,
		snapshotMeta(),
	)

	nbsClient.On("DeleteCheckpoint", ctx, "disk", "checkpoint").
		Return(errors.New("disk unavailable"))
	nbsClient.On("ListDiskStates", ctx).Return([]nbs.DiskState{
		{
			DiskID: "other",
			State:  types.DiskState_DISK_STATE_ONLINE,
		},
		{
			DiskID: "disk",
			State:  types.DiskState_DISK_STATE_TEMPORARILY_UNAVAILABLE,
		},
	}, nil)
	snapshotStorage.On("UnlockSnapshot", ctx, "base", "create-task").Return(nil)

	require.NoError(t, task.deletingSnapshot(ctx, execCtx))
}

func TestDeleteSnapshotSkipsCheckpointWhenDiskIsInError(t *testing.T) {
	ctx, task, nbsClient, snapshotStorage, execCtx := newDeleteSnapshotFixture(
		t,
		snapshotMeta(),
	)

	nbsClient.On("DeleteCheckpoint", ctx, "disk", "checkpoint").
		Return(errors.New("disk unavailable"))
	nbsClient.On("ListDiskStates", ctx).Return([]nbs.DiskState{
		{
			DiskID: "disk",
			State:  types.DiskState_DISK_STATE_ERROR,
		},
	}, nil)
	snapshotStorage.On("UnlockSnapshot", ctx, "base", "create-task").Return(nil)

	require.NoError(t, task.deletingSnapshot(ctx, execCtx))
}

func TestDeleteSnapshotSkipsCheckpointWhenDiskIsGone(t *testing.T) {
	ctx, task, nbsClient, snapshotStorage, execCtx := newDeleteSnapshotFixture(
		t,
		snapshotMeta(),
	)

	nbsClient.On("DeleteCheckpoint", ctx, "disk", "checkpoint").
		Return(errors.New("timeout"))
	nbsClient.On("ListDiskStates", ctx).Return([]nbs.DiskState{}, nil)
	nbsClient.On("Describe", ctx, "disk").Return(
		nbs.DiskParams{},
		&nbs_client.ClientError{
			Code:    nbs_client.E_NOT_FOUND,
			Message: "path does not exist",
		},
	)
	snapshotStorage.On("UnlockSnapshot", ctx, "base", "create-task").Return(nil)

	require.NoError(t, task.deletingSnapshot(ctx, execCtx))
}

func TestDeleteSnapshotRetriesCheckpointWhenDiskIsOnline(t *testing.T) {
	ctx, task, nbsClient, _, execCtx := newDeleteSnapshotFixture(t, snapshotMeta())

	checkpointErr := errors.New("checkpoint delete failed")
	nbsClient.On("DeleteCheckpoint", ctx, "disk", "checkpoint").Return(checkpointErr)
	nbsClient.On("ListDiskStates", ctx).Return([]nbs.DiskState{
		{
			DiskID: "disk",
			State:  types.DiskState_DISK_STATE_ONLINE,
		},
	}, nil)

	require.ErrorIs(t, task.deletingSnapshot(ctx, execCtx), checkpointErr)
}

func TestDeleteSnapshotWithoutCheckpointDoesNotTouchDisk(t *testing.T) {
	meta := snapshotMeta()
	meta.CheckpointID = ""
	ctx, task, _, snapshotStorage, execCtx := newDeleteSnapshotFixture(t, meta)

	snapshotStorage.On("UnlockSnapshot", ctx, "base", "create-task").Return(nil)

	require.NoError(t, task.deletingSnapshot(ctx, execCtx))
}
