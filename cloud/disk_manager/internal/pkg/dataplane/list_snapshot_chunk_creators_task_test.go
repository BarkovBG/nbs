package dataplane

import (
	"testing"

	"github.com/stretchr/testify/require"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func listChunkCreators(
	t *testing.T,
	storage snapshot_storage.Storage,
	snapshotID string,
) []string {

	ctx := test.NewContext()
	execCtx := mocks.NewExecutionContextMock()
	execCtx.On("SaveState", ctx).Return(nil).Maybe()

	task := &listSnapshotChunkCreatorsTask{
		storage: storage,
		request: &protos.ListSnapshotChunkCreatorsRequest{SnapshotId: snapshotID},
		state:   &protos.ListSnapshotChunkCreatorsTaskState{},
	}
	require.NoError(t, task.Run(ctx, execCtx))

	return task.GetResponse().(*protos.ListSnapshotChunkCreatorsResponse).SnapshotIds
}

////////////////////////////////////////////////////////////////////////////////

func TestListSnapshotChunkCreatorsTask(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	chunkA := createSnapshotWithChunk(t, ctx, storage, "A")
	chunkB := createSnapshotWithChunk(t, ctx, storage, "B")

	// C uses chunks of A and B and creates one of its own.
	_, err := storage.CreateSnapshot(ctx, snapshot_storage.SnapshotMeta{ID: "C"})
	require.NoError(t, err)
	for index, chunkID := range []string{chunkA, chunkB, chunkA} {
		err = storage.ShallowCopyChunk(
			ctx,
			snapshot_storage.ChunkMapEntry{
				ChunkIndex: uint32(index),
				ChunkID:    chunkID,
				StoredInS3: true,
			},
			"C",
		)
		require.NoError(t, err)
	}
	_, err = storage.WriteChunk(
		ctx,
		"task",
		"C",
		dataplane_common.Chunk{Index: 3, Data: []byte("own")},
		true, // useS3
	)
	require.NoError(t, err)
	require.NoError(t, storage.SnapshotCreated(ctx, "C", 16384, 16384, 5, nil))

	require.Equal(t, []string{"A", "B"}, listChunkCreators(t, storage, "C"))
	require.Empty(t, listChunkCreators(t, storage, "A"))

	// A deleted creator is still listed: its chunks are kept by C.
	finishSnapshotDeletion(t, ctx, storage, "A")
	require.Equal(t, []string{"A", "B"}, listChunkCreators(t, storage, "C"))
}

func TestListSnapshotChunkCreatorsTaskReturnsNothingForUnavailableSnapshot(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	require.Empty(t, listChunkCreators(t, storage, "missing"))

	createSnapshotWithChunk(t, ctx, storage, "deleting")
	_, err := storage.DeletingSnapshot(ctx, "deleting", "delete")
	require.NoError(t, err)
	require.Empty(t, listChunkCreators(t, storage, "deleting"))
}
