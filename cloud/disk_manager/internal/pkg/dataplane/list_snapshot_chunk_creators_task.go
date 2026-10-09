package dataplane

import (
	"context"
	"sort"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

// Lists the snapshots that created chunks of the snapshot. A chunk ID names its
// creator, so the chunk map of the snapshot alone gives every creator. Takes no
// lock: the result only orders copies, and each copy locks its own snapshot.
type listSnapshotChunkCreatorsTask struct {
	storage storage.Storage
	request *protos.ListSnapshotChunkCreatorsRequest
	state   *protos.ListSnapshotChunkCreatorsTaskState
}

func (t *listSnapshotChunkCreatorsTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *listSnapshotChunkCreatorsTask) Load(request, state []byte) error {
	t.request = &protos.ListSnapshotChunkCreatorsRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.ListSnapshotChunkCreatorsTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *listSnapshotChunkCreatorsTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	meta, err := t.storage.GetSnapshotMeta(ctx, t.request.SnapshotId)
	if err != nil {
		return err
	}

	if meta == nil || !meta.Ready {
		return nil
	}

	t.state.SnapshotIds, err = t.listChunkCreators(ctx, meta)
	if err != nil {
		return err
	}

	return execCtx.SaveState(ctx)
}

func (t *listSnapshotChunkCreatorsTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *listSnapshotChunkCreatorsTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *listSnapshotChunkCreatorsTask) GetResponse() proto.Message {
	return &protos.ListSnapshotChunkCreatorsResponse{
		SnapshotIds: t.state.SnapshotIds,
	}
}

////////////////////////////////////////////////////////////////////////////////

func (t *listSnapshotChunkCreatorsTask) listChunkCreators(
	ctx context.Context,
	meta *storage.SnapshotMeta,
) ([]string, error) {

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	entries, entriesErrors := t.storage.ReadChunkMap(
		readCtx,
		meta.ID,
		0, // milestoneChunkIndex
	)

	creators := make(map[string]struct{})
	for entry := range entries {
		err := validateChunkMapEntry(entry, meta.ID, meta.ChunkCount)
		if err != nil {
			return nil, err
		}

		if len(entry.ChunkID) == 0 {
			continue
		}

		creator := storage.GetSnapshotIDFromChunkID(entry.ChunkID)
		if len(creator) == 0 {
			return nil, errors.NewNonRetriableErrorf(
				"unknown creator of chunk %v",
				entry.ChunkID,
			)
		}

		if creator != meta.ID {
			creators[creator] = struct{}{}
		}
	}

	err := <-entriesErrors
	if err != nil {
		return nil, err
	}

	ids := make([]string, 0, len(creators))
	for id := range creators {
		ids = append(ids, id)
	}

	sort.Strings(ids)
	return ids, nil
}
