package dataplane

import (
	"context"
	"sort"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

type getSnapshotBackupDependenciesTask struct {
	storage storage.Storage
	request *protos.GetSnapshotBackupDependenciesRequest
	state   *protos.GetSnapshotBackupDependenciesState
}

func (t *getSnapshotBackupDependenciesTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *getSnapshotBackupDependenciesTask) Load(request, state []byte) error {
	t.request = &protos.GetSnapshotBackupDependenciesRequest{}
	if err := proto.Unmarshal(request, t.request); err != nil {
		return err
	}
	t.state = &protos.GetSnapshotBackupDependenciesState{}
	return proto.Unmarshal(state, t.state)
}

func (t *getSnapshotBackupDependenciesTask) Run(ctx context.Context, execCtx tasks.ExecutionContext) error {
	return retrySnapshotBackupError(t.run(ctx, execCtx))
}

func (t *getSnapshotBackupDependenciesTask) run(ctx context.Context, execCtx tasks.ExecutionContext) error {
	if !t.state.Finished {
		meta, err := t.storage.GetSnapshotMeta(ctx, t.request.SnapshotId)
		if err != nil {
			return err
		}
		if meta != nil && meta.Ready {
			err = t.storage.HoldSnapshotForBackup(ctx, meta.ID, execCtx.GetTaskID())
			if err != nil {
				// Delete may have won after the metadata read. In that case
				// there is no live dependency to plan; the descendant owns
				// the remaining chunk references and can copy them itself.
				latest, readErr := t.storage.GetSnapshotMeta(ctx, meta.ID)
				if readErr != nil {
					return readErr
				}
				if latest != nil && latest.Ready {
					return err
				}
			} else {
				ids, err := t.readDependencies(ctx, meta)
				if err != nil {
					return err
				}
				t.state.SnapshotIds = ids
			}
		}
		t.state.Finished = true
		if err := execCtx.SaveState(ctx); err != nil {
			return err
		}
	}
	return t.storage.ReleaseSnapshotForBackup(ctx, t.request.SnapshotId, execCtx.GetTaskID())
}

func (t *getSnapshotBackupDependenciesTask) readDependencies(ctx context.Context, meta *storage.SnapshotMeta) ([]string, error) {
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	entries, readErrors := t.storage.ReadChunkMap(readCtx, meta.ID, 0)
	ids := make(map[string]struct{})
	for entry := range entries {
		if err := validateChunkMapEntry(entry, meta.ID, meta.ChunkCount); err != nil {
			return nil, err
		}
		if entry.ChunkID == "" {
			continue
		}
		id := storage.GetSnapshotIDFromChunkID(entry.ChunkID)
		if id == "" {
			return nil, errors.NewNonRetriableErrorf("unknown creator of chunk %v", entry.ChunkID)
		}
		if id != meta.ID {
			ids[id] = struct{}{}
		}
	}
	if err := <-readErrors; err != nil {
		return nil, err
	}
	result := make([]string, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	sort.Strings(result)
	return result, nil
}

func (t *getSnapshotBackupDependenciesTask) Cancel(ctx context.Context, execCtx tasks.ExecutionContext) error {
	return retrySnapshotBackupError(t.storage.ReleaseSnapshotForBackup(ctx, t.request.SnapshotId, execCtx.GetTaskID()))
}

func (t *getSnapshotBackupDependenciesTask) GetMetadata(context.Context) (proto.Message, error) {
	return t.state, nil
}

func (t *getSnapshotBackupDependenciesTask) GetResponse() proto.Message {
	return &protos.GetSnapshotBackupDependenciesResponse{SnapshotIds: t.state.SnapshotIds}
}
