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

type lockSnapshotForBackupTask struct {
	storage storage.Storage
	request *protos.LockSnapshotForBackupRequest
	state   *protos.LockSnapshotForBackupState
}

func (t *lockSnapshotForBackupTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *lockSnapshotForBackupTask) Load(request, state []byte) error {
	t.request = &protos.LockSnapshotForBackupRequest{}
	if err := proto.Unmarshal(request, t.request); err != nil {
		return err
	}
	t.state = &protos.LockSnapshotForBackupState{}
	return proto.Unmarshal(state, t.state)
}

func (t *lockSnapshotForBackupTask) Run(ctx context.Context, execCtx tasks.ExecutionContext) error {
	if t.request.SnapshotId == "" || t.request.OwnerId == "" {
		return errors.NewNonRetriableErrorf("snapshot backup lock requires snapshot and owner IDs")
	}
	return retrySnapshotBackupError(t.run(ctx, execCtx))
}

func (t *lockSnapshotForBackupTask) run(ctx context.Context, execCtx tasks.ExecutionContext) error {
	// Release may arrive before the first execution or after a restart.
	if execCtx.HasEvent(ctx, int64(protos.SnapshotBackupLockEvent_RELEASE)) {
		return t.storage.ReleaseSnapshotForBackup(ctx, t.request.SnapshotId, t.request.OwnerId)
	}
	if !t.state.Ready {
		meta, err := t.storage.GetSnapshotMeta(ctx, t.request.SnapshotId)
		if err != nil {
			return err
		}
		if meta != nil && meta.Ready {
			err = t.storage.HoldSnapshotForBackup(ctx, meta.ID, t.request.OwnerId)
			if err != nil {
				// Admission races deletion in the same source-row transaction.
				// If deletion won, retained descendants copy the inherited chunks.
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
				t.state.Locked = true
			}
		}
		t.state.Ready = true
	}
	// Repeat persistence after a failed save in the same execution instance.
	if err := execCtx.SaveState(ctx); err != nil {
		return err
	}
	if t.state.Locked {
		// Metadata exposes the dependency list while the source stays locked.
		return errors.NewInterruptExecutionError()
	}
	return nil
}

func (t *lockSnapshotForBackupTask) readDependencies(ctx context.Context, meta *storage.SnapshotMeta) ([]string, error) {
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

func (t *lockSnapshotForBackupTask) Cancel(ctx context.Context, execCtx tasks.ExecutionContext) error {
	if t.request.OwnerId == "" {
		return nil
	}
	return retrySnapshotBackupError(t.storage.ReleaseSnapshotForBackup(ctx, t.request.SnapshotId, t.request.OwnerId))
}

func (t *lockSnapshotForBackupTask) GetMetadata(context.Context) (proto.Message, error) {
	return t.state, nil
}

func (t *lockSnapshotForBackupTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
