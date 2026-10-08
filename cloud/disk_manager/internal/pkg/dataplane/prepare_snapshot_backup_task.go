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

// Preparation keeps its hold after success. The coordinator either transfers
// the same owner to the copy or explicitly releases it after excluding a resource.
type prepareSnapshotBackupTask struct {
	storage storage.Storage
	request *protos.PrepareSnapshotBackupRequest
	state   *protos.PrepareSnapshotBackupState
}

func (t *prepareSnapshotBackupTask) Save() ([]byte, error) { return proto.Marshal(t.state) }

func (t *prepareSnapshotBackupTask) Load(request, state []byte) error {
	t.request = &protos.PrepareSnapshotBackupRequest{}
	if err := proto.Unmarshal(request, t.request); err != nil {
		return err
	}
	t.state = &protos.PrepareSnapshotBackupState{}
	return proto.Unmarshal(state, t.state)
}

func (t *prepareSnapshotBackupTask) Run(ctx context.Context, execCtx tasks.ExecutionContext) (err error) {
	held := false
	defer func() {
		if held {
			err = retrySnapshotBackupError(err)
		}
	}()
	if t.state.Prepared {
		return nil
	}
	if t.request.SnapshotId == "" || t.request.OwnerId == "" {
		return errors.NewNonRetriableErrorf("invalid snapshot backup preparation")
	}
	if err := t.storage.HoldSnapshotForBackup(ctx, t.request.SnapshotId, t.request.OwnerId); err != nil {
		return err
	}
	held = true
	meta, err := t.storage.CheckSnapshotReady(ctx, t.request.SnapshotId)
	if err != nil {
		return err
	}
	t.state.BaseSnapshotId = meta.BaseSnapshotID
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	entries, readErrors := t.storage.ReadChunkMap(readCtx, meta.ID, t.state.NextChunkIndex)
	var page uint32
	for entry := range entries {
		if err := validateChunkMapEntry(entry, meta.ID, meta.ChunkCount); err != nil {
			return err
		}
		if entry.ChunkID != "" {
			if _, err := t.storage.ReadChunkBlob(ctx, entry.ChunkID, entry.StoredInS3); err != nil {
				return err
			}
		}
		t.state.NextChunkIndex = entry.ChunkIndex + 1
		page++
		if page == 32 {
			if err := execCtx.SaveState(ctx); err != nil {
				return err
			}
			page = 0
		}
	}
	if err := <-readErrors; err != nil {
		return err
	}
	t.state.Prepared = true
	return execCtx.SaveState(ctx)
}

func (t *prepareSnapshotBackupTask) Cancel(ctx context.Context, execCtx tasks.ExecutionContext) error {
	// The parent remains responsible for the CP barrier and for successful
	// preparations; a failed preparation can release only its own DP hold.
	if err := t.storage.ReleaseSnapshotForBackup(ctx, t.request.SnapshotId, t.request.OwnerId); err != nil {
		return errors.NewRetriableErrorf("failed to release snapshot preparation: %v", err)
	}
	return nil
}

func (t *prepareSnapshotBackupTask) GetMetadata(ctx context.Context) (proto.Message, error) {
	return t.GetResponse(), nil
}

func (t *prepareSnapshotBackupTask) GetResponse() proto.Message {
	return &protos.PrepareSnapshotBackupResponse{BaseSnapshotId: t.state.BaseSnapshotId}
}

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
