package dataplane

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type backupSnapshotDataTask struct {
	storage   storage.Storage
	backupS3  *backup.S3
	batchSize int
	request   *protos.BackupSnapshotDataRequest
	state     *protos.BackupSnapshotDataTaskState
}

func (t *backupSnapshotDataTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *backupSnapshotDataTask) Load(request, state []byte) error {
	t.request = &protos.BackupSnapshotDataRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.BackupSnapshotDataTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *backupSnapshotDataTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	snapshotID := t.request.SnapshotId

	// Persist completion before releasing the hold: a retry after release
	// must not reacquire it or write objects after snapshot deletion started.
	if t.state.Finished {
		return t.finish(ctx, execCtx)
	}

	err := t.storage.HoldSnapshotForBackup(ctx, snapshotID, execCtx.GetTaskID())
	if err != nil {
		return err
	}

	meta, err := t.storage.CheckSnapshotReady(ctx, snapshotID)
	if err != nil {
		return err
	}

	t.state.ChunkCount = meta.ChunkCount

	err = t.enqueueChunks(ctx, execCtx)
	if err != nil {
		return err
	}

	err = t.backupChunkMap(ctx, execCtx, meta)
	if err != nil {
		return err
	}

	if len(t.request.MetaKey) != 0 {
		err = t.backupS3.PutObject(
			ctx,
			t.request.MetaKey,
			persistence.S3Object{Data: t.request.Meta},
		)
		if err != nil {
			return err
		}
	}

	t.state.Finished = true
	err = execCtx.SaveState(ctx)
	if err != nil {
		return err
	}
	return t.finish(ctx, execCtx)
}

func (t *backupSnapshotDataTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) (err error) {

	// Cancellation must not be marked complete while cleanup has failed.
	defer func() {
		if err != nil && !errors.Is(err, errors.NewInterruptExecutionError()) {
			err = errors.NewRetriableErrorf("failed to cancel snapshot backup: %v", err)
		}
	}()

	// Run has stopped enqueueing. Drain also prevents workers with stale
	// queue listings from starting new IO for this attempt.
	return t.finish(ctx, execCtx)
}

func (t *backupSnapshotDataTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *backupSnapshotDataTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *backupSnapshotDataTask) finish(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	taskID := execCtx.GetTaskID()
	err := t.storage.DrainSnapshotBackup(
		ctx,
		t.request.SnapshotId,
		taskID,
		t.batchSize,
	)
	if err != nil {
		return err
	}
	return t.storage.ReleaseSnapshotForBackup(ctx, t.request.SnapshotId, taskID)
}

func validateChunkMapEntry(
	entry storage.ChunkMapEntry,
	snapshotID string,
	chunkCount uint32,
) error {

	if entry.ChunkIndex >= chunkCount {
		return errors.NewNonRetriableErrorf(
			"chunk index %v of snapshot %v is out of range %v",
			entry.ChunkIndex,
			snapshotID,
			chunkCount,
		)
	}

	return nil
}

func (t *backupSnapshotDataTask) enqueueBatch(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	batch []storage.BackupChunkQueueEntry,
	milestoneChunkIndex uint32,
) error {

	if len(batch) != 0 {
		err := t.storage.EnqueueBackupChunks(
			ctx,
			t.request.SnapshotId,
			batch,
		)
		if err != nil {
			return err
		}
	}

	t.state.EnqueuedChunkCount += uint32(len(batch))
	t.state.MilestoneChunkIndex = milestoneChunkIndex
	return execCtx.SaveState(ctx)
}

func (t *backupSnapshotDataTask) enqueueChunks(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	snapshotID := t.request.SnapshotId

	if t.state.MilestoneChunkIndex >= t.state.ChunkCount {
		return nil
	}

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	entries, entriesErrors := t.storage.ReadChunkMap(
		readCtx,
		snapshotID,
		t.state.MilestoneChunkIndex,
	)

	var batch []storage.BackupChunkQueueEntry
	var milestoneChunkIndex uint32

	for entry := range entries {
		milestoneChunkIndex = entry.ChunkIndex + 1

		err := validateChunkMapEntry(entry, snapshotID, t.state.ChunkCount)
		if err != nil {
			return err
		}

		// Zero chunks have no data, and the chunks shallow copied from another
		// snapshot (this snapshot only references them) are copied when that
		// snapshot is backed up.
		if !storage.IsChunkCreatedBySnapshot(entry.ChunkID, snapshotID) {
			continue
		}

		batch = append(batch, storage.BackupChunkQueueEntry{
			SnapshotID: snapshotID,
			ChunkID:    entry.ChunkID,
			StoredInS3: entry.StoredInS3,
		})

		if len(batch) >= t.batchSize {
			err = t.enqueueBatch(ctx, execCtx, batch, milestoneChunkIndex)
			if err != nil {
				return err
			}

			batch = nil
		}
	}

	err := <-entriesErrors
	if err != nil {
		return err
	}

	return t.enqueueBatch(ctx, execCtx, batch, t.state.ChunkCount)
}

func (t *backupSnapshotDataTask) waitForChunksBackupCompleted(
	ctx context.Context,
	snapshotID string,
) error {

	completed, err := t.storage.GetBackedUpChunkCount(ctx, snapshotID)
	if err != nil {
		return err
	}

	if completed < uint64(t.state.EnqueuedChunkCount) {
		logging.Debug(
			ctx,
			"Backup of snapshot with id %v is waiting for its chunks to finish backing up",
			snapshotID,
		)
		return errors.NewInterruptExecutionError()
	}

	return nil
}

func (t *backupSnapshotDataTask) backupChunkMap(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	meta storage.SnapshotMeta,
) error {

	if t.state.ChunkMapBackedUp {
		return nil
	}

	err := t.waitForChunksBackupCompleted(ctx, meta.ID)
	if err != nil {
		return err
	}

	chunkMap := &protos.BackupChunkMap{
		ChunkIds: make([]string, meta.ChunkCount),
	}

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	entries, entriesErrors := t.storage.ReadChunkMap(
		readCtx,
		meta.ID,
		0, // milestoneChunkIndex
	)
	for entry := range entries {
		err := validateChunkMapEntry(entry, meta.ID, meta.ChunkCount)
		if err != nil {
			return err
		}

		chunkMap.ChunkIds[entry.ChunkIndex] = entry.ChunkID
	}

	err = <-entriesErrors
	if err != nil {
		return err
	}

	data, err := proto.Marshal(chunkMap)
	if err != nil {
		return errors.NewNonRetriableError(err)
	}

	err = t.backupS3.PutObject(
		ctx,
		backup.ChunkMapKey(meta.ID),
		persistence.S3Object{Data: data},
	)
	if err != nil {
		return err
	}

	t.state.ChunkMapBackedUp = true
	return execCtx.SaveState(ctx)
}
