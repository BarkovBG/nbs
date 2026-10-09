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

	if t.state.Finished {
		return retrySnapshotBackupError(t.finish(ctx, execCtx))
	}

	err := t.holdSource(ctx, execCtx)
	if err != nil {
		// OwnerId means CP has already accepted and retained the source.
		if t.request.OwnerId != "" {
			return retrySnapshotBackupError(err)
		}
		return err
	}

	// Once accepted, only success or explicit cancellation releases data.
	return retrySnapshotBackupError(t.backupHeldSource(ctx, execCtx))
}

func (t *backupSnapshotDataTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return retrySnapshotBackupError(t.finish(ctx, execCtx))
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

func (t *backupSnapshotDataTask) holdSource(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.backupS3.CheckEncryptedDEK(t.request.EncryptedDek)
	if err != nil {
		return err
	}
	return t.storage.HoldSnapshotForBackup(
		ctx,
		t.request.SnapshotId,
		t.ownerID(execCtx),
	)
}

func (t *backupSnapshotDataTask) backupHeldSource(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	meta, err := t.storage.CheckSnapshotReady(ctx, t.request.SnapshotId)
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
			t.request.EncryptedDek,
			persistence.S3Object{Data: t.request.Meta},
		)
		if err != nil {
			return err
		}
	}

	// Persist success before release so a restarted task only cleans up.
	t.state.Finished = true
	err = execCtx.SaveState(ctx)
	if err != nil {
		return err
	}
	return t.finish(ctx, execCtx)
}

func (t *backupSnapshotDataTask) finish(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	taskID := t.ownerID(execCtx)
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
		err := t.storage.EnqueueBackupChunksForOwner(
			ctx,
			t.request.SnapshotId,
			t.ownerID(execCtx),
			batch,
		)
		if err != nil {
			return err
		}
	}

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

		// Copy every referenced nonzero chunk through the held source.
		if len(entry.ChunkID) == 0 {
			continue
		}

		batch = append(batch, storage.BackupChunkQueueEntry{
			SnapshotID:   snapshotID,
			ChunkID:      entry.ChunkID,
			StoredInS3:   entry.StoredInS3,
			EncryptedDEK: t.request.EncryptedDek,
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

	err := t.storage.CheckBackupChunksCompleted(ctx, snapshotID)
	if errors.Is(err, errors.NewInterruptExecutionError()) {
		logging.Debug(ctx, "Backup of snapshot with id %v is waiting for its chunks to finish backing up", snapshotID)
	}
	return err
}

func (t *backupSnapshotDataTask) backupChunkMap(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	meta storage.SnapshotMeta,
) error {

	if t.state.ChunkMapBackedUp {
		return nil
	}
	if err := t.waitForChunksBackupCompleted(ctx, meta.ID); err != nil {
		if errors.Is(err, errors.NewEmptyNonRetriableError()) {
			t.state.MilestoneChunkIndex = 0
			if saveErr := execCtx.SaveState(ctx); saveErr != nil {
				return saveErr
			}
		}
		return err
	}
	chunkMap, err := t.buildChunkMap(ctx, execCtx, meta)
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
		t.request.EncryptedDek,
		persistence.S3Object{Data: data},
	)
	if err != nil {
		return err
	}
	t.state.ChunkMapBackedUp = true
	return execCtx.SaveState(ctx)
}

func (t *backupSnapshotDataTask) ownerID(execCtx tasks.ExecutionContext) string {
	if t.request.OwnerId != "" {
		return t.request.OwnerId
	}
	return execCtx.GetTaskID()
}

func retrySnapshotBackupError(err error) error {
	if err == nil || errors.Is(err, errors.NewInterruptExecutionError()) || errors.Is(err, errors.NewWrongGenerationError()) || errors.Is(err, context.Canceled) {
		return err
	}
	// A typed non-retriable cause would still trigger runner cancellation.
	return errors.NewRetriableErrorWithIgnoreRetryLimitf("snapshot backup waits for recovery: %v", err)
}

// The source stays held until all queued chunks have completed and the full
// map is published. Dependency ordering never changes the source map.
func (t *backupSnapshotDataTask) buildChunkMap(
	ctx context.Context, execCtx tasks.ExecutionContext, meta storage.SnapshotMeta,
) (*protos.BackupChunkMap, error) {
	owner := t.ownerID(execCtx)
	if err := t.storage.CheckSnapshotBackupOwner(ctx, meta.ID, owner); err != nil {
		return nil, err
	}
	chunkMap := &protos.BackupChunkMap{ChunkIds: make([]string, meta.ChunkCount)}
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	entries, readErrors := t.storage.ReadChunkMap(readCtx, meta.ID, 0)
	for entry := range entries {
		if err := validateChunkMapEntry(entry, meta.ID, meta.ChunkCount); err != nil {
			return nil, err
		}
		chunkMap.ChunkIds[entry.ChunkIndex] = entry.ChunkID
	}
	if err := <-readErrors; err != nil {
		return nil, err
	}
	if err := t.storage.CheckSnapshotBackupOwner(ctx, meta.ID, owner); err != nil {
		return nil, err
	}
	return chunkMap, nil
}
