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

// The source is held by the caller's dataplane.LockSnapshotForBackup task for
// the whole copy, so this task takes no reference of its own.
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

	// Only success or explicit cancellation ends an accepted copy.
	return retrySnapshotBackupError(t.run(ctx, execCtx))
}

func (t *backupSnapshotDataTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return retrySnapshotBackupError(t.clearChunks(ctx))
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

func (t *backupSnapshotDataTask) run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if t.state.Finished {
		return t.clearChunks(ctx)
	}

	// A DEK that cannot be opened fails the chunk copier on every
	// chunk and leaves those queue rows unfinished, so this task
	// interrupts forever. Reject it before anything is enqueued.
	err := t.backupS3.CheckEncryptedDEK(t.request.EncryptedDek)
	if err != nil {
		return err
	}

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

	// Persist success before cleanup so a restarted task only cleans up.
	t.state.Finished = true
	err = execCtx.SaveState(ctx)
	if err != nil {
		return err
	}

	return t.clearChunks(ctx)
}

// Removes the queued and completed chunk entries of the snapshot: the copy is
// complete, or cancelled and must be copied again by its next attempt.
func (t *backupSnapshotDataTask) clearChunks(ctx context.Context) error {
	for {
		cleared, err := t.storage.ClearBackupChunks(
			ctx,
			t.request.SnapshotId,
			t.batchSize,
		)
		if err != nil {
			return err
		}

		// The storage clears fewer than batchSize entries only when no
		// entries are left.
		if cleared < t.batchSize {
			return nil
		}
	}
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

		// Completed creators already have their entire copy in the follower.
		// Missing/deleting creators without that confirmation are copied here.
		if len(entry.ChunkID) == 0 || t.creatorCompleted(entry.ChunkID) {
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

func (t *backupSnapshotDataTask) creatorCompleted(chunkID string) bool {
	creator := storage.GetSnapshotIDFromChunkID(chunkID)
	if creator == "" || creator == t.request.SnapshotId {
		return false
	}

	for _, id := range t.request.CompletedSnapshotIds {
		if creator == id {
			return true
		}
	}

	return false
}

func (t *backupSnapshotDataTask) waitForChunksBackupCompleted(
	ctx context.Context,
	snapshotID string,
) error {

	err := t.storage.CheckBackupChunksCompleted(ctx, snapshotID)
	if errors.Is(err, errors.NewInterruptExecutionError()) {
		logging.Debug(
			ctx,
			"Backup of snapshot with id %v is waiting for its chunks to finish backing up",
			snapshotID,
		)
	}

	return err
}

// The full map is published only after every queued chunk has completed.
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
		t.request.EncryptedDek,
		persistence.S3Object{Data: data},
	)
	if err != nil {
		return err
	}

	t.state.ChunkMapBackedUp = true
	return execCtx.SaveState(ctx)
}

func retrySnapshotBackupError(err error) error {
	if err == nil ||
		errors.Is(err, errors.NewInterruptExecutionError()) ||
		errors.Is(err, errors.NewWrongGenerationError()) ||
		errors.Is(err, context.Canceled) {

		return err
	}

	// A typed non-retriable cause would still trigger runner cancellation.
	return errors.NewRetriableErrorWithIgnoreRetryLimitf(
		"snapshot backup waits for recovery: %v",
		err,
	)
}
