package dataplane

import (
	"bytes"
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks"
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
) (err error) {
	held := false
	defer func() {
		if held || t.request.OwnerId != "" || t.state.Finished {
			err = retrySnapshotBackupError(err)
		}
	}()

	// A DEK that cannot be opened fails the chunk copier on every
	// chunk and leaves those queue rows unfinished, so this task
	// interrupts forever. Reject it before anything is enqueued.
	err = t.backupS3.CheckEncryptedDEK(t.request.EncryptedDek)
	if err != nil {
		return err
	}

	snapshotID := t.request.SnapshotId

	// Persist completion before releasing the hold: a retry after release
	// must not reacquire it or write objects after snapshot deletion started.
	if t.state.Finished {
		return t.finish(ctx, execCtx)
	}

	err = t.storage.HoldSnapshotForBackup(ctx, snapshotID, t.ownerID(execCtx))
	if err != nil {
		return err
	}

	held = true
	meta, err := t.storage.CheckSnapshotReady(ctx, snapshotID)
	if err != nil {
		return err
	}

	t.state.ChunkCount = meta.ChunkCount

	err = t.prepareChunkMap(ctx, execCtx, meta)
	if err != nil {
		return err
	}

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

	err = t.backupS3.DeleteObject(ctx, backup.PendingChunkMapKey(snapshotID))
	if err != nil {
		return err
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

		// Every referenced chunk must exist in the follower, even if its
		// creator was never backed up or has already been deleted.
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

func (t *backupSnapshotDataTask) prepareChunkMap(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	meta storage.SnapshotMeta,
) error {

	if t.state.ChunkMapPrepared {
		return nil
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

	err := <-entriesErrors
	if err != nil {
		return err
	}

	data, err := proto.Marshal(chunkMap)
	if err != nil {
		return errors.NewNonRetriableError(err)
	}

	err = t.backupS3.PutObject(
		ctx,
		backup.PendingChunkMapKey(meta.ID),
		t.request.EncryptedDek,
		persistence.S3Object{Data: data},
	)
	if err != nil {
		return err
	}
	// Older task states may have skipped inherited chunks or already published
	// a map without verifying its objects. Restart their progress under the hold.
	t.state.MilestoneChunkIndex = 0
	t.state.ChunkMapBackedUp = false
	t.state.ChunkMapPrepared = true
	return execCtx.SaveState(ctx)
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
	object, err := t.backupS3.GetObject(ctx, backup.PendingChunkMapKey(meta.ID))
	if err != nil {
		return err
	}
	if err := t.verifyChunkMap(ctx, execCtx, meta, object); err != nil {
		return err
	}
	err = t.backupS3.PutObject(ctx, backup.ChunkMapKey(meta.ID), t.request.EncryptedDek, object)
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

// Status rows are progress, not proof of a complete copy. Before publication
// verify every nonzero map position and the actual encrypted target objects.
func (t *backupSnapshotDataTask) verifyChunkMap(ctx context.Context, execCtx tasks.ExecutionContext, meta storage.SnapshotMeta, object persistence.S3Object) error {
	owner := t.ownerID(execCtx)
	if err := t.storage.CheckSnapshotBackupOwner(ctx, meta.ID, owner); err != nil {
		return err
	}
	chunkMap := &protos.BackupChunkMap{}
	if err := proto.Unmarshal(object.Data, chunkMap); err != nil {
		return errors.NewNonRetriableError(err)
	}
	if uint64(len(chunkMap.ChunkIds)) != uint64(meta.ChunkCount) {
		return errors.NewNonRetriableErrorf("backup map size mismatch for %v", meta.ID)
	}
	var expected, seen uint64
	requeued := false
	for _, id := range chunkMap.ChunkIds {
		if id != "" {
			expected++
		}
	}
	verified := make(map[string]struct{})
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	entries, readErrors := t.storage.ReadChunkMap(readCtx, meta.ID, 0)
	for entry := range entries {
		if err := validateChunkMapEntry(entry, meta.ID, meta.ChunkCount); err != nil {
			return err
		}
		if chunkMap.ChunkIds[entry.ChunkIndex] != entry.ChunkID {
			return errors.NewNonRetriableErrorf("source chunk map changed for %v", meta.ID)
		}
		if entry.ChunkID == "" {
			continue
		}
		seen++
		if _, ok := verified[entry.ChunkID]; ok {
			continue
		}
		blob, err := t.storage.ReadChunkBlob(ctx, entry.ChunkID, entry.StoredInS3)
		if err != nil {
			return err
		}
		target, targetErr := t.backupS3.GetObject(ctx, backup.ChunkKey(entry.ChunkID))
		if targetErr != nil && errors.Is(targetErr, errors.NewEmptyRetriableError()) {
			return targetErr
		}
		wanted := chunks.NewS3Object(blob)
		equal := targetErr == nil && bytes.Equal(wanted.Data, target.Data)
		for key, value := range wanted.Metadata {
			actual := target.Metadata[key]
			if value == nil || actual == nil || *actual != *value {
				equal = false
			}
		}
		if !equal {
			if err := t.storage.RequeueBackupChunk(ctx, owner, storage.BackupChunkQueueEntry{
				SnapshotID: meta.ID, ChunkID: entry.ChunkID, StoredInS3: entry.StoredInS3, EncryptedDEK: t.request.EncryptedDek,
			}); err != nil {
				return err
			}
			requeued = true
		}

		verified[entry.ChunkID] = struct{}{}
	}
	if err := <-readErrors; err != nil {
		return err
	}
	if seen != expected {
		return errors.NewNonRetriableErrorf("backup map of %v references missing source positions", meta.ID)
	}
	if err := t.storage.CheckSnapshotBackupOwner(ctx, meta.ID, owner); err != nil {
		return err
	}
	if requeued {
		return errors.NewInterruptExecutionError()
	}
	return nil
}
