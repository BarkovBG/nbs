package dataplane

import (
	"context"
	"math/rand"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"golang.org/x/sync/errgroup"
)

////////////////////////////////////////////////////////////////////////////////

const backupChunkQueueWindowSize = 1000

////////////////////////////////////////////////////////////////////////////////

type backupChunksTask struct {
	storage       storage.Storage
	backupS3      *backup.S3
	batchSize     int
	inflightLimit int
	registry      metrics.Registry
	state         *protos.BackupChunksTaskState
}

func (t *backupChunksTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *backupChunksTask) Load(_, state []byte) error {
	t.state = &protos.BackupChunksTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *backupChunksTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	// A restarted execution has no local IO from its previous batch. Clear
	// claims left by an interrupted invocation before starting another batch.
	taskID := execCtx.GetTaskID()
	err := t.storage.ReleaseBackupChunkClaims(ctx, taskID)
	if err != nil {
		return err
	}

	for {
		entries, err := t.storage.GetQueuedChunksToBackup(
			ctx,
			backupChunkQueueWindowSize,
		)
		if err != nil {
			return err
		}

		if len(entries) == 0 {
			return errors.NewInterruptExecutionError()
		}

		rand.Shuffle(len(entries), func(i, j int) {
			entries[i], entries[j] = entries[j], entries[i]
		})
		entries = entries[:min(len(entries), t.batchSize)]

		entries, err = t.storage.ClaimBackupChunks(ctx, taskID, entries)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			continue
		}

		copied, copyErr := t.copyChunks(ctx, entries)
		if len(copied) != 0 {
			err = t.storage.ChunksBackupCompleted(ctx, copied)
			if err != nil {
				return err
			}
		}

		// copyChunks joins every goroutine, including on errors. Retain the
		// claims until both object writes and completion updates have stopped.
		err = t.storage.ReleaseBackupChunkClaims(ctx, taskID)
		if err != nil {
			return err
		}
		if len(copied) == 0 {
			return copyErr
		}
	}
}

func (t *backupChunksTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) (err error) {

	// Cancellation must not be marked complete while cleanup has failed.
	defer func() {
		if err != nil && !errors.Is(err, errors.NewInterruptExecutionError()) {
			err = errors.NewRetriableErrorf("failed to cancel backup worker: %v", err)
		}
	}()

	return t.storage.ReleaseBackupChunkClaims(ctx, execCtx.GetTaskID())
}

func (t *backupChunksTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *backupChunksTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *backupChunksTask) copyChunk(
	ctx context.Context,
	entry storage.BackupChunkQueueEntry,
) error {

	exists, err := t.backupS3.ObjectExists(ctx, backup.ChunkKey(entry.ChunkID))
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	chunkBlob, err := t.storage.ReadChunkBlob(
		ctx,
		entry.ChunkID,
		entry.StoredInS3,
	)
	if err != nil {
		return err
	}

	err = t.backupS3.PutObject(
		ctx,
		backup.ChunkKey(entry.ChunkID),
		chunks.NewS3Object(chunkBlob),
	)
	if err != nil {
		return err
	}

	t.registry.Counter("backup/copiedChunks").Inc()
	t.registry.Counter("backup/copiedBytes").Add(int64(len(chunkBlob.Data)))
	return nil
}

// Returns the copied chunks and the first error of the ones that failed.
func (t *backupChunksTask) copyChunks(
	ctx context.Context,
	entries []storage.BackupChunkQueueEntry,
) ([]storage.BackupChunkQueueEntry, error) {

	group, groupCtx := errgroup.WithContext(ctx)
	queue := make(chan storage.BackupChunkQueueEntry)
	copiedEntries := make(chan storage.BackupChunkQueueEntry, len(entries))
	copyErrors := make(chan error, len(entries))

	group.Go(func() error {
		defer close(queue)

		for _, entry := range entries {
			select {
			case queue <- entry:
			case <-groupCtx.Done():
				return groupCtx.Err()
			}
		}

		return nil
	})

	for i := 0; i < t.inflightLimit; i++ {
		group.Go(func() error {
			for entry := range queue {
				if groupCtx.Err() != nil {
					return groupCtx.Err()
				}

				err := t.copyChunk(groupCtx, entry)
				if err != nil {
					logging.Warn(
						groupCtx,
						"Failed to copy chunk %v of snapshot %v, will retry: %v",
						entry.ChunkID,
						entry.SnapshotID,
						err,
					)
					copyErrors <- err
					continue
				}

				copiedEntries <- entry
			}

			return nil
		})
	}

	err := group.Wait()
	if err != nil {
		return nil, err
	}

	close(copiedEntries)
	close(copyErrors)

	var copied []storage.BackupChunkQueueEntry
	for entry := range copiedEntries {
		copied = append(copied, entry)
	}

	return copied, <-copyErrors
}
