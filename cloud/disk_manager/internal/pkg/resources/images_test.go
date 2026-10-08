package resources

import (
	"context"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

func requireImagesAreEqual(t *testing.T, expected ImageMeta, actual ImageMeta) {
	require.Equal(t, expected.ID, actual.ID)
	require.Equal(t, expected.FolderID, actual.FolderID)
	require.True(t, proto.Equal(expected.CreateRequest, actual.CreateRequest))
	require.Equal(t, expected.CheckpointID, actual.CheckpointID)
	require.Equal(t, expected.CreateTaskID, actual.CreateTaskID)
	if !expected.CreatingAt.IsZero() {
		require.WithinDuration(t, expected.CreatingAt, actual.CreatingAt, time.Microsecond)
	}
	require.Equal(t, expected.CreatedBy, actual.CreatedBy)
	require.Equal(t, expected.DeleteTaskID, actual.DeleteTaskID)
	require.True(t, actual.UseDataplaneTasks)
	require.Equal(t, expected.Size, actual.Size)
	require.Equal(t, expected.StorageSize, actual.StorageSize)
	require.Equal(t, expected.Ready, actual.Ready)
}

////////////////////////////////////////////////////////////////////////////////

func TestImagesCreateImage(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	image := ImageMeta{
		ID:        "image",
		FolderID:  "folder",
		SrcDiskID: "disk",
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	created, err := storage.CreateImage(ctx, image)
	require.NoError(t, err)
	require.Equal(t, image.ID, created.ID)
	require.True(t, created.UseDataplaneTasks)

	meta, err := storage.GetImageMeta(ctx, image.ID)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.True(t, meta.UseDataplaneTasks)

	// Check idempotency.
	created, err = storage.CreateImage(ctx, image)
	require.NoError(t, err)
	require.Equal(t, image.ID, created.ID)

	err = storage.ImageCreated(ctx, image.ID, "", time.Now(), 0, 0)
	require.NoError(t, err)

	meta, err = storage.GetImageMeta(ctx, image.ID)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.True(t, meta.UseDataplaneTasks)
	require.True(t, meta.Ready)

	// Check idempotency.
	err = storage.ImageCreated(ctx, image.ID, "", time.Now(), 0, 0)
	require.NoError(t, err)

	// Check idempotency.
	created, err = storage.CreateImage(ctx, image)
	require.NoError(t, err)
	require.Equal(t, image.ID, created.ID)

	require.EqualValues(t, "disk", created.SrcDiskID)

	image.CreateTaskID = "other"
	_, err = storage.CreateImage(ctx, image)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonCancellableError()))
}

func TestImagesDeleteImage(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	image := ImageMeta{
		ID:       "image",
		FolderID: "folder",
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	created, err := storage.CreateImage(ctx, image)
	require.NoError(t, err)
	require.Equal(t, image.ID, created.ID)

	expected := image
	expected.CreateRequest = nil
	expected.DeleteTaskID = "delete"

	actual, err := storage.DeleteImage(ctx, image.ID, "delete", time.Now())
	require.NoError(t, err)
	requireImagesAreEqual(t, expected, *actual)

	err = storage.ImageCreated(ctx, image.ID, "", time.Now(), 0, 0)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	// Check idempotency.
	actual, err = storage.DeleteImage(ctx, image.ID, "delete", time.Now())
	require.NoError(t, err)
	requireImagesAreEqual(t, expected, *actual)

	_, err = storage.CreateImage(ctx, image)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.ImageDeleted(ctx, image.ID, time.Now())
	require.NoError(t, err)

	// Check idempotency.
	actual, err = storage.DeleteImage(ctx, image.ID, "delete", time.Now())
	require.NoError(t, err)
	requireImagesAreEqual(t, expected, *actual)

	_, err = storage.CreateImage(ctx, image)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.ImageCreated(ctx, image.ID, "", time.Now(), 0, 0)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
}

func TestImagesDeleteNonexistentImage(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	image := ImageMeta{
		ID:           "image",
		DeleteTaskID: "delete",
	}

	err = storage.ImageDeleted(ctx, image.ID, time.Now())
	require.NoError(t, err)

	err = storage.ImageCreated(ctx, image.ID, "", time.Now(), 0, 0)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	deletingAt := time.Now()
	actual, err := storage.DeleteImage(ctx, image.ID, "delete", deletingAt)
	require.NoError(t, err)
	require.Nil(t, actual)

	// Check idempotency.
	deletingAt = deletingAt.Add(time.Second)
	actual, err = storage.DeleteImage(ctx, image.ID, "delete", deletingAt)
	require.NoError(t, err)
	require.Nil(t, actual)

	_, err = storage.CreateImage(ctx, image)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.ImageDeleted(ctx, image.ID, time.Now())
	require.NoError(t, err)
}

func TestImagesClearDeletedImages(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	deletedAt := time.Now()
	deletedBefore := deletedAt.Add(-time.Microsecond)

	err = storage.ClearDeletedImages(ctx, deletedBefore, 10)
	require.NoError(t, err)

	image := ImageMeta{
		ID:       "image",
		FolderID: "folder",
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	created, err := storage.CreateImage(ctx, image)
	require.NoError(t, err)
	require.Equal(t, image.ID, created.ID)

	_, err = storage.DeleteImage(ctx, image.ID, "delete", deletedAt)
	require.NoError(t, err)

	err = storage.ImageDeleted(ctx, image.ID, deletedAt)
	require.NoError(t, err)

	err = storage.ClearDeletedImages(ctx, deletedBefore, 10)
	require.NoError(t, err)

	_, err = storage.CreateImage(ctx, image)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	deletedBefore = deletedAt.Add(time.Microsecond)
	err = storage.ClearDeletedImages(ctx, deletedBefore, 10)
	require.NoError(t, err)

	created, err = storage.CreateImage(ctx, image)
	require.NoError(t, err)
	require.Equal(t, image.ID, created.ID)
}

func TestImagesCreateImageShouldFailIfSnapshotAlreadyExists(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	snapshot := SnapshotMeta{
		ID: "id",
		Disk: &types.Disk{
			ZoneId: "zone",
			DiskId: "disk",
		},
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreatingAt: time.Now(),
	}
	_, err = storage.CreateSnapshot(ctx, snapshot)
	require.NoError(t, err)

	_, err = storage.CreateImage(ctx, ImageMeta{ID: snapshot.ID})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonCancellableError()))
}

func TestImagesDeleteImageShouldFailIfSnapshotAlreadyExists(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	snapshot := SnapshotMeta{
		ID: "id",
		Disk: &types.Disk{
			ZoneId: "zone",
			DiskId: "disk",
		},
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreatingAt: time.Now(),
	}
	_, err = storage.CreateSnapshot(ctx, snapshot)
	require.NoError(t, err)

	created, err := storage.DeleteImage(ctx, snapshot.ID, "delete", time.Now())
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonCancellableError()))
	require.Nil(t, created)
}

func TestImagesGetImage(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	imageID := t.Name()
	imageSize := uint64(2 * 1024 * 1024)
	imageStorageSize := uint64(3 * 1024 * 1024)
	checkpointID := "checkpoint"

	actualImage, err := storage.GetImageMeta(ctx, imageID)
	require.NoError(t, err)
	require.Nil(t, actualImage)

	image := ImageMeta{
		ID:       imageID,
		FolderID: "folder",
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	created, err := storage.CreateImage(ctx, image)
	require.NoError(t, err)
	require.Equal(t, image.ID, created.ID)

	expectedImage := image
	expectedImage.CreateRequest = nil

	checkImage := func() {
		actualImage, err := storage.GetImageMeta(ctx, imageID)
		require.NoError(t, err)
		require.NotNil(t, actualImage)
		requireImagesAreEqual(t, expectedImage, *actualImage)
	}
	checkImage()

	err = storage.ImageCreated(
		ctx,
		imageID,
		checkpointID,
		time.Now(),
		imageSize,
		imageStorageSize,
	)
	require.NoError(t, err)

	expectedImage.Size = imageSize
	expectedImage.StorageSize = imageStorageSize
	expectedImage.CheckpointID = checkpointID
	expectedImage.Ready = true
	checkImage()

	// Check idempotency.
	err = storage.ImageCreated(
		ctx,
		imageID,
		checkpointID,
		time.Now(),
		imageSize,
		imageStorageSize,
	)
	require.NoError(t, err)
	checkImage()

	// Checkpoint id differs.
	err = storage.ImageCreated(
		ctx,
		imageID,
		"foo", // checkpointID
		time.Now(),
		imageSize,
		imageStorageSize,
	)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	checkImage()

	// Image size differs.
	err = storage.ImageCreated(
		ctx,
		imageID,
		checkpointID,
		time.Now(),
		42, // imageSize
		imageStorageSize,
	)
	require.NoError(t, err)
	checkImage()

	// Image storage size differs.
	err = storage.ImageCreated(
		ctx,
		imageID,
		checkpointID,
		time.Now(),
		imageSize,
		713, // imageStorageSize
	)
	require.NoError(t, err)
	checkImage()
}

func TestImagesBackup(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	image := ImageMeta{
		ID:       "image",
		FolderID: "folder",
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	_, err = storage.CreateImage(ctx, image)
	require.NoError(t, err)

	ids, err := storage.ListImagesToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, ids)

	err = storage.ImageCreated(ctx, image.ID, "checkpoint", time.Now(), 0, 0)
	require.NoError(t, err)

	ids, err = storage.ListImagesToBackup(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []ImageBackupRequest{{ImageID: image.ID}}, ids)

	err = storage.ImageBackupScheduled(ctx, image.ID, "")
	require.NoError(t, err)

	ids, err = storage.ListImagesToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, ids)

	err = storage.ImageBackupCancelled(ctx, image.ID, "")
	require.NoError(t, err)
}

func TestImagesDeletionStopsBackup(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	image := ImageMeta{
		ID:       "image",
		FolderID: "folder",
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	_, err = storage.CreateImage(ctx, image)
	require.NoError(t, err)

	err = storage.ImageCreated(ctx, image.ID, "checkpoint", time.Now(), 0, 0)
	require.NoError(t, err)

	_, err = storage.DeleteImage(ctx, image.ID, "delete", time.Now())
	require.NoError(t, err)

	ids, err := storage.ListImagesToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, ids)

	imageIDs, err := storage.GetImageBackupDeleteQueue(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []string{"image"}, imageIDs)

	_, err = storage.DeleteImage(ctx, image.ID, "delete", time.Now())
	require.NoError(t, err)

	imageIDs, err = storage.GetImageBackupDeleteQueue(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []string{"image"}, imageIDs)

	err = storage.ImageBackupDeletionsCompleted(ctx, imageIDs)
	require.NoError(t, err)

	imageIDs, err = storage.GetImageBackupDeleteQueue(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, imageIDs)
}

func newImageBackupTestStorage(t *testing.T) (context.Context, *storageYDB) {
	t.Helper()
	ctx, cancel := context.WithCancel(newContext())
	t.Cleanup(cancel)
	db, err := newYDB(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close(ctx)) })

	storage := newStorage(t, ctx, db).(*storageYDB)
	storage.backupEnabled = false
	_, err = storage.CreateImage(ctx, ImageMeta{
		ID:            "image",
		CreateRequest: &wrappers.UInt64Value{Value: 1},
		CreateTaskID:  "create",
		CreatingAt:    time.Now(),
	})
	require.NoError(t, err)
	err = storage.ImageCreated(ctx, "image", "cp", time.Now(), 0, 0)
	require.NoError(t, err)
	storage.backupEnabled = true
	return ctx, storage
}

func TestEnqueueImageBackupIsIdempotent(t *testing.T) {
	ctx, storage := newImageBackupTestStorage(t)

	meta, err := storage.GetImageMeta(ctx, "image")
	require.NoError(t, err)
	require.False(t, meta.BackupCompleted)

	queue, err := storage.ListImagesToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)

	for _, backupID := range []string{"first", "second"} {
		err = storage.EnqueueImageBackup(ctx, "image", backupID)
		require.NoError(t, err)
	}
	queue, err = storage.ListImagesToBackup(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []ImageBackupRequest{{
		ImageID: "image", BackupID: "first",
	}}, queue)

	meta, err = storage.GetImageMeta(ctx, "image")
	require.NoError(t, err)
	require.False(t, meta.BackupCompleted)

	err = storage.ImageBackupScheduled(ctx, "image", "first")
	require.NoError(t, err)
	meta, err = storage.GetImageMeta(ctx, "image")
	require.NoError(t, err)
	require.True(t, meta.BackupCompleted)

	err = storage.ImageBackupCancelled(ctx, "image", "first")
	require.NoError(t, err)
	err = storage.EnqueueImageBackup(ctx, "image", "third")
	require.NoError(t, err)
	queue, err = storage.ListImagesToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)
	meta, err = storage.GetImageMeta(ctx, "image")
	require.NoError(t, err)
	require.True(t, meta.BackupCompleted)
}

func TestEnqueueImageBackupAfterCancellation(t *testing.T) {
	ctx, storage := newImageBackupTestStorage(t)

	err := storage.EnqueueImageBackup(ctx, "image", "first")
	require.NoError(t, err)
	err = storage.ImageBackupCancelled(ctx, "image", "first")
	require.NoError(t, err)
	meta, err := storage.GetImageMeta(ctx, "image")
	require.NoError(t, err)
	require.False(t, meta.BackupCompleted)

	err = storage.EnqueueImageBackup(ctx, "image", "second")
	require.NoError(t, err)

	for _, backupID := range []string{"", "first"} {
		err = storage.ImageBackupScheduled(ctx, "image", backupID)
		require.NoError(t, err)
		err = storage.ImageBackupCancelled(ctx, "image", backupID)
		require.NoError(t, err)
	}
	queue, err := storage.ListImagesToBackup(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []ImageBackupRequest{{
		ImageID: "image", BackupID: "second",
	}}, queue)
	meta, err = storage.GetImageMeta(ctx, "image")
	require.NoError(t, err)
	require.False(t, meta.BackupCompleted)

	err = storage.ImageBackupCancelled(ctx, "image", "second")
	require.NoError(t, err)
	err = storage.EnqueueImageBackup(ctx, "image", "third")
	require.NoError(t, err)
	queue, err = storage.ListImagesToBackup(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []ImageBackupRequest{{
		ImageID: "image", BackupID: "third",
	}}, queue)
}

func TestEnqueueImageBackupRejectsUnavailableImage(t *testing.T) {
	ctx, storage := newImageBackupTestStorage(t)

	_, err := storage.CreateImage(ctx, ImageMeta{
		ID:            "creating",
		CreateRequest: &wrappers.UInt64Value{Value: 1},
		CreateTaskID:  "create2",
		CreatingAt:    time.Now(),
	})
	require.NoError(t, err)
	for _, id := range []string{"", "missing", "creating"} {
		err = storage.EnqueueImageBackup(ctx, id, "backup")
		require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	}
	err = storage.EnqueueImageBackup(ctx, "image", "")
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	_, err = storage.DeleteImage(ctx, "image", "delete", time.Now())
	require.NoError(t, err)
	err = storage.EnqueueImageBackup(ctx, "image", "backup")
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	err = storage.ImageDeleted(ctx, "image", time.Now())
	require.NoError(t, err)
	err = storage.EnqueueImageBackup(ctx, "image", "backup")
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	queue, err := storage.ListImagesToBackup(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)
}
