package resources

import (
	"context"
	"fmt"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) snapshotBackupFolderTx(
	ctx context.Context,
	tx *persistence.Transaction,
	folderID string,
) (string, bool, error) {

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $folder as Utf8;

		select generation, enabled
		from backup_folders
		where folder_id = $folder
	`, s.snapshotsPath),
		persistence.ValueParam("$folder", persistence.UTF8Value(folderID)),
	)
	if err != nil {
		return "", false, err
	}
	defer res.Close()

	var generation string
	var enabled bool
	if res.NextResultSet(ctx) && res.NextRow() {
		err = res.ScanWithDefaults(&generation, &enabled)
		if err != nil {
			return "", false, err
		}
	}

	return generation, enabled, res.Err()
}

func (s *storageYDB) setSnapshotBackupFolder(
	ctx context.Context,
	session *persistence.Session,
	folderID string,
	generation string,
	enabled bool,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	current, selected, err := s.snapshotBackupFolderTx(ctx, tx, folderID)
	if err != nil {
		return err
	}

	if enabled && selected {
		return tx.Commit(ctx)
	}

	if !enabled {
		if current == "" || (generation != "" && generation != current) {
			return tx.Commit(ctx)
		}
		generation = current
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $folder as Utf8;
		declare $generation as Utf8;
		declare $enabled as Bool;

		upsert into backup_folders (folder_id, generation, enabled)
		values ($folder, $generation, $enabled)
	`, s.snapshotsPath),
		persistence.ValueParam("$folder", persistence.UTF8Value(folderID)),
		persistence.ValueParam("$generation", persistence.UTF8Value(generation)),
		persistence.ValueParam("$enabled", persistence.BoolValue(enabled)),
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) enqueueSnapshotBackupForFolder(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	backupID string,
	folderID string,
	generation string,
) (bool, error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	current, enabled, err := s.snapshotBackupFolderTx(ctx, tx, folderID)
	if err != nil {
		return false, err
	}

	if !enabled || current != generation {
		return false, tx.Commit(ctx)
	}

	// The selection guard and queue insertion share a transaction. A delayed
	// producer from an earlier enable cycle cannot enqueue into the new cycle.
	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;
		declare $folder as Utf8;

		select id
		from snapshots
		where id = $id and folder_id = $folder
	`, s.snapshotsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(snapshotID)),
		persistence.ValueParam("$folder", persistence.UTF8Value(folderID)),
	)
	if err != nil {
		return false, err
	}
	defer res.Close()

	found := res.NextResultSet(ctx) && res.NextRow()
	err = res.Err()
	if err != nil {
		return false, err
	}

	if !found {
		return true, tx.Commit(ctx)
	}

	err = s.enqueueSnapshotBackupTx(ctx, tx, snapshotID, backupID)
	if err != nil && !errors.Is(err, errors.NewEmptyNonRetriableError()) {
		return false, err
	}

	// Deletion/readiness may have changed after the page was read. Skip the
	// unavailable source; a later pass picks up resources that become ready.
	return true, tx.Commit(ctx)
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) SetSnapshotBackupFolder(
	ctx context.Context,
	folderID string,
	generation string,
	enabled bool,
) error {

	if folderID == "" || (enabled && generation == "") {
		return errors.NewNonRetriableErrorf(
			"invalid snapshot backup folder selection",
		)
	}

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.setSnapshotBackupFolder(
				ctx,
				session,
				folderID,
				generation,
				enabled,
			)
		},
	)
}

func (s *storageYDB) ListSnapshotBackupFolders(
	ctx context.Context,
) ([]SnapshotBackupFolder, error) {

	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";

		select folder_id, generation
		from backup_folders
		where enabled
	`, s.snapshotsPath))
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var folders []SnapshotBackupFolder
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var folder SnapshotBackupFolder
			err = res.ScanWithDefaults(&folder.FolderID, &folder.Generation)
			if err != nil {
				return nil, err
			}

			folders = append(folders, folder)
		}
	}

	return folders, res.Err()
}

func (s *storageYDB) ListSnapshotBackupCandidates(
	ctx context.Context,
	folderID string,
	afterID string,
	limit int,
) ([]string, error) {

	if folderID == "" || limit <= 0 {
		return nil, errors.NewNonRetriableErrorf("invalid snapshot backup page")
	}

	// Snapshots created before backup_completed was added have NULL there.
	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $folder as Utf8;
		declare $after as Utf8;
		declare $ready as Int64;
		declare $limit as Uint64;

		select id
		from snapshots view folder_id_index
		where folder_id = $folder and id > $after and status = $ready and
			not COALESCE(backup_completed, false)
		order by id
		limit $limit
	`, s.snapshotsPath),
		persistence.ValueParam("$folder", persistence.UTF8Value(folderID)),
		persistence.ValueParam("$after", persistence.UTF8Value(afterID)),
		persistence.ValueParam(
			"$ready",
			persistence.Int64Value(int64(snapshotStatusReady)),
		),
		persistence.ValueParam("$limit", persistence.Uint64Value(uint64(limit))),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var ids []string
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var id string
			err = res.ScanWithDefaults(&id)
			if err != nil {
				return nil, err
			}

			ids = append(ids, id)
		}
	}

	return ids, res.Err()
}

func (s *storageYDB) EnqueueSnapshotBackupForFolder(
	ctx context.Context,
	snapshotID string,
	backupID string,
	folderID string,
	generation string,
) (selected bool, err error) {

	if snapshotID == "" || backupID == "" || folderID == "" || generation == "" {
		return false, errors.NewNonRetriableErrorf(
			"invalid snapshot backup folder request",
		)
	}

	err = s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			selected, err = s.enqueueSnapshotBackupForFolder(
				ctx,
				session,
				snapshotID,
				backupID,
				folderID,
				generation,
			)
			return err
		},
	)
	return selected, err
}

////////////////////////////////////////////////////////////////////////////////

func createSnapshotBackupFolderTable(
	ctx context.Context,
	folder string,
	db *persistence.YDBClient,
	dropUnusedColumns bool,
) error {

	return db.CreateOrAlterTable(
		ctx,
		folder,
		"backup_folders",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("folder_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("generation", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("enabled", persistence.Optional(persistence.TypeBool)),
			persistence.WithPrimaryKeyColumn("folder_id"),
		),
		dropUnusedColumns,
	)
}
