GO_LIBRARY()

SRCS(
    snapshot_backup_folders.go
    common.go
    disks.go
    filesystems.go
    filesystem_snapshots.go
    images.go
    placement_groups.go
    snapshot_backups.go
    snapshots.go
    storage.go
)

GO_TEST_SRCS(
    snapshot_backup_folders_test.go
    common_test.go
    disks_test.go
    filesystems_test.go
    filesystem_snapshots_test.go
    images_test.go
    placement_groups_test.go
    snapshot_backups_test.go
    snapshots_test.go
)

END()

RECURSE_FOR_TESTS(
    mocks
    tests
)
