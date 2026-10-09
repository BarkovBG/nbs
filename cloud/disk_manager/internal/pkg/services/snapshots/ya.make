GO_LIBRARY()

SRCS(
    backup_folder_task.go
    backup_snapshot_graph.go
    backup_snapshot_task.go
    clear_deleted_snapshots_task.go
    create_snapshot_from_disk_task.go
    delete_snapshot_task.go
    interface.go
    register.go
    schedule_backup_snapshot_tasks.go
    service.go
)

GO_TEST_SRCS(
    backup_folder_task_test.go
    schedule_backup_snapshot_tasks_test.go
    delete_snapshot_task_test.go
)

END()

RECURSE(
    config
    protos
)

RECURSE_FOR_TESTS(
    mocks
    tasks_tests
)
