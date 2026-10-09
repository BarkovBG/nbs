PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

SRCS(
    backup_folder_task.proto
    backup_snapshot_task.proto
    create_snapshot_from_disk_task.proto
    delete_snapshot_task.proto
)

PEERDIR(
    cloud/disk_manager/internal/pkg/dataplane/protos
    cloud/disk_manager/internal/pkg/types
)

END()
