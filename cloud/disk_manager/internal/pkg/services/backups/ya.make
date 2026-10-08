GO_LIBRARY()

SRCS(
    backfill_task.go
    register.go
)

GO_TEST_SRCS(
    backfill_task_test.go
)

END()

RECURSE(
    protos
)
