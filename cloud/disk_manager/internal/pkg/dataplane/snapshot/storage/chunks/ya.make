GO_LIBRARY()

SRCS(
    validate.go
    common.go
    storage.go
    storage_s3.go
    storage_ydb.go
)

GO_TEST_SRCS(
    validate_test.go
    storage_test.go
)

END()

RECURSE_FOR_TESTS(
    tests
)
