package chunks

import (
	"hash/crc32"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/compressor"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

func ValidateChunkBlob(blob ChunkBlob) error {
	data, err := compressor.DecompressToBytes(blob.Compression, blob.Data, 4*1024*1024)
	if err != nil {
		return err
	}
	actual := crc32.ChecksumIEEE(data)
	if actual != blob.Checksum {
		return errors.NewNonRetriableErrorf("chunk blob checksum mismatch: expected %v, actual %v", blob.Checksum, actual)
	}
	return nil
}
