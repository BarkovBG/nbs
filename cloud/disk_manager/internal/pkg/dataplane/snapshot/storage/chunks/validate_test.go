package chunks

import (
	"bytes"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/compressor"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	common_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
)

func TestValidateChunkBlobChecksDecodedDataForAllFormats(t *testing.T) {
	data := bytes.Repeat([]byte("snapshot-data"), 300)
	for _, format := range []string{"", "lz4", "lz4_block", "zstd", "zstd_cgo", "gzip"} {
		t.Run(format, func(t *testing.T) {
			compressed, err := compressor.Compress(format, data, metrics.New(common_metrics.NewEmptyRegistry(), "test"), nil)
			require.NoError(t, err)
			blob := ChunkBlob{Data: compressed, Compression: format, Checksum: crc32.ChecksumIEEE(data)}
			require.NoError(t, ValidateChunkBlob(blob))
			blob.Checksum ^= 1
			require.Error(t, ValidateChunkBlob(blob))
		})
	}
}

func TestValidateChunkBlobRejectsTruncatedCompression(t *testing.T) {
	data := bytes.Repeat([]byte("snapshot-data"), 300)
	compressed, err := compressor.Compress("gzip", data, metrics.New(common_metrics.NewEmptyRegistry(), "test"), nil)
	require.NoError(t, err)
	require.Error(t, ValidateChunkBlob(ChunkBlob{Data: compressed[:len(compressed)-3], Compression: "gzip", Checksum: crc32.ChecksumIEEE(data)}))
}
