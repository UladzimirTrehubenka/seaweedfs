package s3api

import (
	"bytes"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestCreateDestinationChunkPreservesEncryption tests that createDestinationChunk preserves CipherKey and IsCompressed
func TestCreateDestinationChunkPreservesEncryption(t *testing.T) {
	s3a := &S3ApiServer{}

	testCases := []struct {
		name             string
		sourceChunk      *filer_pb.FileChunk
		expectedOffset   int64
		expectedSize     uint64
		shouldPreserveCK bool
		shouldPreserveIC bool
	}{
		{
			name: "Encrypted and compressed chunk",
			sourceChunk: &filer_pb.FileChunk{
				Offset:       0,
				Size:         1024,
				CipherKey:    []byte("test-cipher-key-1234567890123456"),
				IsCompressed: true,
				ETag:         "test-etag",
			},
			expectedOffset:   0,
			expectedSize:     1024,
			shouldPreserveCK: true,
			shouldPreserveIC: true,
		},
		{
			name: "Only encrypted chunk",
			sourceChunk: &filer_pb.FileChunk{
				Offset:       1024,
				Size:         2048,
				CipherKey:    []byte("test-cipher-key-1234567890123456"),
				IsCompressed: false,
				ETag:         "test-etag-2",
			},
			expectedOffset:   1024,
			expectedSize:     2048,
			shouldPreserveCK: true,
			shouldPreserveIC: false,
		},
		{
			name: "Only compressed chunk",
			sourceChunk: &filer_pb.FileChunk{
				Offset:       2048,
				Size:         512,
				CipherKey:    nil,
				IsCompressed: true,
				ETag:         "test-etag-3",
			},
			expectedOffset:   2048,
			expectedSize:     512,
			shouldPreserveCK: false,
			shouldPreserveIC: true,
		},
		{
			name: "Unencrypted and uncompressed chunk",
			sourceChunk: &filer_pb.FileChunk{
				Offset:       4096,
				Size:         1024,
				CipherKey:    nil,
				IsCompressed: false,
				ETag:         "test-etag-4",
			},
			expectedOffset:   4096,
			expectedSize:     1024,
			shouldPreserveCK: false,
			shouldPreserveIC: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dstChunk := s3a.createDestinationChunk(tc.sourceChunk, tc.expectedOffset, tc.expectedSize)

			// Verify offset and size
			if dstChunk.GetOffset() != tc.expectedOffset {
				t.Errorf("Expected offset %d, got %d", tc.expectedOffset, dstChunk.GetOffset())
			}
			if dstChunk.GetSize() != tc.expectedSize {
				t.Errorf("Expected size %d, got %d", tc.expectedSize, dstChunk.GetSize())
			}

			// Verify CipherKey preservation
			if tc.shouldPreserveCK {
				if !bytes.Equal(dstChunk.GetCipherKey(), tc.sourceChunk.GetCipherKey()) {
					t.Errorf("CipherKey not preserved: expected %v, got %v", tc.sourceChunk.GetCipherKey(), dstChunk.GetCipherKey())
				}
			} else {
				if len(dstChunk.GetCipherKey()) > 0 {
					t.Errorf("Expected no CipherKey, got %v", dstChunk.GetCipherKey())
				}
			}

			// Verify IsCompressed preservation
			if dstChunk.GetIsCompressed() != tc.shouldPreserveIC {
				t.Errorf("IsCompressed not preserved: expected %v, got %v", tc.shouldPreserveIC, dstChunk.GetIsCompressed())
			}

			// Verify ETag preservation
			if dstChunk.GetETag() != tc.sourceChunk.GetETag() {
				t.Errorf("ETag not preserved: expected %s, got %s", tc.sourceChunk.GetETag(), dstChunk.GetETag())
			}
		})
	}
}

// TestEncryptedVolumeCopyScenario documents the expected behavior for encrypted volumes (issue #7530)
func TestEncryptedVolumeCopyScenario(t *testing.T) {
	t.Run("Scenario: Copy file on encrypted volume with multiple chunks", func(t *testing.T) {
		// Scenario description for issue #7530:
		// 1. Volume is started with -filer.encryptVolumeData
		// 2. File is uploaded via S3 (automatically encrypted, multiple chunks)
		// 3. File is copied/renamed via S3 CopyObject
		// 4. Copied file should be readable
		//
		// The bug was that IsCompressed flag was not preserved during copy,
		// causing the upload logic to potentially double-compress the data,
		// making the copied file unreadable.

		sourceChunks := []*filer_pb.FileChunk{
			{
				FileId:       "1,abc123",
				Offset:       0,
				Size:         4194304,
				CipherKey:    util.GenCipherKey(), // Simulates encrypted volume
				IsCompressed: true,                // Simulates compression
				ETag:         "etag1",
			},
			{
				FileId:       "2,def456",
				Offset:       4194304,
				Size:         4194304,
				CipherKey:    util.GenCipherKey(),
				IsCompressed: true,
				ETag:         "etag2",
			},
		}

		s3a := &S3ApiServer{}

		// Verify that createDestinationChunk preserves all necessary metadata
		for i, srcChunk := range sourceChunks {
			dstChunk := s3a.createDestinationChunk(srcChunk, srcChunk.GetOffset(), srcChunk.GetSize())

			// Critical checks for issue #7530
			if !dstChunk.GetIsCompressed() {
				t.Errorf("Chunk %d: IsCompressed flag MUST be preserved to prevent double-compression", i)
			}
			if !bytes.Equal(dstChunk.GetCipherKey(), srcChunk.GetCipherKey()) {
				t.Errorf("Chunk %d: CipherKey MUST be preserved for encrypted volumes", i)
			}
			if dstChunk.GetOffset() != srcChunk.GetOffset() {
				t.Errorf("Chunk %d: Offset must be preserved", i)
			}
			if dstChunk.GetSize() != srcChunk.GetSize() {
				t.Errorf("Chunk %d: Size must be preserved", i)
			}
			if dstChunk.GetETag() != srcChunk.GetETag() {
				t.Errorf("Chunk %d: ETag must be preserved", i)
			}
		}

		t.Log("✓ All chunk metadata properly preserved for encrypted volume copy scenario")
	})
}
