package filer

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestDoMaybeManifestize(t *testing.T) {
	var manifestTests = []struct {
		inputs   []*filer_pb.FileChunk
		expected []*filer_pb.FileChunk
	}{
		{
			inputs: []*filer_pb.FileChunk{
				{FileId: "1", IsChunkManifest: false},
				{FileId: "2", IsChunkManifest: false},
				{FileId: "3", IsChunkManifest: false},
				{FileId: "4", IsChunkManifest: false},
			},
			expected: []*filer_pb.FileChunk{
				{FileId: "12", IsChunkManifest: true},
				{FileId: "34", IsChunkManifest: true},
			},
		},
		{
			inputs: []*filer_pb.FileChunk{
				{FileId: "1", IsChunkManifest: true},
				{FileId: "2", IsChunkManifest: false},
				{FileId: "3", IsChunkManifest: false},
				{FileId: "4", IsChunkManifest: false},
			},
			expected: []*filer_pb.FileChunk{
				{FileId: "1", IsChunkManifest: true},
				{FileId: "23", IsChunkManifest: true},
				{FileId: "4", IsChunkManifest: false},
			},
		},
		{
			inputs: []*filer_pb.FileChunk{
				{FileId: "1", IsChunkManifest: false},
				{FileId: "2", IsChunkManifest: true},
				{FileId: "3", IsChunkManifest: false},
				{FileId: "4", IsChunkManifest: false},
			},
			expected: []*filer_pb.FileChunk{
				{FileId: "2", IsChunkManifest: true},
				{FileId: "13", IsChunkManifest: true},
				{FileId: "4", IsChunkManifest: false},
			},
		},
		{
			inputs: []*filer_pb.FileChunk{
				{FileId: "1", IsChunkManifest: true},
				{FileId: "2", IsChunkManifest: true},
				{FileId: "3", IsChunkManifest: false},
				{FileId: "4", IsChunkManifest: false},
			},
			expected: []*filer_pb.FileChunk{
				{FileId: "1", IsChunkManifest: true},
				{FileId: "2", IsChunkManifest: true},
				{FileId: "34", IsChunkManifest: true},
			},
		},
	}

	for i, mtest := range manifestTests {
		println("test", i)
		actual, _ := doMaybeManifestize(nil, mtest.inputs, 2, mockMerge)
		assertEqualChunks(t, mtest.expected, actual)
	}
}

func assertEqualChunks(t *testing.T, expected, actual []*filer_pb.FileChunk) {
	assert.Len(t, actual, len(expected))
	for i := range actual {
		assertEqualChunk(t, actual[i], expected[i])
	}
}
func assertEqualChunk(t *testing.T, expected, actual *filer_pb.FileChunk) {
	assert.Equal(t, expected.GetFileId(), actual.GetFileId())
	assert.Equal(t, expected.GetIsChunkManifest(), actual.GetIsChunkManifest())
}

func mockMerge(saveFunc SaveDataAsChunkFunctionType, dataChunks []*filer_pb.FileChunk) (manifestChunk *filer_pb.FileChunk, err error) {
	var buf bytes.Buffer
	minOffset, maxOffset := int64(math.MaxInt64), int64(math.MinInt64)
	for k := range dataChunks {
		chunk := dataChunks[k]
		buf.WriteString(chunk.GetFileId())
		if minOffset > int64(chunk.GetOffset()) {
			minOffset = chunk.GetOffset()
		}
		if maxOffset < int64(chunk.GetSize())+chunk.GetOffset() {
			maxOffset = int64(chunk.GetSize()) + chunk.GetOffset()
		}
	}

	manifestChunk = &filer_pb.FileChunk{
		FileId: buf.String(),
	}
	manifestChunk.IsChunkManifest = true
	manifestChunk.Offset = minOffset
	manifestChunk.Size = uint64(maxOffset - minOffset)

	return
}
