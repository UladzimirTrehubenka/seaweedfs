package filer

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
)

func TestFilerRemoteStorage_FindRemoteStorageClient(t *testing.T) {
	conf := &remote_pb.RemoteConf{
		Name: "s7",
		Type: "s3",
	}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.GetName()] = conf

	rs.mapDirectoryToRemoteStorage("/a/b/c", &remote_pb.RemoteStorageLocation{
		Name:   "s7",
		Bucket: "some",
		Path:   "/dir",
	})

	_, _, found := rs.FindRemoteStorageClient("/a/b/c/d/e/f")
	assert.True(t, found, "find storage client")

	_, _, found2 := rs.FindRemoteStorageClient("/a/b")
	assert.False(t, found2, "should not find storage client")

	_, _, found3 := rs.FindRemoteStorageClient("/a/b/c")
	assert.False(t, found3, "should not find storage client")

	_, _, found4 := rs.FindRemoteStorageClient("/a/b/cc")
	assert.False(t, found4, "should not find storage client")
}
