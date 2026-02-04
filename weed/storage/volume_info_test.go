package storage

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func TestSortVolumeInfos(t *testing.T) {
	vis := []*VolumeInfo{
		{
			Id: 2,
		},
		{
			Id: 1,
		},
		{
			Id: 3,
		},
	}
	sortVolumeInfos(vis)
	for i := range vis {
		if vis[i].Id != needle.VolumeId(i+1) {
			t.Fatal()
		}
	}
}
