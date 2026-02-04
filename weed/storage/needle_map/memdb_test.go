package needle_map

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func BenchmarkMemDb(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		nm := NewMemDb()

		nid := types.NeedleId(345)
		offset := types.Offset{
			OffsetHigher: types.OffsetHigher{},
			OffsetLower:  types.OffsetLower{},
		}
		nm.Set(nid, offset, 324)
		nm.Close()
	}
}
