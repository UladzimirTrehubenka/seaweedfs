package sequence

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestSequencer(t *testing.T) {
	seq, err := NewSnowflakeSequencer("for_test", 1)
	assert.NoError(t, err)
	last := uint64(0)
	bytes := make([]byte, types.NeedleIdSize)
	for range 100 {
		next := seq.NextFileId(1)
		types.NeedleIdToBytes(bytes, types.NeedleId(next))
		println(hex.EncodeToString(bytes))
		if last == next {
			t.Errorf("last %d next %d", last, next)
		}
		last = next
	}
}
