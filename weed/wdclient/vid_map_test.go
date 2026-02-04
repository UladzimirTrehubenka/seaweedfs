package wdclient

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb"
)

func TestLocationIndex(t *testing.T) {
	vm := &vidMap{}
	// test must be failed
	mustFailed := func(length int) {
		_, err := vm.getLocationIndex(length)
		if err == nil {
			t.Errorf("length %d must be failed", length)
		}
		if err.Error() != fmt.Sprintf("invalid length: %d", length) {
			t.Errorf("length %d must be failed. error: %v", length, err)
		}
	}

	mustFailed(-1)
	mustFailed(0)

	mustOk := func(length, cursor, expect int) {
		if length <= 0 {
			t.Fatal("please don't do this")
		}
		vm.cursor = int32(cursor)
		got, err := vm.getLocationIndex(length)
		if err != nil {
			t.Errorf("length: %d, why? %v\n", length, err)

			return
		}
		if got != expect {
			t.Errorf("cursor: %d, length: %d, expect: %d, got: %d\n", cursor, length, expect, got)

			return
		}
	}

	for i := -1; i < 100; i++ {
		mustOk(7, i, (i+1)%7)
	}

	// when cursor reaches MaxInt64
	mustOk(7, maxCursorIndex, 0)

	// test with constructor
	vm = newVidMap("")
	length := 7
	for i := range 100 {
		got, err := vm.getLocationIndex(length)
		if err != nil {
			t.Errorf("length: %d, why? %v\n", length, err)

			return
		}
		if got != i%length {
			t.Errorf("length: %d, i: %d, got: %d\n", length, i, got)
		}
	}
}

func TestLookupFileId(t *testing.T) {
	mc := NewMasterClient(grpc.EmptyDialOption{}, "", "", "", "", "", pb.ServerDiscovery{})
	length := 5

	// Construct a cache linked list of length 5
	for i := range length {
		mc.addLocation(uint32(i), Location{Url: strconv.FormatInt(int64(i), 10)})
		mc.resetVidMap()
	}
	for i := range length {
		locations, found := mc.GetLocations(uint32(i))
		if !found || len(locations) != 1 || locations[0].Url != strconv.FormatInt(int64(i), 10) {
			t.Fatalf("urls of vid=%d is not valid.", i)
		}
	}

	// When continue to add nodes to the linked list, the previous node will be deleted, and the cache of the response will be gone.
	for i := length; i < length+5; i++ {
		mc.addLocation(uint32(i), Location{Url: strconv.FormatInt(int64(i), 10)})
		mc.resetVidMap()
	}
	for i := range length {
		locations, found := mc.GetLocations(uint32(i))
		if found {
			t.Fatalf("urls of vid[%d] should not exists, but found: %v", i, locations)
		}
	}

	// The delete operation will be applied to all cache nodes
	_, found := mc.GetLocations(uint32(length))
	if !found {
		t.Fatalf("urls of vid[%d] not found", length)
	}

	// If the locations of the current node exist, return directly
	newUrl := "abc"
	mc.addLocation(uint32(length), Location{Url: newUrl})
	locations, found := mc.GetLocations(uint32(length))
	if !found || locations[0].Url != newUrl {
		t.Fatalf("urls of vid[%d] not found", length)
	}

	// After delete `abc`, cache nodes are searched
	deleteLoc := Location{Url: newUrl}
	mc.deleteLocation(uint32(length), deleteLoc)
	locations, found = mc.GetLocations(uint32(length))
	if found && locations[0].Url != strconv.FormatInt(int64(length), 10) {
		t.Fatalf("urls of vid[%d] not expected", length)
	}

	//lock: concurrent test
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				for i := range 20 {
					_, _ = mc.GetLocations(uint32(i))
				}
			}
		}()
	}

	for i := range 100 {
		mc.addLocation(uint32(i), Location{})
	}
	wg.Wait()
}

func TestConcurrentGetLocations(t *testing.T) {
	mc := NewMasterClient(grpc.EmptyDialOption{}, "", "", "", "", "", pb.ServerDiscovery{})
	location := Location{Url: "TestDataRacing"}
	mc.addLocation(1, location)

	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					_, found := mc.GetLocations(1)
					if !found {
						cancel()
						t.Error("vid map invalid due to data racing. ")

						return
					}
				}
			}
		}()
	}

	// Simulate vidmap reset with cache when leader changes
	for range 100 {
		mc.resetVidMap()
		mc.addLocation(1, location)
		time.Sleep(1 * time.Microsecond)
	}
	cancel()
	wg.Wait()
}

func BenchmarkLocationIndex(b *testing.B) {
	b.SetParallelism(8)
	vm := vidMap{
		cursor: maxCursorIndex - 4000,
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := vm.getLocationIndex(3)
			if err != nil {
				b.Error(err)
			}
		}
	})
}
