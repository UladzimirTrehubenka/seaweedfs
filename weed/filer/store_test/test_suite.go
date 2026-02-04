package store_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestFilerStore(t *testing.T, store filer.FilerStore) {
	ctx := context.Background()

	store.InsertEntry(ctx, makeEntry(util.FullPath("/"), true))
	store.InsertEntry(ctx, makeEntry(util.FullPath("/a"), true))
	store.InsertEntry(ctx, makeEntry(util.FullPath("/a/b"), true))
	store.InsertEntry(ctx, makeEntry(util.FullPath("/a/b/c"), true))
	for i := range 2000 {
		store.InsertEntry(ctx, makeEntry(util.FullPath(fmt.Sprintf("/a/b/c/f%05d", i)), false))
	}

	{
		var counter int
		lastFileName, err := store.ListDirectoryEntries(ctx, util.FullPath("/a/b/c"), "", false, 3, func(entry *filer.Entry) (bool, error) {
			counter++

			return true, nil
		})
		assert.NoError(t, err, "list directory")
		assert.Equal(t, 3, counter, "directory list counter")
		assert.Equal(t, "f00002", lastFileName, "directory list last file")
		lastFileName, err = store.ListDirectoryEntries(ctx, util.FullPath("/a/b/c"), lastFileName, false, 1024, func(entry *filer.Entry) (bool, error) {
			counter++

			return true, nil
		})
		assert.NoError(t, err, "list directory")
		assert.Equal(t, 1027, counter, "directory list counter")
		assert.Equal(t, "f01026", lastFileName, "directory list last file")
	}

	testKey := []byte("test_key")
	testValue1 := []byte("test_value1")
	testValue2 := []byte("test_value2")

	err := store.KvPut(ctx, testKey, testValue1)
	assert.NoError(t, err, "KV put")
	value, err := store.KvGet(ctx, testKey)
	assert.Equal(t, value, testValue1, "KV get")

	err = store.KvPut(ctx, testKey, testValue2)
	assert.NoError(t, err, "KV update")
	value, err = store.KvGet(ctx, testKey)
	assert.Equal(t, value, testValue2, "KV get after update")
}

func makeEntry(fullPath util.FullPath, isDirectory bool) *filer.Entry {
	var mode os.FileMode
	if isDirectory {
		mode = os.ModeDir
	}

	return &filer.Entry{
		FullPath: fullPath,
		Attr: filer.Attr{
			Mode: mode,
		},
	}
}
