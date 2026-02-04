package leveldb

import (
	"context"
	"errors"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"

	"github.com/seaweedfs/seaweedfs/weed/filer"
)

func (store *LevelDB2Store) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	partitionId := bucketKvKey(key, store.dbCount)

	err = store.dbs[partitionId].Put(key, value, nil)

	if err != nil {
		return fmt.Errorf("kv bucket %d put: %w", partitionId, err)
	}

	return nil
}

func (store *LevelDB2Store) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	partitionId := bucketKvKey(key, store.dbCount)

	value, err = store.dbs[partitionId].Get(key, nil)

	if errors.Is(err, leveldb.ErrNotFound) {
		return nil, filer.ErrKvNotFound
	}

	if err != nil {
		return nil, fmt.Errorf("kv bucket %d get: %w", partitionId, err)
	}

	return
}

func (store *LevelDB2Store) KvDelete(ctx context.Context, key []byte) (err error) {
	partitionId := bucketKvKey(key, store.dbCount)

	err = store.dbs[partitionId].Delete(key, nil)

	if err != nil {
		return fmt.Errorf("kv bucket %d delete: %w", partitionId, err)
	}

	return nil
}

func bucketKvKey(key []byte, dbCount int) (partitionId int) {
	return int(key[len(key)-1]) % dbCount
}
