package redis3

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

const maxNameBatchSizeLimit = 1000000

func insertChild(ctx context.Context, redisStore *UniversalRedis3Store, key string, name string) error {
	// lock and unlock
	mutex := redisStore.redsync.NewMutex(key + "lock")
	if err := mutex.Lock(); err != nil {
		return fmt.Errorf("lock %s: %w", key, err)
	}
	defer func() {
		mutex.Unlock()
	}()

	client := redisStore.Client
	data, err := client.Get(ctx, key).Result()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			return fmt.Errorf("read %s: %w", key, err)
		}
	}
	store := newSkipListElementStore(key, client)
	nameList := LoadItemList([]byte(data), key, client, store, maxNameBatchSizeLimit)

	if err := nameList.WriteName(name); err != nil {
		glog.ErrorfCtx(ctx, "add %s %s: %v", key, name, err)

		return err
	}

	if !nameList.HasChanges() {
		return nil
	}

	if err := client.Set(ctx, key, nameList.ToBytes(), 0).Err(); err != nil {
		return err
	}

	return nil
}

func removeChild(ctx context.Context, redisStore *UniversalRedis3Store, key string, name string) error {
	// lock and unlock
	mutex := redisStore.redsync.NewMutex(key + "lock")
	if err := mutex.Lock(); err != nil {
		return fmt.Errorf("lock %s: %w", key, err)
	}
	defer mutex.Unlock()

	client := redisStore.Client
	data, err := client.Get(ctx, key).Result()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			return fmt.Errorf("read %s: %w", key, err)
		}
	}
	store := newSkipListElementStore(key, client)
	nameList := LoadItemList([]byte(data), key, client, store, maxNameBatchSizeLimit)

	if err := nameList.DeleteName(name); err != nil {
		return err
	}
	if !nameList.HasChanges() {
		return nil
	}

	if err := client.Set(ctx, key, nameList.ToBytes(), 0).Err(); err != nil {
		return err
	}

	return nil
}

func removeChildren(ctx context.Context, redisStore *UniversalRedis3Store, key string, onDeleteFn func(name string) error) error {
	// lock and unlock
	mutex := redisStore.redsync.NewMutex(key + "lock")
	if err := mutex.Lock(); err != nil {
		return fmt.Errorf("lock %s: %w", key, err)
	}
	defer mutex.Unlock()

	client := redisStore.Client
	data, err := client.Get(ctx, key).Result()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			return fmt.Errorf("read %s: %w", key, err)
		}
	}
	store := newSkipListElementStore(key, client)
	nameList := LoadItemList([]byte(data), key, client, store, maxNameBatchSizeLimit)

	if err = nameList.ListNames("", func(name string) bool {
		if err := onDeleteFn(name); err != nil {
			glog.ErrorfCtx(ctx, "delete %s child %s: %v", key, name, err)

			return false
		}

		return true
	}); err != nil {
		return err
	}

	if err = nameList.RemoteAllListElement(); err != nil {
		return err
	}

	return nil
}

func listChildren(ctx context.Context, redisStore *UniversalRedis3Store, key string, startFileName string, eachFn func(name string) bool) error {
	client := redisStore.Client
	data, err := client.Get(ctx, key).Result()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			return fmt.Errorf("read %s: %w", key, err)
		}
	}
	store := newSkipListElementStore(key, client)
	nameList := LoadItemList([]byte(data), key, client, store, maxNameBatchSizeLimit)

	if err = nameList.ListNames(startFileName, func(name string) bool {
		return eachFn(name)
	}); err != nil {
		return err
	}

	return nil
}
