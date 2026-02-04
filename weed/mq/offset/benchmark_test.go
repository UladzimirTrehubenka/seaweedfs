package offset

import (
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// BenchmarkOffsetAssignment benchmarks sequential offset assignment
func BenchmarkOffsetAssignment(b *testing.B) {
	storage := NewInMemoryOffsetStorage()

	partition := &schema_pb.Partition{
		RingSize:   1024,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: time.Now().UnixNano(),
	}

	manager, err := NewPartitionOffsetManager("test-namespace", "test-topic", partition, storage)
	if err != nil {
		b.Fatalf("Failed to create partition manager: %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			manager.AssignOffset()
		}
	})
}

// BenchmarkBatchOffsetAssignment benchmarks batch offset assignment
func BenchmarkBatchOffsetAssignment(b *testing.B) {
	storage := NewInMemoryOffsetStorage()

	partition := &schema_pb.Partition{
		RingSize:   1024,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: time.Now().UnixNano(),
	}

	manager, err := NewPartitionOffsetManager("test-namespace", "test-topic", partition, storage)
	if err != nil {
		b.Fatalf("Failed to create partition manager: %v", err)
	}

	batchSizes := []int64{1, 10, 100, 1000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize%d", batchSize), func(b *testing.B) {
			b.ResetTimer()
			for range b.N {
				manager.AssignOffsets(batchSize)
			}
		})
	}
}

// BenchmarkSQLOffsetStorage benchmarks SQL storage operations
func BenchmarkSQLOffsetStorage(b *testing.B) {
	// Create temporary database
	tmpFile, err := os.CreateTemp(b.TempDir(), "benchmark_*.db")
	if err != nil {
		b.Fatalf("Failed to create temp database: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	db, err := CreateDatabase(tmpFile.Name())
	if err != nil {
		b.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	storage, err := NewSQLOffsetStorage(db)
	if err != nil {
		b.Fatalf("Failed to create SQL storage: %v", err)
	}
	defer storage.Close()

	partition := &schema_pb.Partition{
		RingSize:   1024,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: time.Now().UnixNano(),
	}

	partitionKey := partitionKey(partition)

	b.Run("SaveCheckpoint", func(b *testing.B) {
		b.ResetTimer()
		for i := range b.N {
			storage.SaveCheckpoint("test-namespace", "test-topic", partition, int64(i))
		}
	})

	b.Run("LoadCheckpoint", func(b *testing.B) {
		storage.SaveCheckpoint("test-namespace", "test-topic", partition, 1000)
		b.ResetTimer()
		for range b.N {
			storage.LoadCheckpoint("test-namespace", "test-topic", partition)
		}
	})

	b.Run("SaveOffsetMapping", func(b *testing.B) {
		b.ResetTimer()
		for i := range b.N {
			storage.SaveOffsetMapping(partitionKey, int64(i), int64(i*1000), 100)
		}
	})

	// Pre-populate for read benchmarks
	for i := range 1000 {
		storage.SaveOffsetMapping(partitionKey, int64(i), int64(i*1000), 100)
	}

	b.Run("GetHighestOffset", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			storage.GetHighestOffset("test-namespace", "test-topic", partition)
		}
	})

	b.Run("LoadOffsetMappings", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			storage.LoadOffsetMappings(partitionKey)
		}
	})

	b.Run("GetOffsetMappingsByRange", func(b *testing.B) {
		b.ResetTimer()
		for i := range b.N {
			start := int64(i % 900)
			end := start + 100
			storage.GetOffsetMappingsByRange(partitionKey, start, end)
		}
	})

	b.Run("GetPartitionStats", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			storage.GetPartitionStats(partitionKey)
		}
	})
}

// BenchmarkInMemoryVsSQL compares in-memory and SQL storage performance
func BenchmarkInMemoryVsSQL(b *testing.B) {
	partition := &schema_pb.Partition{
		RingSize:   1024,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: time.Now().UnixNano(),
	}

	// In-memory storage benchmark
	b.Run("InMemory", func(b *testing.B) {
		storage := NewInMemoryOffsetStorage()
		manager, err := NewPartitionOffsetManager("test-namespace", "test-topic", partition, storage)
		if err != nil {
			b.Fatalf("Failed to create partition manager: %v", err)
		}

		b.ResetTimer()
		for range b.N {
			manager.AssignOffset()
		}
	})

	// SQL storage benchmark
	b.Run("SQL", func(b *testing.B) {
		tmpFile, err := os.CreateTemp(b.TempDir(), "benchmark_sql_*.db")
		if err != nil {
			b.Fatalf("Failed to create temp database: %v", err)
		}
		tmpFile.Close()
		defer os.Remove(tmpFile.Name())

		db, err := CreateDatabase(tmpFile.Name())
		if err != nil {
			b.Fatalf("Failed to create database: %v", err)
		}
		defer db.Close()

		storage, err := NewSQLOffsetStorage(db)
		if err != nil {
			b.Fatalf("Failed to create SQL storage: %v", err)
		}
		defer storage.Close()

		manager, err := NewPartitionOffsetManager("test-namespace", "test-topic", partition, storage)
		if err != nil {
			b.Fatalf("Failed to create partition manager: %v", err)
		}

		b.ResetTimer()
		for range b.N {
			manager.AssignOffset()
		}
	})
}

// BenchmarkOffsetSubscription benchmarks subscription operations
func BenchmarkOffsetSubscription(b *testing.B) {
	storage := NewInMemoryOffsetStorage()
	registry := NewPartitionOffsetRegistry(storage)
	subscriber := NewOffsetSubscriber(registry)

	partition := &schema_pb.Partition{
		RingSize:   1024,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: time.Now().UnixNano(),
	}

	// Pre-assign offsets
	registry.AssignOffsets("test-namespace", "test-topic", partition, 10000)

	b.Run("CreateSubscription", func(b *testing.B) {
		b.ResetTimer()
		for i := range b.N {
			subscriptionID := fmt.Sprintf("bench-sub-%d", i)
			_, err := subscriber.CreateSubscription(
				subscriptionID,
				"test-namespace", "test-topic",
				partition,
				schema_pb.OffsetType_RESET_TO_EARLIEST,
				0,
			)
			if err != nil {
				b.Fatalf("Failed to create subscription: %v", err)
			}
			subscriber.CloseSubscription(subscriptionID)
		}
	})

	// Create subscription for other benchmarks
	sub, err := subscriber.CreateSubscription(
		"bench-sub",
		"test-namespace", "test-topic",
		partition,
		schema_pb.OffsetType_RESET_TO_EARLIEST,
		0,
	)
	if err != nil {
		b.Fatalf("Failed to create subscription: %v", err)
	}

	b.Run("GetOffsetRange", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			sub.GetOffsetRange(100)
		}
	})

	b.Run("AdvanceOffset", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			sub.AdvanceOffset()
		}
	})

	b.Run("GetLag", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			sub.GetLag()
		}
	})

	b.Run("SeekToOffset", func(b *testing.B) {
		b.ResetTimer()
		for i := range b.N {
			offset := int64(i % 9000) // Stay within bounds
			sub.SeekToOffset(offset)
		}
	})
}

// BenchmarkSMQOffsetIntegration benchmarks the full integration layer
func BenchmarkSMQOffsetIntegration(b *testing.B) {
	storage := NewInMemoryOffsetStorage()
	integration := NewSMQOffsetIntegration(storage)

	partition := &schema_pb.Partition{
		RingSize:   1024,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: time.Now().UnixNano(),
	}

	b.Run("PublishRecord", func(b *testing.B) {
		b.ResetTimer()
		for i := range b.N {
			key := fmt.Sprintf("key-%d", i)
			integration.PublishRecord("test-namespace", "test-topic", partition, []byte(key), &schema_pb.RecordValue{})
		}
	})

	b.Run("PublishRecordBatch", func(b *testing.B) {
		batchSizes := []int{1, 10, 100}

		for _, batchSize := range batchSizes {
			b.Run(fmt.Sprintf("BatchSize%d", batchSize), func(b *testing.B) {
				b.ResetTimer()
				for i := range b.N {
					records := make([]PublishRecordRequest, batchSize)
					for j := range batchSize {
						records[j] = PublishRecordRequest{
							Key:   fmt.Appendf(nil, "batch-%d-key-%d", i, j),
							Value: &schema_pb.RecordValue{},
						}
					}
					integration.PublishRecordBatch("test-namespace", "test-topic", partition, records)
				}
			})
		}
	})

	// Pre-populate for subscription benchmarks
	records := make([]PublishRecordRequest, 1000)
	for i := range 1000 {
		records[i] = PublishRecordRequest{
			Key:   fmt.Appendf(nil, "pre-key-%d", i),
			Value: &schema_pb.RecordValue{},
		}
	}
	integration.PublishRecordBatch("test-namespace", "test-topic", partition, records)

	b.Run("CreateSubscription", func(b *testing.B) {
		b.ResetTimer()
		for i := range b.N {
			subscriptionID := fmt.Sprintf("integration-sub-%d", i)
			_, err := integration.CreateSubscription(
				subscriptionID,
				"test-namespace", "test-topic",
				partition,
				schema_pb.OffsetType_RESET_TO_EARLIEST,
				0,
			)
			if err != nil {
				b.Fatalf("Failed to create subscription: %v", err)
			}
			integration.CloseSubscription(subscriptionID)
		}
	})

	b.Run("GetHighWaterMark", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			integration.GetHighWaterMark("test-namespace", "test-topic", partition)
		}
	})

	b.Run("GetPartitionOffsetInfo", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			integration.GetPartitionOffsetInfo("test-namespace", "test-topic", partition)
		}
	})
}

// BenchmarkConcurrentOperations benchmarks concurrent offset operations
func BenchmarkConcurrentOperations(b *testing.B) {
	storage := NewInMemoryOffsetStorage()
	integration := NewSMQOffsetIntegration(storage)

	partition := &schema_pb.Partition{
		RingSize:   1024,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: time.Now().UnixNano(),
	}

	b.Run("ConcurrentPublish", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := fmt.Sprintf("concurrent-key-%d", i)
				integration.PublishRecord("test-namespace", "test-topic", partition, []byte(key), &schema_pb.RecordValue{})
				i++
			}
		})
	})

	// Pre-populate for concurrent reads
	for i := range 1000 {
		key := fmt.Sprintf("read-key-%d", i)
		integration.PublishRecord("test-namespace", "test-topic", partition, []byte(key), &schema_pb.RecordValue{})
	}

	b.Run("ConcurrentRead", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				integration.GetHighWaterMark("test-namespace", "test-topic", partition)
			}
		})
	})

	b.Run("ConcurrentMixed", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				if i%10 == 0 {
					// 10% writes
					key := fmt.Sprintf("mixed-key-%d", i)
					integration.PublishRecord("test-namespace", "test-topic", partition, []byte(key), &schema_pb.RecordValue{})
				} else {
					// 90% reads
					integration.GetHighWaterMark("test-namespace", "test-topic", partition)
				}
				i++
			}
		})
	})
}

// BenchmarkMemoryUsage benchmarks memory usage patterns
func BenchmarkMemoryUsage(b *testing.B) {
	b.Run("InMemoryStorage", func(b *testing.B) {
		storage := NewInMemoryOffsetStorage()
		partition := &schema_pb.Partition{
			RingSize:   1024,
			RangeStart: 0,
			RangeStop:  31,
			UnixTimeNs: time.Now().UnixNano(),
		}

		manager, err := NewPartitionOffsetManager("test-namespace", "test-topic", partition, storage)
		if err != nil {
			b.Fatalf("Failed to create partition manager: %v", err)
		}

		b.ResetTimer()
		for range b.N {
			manager.AssignOffset()
			// Note: Checkpointing now happens automatically in background every 2 seconds
		}

		// Clean up background goroutine
		manager.Close()
	})
}
