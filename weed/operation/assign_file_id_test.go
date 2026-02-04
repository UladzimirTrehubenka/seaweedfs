package operation

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb"
)

func BenchmarkWithConcurrency(b *testing.B) {
	concurrencyLevels := []int{1, 10, 100, 1000}

	ap, _ := NewAssignProxy(func(_ context.Context) pb.ServerAddress {
		return pb.ServerAddress("localhost:9333")
	}, grpc.WithInsecure(), 16)

	for _, concurrency := range concurrencyLevels {
		b.Run(
			fmt.Sprintf("Concurrency-%d", concurrency),
			func(b *testing.B) {
				for range b.N {
					done := make(chan struct{})
					startTime := time.Now()

					for range concurrency {
						go func() {
							ap.Assign(&VolumeAssignRequest{
								Count: 1,
							})

							done <- struct{}{}
						}()
					}

					for range concurrency {
						<-done
					}

					duration := time.Since(startTime)
					b.Logf("Concurrency: %d, Duration: %v", concurrency, duration)
				}
			},
		)
	}
}

func BenchmarkStreamAssign(b *testing.B) {
	ap, _ := NewAssignProxy(func(_ context.Context) pb.ServerAddress {
		return pb.ServerAddress("localhost:9333")
	}, grpc.WithInsecure(), 16)
	for b.Loop() {
		ap.Assign(&VolumeAssignRequest{
			Count: 1,
		})
	}
}

func BenchmarkUnaryAssign(b *testing.B) {
	for b.Loop() {
		Assign(context.Background(), func(_ context.Context) pb.ServerAddress {
			return pb.ServerAddress("localhost:9333")
		}, grpc.WithInsecure(), &VolumeAssignRequest{
			Count: 1,
		})
	}
}
