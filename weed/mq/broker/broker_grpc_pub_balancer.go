package broker

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

// PublisherToPubBalancer receives connections from brokers and collects stats
func (b *MessageQueueBroker) PublisherToPubBalancer(stream mq_pb.SeaweedMessaging_PublisherToPubBalancerServer) error {
	if !b.isLockOwner() {
		return status.Errorf(codes.Unavailable, "not current broker balancer")
	}
	req, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("receive init message: %w", err)
	}

	// process init message
	initMessage := req.GetInit()
	var brokerStats *pub_balancer.BrokerStats
	if initMessage != nil {
		brokerStats = b.PubBalancer.AddBroker(initMessage.GetBroker())
	} else {
		return status.Errorf(codes.InvalidArgument, "balancer init message is empty")
	}
	defer func() {
		b.PubBalancer.RemoveBroker(initMessage.GetBroker(), brokerStats)
	}()

	// process stats message
	for {
		req, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("receive stats message from %s: %w", initMessage.GetBroker(), err)
		}
		if !b.isLockOwner() {
			return status.Errorf(codes.Unavailable, "not current broker balancer")
		}
		if receivedStats := req.GetStats(); receivedStats != nil {
			b.PubBalancer.OnBrokerStatsUpdated(initMessage.GetBroker(), brokerStats, receivedStats)
			// glog.V(4).Infof("received from %v: %+v", initMessage.Broker, receivedStats)
		}
	}
}
