package sub_coordinator

import (
	"fmt"

	cmap "github.com/orcaman/concurrent-map/v2"

	"github.com/seaweedfs/seaweedfs/weed/filer_client"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

type TopicConsumerGroups struct {
	// map a consumer group name to a consumer group
	ConsumerGroups cmap.ConcurrentMap[string, *ConsumerGroup]
}

// SubCoordinator coordinates the instances in the consumer group for one topic.
// It is responsible for:
// 1. (Maybe) assigning partitions when a consumer instance is up/down.

type SubCoordinator struct {
	// map topic name to consumer groups
	TopicSubscribers    cmap.ConcurrentMap[string, *TopicConsumerGroups]
	FilerClientAccessor *filer_client.FilerClientAccessor
}

func NewSubCoordinator() *SubCoordinator {
	return &SubCoordinator{
		TopicSubscribers: cmap.New[*TopicConsumerGroups](),
	}
}

func (c *SubCoordinator) GetTopicConsumerGroups(topic *schema_pb.Topic, createIfMissing bool) *TopicConsumerGroups {
	topicName := toTopicName(topic)
	tcg, _ := c.TopicSubscribers.Get(topicName)
	if tcg == nil && createIfMissing {
		tcg = &TopicConsumerGroups{
			ConsumerGroups: cmap.New[*ConsumerGroup](),
		}
		if !c.TopicSubscribers.SetIfAbsent(topicName, tcg) {
			tcg, _ = c.TopicSubscribers.Get(topicName)
		}
	}

	return tcg
}
func (c *SubCoordinator) RemoveTopic(topic *schema_pb.Topic) {
	topicName := toTopicName(topic)
	c.TopicSubscribers.Remove(topicName)
}

func toTopicName(topic *schema_pb.Topic) string {
	topicName := topic.GetNamespace() + "." + topic.GetName()

	return topicName
}

func (c *SubCoordinator) AddSubscriber(initMessage *mq_pb.SubscriberToSubCoordinatorRequest_InitMessage) (*ConsumerGroup, *ConsumerGroupInstance, error) {
	tcg := c.GetTopicConsumerGroups(initMessage.GetTopic(), true)
	cg, _ := tcg.ConsumerGroups.Get(initMessage.GetConsumerGroup())
	if cg == nil {
		cg = NewConsumerGroup(initMessage.GetTopic(), initMessage.GetRebalanceSeconds(), c.FilerClientAccessor)
		if cg != nil {
			tcg.ConsumerGroups.SetIfAbsent(initMessage.GetConsumerGroup(), cg)
		}
		cg, _ = tcg.ConsumerGroups.Get(initMessage.GetConsumerGroup())
	}
	if cg == nil {
		return nil, nil, fmt.Errorf("fail to create consumer group %s: topic %s not found", initMessage.GetConsumerGroup(), initMessage.GetTopic())
	}
	cgi, _ := cg.ConsumerGroupInstances.Get(initMessage.GetConsumerGroupInstanceId())
	if cgi == nil {
		cgi = NewConsumerGroupInstance(initMessage.GetConsumerGroupInstanceId(), initMessage.GetMaxPartitionCount())
		if !cg.ConsumerGroupInstances.SetIfAbsent(initMessage.GetConsumerGroupInstanceId(), cgi) {
			cgi, _ = cg.ConsumerGroupInstances.Get(initMessage.GetConsumerGroupInstanceId())
		}
	}
	cgi.MaxPartitionCount = initMessage.GetMaxPartitionCount()
	cg.Market.AddConsumerInstance(cgi)

	return cg, cgi, nil
}

func (c *SubCoordinator) RemoveSubscriber(initMessage *mq_pb.SubscriberToSubCoordinatorRequest_InitMessage) {
	tcg := c.GetTopicConsumerGroups(initMessage.GetTopic(), false)
	if tcg == nil {
		return
	}
	cg, _ := tcg.ConsumerGroups.Get(initMessage.GetConsumerGroup())
	if cg == nil {
		return
	}
	cg.ConsumerGroupInstances.Remove(initMessage.GetConsumerGroupInstanceId())
	cg.Market.RemoveConsumerInstance(ConsumerGroupInstanceId(initMessage.GetConsumerGroupInstanceId()))
	if cg.ConsumerGroupInstances.Count() == 0 {
		tcg.ConsumerGroups.Remove(initMessage.GetConsumerGroup())
		cg.Shutdown()
	}
	if tcg.ConsumerGroups.Count() == 0 {
		c.RemoveTopic(initMessage.GetTopic())
	}
}

func (c *SubCoordinator) OnPartitionChange(topic *schema_pb.Topic, assignments []*mq_pb.BrokerPartitionAssignment) {
	tcg, _ := c.TopicSubscribers.Get(toTopicName(topic))
	if tcg == nil {
		return
	}
	for _, cg := range tcg.ConsumerGroups.Items() {
		cg.OnPartitionListChange(assignments)
	}
}
