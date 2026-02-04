package skiplist

import (
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

func LoadNameList(data []byte, store ListStore, batchSize int) *NameList {
	nl := &NameList{
		skipList:  New(store),
		batchSize: batchSize,
	}

	if len(data) == 0 {
		return nl
	}

	message := &SkipListProto{}
	if err := proto.Unmarshal(data, message); err != nil {
		glog.Errorf("loading skiplist: %v", err)
	}
	nl.skipList.MaxNewLevel = int(message.GetMaxNewLevel())
	nl.skipList.MaxLevel = int(message.GetMaxLevel())
	for i, ref := range message.GetStartLevels() {
		nl.skipList.StartLevels[i] = &SkipListElementReference{
			ElementPointer: ref.GetElementPointer(),
			Key:            ref.GetKey(),
		}
	}
	for i, ref := range message.GetEndLevels() {
		nl.skipList.EndLevels[i] = &SkipListElementReference{
			ElementPointer: ref.GetElementPointer(),
			Key:            ref.GetKey(),
		}
	}

	return nl
}

func (nl *NameList) HasChanges() bool {
	return nl.skipList.HasChanges
}

func (nl *NameList) ToBytes() []byte {
	message := &SkipListProto{}
	message.MaxNewLevel = int32(nl.skipList.MaxNewLevel)
	message.MaxLevel = int32(nl.skipList.MaxLevel)
	for _, ref := range nl.skipList.StartLevels {
		if ref == nil {
			break
		}
		message.StartLevels = append(message.StartLevels, &SkipListElementReference{
			ElementPointer: ref.GetElementPointer(),
			Key:            ref.GetKey(),
		})
	}
	for _, ref := range nl.skipList.EndLevels {
		if ref == nil {
			break
		}
		message.EndLevels = append(message.EndLevels, &SkipListElementReference{
			ElementPointer: ref.GetElementPointer(),
			Key:            ref.GetKey(),
		})
	}
	data, err := proto.Marshal(message)
	if err != nil {
		glog.Errorf("marshal skiplist: %v", err)
	}

	return data
}
