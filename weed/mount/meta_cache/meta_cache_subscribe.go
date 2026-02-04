package meta_cache

import (
	"context"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type MetadataFollower struct {
	PathPrefixToWatch string
	ProcessEventFn    func(resp *filer_pb.SubscribeMetadataResponse) error
}

func mergeProcessors(mainProcessor func(resp *filer_pb.SubscribeMetadataResponse) error, followers ...*MetadataFollower) func(resp *filer_pb.SubscribeMetadataResponse) error {
	return func(resp *filer_pb.SubscribeMetadataResponse) error {
		// build the full path
		entry := resp.GetEventNotification().GetNewEntry()
		if entry == nil {
			entry = resp.GetEventNotification().GetOldEntry()
		}
		if entry != nil {
			dir := resp.GetDirectory()
			if resp.GetEventNotification().GetNewParentPath() != "" {
				dir = resp.GetEventNotification().GetNewParentPath()
			}
			fp := util.NewFullPath(dir, entry.GetName())

			for _, follower := range followers {
				if strings.HasPrefix(string(fp), follower.PathPrefixToWatch) {
					if err := follower.ProcessEventFn(resp); err != nil {
						return err
					}
				}
			}
		}

		return mainProcessor(resp)
	}
}

func SubscribeMetaEvents(mc *MetaCache, selfSignature int32, client filer_pb.FilerClient, dir string, lastTsNs int64, onRetry func(lastTsNs int64, err error), followers ...*MetadataFollower) error {
	var prefixes []string
	for _, follower := range followers {
		prefixes = append(prefixes, follower.PathPrefixToWatch)
	}

	processEventFn := func(resp *filer_pb.SubscribeMetadataResponse) error {
		message := resp.GetEventNotification()

		for _, sig := range message.GetSignatures() {
			if sig == selfSignature && selfSignature != 0 {
				return nil
			}
		}

		dir := resp.GetDirectory()
		var oldPath util.FullPath
		var newEntry *filer.Entry
		if message.GetOldEntry() != nil {
			oldPath = util.NewFullPath(dir, message.GetOldEntry().GetName())
			glog.V(4).Infof("deleting %v", oldPath)
		}

		if message.GetNewEntry() != nil {
			if message.GetNewParentPath() != "" {
				dir = message.GetNewParentPath()
			}
			key := util.NewFullPath(dir, message.GetNewEntry().GetName())
			glog.V(4).Infof("creating %v", key)
			newEntry = filer.FromPbEntry(dir, message.GetNewEntry())
		}
		err := mc.AtomicUpdateEntryFromFiler(context.Background(), oldPath, newEntry)
		if err == nil {
			if message.GetNewEntry() != nil || message.GetOldEntry() != nil {
				dirsToNotify := make(map[util.FullPath]struct{})
				if oldPath != "" {
					parent, _ := oldPath.DirAndName()
					dirsToNotify[util.FullPath(parent)] = struct{}{}
				}
				if newEntry != nil {
					newParent, _ := newEntry.DirAndName()
					dirsToNotify[util.FullPath(newParent)] = struct{}{}
				}
				if message.GetNewEntry() != nil && message.GetNewEntry().GetIsDirectory() {
					childPath := util.NewFullPath(dir, message.GetNewEntry().GetName())
					dirsToNotify[childPath] = struct{}{}
				}
				for dirPath := range dirsToNotify {
					mc.noteDirectoryUpdate(dirPath)
				}
			}
			if message.GetOldEntry() != nil && message.GetNewEntry() != nil {
				oldKey := util.NewFullPath(resp.GetDirectory(), message.GetOldEntry().GetName())
				mc.invalidateFunc(oldKey, message.GetOldEntry())
				if message.GetOldEntry().GetName() != message.GetNewEntry().GetName() {
					newKey := util.NewFullPath(dir, message.GetNewEntry().GetName())
					mc.invalidateFunc(newKey, message.GetNewEntry())
				}
			} else if filer_pb.IsCreate(resp) {
				// no need to invalidate
			} else if filer_pb.IsDelete(resp) {
				oldKey := util.NewFullPath(resp.GetDirectory(), message.GetOldEntry().GetName())
				mc.invalidateFunc(oldKey, message.GetOldEntry())
			}
		}

		return err
	}

	prefix := dir
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	metadataFollowOption := &pb.MetadataFollowOption{
		ClientName:             "mount",
		ClientId:               selfSignature,
		ClientEpoch:            1,
		SelfSignature:          selfSignature,
		PathPrefix:             prefix,
		AdditionalPathPrefixes: prefixes,
		DirectoriesToWatch:     nil,
		StartTsNs:              lastTsNs,
		StopTsNs:               0,
		EventErrorType:         pb.FatalOnError,
	}
	util.RetryUntil("followMetaUpdates", func() error {
		metadataFollowOption.ClientEpoch++

		return pb.WithFilerClientFollowMetadata(client, metadataFollowOption, mergeProcessors(processEventFn, followers...))
	}, func(err error) bool {
		if onRetry != nil {
			onRetry(metadataFollowOption.StartTsNs, err)
		}
		glog.Errorf("follow metadata updates: %v", err)

		return true
	})

	return nil
}
