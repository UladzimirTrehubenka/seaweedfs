package replication

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/sink"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type Replicator struct {
	sink        sink.ReplicationSink
	source      *source.FilerSource
	excludeDirs []string
}

func NewReplicator(sourceConfig util.Configuration, configPrefix string, dataSink sink.ReplicationSink) *Replicator {
	source := &source.FilerSource{}
	source.Initialize(sourceConfig, configPrefix)

	dataSink.SetSourceFiler(source)

	return &Replicator{
		sink:        dataSink,
		source:      source,
		excludeDirs: sourceConfig.GetStringSlice(configPrefix + "excludeDirectories"),
	}
}

func (r *Replicator) Replicate(ctx context.Context, key string, message *filer_pb.EventNotification) error {
	if message.GetIsFromOtherCluster() && r.sink.GetName() == "filer" {
		return nil
	}
	if !strings.HasPrefix(key, r.source.Dir) {
		glog.V(4).Infof("skipping %v outside of %v", key, r.source.Dir)

		return nil
	}
	for _, excludeDir := range r.excludeDirs {
		if strings.HasPrefix(key, excludeDir) {
			glog.V(4).Infof("skipping %v of exclude dir %v", key, excludeDir)

			return nil
		}
	}

	var dateKey string
	if r.sink.IsIncremental() {
		var mTime int64
		if message.GetNewEntry() != nil {
			mTime = message.GetNewEntry().GetAttributes().GetMtime()
		} else if message.GetOldEntry() != nil {
			mTime = message.GetOldEntry().GetAttributes().GetMtime()
		}
		dateKey = time.Unix(mTime, 0).Format("2006-01-02")
	}
	newKey := util.Join(r.sink.GetSinkToDirectory(), dateKey, key[len(r.source.Dir):])
	glog.V(3).Infof("replicate %s => %s", key, newKey)
	key = newKey
	if message.GetOldEntry() != nil && message.GetNewEntry() == nil {
		glog.V(4).Infof("deleting %v", key)

		return r.sink.DeleteEntry(key, message.GetOldEntry().GetIsDirectory(), message.GetDeleteChunks(), message.GetSignatures())
	}
	if message.GetOldEntry() == nil && message.GetNewEntry() != nil {
		glog.V(4).Infof("creating %v", key)

		return r.sink.CreateEntry(key, message.GetNewEntry(), message.GetSignatures())
	}
	if message.GetOldEntry() == nil && message.GetNewEntry() == nil {
		glog.V(0).Infof("weird message %+v", message)

		return nil
	}

	foundExisting, err := r.sink.UpdateEntry(key, message.GetOldEntry(), message.GetNewParentPath(), message.GetNewEntry(), message.GetDeleteChunks(), message.GetSignatures())
	if foundExisting {
		glog.V(4).Infof("updated %v", key)

		return err
	}

	err = r.sink.DeleteEntry(key, message.GetOldEntry().GetIsDirectory(), false, message.GetSignatures())
	if err != nil {
		return fmt.Errorf("delete old entry %v: %w", key, err)
	}

	glog.V(4).Infof("creating missing %v", key)

	return r.sink.CreateEntry(key, message.GetNewEntry(), message.GetSignatures())
}

func ReadFilerSignature(grpcDialOption grpc.DialOption, filer pb.ServerAddress) (filerSignature int32, readErr error) {
	if readErr = pb.WithFilerClient(false, 0, filer, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		if resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{}); err != nil {
			return fmt.Errorf("GetFilerConfiguration %s: %w", filer, err)
		} else {
			filerSignature = resp.GetSignature()
		}

		return nil
	}); readErr != nil {
		return 0, readErr
	}

	return filerSignature, nil
}
