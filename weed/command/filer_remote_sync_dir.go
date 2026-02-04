package command

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func followUpdatesAndUploadToRemote(option *RemoteSyncOptions, filerSource *source.FilerSource, mountedDir string) error {
	// read filer remote storage mount mappings
	_, _, remoteStorageMountLocation, remoteStorage, detectErr := filer.DetectMountInfo(option.grpcDialOption, pb.ServerAddress(*option.filerAddress), mountedDir)
	if detectErr != nil {
		return fmt.Errorf("read mount info: %w", detectErr)
	}

	eachEntryFunc, err := option.makeEventProcessor(remoteStorage, mountedDir, remoteStorageMountLocation, filerSource)
	if err != nil {
		return err
	}

	lastOffsetTs := collectLastSyncOffset(option, option.grpcDialOption, pb.ServerAddress(*option.filerAddress), mountedDir, *option.timeAgo)
	processor := NewMetadataProcessor(eachEntryFunc, 128, lastOffsetTs.UnixNano())

	var lastLogTsNs = time.Now().UnixNano()
	processEventFnWithOffset := pb.AddOffsetFunc(func(resp *filer_pb.SubscribeMetadataResponse) error {
		if resp.GetEventNotification().GetNewEntry() != nil {
			if *option.storageClass == "" {
				delete(resp.GetEventNotification().GetNewEntry().GetExtended(), s3_constants.AmzStorageClass)
			} else {
				resp.EventNotification.NewEntry.Extended[s3_constants.AmzStorageClass] = []byte(*option.storageClass)
			}
		}

		processor.AddSyncJob(resp)

		return nil
	}, 3*time.Second, func(counter int64, lastTsNs int64) error {
		offsetTsNs := processor.processedTsWatermark.Load()
		if offsetTsNs == 0 {
			return nil
		}
		// use processor.processedTsWatermark instead of the lastTsNs from the most recent job
		now := time.Now().UnixNano()
		glog.V(0).Infof("remote sync %s progressed to %v %0.2f/sec", *option.filerAddress, time.Unix(0, offsetTsNs), float64(counter)/(float64(now-lastLogTsNs)/1e9))
		lastLogTsNs = now

		return remote_storage.SetSyncOffset(option.grpcDialOption, pb.ServerAddress(*option.filerAddress), mountedDir, offsetTsNs)
	})

	option.clientEpoch++

	prefix := mountedDir
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	metadataFollowOption := &pb.MetadataFollowOption{
		ClientName:             "filer.remote.sync",
		ClientId:               option.clientId,
		ClientEpoch:            option.clientEpoch,
		SelfSignature:          0,
		PathPrefix:             prefix,
		AdditionalPathPrefixes: []string{filer.DirectoryEtcRemote},
		DirectoriesToWatch:     nil,
		StartTsNs:              lastOffsetTs.UnixNano(),
		StopTsNs:               0,
		EventErrorType:         pb.RetryForeverOnError,
	}

	return pb.FollowMetadata(pb.ServerAddress(*option.filerAddress), option.grpcDialOption, metadataFollowOption, processEventFnWithOffset)
}

func (option *RemoteSyncOptions) makeEventProcessor(remoteStorage *remote_pb.RemoteConf, mountedDir string, remoteStorageMountLocation *remote_pb.RemoteStorageLocation, filerSource *source.FilerSource) (pb.ProcessMetadataFunc, error) {
	client, err := remote_storage.GetRemoteStorage(remoteStorage)
	if err != nil {
		return nil, err
	}

	handleEtcRemoteChanges := func(resp *filer_pb.SubscribeMetadataResponse) error {
		message := resp.GetEventNotification()
		if message.GetNewEntry() == nil {
			return nil
		}
		if message.GetNewEntry().GetName() == filer.REMOTE_STORAGE_MOUNT_FILE {
			mappings, readErr := filer.UnmarshalRemoteStorageMappings(message.GetNewEntry().GetContent())
			if readErr != nil {
				return fmt.Errorf("unmarshal mappings: %w", readErr)
			}
			if remoteLoc, found := mappings.GetMappings()[mountedDir]; found {
				if remoteStorageMountLocation.GetBucket() != remoteLoc.GetBucket() || remoteStorageMountLocation.GetPath() != remoteLoc.GetPath() {
					glog.Fatalf("Unexpected mount changes %+v => %+v", remoteStorageMountLocation, remoteLoc)
				}
			} else {
				glog.V(0).Infof("unmounted %s exiting ...", mountedDir)
				os.Exit(0)
			}
		}
		if message.GetNewEntry().GetName() == remoteStorage.GetName()+filer.REMOTE_STORAGE_CONF_SUFFIX {
			conf := &remote_pb.RemoteConf{}
			if err := proto.Unmarshal(message.GetNewEntry().GetContent(), conf); err != nil {
				return fmt.Errorf("unmarshal %s/%s: %w", filer.DirectoryEtcRemote, message.GetNewEntry().GetName(), err)
			}
			remoteStorage = conf
			if newClient, err := remote_storage.GetRemoteStorage(remoteStorage); err == nil {
				client = newClient
			} else {
				return err
			}
		}

		return nil
	}

	eachEntryFunc := func(resp *filer_pb.SubscribeMetadataResponse) error {
		message := resp.GetEventNotification()
		if strings.HasPrefix(resp.GetDirectory(), filer.DirectoryEtcRemote) {
			return handleEtcRemoteChanges(resp)
		}

		if filer_pb.IsEmpty(resp) {
			return nil
		}
		if filer_pb.IsCreate(resp) {
			if isMultipartUploadFile(message.GetNewParentPath(), message.GetNewEntry().GetName()) {
				return nil
			}
			if !filer.HasData(message.GetNewEntry()) {
				return nil
			}
			glog.V(2).Infof("create: %+v", resp)
			if !shouldSendToRemote(message.GetNewEntry()) {
				glog.V(2).Infof("skipping creating: %+v", resp)

				return nil
			}
			dest := toRemoteStorageLocation(util.FullPath(mountedDir), util.NewFullPath(message.GetNewParentPath(), message.GetNewEntry().GetName()), remoteStorageMountLocation)
			if message.GetNewEntry().GetIsDirectory() {
				glog.V(0).Infof("mkdir  %s", remote_storage.FormatLocation(dest))

				return client.WriteDirectory(dest, message.GetNewEntry())
			}
			glog.V(0).Infof("create %s", remote_storage.FormatLocation(dest))
			remoteEntry, writeErr := retriedWriteFile(client, filerSource, message.GetNewEntry(), dest)
			if writeErr != nil {
				return writeErr
			}

			return updateLocalEntry(option, message.GetNewParentPath(), message.GetNewEntry(), remoteEntry)
		}
		if filer_pb.IsDelete(resp) {
			glog.V(2).Infof("delete: %+v", resp)
			dest := toRemoteStorageLocation(util.FullPath(mountedDir), util.NewFullPath(resp.GetDirectory(), message.GetOldEntry().GetName()), remoteStorageMountLocation)
			if message.GetOldEntry().GetIsDirectory() {
				glog.V(0).Infof("rmdir  %s", remote_storage.FormatLocation(dest))

				return client.RemoveDirectory(dest)
			}
			glog.V(0).Infof("delete %s", remote_storage.FormatLocation(dest))

			return client.DeleteFile(dest)
		}
		if message.GetOldEntry() != nil && message.GetNewEntry() != nil {
			if isMultipartUploadFile(message.GetNewParentPath(), message.GetNewEntry().GetName()) {
				return nil
			}
			oldDest := toRemoteStorageLocation(util.FullPath(mountedDir), util.NewFullPath(resp.GetDirectory(), message.GetOldEntry().GetName()), remoteStorageMountLocation)
			dest := toRemoteStorageLocation(util.FullPath(mountedDir), util.NewFullPath(message.GetNewParentPath(), message.GetNewEntry().GetName()), remoteStorageMountLocation)
			if !shouldSendToRemote(message.GetNewEntry()) {
				glog.V(2).Infof("skipping updating: %+v", resp)

				return nil
			}
			if message.GetNewEntry().GetIsDirectory() {
				return client.WriteDirectory(dest, message.GetNewEntry())
			}
			if resp.GetDirectory() == message.GetNewParentPath() && message.GetOldEntry().GetName() == message.GetNewEntry().GetName() {
				if filer.IsSameData(message.GetOldEntry(), message.GetNewEntry()) {
					glog.V(2).Infof("update meta: %+v", resp)

					return client.UpdateFileMetadata(dest, message.GetOldEntry(), message.GetNewEntry())
				}
			}
			glog.V(2).Infof("update: %+v", resp)
			glog.V(0).Infof("delete %s", remote_storage.FormatLocation(oldDest))
			if err := client.DeleteFile(oldDest); err != nil {
				if isMultipartUploadFile(resp.GetDirectory(), message.GetOldEntry().GetName()) {
					return nil
				}
			}
			remoteEntry, writeErr := retriedWriteFile(client, filerSource, message.GetNewEntry(), dest)
			if writeErr != nil {
				return writeErr
			}

			return updateLocalEntry(option, message.GetNewParentPath(), message.GetNewEntry(), remoteEntry)
		}

		return nil
	}

	return eachEntryFunc, nil
}

func retriedWriteFile(client remote_storage.RemoteStorageClient, filerSource *source.FilerSource, newEntry *filer_pb.Entry, dest *remote_pb.RemoteStorageLocation) (remoteEntry *filer_pb.RemoteEntry, err error) {
	var writeErr error
	err = util.Retry("writeFile", func() error {
		reader := filer.NewFileReader(filerSource, newEntry)
		glog.V(0).Infof("create %s", remote_storage.FormatLocation(dest))
		remoteEntry, writeErr = client.WriteFile(dest, newEntry, reader)
		if writeErr != nil {
			return writeErr
		}

		return nil
	})
	if err != nil {
		glog.Errorf("write to %s: %v", dest, err)
	}

	return
}

func collectLastSyncOffset(filerClient filer_pb.FilerClient, grpcDialOption grpc.DialOption, filerAddress pb.ServerAddress, mountedDir string, timeAgo time.Duration) time.Time {
	// 1. specified by timeAgo
	// 2. last offset timestamp for this directory
	// 3. directory creation time
	var lastOffsetTs time.Time
	if timeAgo == 0 {
		mountedDirEntry, err := filer_pb.GetEntry(context.Background(), filerClient, util.FullPath(mountedDir))
		if err != nil {
			glog.V(0).Infof("get mounted directory %s: %v", mountedDir, err)

			return time.Now()
		}

		lastOffsetTsNs, err := remote_storage.GetSyncOffset(grpcDialOption, filerAddress, mountedDir)
		if mountedDirEntry != nil {
			if err == nil && mountedDirEntry.GetAttributes().GetCrtime() < lastOffsetTsNs/1000000 {
				lastOffsetTs = time.Unix(0, lastOffsetTsNs)
				glog.V(0).Infof("resume from %v", lastOffsetTs)
			} else {
				lastOffsetTs = time.Unix(mountedDirEntry.GetAttributes().GetCrtime(), 0)
			}
		} else {
			lastOffsetTs = time.Now()
		}
	} else {
		lastOffsetTs = time.Now().Add(-timeAgo)
	}

	return lastOffsetTs
}

func toRemoteStorageLocation(mountDir, sourcePath util.FullPath, remoteMountLocation *remote_pb.RemoteStorageLocation) *remote_pb.RemoteStorageLocation {
	source := string(sourcePath[len(mountDir):])
	dest := util.FullPath(remoteMountLocation.GetPath()).Child(source)

	return &remote_pb.RemoteStorageLocation{
		Name:   remoteMountLocation.GetName(),
		Bucket: remoteMountLocation.GetBucket(),
		Path:   string(dest),
	}
}

func shouldSendToRemote(entry *filer_pb.Entry) bool {
	if entry.GetRemoteEntry() == nil {
		return true
	}
	if entry.GetRemoteEntry().GetRemoteMtime() < entry.GetAttributes().GetMtime() {
		return true
	}

	return false
}

func updateLocalEntry(filerClient filer_pb.FilerClient, dir string, entry *filer_pb.Entry, remoteEntry *filer_pb.RemoteEntry) error {
	remoteEntry.LastLocalSyncTsNs = time.Now().UnixNano()
	entry.RemoteEntry = remoteEntry

	return filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{
			Directory: dir,
			Entry:     entry,
		})

		return err
	})
}

func isMultipartUploadFile(dir string, name string) bool {
	return isMultipartUploadDir(dir) && strings.HasSuffix(name, ".part")
}

func isMultipartUploadDir(dir string) bool {
	return strings.HasPrefix(dir, "/buckets/") &&
		strings.Contains(dir, "/"+s3_constants.MultipartUploadsFolder+"/")
}
