package command

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"strings"
	"time"

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

func (option *RemoteGatewayOptions) followBucketUpdatesAndUploadToRemote(filerSource *source.FilerSource) error {
	// read filer remote storage mount mappings
	if detectErr := option.collectRemoteStorageConf(); detectErr != nil {
		return fmt.Errorf("read mount info: %w", detectErr)
	}

	eachEntryFunc, err := option.makeBucketedEventProcessor(filerSource)
	if err != nil {
		return err
	}

	lastOffsetTs := collectLastSyncOffset(option, option.grpcDialOption, pb.ServerAddress(*option.filerAddress), option.bucketsDir, *option.timeAgo)
	processor := NewMetadataProcessor(eachEntryFunc, 128, lastOffsetTs.UnixNano())

	var lastLogTsNs = time.Now().UnixNano()
	processEventFnWithOffset := pb.AddOffsetFunc(func(resp *filer_pb.SubscribeMetadataResponse) error {
		processor.AddSyncJob(resp)

		return nil
	}, 3*time.Second, func(counter int64, lastTsNs int64) error {
		offsetTsNs := processor.processedTsWatermark.Load()
		if offsetTsNs == 0 {
			return nil
		}
		now := time.Now().UnixNano()
		glog.V(0).Infof("remote sync %s progressed to %v %0.2f/sec", *option.filerAddress, time.Unix(0, offsetTsNs), float64(counter)/(float64(now-lastLogTsNs)/1e9))
		lastLogTsNs = now

		return remote_storage.SetSyncOffset(option.grpcDialOption, pb.ServerAddress(*option.filerAddress), option.bucketsDir, offsetTsNs)
	})

	option.clientEpoch++

	metadataFollowOption := &pb.MetadataFollowOption{
		ClientName:             "filer.remote.sync",
		ClientId:               option.clientId,
		ClientEpoch:            option.clientEpoch,
		SelfSignature:          0,
		PathPrefix:             option.bucketsDir + "/",
		AdditionalPathPrefixes: []string{filer.DirectoryEtcRemote},
		DirectoriesToWatch:     nil,
		StartTsNs:              lastOffsetTs.UnixNano(),
		StopTsNs:               0,
		EventErrorType:         pb.RetryForeverOnError,
	}

	return pb.FollowMetadata(pb.ServerAddress(*option.filerAddress), option.grpcDialOption, metadataFollowOption, processEventFnWithOffset)
}

func (option *RemoteGatewayOptions) makeBucketedEventProcessor(filerSource *source.FilerSource) (pb.ProcessMetadataFunc, error) {
	handleCreateBucket := func(entry *filer_pb.Entry) error {
		if !entry.GetIsDirectory() {
			return nil
		}
		if entry.GetRemoteEntry() != nil {
			// this directory is imported from "remote.mount.buckets" or "remote.mount"
			return nil
		}
		if option.mappings.GetPrimaryBucketStorageName() != "" && *option.createBucketAt == "" {
			*option.createBucketAt = option.mappings.GetPrimaryBucketStorageName()
			glog.V(0).Infof("%s is set as the primary remote storage", *option.createBucketAt)
		}
		if len(option.mappings.GetMappings()) == 1 && *option.createBucketAt == "" {
			for k := range option.mappings.GetMappings() {
				*option.createBucketAt = k
				glog.V(0).Infof("%s is set as the only remote storage", *option.createBucketAt)
			}
		}
		if *option.createBucketAt == "" {
			return nil
		}
		remoteConf, found := option.remoteConfs[*option.createBucketAt]
		if !found {
			return fmt.Errorf("un-configured remote storage %s", *option.createBucketAt)
		}

		client, err := remote_storage.GetRemoteStorage(remoteConf)
		if err != nil {
			return err
		}

		bucketName := strings.ToLower(entry.GetName())
		if *option.include != "" {
			if ok, _ := filepath.Match(*option.include, entry.GetName()); !ok {
				return nil
			}
		}
		if *option.exclude != "" {
			if ok, _ := filepath.Match(*option.exclude, entry.GetName()); ok {
				return nil
			}
		}

		bucketPath := util.FullPath(option.bucketsDir).Child(entry.GetName())
		remoteLocation, found := option.mappings.GetMappings()[string(bucketPath)]
		if !found {
			if *option.createBucketRandomSuffix {
				// https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
				if len(bucketName)+5 > 63 {
					bucketName = bucketName[:58]
				}
				bucketName = fmt.Sprintf("%s-%04d", bucketName, rand.Uint32()%10000)
			}
			remoteLocation = &remote_pb.RemoteStorageLocation{
				Name:   *option.createBucketAt,
				Bucket: bucketName,
				Path:   "/",
			}
			// need to add new mapping here before getting updates from metadata tailing
			option.mappings.Mappings[string(bucketPath)] = remoteLocation
		} else {
			bucketName = remoteLocation.GetBucket()
		}

		glog.V(0).Infof("create bucket %s", bucketName)
		if err := client.CreateBucket(bucketName); err != nil {
			return fmt.Errorf("create bucket %s in %s: %w", bucketName, remoteConf.GetName(), err)
		}

		return filer.InsertMountMapping(option, string(bucketPath), remoteLocation)
	}
	handleDeleteBucket := func(entry *filer_pb.Entry) error {
		if !entry.GetIsDirectory() {
			return nil
		}

		client, remoteStorageMountLocation, err := option.findRemoteStorageClient(entry.GetName())
		if err != nil {
			return fmt.Errorf("findRemoteStorageClient %s: %w", entry.GetName(), err)
		}

		glog.V(0).Infof("delete remote bucket %s", remoteStorageMountLocation.GetBucket())
		if err := client.DeleteBucket(remoteStorageMountLocation.GetBucket()); err != nil {
			return fmt.Errorf("delete remote bucket %s: %w", remoteStorageMountLocation.GetBucket(), err)
		}

		bucketPath := util.FullPath(option.bucketsDir).Child(entry.GetName())

		return filer.DeleteMountMapping(option, string(bucketPath))
	}

	handleEtcRemoteChanges := func(resp *filer_pb.SubscribeMetadataResponse) error {
		message := resp.GetEventNotification()
		if message.GetNewEntry() != nil {
			// update
			if message.GetNewEntry().GetName() == filer.REMOTE_STORAGE_MOUNT_FILE {
				newMappings, readErr := filer.UnmarshalRemoteStorageMappings(message.GetNewEntry().GetContent())
				if readErr != nil {
					return fmt.Errorf("unmarshal mappings: %w", readErr)
				}
				option.mappings = newMappings
			}
			if strings.HasSuffix(message.GetNewEntry().GetName(), filer.REMOTE_STORAGE_CONF_SUFFIX) {
				conf := &remote_pb.RemoteConf{}
				if err := proto.Unmarshal(message.GetNewEntry().GetContent(), conf); err != nil {
					return fmt.Errorf("unmarshal %s/%s: %w", filer.DirectoryEtcRemote, message.GetNewEntry().GetName(), err)
				}
				option.remoteConfs[conf.GetName()] = conf
			}
		} else if message.GetOldEntry() != nil {
			// deletion
			if strings.HasSuffix(message.GetOldEntry().GetName(), filer.REMOTE_STORAGE_CONF_SUFFIX) {
				conf := &remote_pb.RemoteConf{}
				if err := proto.Unmarshal(message.GetOldEntry().GetContent(), conf); err != nil {
					return fmt.Errorf("unmarshal %s/%s: %w", filer.DirectoryEtcRemote, message.GetOldEntry().GetName(), err)
				}
				delete(option.remoteConfs, conf.GetName())
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
			if message.GetNewParentPath() == option.bucketsDir {
				return handleCreateBucket(message.GetNewEntry())
			}
			if isMultipartUploadFile(message.GetNewParentPath(), message.GetNewEntry().GetName()) {
				return nil
			}
			if !filer.HasData(message.GetNewEntry()) {
				return nil
			}
			bucket, remoteStorageMountLocation, remoteStorage, ok := option.detectBucketInfo(message.GetNewParentPath())
			if !ok {
				return nil
			}
			client, err := remote_storage.GetRemoteStorage(remoteStorage)
			if err != nil {
				return err
			}
			glog.V(2).Infof("create: %+v", resp)
			if !shouldSendToRemote(message.GetNewEntry()) {
				glog.V(2).Infof("skipping creating: %+v", resp)

				return nil
			}
			dest := toRemoteStorageLocation(bucket, util.NewFullPath(message.GetNewParentPath(), message.GetNewEntry().GetName()), remoteStorageMountLocation)
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
			if resp.GetDirectory() == option.bucketsDir {
				return handleDeleteBucket(message.GetOldEntry())
			}
			bucket, remoteStorageMountLocation, remoteStorage, ok := option.detectBucketInfo(resp.GetDirectory())
			if !ok {
				return nil
			}
			client, err := remote_storage.GetRemoteStorage(remoteStorage)
			if err != nil {
				return err
			}
			glog.V(2).Infof("delete: %+v", resp)
			dest := toRemoteStorageLocation(bucket, util.NewFullPath(resp.GetDirectory(), message.GetOldEntry().GetName()), remoteStorageMountLocation)
			if message.GetOldEntry().GetIsDirectory() {
				glog.V(0).Infof("rmdir  %s", remote_storage.FormatLocation(dest))

				return client.RemoveDirectory(dest)
			}
			glog.V(0).Infof("delete %s", remote_storage.FormatLocation(dest))

			return client.DeleteFile(dest)
		}
		if message.GetOldEntry() != nil && message.GetNewEntry() != nil {
			if resp.GetDirectory() == option.bucketsDir {
				if message.GetNewParentPath() == option.bucketsDir {
					if message.GetOldEntry().GetName() == message.GetNewEntry().GetName() {
						return nil
					}
					if err := handleCreateBucket(message.GetNewEntry()); err != nil {
						return err
					}
					if err := handleDeleteBucket(message.GetOldEntry()); err != nil {
						return err
					}
				}
			}
			if isMultipartUploadFile(message.GetNewParentPath(), message.GetNewEntry().GetName()) {
				return nil
			}
			oldBucket, oldRemoteStorageMountLocation, oldRemoteStorage, oldOk := option.detectBucketInfo(resp.GetDirectory())
			newBucket, newRemoteStorageMountLocation, newRemoteStorage, newOk := option.detectBucketInfo(message.GetNewParentPath())
			if oldOk && newOk {
				if !shouldSendToRemote(message.GetNewEntry()) {
					glog.V(2).Infof("skipping updating: %+v", resp)

					return nil
				}
				client, err := remote_storage.GetRemoteStorage(oldRemoteStorage)
				if err != nil {
					return err
				}
				if resp.GetDirectory() == message.GetNewParentPath() && message.GetOldEntry().GetName() == message.GetNewEntry().GetName() {
					// update the same entry
					if message.GetNewEntry().GetIsDirectory() {
						// update directory property
						return nil
					}
					if message.GetOldEntry().GetRemoteEntry() != nil && filer.IsSameData(message.GetOldEntry(), message.GetNewEntry()) {
						glog.V(2).Infof("update meta: %+v", resp)
						oldDest := toRemoteStorageLocation(oldBucket, util.NewFullPath(resp.GetDirectory(), message.GetOldEntry().GetName()), oldRemoteStorageMountLocation)

						return client.UpdateFileMetadata(oldDest, message.GetOldEntry(), message.GetNewEntry())
					} else {
						newDest := toRemoteStorageLocation(newBucket, util.NewFullPath(message.GetNewParentPath(), message.GetNewEntry().GetName()), newRemoteStorageMountLocation)
						remoteEntry, writeErr := retriedWriteFile(client, filerSource, message.GetNewEntry(), newDest)
						if writeErr != nil {
							return writeErr
						}

						return updateLocalEntry(option, message.GetNewParentPath(), message.GetNewEntry(), remoteEntry)
					}
				}
			}

			// the following is entry rename
			if oldOk {
				client, err := remote_storage.GetRemoteStorage(oldRemoteStorage)
				if err != nil {
					return err
				}
				oldDest := toRemoteStorageLocation(oldBucket, util.NewFullPath(resp.GetDirectory(), message.GetOldEntry().GetName()), oldRemoteStorageMountLocation)
				if message.GetOldEntry().GetIsDirectory() {
					return client.RemoveDirectory(oldDest)
				}
				glog.V(0).Infof("delete %s", remote_storage.FormatLocation(oldDest))
				if err := client.DeleteFile(oldDest); err != nil {
					return err
				}
			}
			if newOk {
				if !shouldSendToRemote(message.GetNewEntry()) {
					glog.V(2).Infof("skipping updating: %+v", resp)

					return nil
				}
				client, err := remote_storage.GetRemoteStorage(newRemoteStorage)
				if err != nil {
					return err
				}
				newDest := toRemoteStorageLocation(newBucket, util.NewFullPath(message.GetNewParentPath(), message.GetNewEntry().GetName()), newRemoteStorageMountLocation)
				if message.GetNewEntry().GetIsDirectory() {
					return client.WriteDirectory(newDest, message.GetNewEntry())
				}
				remoteEntry, writeErr := retriedWriteFile(client, filerSource, message.GetNewEntry(), newDest)
				if writeErr != nil {
					return writeErr
				}

				return updateLocalEntry(option, message.GetNewParentPath(), message.GetNewEntry(), remoteEntry)
			}
		}

		return nil
	}

	return eachEntryFunc, nil
}

func (option *RemoteGatewayOptions) findRemoteStorageClient(bucketName string) (client remote_storage.RemoteStorageClient, remoteStorageMountLocation *remote_pb.RemoteStorageLocation, err error) {
	bucket := util.FullPath(option.bucketsDir).Child(bucketName)

	var isMounted bool
	remoteStorageMountLocation, isMounted = option.mappings.GetMappings()[string(bucket)]
	if !isMounted {
		return nil, remoteStorageMountLocation, fmt.Errorf("%s is not mounted", bucket)
	}
	remoteConf, hasClient := option.remoteConfs[remoteStorageMountLocation.GetName()]
	if !hasClient {
		return nil, remoteStorageMountLocation, fmt.Errorf("%s mounted to un-configured %+v", bucket, remoteStorageMountLocation)
	}

	client, err = remote_storage.GetRemoteStorage(remoteConf)
	if err != nil {
		return nil, remoteStorageMountLocation, err
	}

	return client, remoteStorageMountLocation, nil
}

func (option *RemoteGatewayOptions) detectBucketInfo(actualDir string) (bucket util.FullPath, remoteStorageMountLocation *remote_pb.RemoteStorageLocation, remoteConf *remote_pb.RemoteConf, ok bool) {
	bucket, ok = extractBucketPath(option.bucketsDir, actualDir)
	if !ok {
		return "", nil, nil, false
	}
	var isMounted bool
	remoteStorageMountLocation, isMounted = option.mappings.GetMappings()[string(bucket)]
	if !isMounted {
		glog.Warningf("%s is not mounted", bucket)

		return "", nil, nil, false
	}
	var hasClient bool
	remoteConf, hasClient = option.remoteConfs[remoteStorageMountLocation.GetName()]
	if !hasClient {
		glog.Warningf("%s mounted to un-configured %+v", bucket, remoteStorageMountLocation)

		return "", nil, nil, false
	}

	return bucket, remoteStorageMountLocation, remoteConf, true
}

func extractBucketPath(bucketsDir, dir string) (util.FullPath, bool) {
	if !strings.HasPrefix(dir, bucketsDir+"/") {
		return "", false
	}
	parts := strings.SplitN(dir[len(bucketsDir)+1:], "/", 2)

	return util.FullPath(bucketsDir).Child(parts[0]), true
}

func (option *RemoteGatewayOptions) collectRemoteStorageConf() (err error) {
	if mappings, err := filer.ReadMountMappings(option.grpcDialOption, pb.ServerAddress(*option.filerAddress)); err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return errors.New("remote storage is not configured in filer server")
		}

		return err
	} else {
		option.mappings = mappings
	}

	option.remoteConfs = make(map[string]*remote_pb.RemoteConf)
	var lastConfName string
	err = filer_pb.List(context.Background(), option, filer.DirectoryEtcRemote, "", func(entry *filer_pb.Entry, isLast bool) error {
		if !strings.HasSuffix(entry.GetName(), filer.REMOTE_STORAGE_CONF_SUFFIX) {
			return nil
		}
		conf := &remote_pb.RemoteConf{}
		if err := proto.Unmarshal(entry.GetContent(), conf); err != nil {
			return fmt.Errorf("unmarshal %s/%s: %w", filer.DirectoryEtcRemote, entry.GetName(), err)
		}
		option.remoteConfs[conf.GetName()] = conf
		lastConfName = conf.GetName()

		return nil
	}, "", false, math.MaxUint32)

	if option.mappings.GetPrimaryBucketStorageName() == "" && len(option.remoteConfs) == 1 {
		glog.V(0).Infof("%s is set to the default remote storage", lastConfName)
		option.mappings.PrimaryBucketStorageName = lastConfName
	}

	return err
}
