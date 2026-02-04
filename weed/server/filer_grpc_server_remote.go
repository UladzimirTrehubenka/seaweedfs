package weed_server

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (fs *FilerServer) CacheRemoteObjectToLocalCluster(ctx context.Context, req *filer_pb.CacheRemoteObjectToLocalClusterRequest) (*filer_pb.CacheRemoteObjectToLocalClusterResponse, error) {
	// Use singleflight to deduplicate concurrent caching requests for the same object
	// This benefits all clients: S3 API, filer HTTP, Hadoop, etc.
	cacheKey := req.GetDirectory() + "/" + req.GetName()

	result, err, shared := fs.remoteCacheGroup.Do(cacheKey, func() (any, error) {
		return fs.doCacheRemoteObjectToLocalCluster(ctx, req)
	})

	if shared {
		glog.V(2).Infof("CacheRemoteObjectToLocalCluster: shared result for %s", cacheKey)
	}

	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, errors.New("unexpected nil result from singleflight")
	}

	resp, ok := result.(*filer_pb.CacheRemoteObjectToLocalClusterResponse)
	if !ok {
		return nil, errors.New("unexpected result type from singleflight")
	}

	return resp, nil
}

// doCacheRemoteObjectToLocalCluster performs the actual caching operation.
// This is called from singleflight, so only one instance runs per object.
func (fs *FilerServer) doCacheRemoteObjectToLocalCluster(ctx context.Context, req *filer_pb.CacheRemoteObjectToLocalClusterRequest) (*filer_pb.CacheRemoteObjectToLocalClusterResponse, error) {
	// find the entry first to check if already cached
	entry, err := fs.filer.FindEntry(ctx, util.JoinPath(req.GetDirectory(), req.GetName()))
	if errors.Is(err, filer_pb.ErrNotFound) {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("find entry %s/%s: %w", req.GetDirectory(), req.GetName(), err)
	}

	resp := &filer_pb.CacheRemoteObjectToLocalClusterResponse{}

	// Early return if not a remote-only object or already cached
	if entry.Remote == nil || entry.Remote.GetRemoteSize() == 0 {
		resp.Entry = entry.ToProtoEntry()

		return resp, nil
	}
	if len(entry.GetChunks()) > 0 {
		// Already has local chunks - already cached
		glog.V(2).Infof("CacheRemoteObjectToLocalCluster: %s/%s already cached (%d chunks)", req.GetDirectory(), req.GetName(), len(entry.GetChunks()))
		resp.Entry = entry.ToProtoEntry()

		return resp, nil
	}

	glog.V(1).Infof("CacheRemoteObjectToLocalCluster: caching %s/%s (remote size: %d)", req.GetDirectory(), req.GetName(), entry.Remote.GetRemoteSize())

	// load all mappings
	mappingEntry, err := fs.filer.FindEntry(ctx, util.JoinPath(filer.DirectoryEtcRemote, filer.REMOTE_STORAGE_MOUNT_FILE))
	if err != nil {
		return nil, err
	}
	mappings, err := filer.UnmarshalRemoteStorageMappings(mappingEntry.Content)
	if err != nil {
		return nil, err
	}

	// find mapping
	var remoteStorageMountedLocation *remote_pb.RemoteStorageLocation
	var localMountedDir string
	for k, loc := range mappings.GetMappings() {
		if strings.HasPrefix(req.GetDirectory(), k) {
			localMountedDir, remoteStorageMountedLocation = k, loc
		}
	}
	if localMountedDir == "" {
		return nil, fmt.Errorf("%s is not mounted", req.GetDirectory())
	}

	// find storage configuration
	storageConfEntry, err := fs.filer.FindEntry(ctx, util.JoinPath(filer.DirectoryEtcRemote, remoteStorageMountedLocation.GetName()+filer.REMOTE_STORAGE_CONF_SUFFIX))
	if err != nil {
		return nil, err
	}
	storageConf := &remote_pb.RemoteConf{}
	if unMarshalErr := proto.Unmarshal(storageConfEntry.Content, storageConf); unMarshalErr != nil {
		return nil, fmt.Errorf("unmarshal remote storage conf %s/%s: %w", filer.DirectoryEtcRemote, remoteStorageMountedLocation.GetName()+filer.REMOTE_STORAGE_CONF_SUFFIX, unMarshalErr)
	}

	// detect storage option
	so, err := fs.detectStorageOption(ctx, req.GetDirectory(), "", "", 0, "", "", "", "")
	if err != nil {
		return resp, err
	}
	assignRequest, altRequest := so.ToAssignRequests(1)

	// find a good chunk size
	chunkSize := int64(5 * 1024 * 1024)
	chunkCount := entry.Remote.GetRemoteSize()/chunkSize + 1
	for chunkCount > 1000 && chunkSize < int64(fs.option.MaxMB)*1024*1024/2 {
		chunkSize *= 2
		chunkCount = entry.Remote.GetRemoteSize()/chunkSize + 1
	}

	dest := util.FullPath(remoteStorageMountedLocation.GetPath()).Child(string(util.FullPath(req.GetDirectory()).Child(req.GetName()))[len(localMountedDir):])

	var chunks []*filer_pb.FileChunk
	var chunksMu sync.Mutex
	var fetchAndWriteErr error
	var wg sync.WaitGroup

	limitedConcurrentExecutor := util.NewLimitedConcurrentExecutor(8)
	for offset := int64(0); offset < entry.Remote.GetRemoteSize(); offset += chunkSize {
		localOffset := offset

		wg.Add(1)
		limitedConcurrentExecutor.Execute(func() {
			defer wg.Done()
			size := chunkSize
			if localOffset+chunkSize > entry.Remote.GetRemoteSize() {
				size = entry.Remote.GetRemoteSize() - localOffset
			}

			// assign one volume server
			assignResult, err := operation.Assign(ctx, fs.filer.GetMaster, fs.grpcDialOption, assignRequest, altRequest)
			if err != nil {
				chunksMu.Lock()
				if fetchAndWriteErr == nil {
					fetchAndWriteErr = err
				}
				chunksMu.Unlock()

				return
			}
			if assignResult.Error != "" {
				chunksMu.Lock()
				if fetchAndWriteErr == nil {
					fetchAndWriteErr = fmt.Errorf("assign: %v", assignResult.Error)
				}
				chunksMu.Unlock()

				return
			}
			fileId, parseErr := needle.ParseFileIdFromString(assignResult.Fid)
			if parseErr != nil {
				chunksMu.Lock()
				if fetchAndWriteErr == nil {
					fetchAndWriteErr = fmt.Errorf("unrecognized file id %s: %w", assignResult.Fid, parseErr)
				}
				chunksMu.Unlock()

				return
			}

			var replicas []*volume_server_pb.FetchAndWriteNeedleRequest_Replica
			for _, r := range assignResult.Replicas {
				replicas = append(replicas, &volume_server_pb.FetchAndWriteNeedleRequest_Replica{
					Url:       r.Url,
					PublicUrl: r.PublicUrl,
					GrpcPort:  int32(r.GrpcPort),
				})
			}

			// tell filer to tell volume server to download into needles
			assignedServerAddress := pb.NewServerAddressWithGrpcPort(assignResult.Url, assignResult.GrpcPort)
			var etag string
			err = operation.WithVolumeServerClient(false, assignedServerAddress, fs.grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
				resp, fetchErr := volumeServerClient.FetchAndWriteNeedle(context.Background(), &volume_server_pb.FetchAndWriteNeedleRequest{
					VolumeId:   uint32(fileId.VolumeId),
					NeedleId:   uint64(fileId.Key),
					Cookie:     uint32(fileId.Cookie),
					Offset:     localOffset,
					Size:       size,
					Replicas:   replicas,
					Auth:       string(assignResult.Auth),
					RemoteConf: storageConf,
					RemoteLocation: &remote_pb.RemoteStorageLocation{
						Name:   remoteStorageMountedLocation.GetName(),
						Bucket: remoteStorageMountedLocation.GetBucket(),
						Path:   string(dest),
					},
				})
				if fetchErr != nil {
					return fmt.Errorf("volume server %s fetchAndWrite %s: %w", assignResult.Url, dest, fetchErr)
				}
				etag = resp.GetETag()

				return nil
			})

			if err != nil {
				chunksMu.Lock()
				if fetchAndWriteErr == nil {
					fetchAndWriteErr = err
				}
				chunksMu.Unlock()

				return
			}

			chunk := &filer_pb.FileChunk{
				FileId:       assignResult.Fid,
				Offset:       localOffset,
				Size:         uint64(size),
				ModifiedTsNs: time.Now().UnixNano(),
				ETag:         etag,
				Fid: &filer_pb.FileId{
					VolumeId: uint32(fileId.VolumeId),
					FileKey:  uint64(fileId.Key),
					Cookie:   uint32(fileId.Cookie),
				},
			}
			chunksMu.Lock()
			chunks = append(chunks, chunk)
			chunksMu.Unlock()
		})
	}

	wg.Wait()

	chunksMu.Lock()
	err = fetchAndWriteErr
	// Sort chunks by offset to maintain file order
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].GetOffset() < chunks[j].GetOffset()
	})
	chunksMu.Unlock()
	if err != nil {
		return nil, err
	}

	garbage := entry.GetChunks()

	newEntry := entry.ShallowClone()
	newEntry.Chunks = chunks
	newEntry.Remote = proto.Clone(entry.Remote).(*filer_pb.RemoteEntry)
	newEntry.Remote.LastLocalSyncTsNs = time.Now().UnixNano()

	// this skips meta data log events

	if err := fs.filer.Store.UpdateEntry(context.Background(), newEntry); err != nil {
		fs.filer.DeleteUncommittedChunks(ctx, chunks)

		return nil, err
	}
	fs.filer.DeleteChunks(ctx, entry.FullPath, garbage)

	fs.filer.NotifyUpdateEvent(ctx, entry, newEntry, true, false, nil)

	resp.Entry = newEntry.ToProtoEntry()

	return resp, nil
}
