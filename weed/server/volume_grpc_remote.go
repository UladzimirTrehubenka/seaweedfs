package weed_server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func (vs *VolumeServer) FetchAndWriteNeedle(ctx context.Context, req *volume_server_pb.FetchAndWriteNeedleRequest) (resp *volume_server_pb.FetchAndWriteNeedleResponse, err error) {
	if err := vs.CheckMaintenanceMode(); err != nil {
		return nil, err
	}

	resp = &volume_server_pb.FetchAndWriteNeedleResponse{}
	v := vs.store.GetVolume(needle.VolumeId(req.GetVolumeId()))
	if v == nil {
		return nil, fmt.Errorf("not found volume id %d", req.GetVolumeId())
	}

	remoteConf := req.GetRemoteConf()

	client, getClientErr := remote_storage.GetRemoteStorage(remoteConf)
	if getClientErr != nil {
		return nil, fmt.Errorf("get remote client: %w", getClientErr)
	}

	remoteStorageLocation := req.GetRemoteLocation()

	data, ReadRemoteErr := client.ReadFile(remoteStorageLocation, req.GetOffset(), req.GetSize())
	if ReadRemoteErr != nil {
		return nil, fmt.Errorf("read from remote %+v: %w", remoteStorageLocation, ReadRemoteErr)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		n := new(needle.Needle)
		n.Id = types.NeedleId(req.GetNeedleId())
		n.Cookie = types.Cookie(req.GetCookie())
		n.Data, n.DataSize = data, uint32(len(data))
		// copied from *Needle.prepareWriteBuffer()
		n.Size = 4 + types.Size(n.DataSize) + 1
		n.Checksum = needle.NewCRC(n.Data)
		n.LastModified = uint64(time.Now().Unix())
		n.SetHasLastModifiedDate()
		if _, localWriteErr := vs.store.WriteVolumeNeedle(v.Id, n, true, false); localWriteErr != nil {
			if err == nil {
				err = fmt.Errorf("local write needle %d size %d: %w", req.GetNeedleId(), req.GetSize(), localWriteErr)
			}
		} else {
			resp.ETag = n.Etag()
		}
	}()
	if len(req.GetReplicas()) > 0 {
		fileId := needle.NewFileId(v.Id, req.GetNeedleId(), req.GetCookie())
		for _, replica := range req.GetReplicas() {
			wg.Add(1)
			go func(targetVolumeServer string) {
				defer wg.Done()
				uploadOption := &operation.UploadOption{
					UploadUrl:         fmt.Sprintf("http://%s/%s?type=replicate", targetVolumeServer, fileId.String()),
					Filename:          "",
					Cipher:            false,
					IsInputCompressed: false,
					MimeType:          "",
					PairMap:           nil,
					Jwt:               security.EncodedJwt(req.GetAuth()),
				}

				uploader, uploaderErr := operation.NewUploader()
				if uploaderErr != nil && err == nil {
					err = fmt.Errorf("remote write needle %d size %d: %w", req.GetNeedleId(), req.GetSize(), uploaderErr)

					return
				}

				if _, replicaWriteErr := uploader.UploadData(ctx, data, uploadOption); replicaWriteErr != nil && err == nil {
					err = fmt.Errorf("remote write needle %d size %d: %w", req.GetNeedleId(), req.GetSize(), replicaWriteErr)
				}
			}(replica.GetUrl())
		}
	}

	wg.Wait()

	return resp, err
}
