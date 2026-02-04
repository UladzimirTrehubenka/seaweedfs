package operation

import (
	"context"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func GetVolumeSyncStatus(server pb.ServerAddress, grpcDialOption grpc.DialOption, vid uint32) (resp *volume_server_pb.VolumeSyncStatusResponse, err error) {
	WithVolumeServerClient(false, server, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		resp, err = client.VolumeSyncStatus(context.Background(), &volume_server_pb.VolumeSyncStatusRequest{
			VolumeId: vid,
		})

		return nil
	})

	return
}
