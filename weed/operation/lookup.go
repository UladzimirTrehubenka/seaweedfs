package operation

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

type Location struct {
	Url        string `json:"url,omitempty"`
	PublicUrl  string `json:"publicUrl,omitempty"`
	DataCenter string `json:"dataCenter,omitempty"`
	GrpcPort   int    `json:"grpcPort,omitempty"`
}

func (l *Location) ServerAddress() pb.ServerAddress {
	return pb.NewServerAddressWithGrpcPort(l.Url, l.GrpcPort)
}

type LookupResult struct {
	VolumeOrFileId string     `json:"volumeOrFileId,omitempty"`
	Locations      []Location `json:"locations,omitempty"`
	Jwt            string     `json:"jwt,omitempty"`
	Error          string     `json:"error,omitempty"`
}

func (lr *LookupResult) String() string {
	return fmt.Sprintf("VolumeOrFileId:%s, Locations:%v, Error:%s", lr.VolumeOrFileId, lr.Locations, lr.Error)
}

var (
	vc VidCache // caching of volume locations, re-check if after 10 minutes
)

func LookupFileId(masterFn GetMasterFn, grpcDialOption grpc.DialOption, fileId string) (fullUrl string, jwt string, err error) {
	parts := strings.Split(fileId, ",")
	if len(parts) != 2 {
		return "", jwt, errors.New("Invalid fileId " + fileId)
	}
	lookup, lookupError := LookupVolumeId(masterFn, grpcDialOption, parts[0])
	if lookupError != nil {
		return "", jwt, lookupError
	}
	if len(lookup.Locations) == 0 {
		return "", jwt, errors.New("File Not Found")
	}

	return "http://" + lookup.Locations[rand.IntN(len(lookup.Locations))].Url + "/" + fileId, lookup.Jwt, nil
}

func LookupVolumeId(masterFn GetMasterFn, grpcDialOption grpc.DialOption, vid string) (*LookupResult, error) {
	results, err := LookupVolumeIds(masterFn, grpcDialOption, []string{vid})

	return results[vid], err
}

// LookupVolumeIds find volume locations by cache and actual lookup
func LookupVolumeIds(masterFn GetMasterFn, grpcDialOption grpc.DialOption, vids []string) (map[string]*LookupResult, error) {
	ret := make(map[string]*LookupResult)
	var unknown_vids []string

	// check vid cache first
	for _, vid := range vids {
		locations, cacheErr := vc.Get(vid)
		if cacheErr == nil {
			ret[vid] = &LookupResult{VolumeOrFileId: vid, Locations: locations}
		} else {
			unknown_vids = append(unknown_vids, vid)
		}
	}
	// return success if all volume ids are known
	if len(unknown_vids) == 0 {
		return ret, nil
	}

	// only query unknown_vids

	err := WithMasterServerClient(false, masterFn(context.Background()), grpcDialOption, func(masterClient master_pb.SeaweedClient) error {
		req := &master_pb.LookupVolumeRequest{
			VolumeOrFileIds: unknown_vids,
		}
		resp, grpcErr := masterClient.LookupVolume(context.Background(), req)
		if grpcErr != nil {
			return grpcErr
		}

		// set newly checked vids to cache
		for _, vidLocations := range resp.GetVolumeIdLocations() {
			var locations []Location
			for _, loc := range vidLocations.GetLocations() {
				locations = append(locations, Location{
					Url:        loc.GetUrl(),
					PublicUrl:  loc.GetPublicUrl(),
					DataCenter: loc.GetDataCenter(),
					GrpcPort:   int(loc.GetGrpcPort()),
				})
			}
			if vidLocations.GetError() == "" {
				vc.Set(vidLocations.GetVolumeOrFileId(), locations, 10*time.Minute)
			}
			ret[vidLocations.GetVolumeOrFileId()] = &LookupResult{
				VolumeOrFileId: vidLocations.GetVolumeOrFileId(),
				Locations:      locations,
				Jwt:            vidLocations.GetAuth(),
				Error:          vidLocations.GetError(),
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return ret, nil
}
