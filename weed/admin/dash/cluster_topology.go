package dash

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// GetClusterTopology returns the current cluster topology with caching
func (s *AdminServer) GetClusterTopology() (*ClusterTopology, error) {
	now := time.Now()
	if s.cachedTopology != nil && now.Sub(s.lastCacheUpdate) < s.cacheExpiration {
		return s.cachedTopology, nil
	}

	topology := &ClusterTopology{
		UpdatedAt: now,
	}

	// Use gRPC only
	err := s.getTopologyViaGRPC(topology)
	if err != nil {
		currentMaster := s.masterClient.GetMaster(context.Background())
		glog.Errorf("Failed to connect to master server %s: %v", currentMaster, err)

		return nil, fmt.Errorf("gRPC topology request failed: %w", err)
	}

	// Cache the result
	s.cachedTopology = topology
	s.lastCacheUpdate = now

	return topology, nil
}

// getTopologyViaGRPC gets topology using gRPC (original method)
func (s *AdminServer) getTopologyViaGRPC(topology *ClusterTopology) error {
	// Get cluster status from master
	err := s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		if err != nil {
			currentMaster := s.masterClient.GetMaster(context.Background())
			glog.Errorf("Failed to get volume list from master %s: %v", currentMaster, err)

			return err
		}

		if resp.GetTopologyInfo() != nil {
			// Process gRPC response
			for _, dc := range resp.GetTopologyInfo().GetDataCenterInfos() {
				dataCenter := DataCenter{
					ID:    dc.GetId(),
					Racks: []Rack{},
				}

				for _, rack := range dc.GetRackInfos() {
					rackObj := Rack{
						ID:    rack.GetId(),
						Nodes: []VolumeServer{},
					}

					for _, node := range rack.GetDataNodeInfos() {
						// Calculate totals from disk infos
						var totalVolumes int64
						var totalMaxVolumes int64
						var totalSize int64
						var totalFiles int64

						for _, diskInfo := range node.GetDiskInfos() {
							totalVolumes += diskInfo.GetVolumeCount()
							totalMaxVolumes += diskInfo.GetMaxVolumeCount()

							// Sum up individual volume information
							for _, volInfo := range diskInfo.GetVolumeInfos() {
								totalSize += int64(volInfo.GetSize())
								totalFiles += int64(volInfo.GetFileCount())
							}

							// Sum up EC shard sizes
							for _, ecShardInfo := range diskInfo.GetEcShardInfos() {
								for _, shardSize := range ecShardInfo.GetShardSizes() {
									totalSize += shardSize
								}
							}
						}

						vs := VolumeServer{
							ID:            node.GetId(),
							Address:       node.GetId(),
							DataCenter:    dc.GetId(),
							Rack:          rack.GetId(),
							PublicURL:     node.GetId(),
							Volumes:       int(totalVolumes),
							MaxVolumes:    int(totalMaxVolumes),
							DiskUsage:     totalSize,
							DiskCapacity:  totalMaxVolumes * int64(resp.GetVolumeSizeLimitMb()) * 1024 * 1024,
							LastHeartbeat: time.Now(),
						}

						rackObj.Nodes = append(rackObj.Nodes, vs)
						topology.VolumeServers = append(topology.VolumeServers, vs)
						topology.TotalVolumes += vs.Volumes
						topology.TotalFiles += totalFiles
						topology.TotalSize += totalSize
					}

					dataCenter.Racks = append(dataCenter.Racks, rackObj)
				}

				topology.DataCenters = append(topology.DataCenters, dataCenter)
			}
		}

		return nil
	})

	return err
}

// InvalidateCache forces a refresh of cached data
func (s *AdminServer) InvalidateCache() {
	s.lastCacheUpdate = time.Now().Add(-s.cacheExpiration)
	s.cachedTopology = nil
	s.lastFilerUpdate = time.Now().Add(-s.filerCacheExpiration)
	s.cachedFilers = nil
	s.lastCollectionStatsUpdate = time.Now().Add(-s.collectionStatsCacheThreshold)
	s.collectionStatsCache = nil
}
