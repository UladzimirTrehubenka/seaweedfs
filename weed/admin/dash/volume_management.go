package dash

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// GetClusterVolumes retrieves cluster volumes data with pagination, sorting, and filtering
func (s *AdminServer) GetClusterVolumes(page int, pageSize int, sortBy string, sortOrder string, collection string) (*ClusterVolumesData, error) {
	// Set defaults
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 1000 {
		pageSize = 100
	}
	if sortBy == "" {
		sortBy = "id"
	}
	if sortOrder == "" {
		sortOrder = "asc"
	}
	var volumes []VolumeWithTopology
	var totalSize int64
	var cachedTopologyInfo *master_pb.TopologyInfo

	// Get detailed volume information via gRPC
	err := s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		if err != nil {
			return err
		}

		// Cache the topology info for reuse
		cachedTopologyInfo = resp.GetTopologyInfo()

		if resp.GetTopologyInfo() != nil {
			for _, dc := range resp.GetTopologyInfo().GetDataCenterInfos() {
				for _, rack := range dc.GetRackInfos() {
					for _, node := range rack.GetDataNodeInfos() {
						for _, diskInfo := range node.GetDiskInfos() {
							// Process regular volumes
							for _, volInfo := range diskInfo.GetVolumeInfos() {
								volume := VolumeWithTopology{
									VolumeInformationMessage: volInfo,
									Server:                   node.GetId(),
									DataCenter:               dc.GetId(),
									Rack:                     rack.GetId(),
								}
								volumes = append(volumes, volume)
								totalSize += int64(volInfo.GetSize())
							}

							// Process EC shards in the same loop
							for _, ecShardInfo := range diskInfo.GetEcShardInfos() {
								// Add all shard sizes for this EC volume
								for _, shardSize := range ecShardInfo.GetShardSizes() {
									totalSize += shardSize
								}
							}
						}
					}
				}
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Filter by collection if specified
	if collection != "" {
		var filteredVolumes []VolumeWithTopology
		var filteredTotalSize int64
		var filteredEcTotalSize int64

		for _, volume := range volumes {
			if matchesCollection(volume.Collection, collection) {
				filteredVolumes = append(filteredVolumes, volume)
				filteredTotalSize += int64(volume.Size)
			}
		}

		// Filter EC shard sizes by collection using already processed data
		// This reuses the topology traversal done above (lines 43-71) to avoid a second pass
		if cachedTopologyInfo != nil {
			for _, dc := range cachedTopologyInfo.GetDataCenterInfos() {
				for _, rack := range dc.GetRackInfos() {
					for _, node := range rack.GetDataNodeInfos() {
						for _, diskInfo := range node.GetDiskInfos() {
							for _, ecShardInfo := range diskInfo.GetEcShardInfos() {
								if matchesCollection(ecShardInfo.GetCollection(), collection) {
									// Add all shard sizes for this EC volume
									for _, shardSize := range ecShardInfo.GetShardSizes() {
										filteredEcTotalSize += shardSize
									}
								}
							}
						}
					}
				}
			}
		}

		volumes = filteredVolumes
		totalSize = filteredTotalSize + filteredEcTotalSize
	}

	// Calculate unique data center, rack, disk type, collection, and version counts from filtered volumes
	dataCenterMap := make(map[string]bool)
	rackMap := make(map[string]bool)
	diskTypeMap := make(map[string]bool)
	collectionMap := make(map[string]bool)
	versionMap := make(map[string]bool)
	for _, volume := range volumes {
		if volume.DataCenter != "" {
			dataCenterMap[volume.DataCenter] = true
		}
		if volume.Rack != "" {
			rackMap[volume.Rack] = true
		}
		diskType := volume.DiskType
		if diskType == "" {
			diskType = "hdd" // Default to hdd if not specified
		}
		diskTypeMap[diskType] = true

		// Handle collection for display purposes
		collectionName := volume.Collection
		if collectionName == "" {
			collectionName = "default"
		}
		collectionMap[collectionName] = true

		versionMap[strconv.FormatUint(uint64(volume.Version), 10)] = true
	}
	dataCenterCount := len(dataCenterMap)
	rackCount := len(rackMap)
	diskTypeCount := len(diskTypeMap)
	collectionCount := len(collectionMap)
	versionCount := len(versionMap)

	// Sort volumes
	s.sortVolumes(volumes, sortBy, sortOrder)

	// Get volume size limit from master
	var volumeSizeLimit uint64
	err = s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
		if err != nil {
			return err
		}
		volumeSizeLimit = uint64(resp.GetVolumeSizeLimitMB()) * 1024 * 1024 // Convert MB to bytes

		return nil
	})
	if err != nil {
		// If we can't get the limit, set a default
		volumeSizeLimit = 30 * 1024 * 1024 * 1024 // 30GB default
	}

	// Calculate pagination
	totalVolumes := len(volumes)
	totalPages := (totalVolumes + pageSize - 1) / pageSize
	if totalPages == 0 {
		totalPages = 1
	}

	// Apply pagination
	startIndex := (page - 1) * pageSize
	endIndex := startIndex + pageSize
	if startIndex >= totalVolumes {
		volumes = []VolumeWithTopology{}
	} else {
		if endIndex > totalVolumes {
			endIndex = totalVolumes
		}
		volumes = volumes[startIndex:endIndex]
	}

	// Determine conditional display flags and extract single values
	showDataCenterColumn := dataCenterCount > 1
	showRackColumn := rackCount > 1
	showDiskTypeColumn := diskTypeCount > 1
	showCollectionColumn := collectionCount > 1 && collection == "" // Hide column when filtering by collection
	showVersionColumn := versionCount > 1

	var singleDataCenter, singleRack, singleDiskType, singleCollection, singleVersion string
	var allVersions, allDiskTypes []string

	if dataCenterCount == 1 {
		for dc := range dataCenterMap {
			singleDataCenter = dc

			break
		}
	}
	if rackCount == 1 {
		for rack := range rackMap {
			singleRack = rack

			break
		}
	}
	if diskTypeCount == 1 {
		for diskType := range diskTypeMap {
			singleDiskType = diskType

			break
		}
	} else {
		// Collect all disk types and sort them
		for diskType := range diskTypeMap {
			allDiskTypes = append(allDiskTypes, diskType)
		}
		sort.Strings(allDiskTypes)
	}
	if collectionCount == 1 {
		for collection := range collectionMap {
			singleCollection = collection

			break
		}
	}
	if versionCount == 1 {
		for version := range versionMap {
			singleVersion = "v" + version

			break
		}
	} else {
		// Collect all versions and sort them
		for version := range versionMap {
			allVersions = append(allVersions, "v"+version)
		}
		sort.Strings(allVersions)
	}

	return &ClusterVolumesData{
		Volumes:              volumes,
		TotalVolumes:         totalVolumes,
		TotalSize:            totalSize,
		VolumeSizeLimit:      volumeSizeLimit,
		LastUpdated:          time.Now(),
		CurrentPage:          page,
		TotalPages:           totalPages,
		PageSize:             pageSize,
		SortBy:               sortBy,
		SortOrder:            sortOrder,
		DataCenterCount:      dataCenterCount,
		RackCount:            rackCount,
		DiskTypeCount:        diskTypeCount,
		CollectionCount:      collectionCount,
		VersionCount:         versionCount,
		ShowDataCenterColumn: showDataCenterColumn,
		ShowRackColumn:       showRackColumn,
		ShowDiskTypeColumn:   showDiskTypeColumn,
		ShowCollectionColumn: showCollectionColumn,
		ShowVersionColumn:    showVersionColumn,
		SingleDataCenter:     singleDataCenter,
		SingleRack:           singleRack,
		SingleDiskType:       singleDiskType,
		SingleCollection:     singleCollection,
		SingleVersion:        singleVersion,
		AllVersions:          allVersions,
		AllDiskTypes:         allDiskTypes,
		FilterCollection:     collection,
	}, nil
}

// sortVolumes sorts the volumes slice based on the specified field and order
func (s *AdminServer) sortVolumes(volumes []VolumeWithTopology, sortBy string, sortOrder string) {
	sort.Slice(volumes, func(i, j int) bool {
		var less bool

		switch sortBy {
		case "id":
			less = volumes[i].Id < volumes[j].Id
		case "server":
			less = volumes[i].Server < volumes[j].Server
		case "datacenter":
			less = volumes[i].DataCenter < volumes[j].DataCenter
		case "rack":
			less = volumes[i].Rack < volumes[j].Rack
		case "collection":
			less = volumes[i].Collection < volumes[j].Collection
		case "size":
			less = volumes[i].Size < volumes[j].Size
		case "filecount":
			less = volumes[i].FileCount < volumes[j].FileCount
		case "replication":
			less = volumes[i].ReplicaPlacement < volumes[j].ReplicaPlacement
		case "disktype":
			less = volumes[i].DiskType < volumes[j].DiskType
		case "version":
			less = volumes[i].Version < volumes[j].Version
		default:
			less = volumes[i].Id < volumes[j].Id
		}

		if sortOrder == "desc" {
			return !less
		}

		return less
	})
}

// GetVolumeDetails retrieves detailed information about a specific volume
func (s *AdminServer) GetVolumeDetails(volumeID int, server string) (*VolumeDetailsData, error) {
	var primaryVolume VolumeWithTopology
	var replicas []VolumeWithTopology
	var volumeSizeLimit uint64
	var found bool

	// Find the volume and all its replicas in the cluster
	err := s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		if err != nil {
			return err
		}

		if resp.GetTopologyInfo() != nil {
			for _, dc := range resp.GetTopologyInfo().GetDataCenterInfos() {
				for _, rack := range dc.GetRackInfos() {
					for _, node := range rack.GetDataNodeInfos() {
						for _, diskInfo := range node.GetDiskInfos() {
							for _, volInfo := range diskInfo.GetVolumeInfos() {
								if int(volInfo.GetId()) == volumeID {
									diskType := volInfo.GetDiskType()
									if diskType == "" {
										diskType = "hdd"
									}

									volume := VolumeWithTopology{
										VolumeInformationMessage: volInfo,
										Server:                   node.GetId(),
										DataCenter:               dc.GetId(),
										Rack:                     rack.GetId(),
									}

									// If this is the requested server, it's the primary volume
									if node.GetId() == server {
										primaryVolume = volume
										found = true
									} else {
										// This is a replica on another server
										replicas = append(replicas, volume)
									}
								}
							}
						}
					}
				}
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	if !found {
		return nil, fmt.Errorf("volume %d not found on server %s", volumeID, server)
	}

	// Get volume size limit from master
	err = s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
		if err != nil {
			return err
		}
		volumeSizeLimit = uint64(resp.GetVolumeSizeLimitMB()) * 1024 * 1024 // Convert MB to bytes

		return nil
	})

	if err != nil {
		// If we can't get the limit, set a default
		volumeSizeLimit = 30 * 1024 * 1024 * 1024 // 30GB default
	}

	return &VolumeDetailsData{
		Volume:           primaryVolume,
		Replicas:         replicas,
		VolumeSizeLimit:  volumeSizeLimit,
		ReplicationCount: len(replicas) + 1, // Include the primary volume
		LastUpdated:      time.Now(),
	}, nil
}

// VacuumVolume performs a vacuum operation on a specific volume
func (s *AdminServer) VacuumVolume(volumeID int, server string) error {
	// Validate volumeID range before converting to uint32
	if volumeID < 0 || uint64(volumeID) > math.MaxUint32 {
		return fmt.Errorf("volume ID out of range: %d", volumeID)
	}

	return s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		_, err := client.VacuumVolume(context.Background(), &master_pb.VacuumVolumeRequest{
			// lgtm[go/incorrect-integer-conversion]
			// Safe conversion: volumeID has been validated to be in range [0, 0xFFFFFFFF] above
			VolumeId:         uint32(volumeID),
			GarbageThreshold: 0.0001, // A very low threshold to ensure all garbage is collected
			Collection:       "",     // Empty for all collections
		})

		return err
	})
}

// GetClusterVolumeServers retrieves cluster volume servers data including EC shard information
func (s *AdminServer) GetClusterVolumeServers() (*ClusterVolumeServersData, error) {
	var volumeServerMap map[string]*VolumeServer

	// Make only ONE VolumeList call and use it for both topology building AND EC shard processing
	err := s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		if err != nil {
			return err
		}

		// Get volume size limit from response, default to 30GB if not set
		volumeSizeLimitMB := resp.GetVolumeSizeLimitMb()
		if volumeSizeLimitMB == 0 {
			volumeSizeLimitMB = 30000 // default to 30000MB (30GB)
		}

		// Build basic topology from the VolumeList response (replaces GetClusterTopology call)
		volumeServerMap = make(map[string]*VolumeServer)

		if resp.GetTopologyInfo() != nil {
			// Process topology to build basic volume server info (similar to cluster_topology.go logic)
			for _, dc := range resp.GetTopologyInfo().GetDataCenterInfos() {
				for _, rack := range dc.GetRackInfos() {
					for _, node := range rack.GetDataNodeInfos() {
						// Initialize volume server if not exists
						if volumeServerMap[node.GetId()] == nil {
							volumeServerMap[node.GetId()] = &VolumeServer{
								Address:        node.GetId(),
								DataCenter:     dc.GetId(),
								Rack:           rack.GetId(),
								Volumes:        0,
								DiskUsage:      0,
								DiskCapacity:   0,
								EcVolumes:      0,
								EcShards:       0,
								EcShardDetails: []VolumeServerEcInfo{},
							}
						}
						vs := volumeServerMap[node.GetId()]

						// Process EC shard information for this server at volume server level (not per-disk)
						ecVolumeMap := make(map[uint32]*VolumeServerEcInfo)
						// Temporary map to accumulate shard info across disks
						ecShardAccumulator := make(map[uint32][]*master_pb.VolumeEcShardInformationMessage)

						// Process disk information
						for _, diskInfo := range node.GetDiskInfos() {
							vs.DiskCapacity += int64(diskInfo.GetMaxVolumeCount()) * int64(volumeSizeLimitMB) * 1024 * 1024 // Use actual volume size limit

							// Count regular volumes and calculate disk usage
							for _, volInfo := range diskInfo.GetVolumeInfos() {
								vs.Volumes++
								vs.DiskUsage += int64(volInfo.GetSize())
							}

							// Accumulate EC shard information across all disks for this volume server
							for _, ecShardInfo := range diskInfo.GetEcShardInfos() {
								volumeId := ecShardInfo.GetId()
								ecShardAccumulator[volumeId] = append(ecShardAccumulator[volumeId], ecShardInfo)
							}
						}

						// Process accumulated EC shard information per volume
						for volumeId, ecShardInfos := range ecShardAccumulator {
							if len(ecShardInfos) == 0 {
								continue
							}

							// Initialize EC volume info
							ecInfo := &VolumeServerEcInfo{
								VolumeID:     volumeId,
								Collection:   ecShardInfos[0].GetCollection(),
								ShardCount:   0,
								EcIndexBits:  0,
								ShardNumbers: []int{},
								ShardSizes:   make(map[int]int64),
								TotalSize:    0,
							}

							// Merge EcIndexBits from all disks and collect shard sizes
							allShardSizes := make(map[erasure_coding.ShardId]int64)
							for _, ecShardInfo := range ecShardInfos {
								si := erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(ecShardInfo)
								ecInfo.EcIndexBits |= si.Bitmap()

								// Collect shard sizes from this disk
								for _, id := range si.Ids() {
									allShardSizes[id] += int64(si.Size(id))
								}
							}

							// Process final merged shard information
							for shardId := range allShardSizes {
								ecInfo.ShardCount++
								ecInfo.ShardNumbers = append(ecInfo.ShardNumbers, int(shardId))
								vs.EcShards++

								// Add shard size if available
								if shardSize, exists := allShardSizes[shardId]; exists {
									ecInfo.ShardSizes[int(shardId)] = shardSize
									ecInfo.TotalSize += shardSize
									vs.DiskUsage += shardSize // Add EC shard size to total disk usage
								}
							}

							ecVolumeMap[volumeId] = ecInfo
						}

						// Convert EC volume map to slice and update volume server (after processing all disks)
						for _, ecInfo := range ecVolumeMap {
							vs.EcShardDetails = append(vs.EcShardDetails, *ecInfo)
							vs.EcVolumes++
						}
					}
				}
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Convert map back to slice
	var volumeServers []VolumeServer
	for _, vs := range volumeServerMap {
		volumeServers = append(volumeServers, *vs)
	}

	// Sort volume servers by address for consistent ordering on page refresh
	sort.Slice(volumeServers, func(i, j int) bool {
		return volumeServers[i].GetDisplayAddress() < volumeServers[j].GetDisplayAddress()
	})

	var totalCapacity int64
	var totalVolumes int
	for _, vs := range volumeServers {
		totalCapacity += vs.DiskCapacity
		totalVolumes += vs.Volumes
	}

	return &ClusterVolumeServersData{
		VolumeServers:      volumeServers,
		TotalVolumeServers: len(volumeServers),
		TotalVolumes:       totalVolumes,
		TotalCapacity:      totalCapacity,
		LastUpdated:        time.Now(),
	}, nil
}
