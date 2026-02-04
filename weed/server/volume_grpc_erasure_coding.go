package weed_server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

/*

Steps to apply erasure coding to .dat .idx files
0. ensure the volume is readonly
1. client call VolumeEcShardsGenerate to generate the .ecx and .ec00 ~ .ec13 files
2. client ask master for possible servers to hold the ec files
3. client call VolumeEcShardsCopy on above target servers to copy ec files from the source server
4. target servers report the new ec files to the master
5.   master stores vid -> [14]*DataNode
6. client checks master. If all 14 slices are ready, delete the original .idx, .idx files

*/

// VolumeEcShardsGenerate generates the .ecx and .ec00 ~ .ec13 files
func (vs *VolumeServer) VolumeEcShardsGenerate(ctx context.Context, req *volume_server_pb.VolumeEcShardsGenerateRequest) (*volume_server_pb.VolumeEcShardsGenerateResponse, error) {
	if err := vs.CheckMaintenanceMode(); err != nil {
		return nil, err
	}

	glog.V(0).Infof("VolumeEcShardsGenerate: %v", req)

	v := vs.store.GetVolume(needle.VolumeId(req.GetVolumeId()))
	if v == nil {
		return nil, fmt.Errorf("volume %d not found", req.GetVolumeId())
	}
	baseFileName := v.DataFileName()

	if v.Collection != req.GetCollection() {
		return nil, fmt.Errorf("existing collection:%v unexpected input: %v", v.Collection, req.GetCollection())
	}

	// Create EC context - prefer existing .vif config if present (for regeneration scenarios)
	ecCtx := erasure_coding.NewDefaultECContext(req.GetCollection(), needle.VolumeId(req.GetVolumeId()))
	if volumeInfo, _, found, _ := volume_info.MaybeLoadVolumeInfo(baseFileName + ".vif"); found && volumeInfo.GetEcShardConfig() != nil {
		ds := int(volumeInfo.GetEcShardConfig().GetDataShards())
		ps := int(volumeInfo.GetEcShardConfig().GetParityShards())

		// Validate and use existing EC config
		if ds > 0 && ps > 0 && ds+ps <= erasure_coding.MaxShardCount {
			ecCtx.DataShards = ds
			ecCtx.ParityShards = ps
			glog.V(0).Infof("Using existing EC config for volume %d: %s", req.GetVolumeId(), ecCtx.String())
		} else {
			glog.Warningf("Invalid EC config in .vif for volume %d (data=%d, parity=%d), using defaults", req.GetVolumeId(), ds, ps)
		}
	} else {
		glog.V(0).Infof("Using default EC config for volume %d: %s", req.GetVolumeId(), ecCtx.String())
	}

	shouldCleanup := true
	defer func() {
		if !shouldCleanup {
			return
		}
		for i := 0; i < ecCtx.Total(); i++ {
			os.Remove(baseFileName + ecCtx.ToExt(i))
		}
		os.Remove(v.IndexFileName() + ".ecx")
	}()

	// write .ec00 ~ .ec[TotalShards-1] files using context
	if err := erasure_coding.WriteEcFilesWithContext(baseFileName, ecCtx); err != nil {
		return nil, fmt.Errorf("WriteEcFilesWithContext %s: %w", baseFileName, err)
	}

	// write .ecx file
	if err := erasure_coding.WriteSortedFileFromIdx(v.IndexFileName(), ".ecx"); err != nil {
		return nil, fmt.Errorf("WriteSortedFileFromIdx %s: %w", v.IndexFileName(), err)
	}

	// write .vif files
	var expireAtSec uint64
	if v.Ttl != nil {
		ttlSecond := v.Ttl.ToSeconds()
		if ttlSecond > 0 {
			expireAtSec = uint64(time.Now().Unix()) + ttlSecond // calculated expiration time
		}
	}
	volumeInfo := &volume_server_pb.VolumeInfo{Version: uint32(v.Version())}
	volumeInfo.ExpireAtSec = expireAtSec

	datSize, _, _ := v.FileStat()
	volumeInfo.DatFileSize = int64(datSize)

	// Validate EC configuration before saving to .vif
	if ecCtx.DataShards <= 0 || ecCtx.ParityShards <= 0 || ecCtx.Total() > erasure_coding.MaxShardCount {
		return nil, fmt.Errorf("invalid EC config before saving: data=%d, parity=%d, total=%d (max=%d)",
			ecCtx.DataShards, ecCtx.ParityShards, ecCtx.Total(), erasure_coding.MaxShardCount)
	}

	// Save EC configuration to VolumeInfo
	volumeInfo.EcShardConfig = &volume_server_pb.EcShardConfig{
		DataShards:   uint32(ecCtx.DataShards),
		ParityShards: uint32(ecCtx.ParityShards),
	}
	glog.V(1).Infof("Saving EC config to .vif for volume %d: %d+%d (total: %d)",
		req.GetVolumeId(), ecCtx.DataShards, ecCtx.ParityShards, ecCtx.Total())

	if err := volume_info.SaveVolumeInfo(baseFileName+".vif", volumeInfo); err != nil {
		return nil, fmt.Errorf("SaveVolumeInfo %s: %w", baseFileName, err)
	}

	shouldCleanup = false

	return &volume_server_pb.VolumeEcShardsGenerateResponse{}, nil
}

// VolumeEcShardsRebuild generates the any of the missing .ec00 ~ .ec13 files
func (vs *VolumeServer) VolumeEcShardsRebuild(ctx context.Context, req *volume_server_pb.VolumeEcShardsRebuildRequest) (*volume_server_pb.VolumeEcShardsRebuildResponse, error) {
	if err := vs.CheckMaintenanceMode(); err != nil {
		return nil, err
	}

	glog.V(0).Infof("VolumeEcShardsRebuild: %v", req)
	baseFileName := erasure_coding.EcShardBaseFileName(req.GetCollection(), int(req.GetVolumeId()))

	var rebuiltShardIds []uint32

	for _, location := range vs.store.Locations {
		_, _, existingShardCount, err := checkEcVolumeStatus(baseFileName, location)
		if err != nil {
			return nil, err
		}

		if existingShardCount == 0 {
			continue
		}

		if util.FileExists(path.Join(location.IdxDirectory, baseFileName+".ecx")) {
			// write .ec00 ~ .ec13 files
			dataBaseFileName := path.Join(location.Directory, baseFileName)
			if generatedShardIds, err := erasure_coding.RebuildEcFiles(dataBaseFileName); err != nil {
				return nil, fmt.Errorf("RebuildEcFiles %s: %w", dataBaseFileName, err)
			} else {
				rebuiltShardIds = generatedShardIds
			}

			indexBaseFileName := path.Join(location.IdxDirectory, baseFileName)
			if err := erasure_coding.RebuildEcxFile(indexBaseFileName); err != nil {
				return nil, fmt.Errorf("RebuildEcxFile %s: %w", dataBaseFileName, err)
			}

			break
		}
	}

	return &volume_server_pb.VolumeEcShardsRebuildResponse{
		RebuiltShardIds: rebuiltShardIds,
	}, nil
}

// VolumeEcShardsCopy copy the .ecx and some ec data slices
func (vs *VolumeServer) VolumeEcShardsCopy(ctx context.Context, req *volume_server_pb.VolumeEcShardsCopyRequest) (*volume_server_pb.VolumeEcShardsCopyResponse, error) {
	if err := vs.CheckMaintenanceMode(); err != nil {
		return nil, err
	}

	glog.V(0).Infof("VolumeEcShardsCopy: %v", req)

	var location *storage.DiskLocation

	// Use disk_id if provided (disk-aware storage)
	if req.GetDiskId() > 0 || (req.GetDiskId() == 0 && len(vs.store.Locations) > 0) {
		// Validate disk ID is within bounds
		if int(req.GetDiskId()) >= len(vs.store.Locations) {
			return nil, fmt.Errorf("invalid disk_id %d: only have %d disks", req.GetDiskId(), len(vs.store.Locations))
		}

		// Use the specific disk location
		location = vs.store.Locations[req.GetDiskId()]
		glog.V(1).Infof("Using disk %d for EC shard copy: %s", req.GetDiskId(), location.Directory)
	} else {
		// Fallback to old behavior for backward compatibility
		if req.GetCopyEcxFile() {
			location = vs.store.FindFreeLocation(func(location *storage.DiskLocation) bool {
				return location.DiskType == types.HardDriveType
			})
		} else {
			location = vs.store.FindFreeLocation(func(location *storage.DiskLocation) bool {
				return true
			})
		}
		if location == nil {
			return nil, errors.New("no space left")
		}
	}

	dataBaseFileName := storage.VolumeFileName(location.Directory, req.GetCollection(), int(req.GetVolumeId()))
	indexBaseFileName := storage.VolumeFileName(location.IdxDirectory, req.GetCollection(), int(req.GetVolumeId()))

	err := operation.WithVolumeServerClient(true, pb.ServerAddress(req.GetSourceDataNode()), vs.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		// copy ec data slices
		for _, shardId := range req.GetShardIds() {
			if _, err := vs.doCopyFile(client, true, req.GetCollection(), req.GetVolumeId(), math.MaxUint32, math.MaxInt64, dataBaseFileName, erasure_coding.ToExt(int(shardId)), false, false, nil); err != nil {
				return err
			}
		}

		if req.GetCopyEcxFile() {
			// copy ecx file
			if _, err := vs.doCopyFile(client, true, req.GetCollection(), req.GetVolumeId(), math.MaxUint32, math.MaxInt64, indexBaseFileName, ".ecx", false, false, nil); err != nil {
				return err
			}
		}

		if req.GetCopyEcjFile() {
			// copy ecj file
			if _, err := vs.doCopyFile(client, true, req.GetCollection(), req.GetVolumeId(), math.MaxUint32, math.MaxInt64, indexBaseFileName, ".ecj", true, true, nil); err != nil {
				return err
			}
		}

		if req.GetCopyVifFile() {
			// copy vif file
			if _, err := vs.doCopyFile(client, true, req.GetCollection(), req.GetVolumeId(), math.MaxUint32, math.MaxInt64, dataBaseFileName, ".vif", false, true, nil); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("VolumeEcShardsCopy volume %d: %w", req.GetVolumeId(), err)
	}

	return &volume_server_pb.VolumeEcShardsCopyResponse{}, nil
}

// VolumeEcShardsDelete local delete the .ecx and some ec data slices if not needed
// the shard should not be mounted before calling this.
func (vs *VolumeServer) VolumeEcShardsDelete(ctx context.Context, req *volume_server_pb.VolumeEcShardsDeleteRequest) (*volume_server_pb.VolumeEcShardsDeleteResponse, error) {
	if err := vs.CheckMaintenanceMode(); err != nil {
		return nil, err
	}

	bName := erasure_coding.EcShardBaseFileName(req.GetCollection(), int(req.GetVolumeId()))

	glog.V(0).Infof("ec volume %s shard delete %v", bName, req.GetShardIds())

	for _, location := range vs.store.Locations {
		if err := deleteEcShardIdsForEachLocation(bName, location, req.GetShardIds()); err != nil {
			glog.Errorf("deleteEcShards from %s %s.%v: %v", location.Directory, bName, req.GetShardIds(), err)

			return nil, err
		}
	}

	return &volume_server_pb.VolumeEcShardsDeleteResponse{}, nil
}

func deleteEcShardIdsForEachLocation(bName string, location *storage.DiskLocation, shardIds []uint32) error {
	found := false

	indexBaseFilename := path.Join(location.IdxDirectory, bName)
	dataBaseFilename := path.Join(location.Directory, bName)

	if util.FileExists(path.Join(location.IdxDirectory, bName+".ecx")) {
		for _, shardId := range shardIds {
			shardFileName := dataBaseFilename + erasure_coding.ToExt(int(shardId))
			if util.FileExists(shardFileName) {
				found = true
				os.Remove(shardFileName)
			}
		}
	}

	if !found {
		return nil
	}

	hasEcxFile, hasIdxFile, existingShardCount, err := checkEcVolumeStatus(bName, location)
	if err != nil {
		return err
	}

	if hasEcxFile && existingShardCount == 0 {
		if err := os.Remove(indexBaseFilename + ".ecx"); err != nil {
			return err
		}
		os.Remove(indexBaseFilename + ".ecj")

		if !hasIdxFile {
			// .vif is used for ec volumes and normal volumes
			os.Remove(dataBaseFilename + ".vif")
		}
	}

	return nil
}

func checkEcVolumeStatus(bName string, location *storage.DiskLocation) (hasEcxFile bool, hasIdxFile bool, existingShardCount int, err error) {
	// check whether to delete the .ecx and .ecj file also
	fileInfos, err := os.ReadDir(location.Directory)
	if err != nil {
		return false, false, 0, err
	}
	if location.IdxDirectory != location.Directory {
		idxFileInfos, err := os.ReadDir(location.IdxDirectory)
		if err != nil {
			return false, false, 0, err
		}
		fileInfos = append(fileInfos, idxFileInfos...)
	}
	for _, fileInfo := range fileInfos {
		if fileInfo.Name() == bName+".ecx" || fileInfo.Name() == bName+".ecj" {
			hasEcxFile = true

			continue
		}
		if fileInfo.Name() == bName+".idx" {
			hasIdxFile = true

			continue
		}
		if strings.HasPrefix(fileInfo.Name(), bName+".ec") {
			existingShardCount++
		}
	}

	return hasEcxFile, hasIdxFile, existingShardCount, nil
}

func (vs *VolumeServer) VolumeEcShardsMount(ctx context.Context, req *volume_server_pb.VolumeEcShardsMountRequest) (*volume_server_pb.VolumeEcShardsMountResponse, error) {
	glog.V(0).Infof("VolumeEcShardsMount: %v", req)

	for _, shardId := range req.GetShardIds() {
		err := vs.store.MountEcShards(req.GetCollection(), needle.VolumeId(req.GetVolumeId()), erasure_coding.ShardId(shardId))

		if err != nil {
			glog.Errorf("ec shard mount %v: %v", req, err)
		} else {
			glog.V(2).Infof("ec shard mount %v", req)
		}

		if err != nil {
			return nil, fmt.Errorf("mount %d.%d: %w", req.GetVolumeId(), shardId, err)
		}
	}

	return &volume_server_pb.VolumeEcShardsMountResponse{}, nil
}

func (vs *VolumeServer) VolumeEcShardsUnmount(ctx context.Context, req *volume_server_pb.VolumeEcShardsUnmountRequest) (*volume_server_pb.VolumeEcShardsUnmountResponse, error) {
	glog.V(0).Infof("VolumeEcShardsUnmount: %v", req)

	for _, shardId := range req.GetShardIds() {
		err := vs.store.UnmountEcShards(needle.VolumeId(req.GetVolumeId()), erasure_coding.ShardId(shardId))

		if err != nil {
			glog.Errorf("ec shard unmount %v: %v", req, err)
		} else {
			glog.V(2).Infof("ec shard unmount %v", req)
		}

		if err != nil {
			return nil, fmt.Errorf("unmount %d.%d: %w", req.GetVolumeId(), shardId, err)
		}
	}

	return &volume_server_pb.VolumeEcShardsUnmountResponse{}, nil
}

func (vs *VolumeServer) VolumeEcShardRead(req *volume_server_pb.VolumeEcShardReadRequest, stream volume_server_pb.VolumeServer_VolumeEcShardReadServer) error {
	ecVolume, found := vs.store.FindEcVolume(needle.VolumeId(req.GetVolumeId()))
	if !found {
		return fmt.Errorf("VolumeEcShardRead not found ec volume id %d", req.GetVolumeId())
	}
	ecShard, found := ecVolume.FindEcVolumeShard(erasure_coding.ShardId(req.GetShardId()))
	if !found {
		return fmt.Errorf("not found ec shard %d.%d", req.GetVolumeId(), req.GetShardId())
	}

	if req.GetFileKey() != 0 {
		_, size, _ := ecVolume.FindNeedleFromEcx(types.Uint64ToNeedleId(req.GetFileKey()))
		if size.IsDeleted() {
			return stream.Send(&volume_server_pb.VolumeEcShardReadResponse{
				IsDeleted: true,
			})
		}
	}

	bufSize := min(req.GetSize(), BufferSizeLimit)
	buffer := make([]byte, bufSize)

	startOffset, bytesToRead := req.GetOffset(), req.GetSize()

	for bytesToRead > 0 {
		// min of bytesToRead and bufSize
		bufferSize := min(bufSize, bytesToRead)
		bytesread, err := ecShard.ReadAt(buffer[0:bufferSize], startOffset)

		// println("read", ecShard.FileName(), "startOffset", startOffset, bytesread, "bytes, with target", bufferSize)
		if bytesread > 0 {
			if int64(bytesread) > bytesToRead {
				bytesread = int(bytesToRead)
			}
			err = stream.Send(&volume_server_pb.VolumeEcShardReadResponse{
				Data: buffer[:bytesread],
			})
			if err != nil {
				// println("sending", bytesread, "bytes err", err.Error())
				return err
			}

			startOffset += int64(bytesread)
			bytesToRead -= int64(bytesread)
		}

		if err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}

			return nil
		}
	}

	return nil
}

func (vs *VolumeServer) VolumeEcBlobDelete(ctx context.Context, req *volume_server_pb.VolumeEcBlobDeleteRequest) (*volume_server_pb.VolumeEcBlobDeleteResponse, error) {
	if err := vs.CheckMaintenanceMode(); err != nil {
		return nil, err
	}

	glog.V(0).Infof("VolumeEcBlobDelete: %v", req)

	resp := &volume_server_pb.VolumeEcBlobDeleteResponse{}

	for _, location := range vs.store.Locations {
		if localEcVolume, found := location.FindEcVolume(needle.VolumeId(req.GetVolumeId())); found {
			_, size, _, err := localEcVolume.LocateEcShardNeedle(types.NeedleId(req.GetFileKey()), needle.Version(req.GetVersion()))
			if err != nil {
				return nil, fmt.Errorf("locate in local ec volume: %w", err)
			}
			if size.IsDeleted() {
				return resp, nil
			}

			err = localEcVolume.DeleteNeedleFromEcx(types.NeedleId(req.GetFileKey()))
			if err != nil {
				return nil, err
			}

			break
		}
	}

	return resp, nil
}

// VolumeEcShardsToVolume generates the .idx, .dat files from .ecx, .ecj and .ec01 ~ .ec14 files
func (vs *VolumeServer) VolumeEcShardsToVolume(ctx context.Context, req *volume_server_pb.VolumeEcShardsToVolumeRequest) (*volume_server_pb.VolumeEcShardsToVolumeResponse, error) {
	if err := vs.CheckMaintenanceMode(); err != nil {
		return nil, err
	}

	glog.V(0).Infof("VolumeEcShardsToVolume: %v", req)

	// Collect all EC shards (NewEcVolume will load EC config from .vif into v.ECContext)
	// Use MaxShardCount (32) to support custom EC ratios up to 32 total shards
	tempShards := make([]string, erasure_coding.MaxShardCount)
	v, found := vs.store.CollectEcShards(needle.VolumeId(req.GetVolumeId()), tempShards)
	if !found {
		return nil, fmt.Errorf("ec volume %d not found", req.GetVolumeId())
	}

	if v.Collection != req.GetCollection() {
		return nil, fmt.Errorf("existing collection:%v unexpected input: %v", v.Collection, req.GetCollection())
	}

	// Use EC context (already loaded from .vif) to determine data shard count
	dataShards := v.ECContext.DataShards

	// Defensive validation to prevent panics from corrupted ECContext
	if dataShards <= 0 || dataShards > erasure_coding.MaxShardCount {
		return nil, fmt.Errorf("invalid data shard count %d for volume %d (must be 1..%d)", dataShards, req.GetVolumeId(), erasure_coding.MaxShardCount)
	}

	shardFileNames := tempShards[:dataShards]
	glog.V(1).Infof("Using EC config from volume %d: %d data shards", req.GetVolumeId(), dataShards)

	// Verify all data shards are present
	for shardId := range dataShards {
		if shardFileNames[shardId] == "" {
			return nil, fmt.Errorf("ec volume %d missing shard %d", req.GetVolumeId(), shardId)
		}
	}

	dataBaseFileName, indexBaseFileName := v.DataBaseFileName(), v.IndexBaseFileName()

	// If the EC index contains no live entries, decoding should be a no-op:
	// just allow the caller to purge EC shards and do not generate an empty normal volume.
	hasLive, err := erasure_coding.HasLiveNeedles(indexBaseFileName)
	if err != nil {
		return nil, fmt.Errorf("HasLiveNeedles %s: %w", indexBaseFileName, err)
	}
	if !hasLive {
		return nil, status.Errorf(codes.FailedPrecondition, "ec volume %d %s", req.GetVolumeId(), erasure_coding.EcNoLiveEntriesSubstring)
	}

	// calculate .dat file size
	datFileSize, err := erasure_coding.FindDatFileSize(dataBaseFileName, indexBaseFileName)
	if err != nil {
		return nil, fmt.Errorf("FindDatFileSize %s: %w", dataBaseFileName, err)
	}

	// write .dat file from .ec00 ~ .ec09 files
	if err := erasure_coding.WriteDatFile(dataBaseFileName, datFileSize, shardFileNames); err != nil {
		return nil, fmt.Errorf("WriteDatFile %s: %w", dataBaseFileName, err)
	}

	// write .idx file from .ecx and .ecj files
	if err := erasure_coding.WriteIdxFileFromEcIndex(indexBaseFileName); err != nil {
		return nil, fmt.Errorf("WriteIdxFileFromEcIndex %s: %w", v.IndexBaseFileName(), err)
	}

	return &volume_server_pb.VolumeEcShardsToVolumeResponse{}, nil
}

func (vs *VolumeServer) VolumeEcShardsInfo(ctx context.Context, req *volume_server_pb.VolumeEcShardsInfoRequest) (*volume_server_pb.VolumeEcShardsInfoResponse, error) {
	glog.V(0).Infof("VolumeEcShardsInfo: volume %d", req.GetVolumeId())

	var ecShardInfos []*volume_server_pb.EcShardInfo

	// Find the EC volume
	for _, location := range vs.store.Locations {
		if v, found := location.FindEcVolume(needle.VolumeId(req.GetVolumeId())); found {
			// Get shard details from the EC volume
			for _, si := range erasure_coding.ShardsInfoFromVolume(v).AsSlice() {
				ecShardInfo := &volume_server_pb.EcShardInfo{
					ShardId:    uint32(si.Id),
					Size:       int64(si.Size),
					Collection: v.Collection,
					VolumeId:   uint32(v.VolumeId),
				}
				ecShardInfos = append(ecShardInfos, ecShardInfo)
			}

			break
		}
	}

	return &volume_server_pb.VolumeEcShardsInfoResponse{
		EcShardInfos: ecShardInfos,
	}, nil
}
