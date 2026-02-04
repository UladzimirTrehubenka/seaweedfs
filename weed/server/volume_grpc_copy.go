package weed_server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const BufferSizeLimit = 1024 * 1024 * 2

// VolumeCopy copy the .idx .dat .vif files, and mount the volume
func (vs *VolumeServer) VolumeCopy(req *volume_server_pb.VolumeCopyRequest, stream volume_server_pb.VolumeServer_VolumeCopyServer) error {
	if err := vs.CheckMaintenanceMode(); err != nil {
		return err
	}

	v := vs.store.GetVolume(needle.VolumeId(req.GetVolumeId()))
	if v != nil {
		glog.V(0).Infof("volume %d already exists. deleted before copying...", req.GetVolumeId())

		err := vs.store.DeleteVolume(needle.VolumeId(req.GetVolumeId()), false)
		if err != nil {
			return fmt.Errorf("failed to delete existing volume %d: %w", req.GetVolumeId(), err)
		}

		glog.V(0).Infof("deleted existing volume %d before copying.", req.GetVolumeId())
	}

	// the master will not start compaction for read-only volumes, so it is safe to just copy files directly
	// copy .dat and .idx files
	//   read .idx .dat file size and timestamp
	//   send .idx file
	//   send .dat file
	//   confirm size and timestamp
	var volFileInfoResp *volume_server_pb.ReadVolumeFileStatusResponse
	var dataBaseFileName, indexBaseFileName, idxFileName, datFileName string
	var hasRemoteDatFile bool
	err := operation.WithVolumeServerClient(true, pb.ServerAddress(req.GetSourceDataNode()), vs.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		var err error
		volFileInfoResp, err = client.ReadVolumeFileStatus(context.Background(),
			&volume_server_pb.ReadVolumeFileStatusRequest{
				VolumeId: req.GetVolumeId(),
			})
		if nil != err {
			return fmt.Errorf("read volume file status failed, %w", err)
		}

		diskType := volFileInfoResp.GetDiskType()
		if req.GetDiskType() != "" {
			diskType = req.GetDiskType()
		}
		location := vs.store.FindFreeLocation(func(location *storage.DiskLocation) bool {
			return location.DiskType == types.ToDiskType(diskType)
		})
		if location == nil {
			return fmt.Errorf("no space left for disk type %s", types.ToDiskType(diskType).ReadableString())
		}

		dataBaseFileName = storage.VolumeFileName(location.Directory, volFileInfoResp.GetCollection(), int(req.GetVolumeId()))
		indexBaseFileName = storage.VolumeFileName(location.IdxDirectory, volFileInfoResp.GetCollection(), int(req.GetVolumeId()))
		hasRemoteDatFile = volFileInfoResp.GetVolumeInfo() != nil && len(volFileInfoResp.GetVolumeInfo().GetFiles()) > 0

		util.WriteFile(dataBaseFileName+".note", []byte("copying from "+req.GetSourceDataNode()), 0755)

		defer func() {
			if err != nil {
				os.Remove(dataBaseFileName + ".dat")
				os.Remove(indexBaseFileName + ".idx")
				os.Remove(dataBaseFileName + ".vif")
				os.Remove(dataBaseFileName + ".note")
			}
		}()

		var preallocateSize int64
		if grpcErr := pb.WithMasterClient(false, vs.GetMaster(context.Background()), vs.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
			resp, err := client.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
			if err != nil {
				return fmt.Errorf("get master %s configuration: %w", vs.GetMaster(context.Background()), err)
			}
			if resp.GetVolumePreallocate() {
				preallocateSize = int64(resp.GetVolumeSizeLimitMB()) * (1 << 20)
			}

			return nil
		}); grpcErr != nil {
			glog.V(0).Infof("connect to %s: %v", vs.GetMaster(context.Background()), grpcErr)
		}

		if preallocateSize > 0 && !hasRemoteDatFile {
			volumeFile := dataBaseFileName + ".dat"
			_, err := backend.CreateVolumeFile(volumeFile, preallocateSize, 0)
			if err != nil {
				return fmt.Errorf("create volume file %s: %w", volumeFile, err)
			}
		}

		// println("source:", volFileInfoResp.String())
		copyResponse := &volume_server_pb.VolumeCopyResponse{}
		reportInterval := int64(1024 * 1024 * 128)
		nextReportTarget := reportInterval
		var modifiedTsNs int64
		var sendErr error
		var ioBytePerSecond int64
		if req.GetIoBytePerSecond() <= 0 {
			ioBytePerSecond = vs.maintenanceBytePerSecond
		} else {
			ioBytePerSecond = req.GetIoBytePerSecond()
		}
		throttler := util.NewWriteThrottler(ioBytePerSecond)

		if !hasRemoteDatFile {
			if modifiedTsNs, err = vs.doCopyFileWithThrottler(client, false, req.GetCollection(), req.GetVolumeId(), volFileInfoResp.GetCompactionRevision(), volFileInfoResp.GetDatFileSize(), dataBaseFileName, ".dat", false, true, func(processed int64) bool {
				if processed > nextReportTarget {
					copyResponse.ProcessedBytes = processed
					if sendErr = stream.Send(copyResponse); sendErr != nil {
						return false
					}
					nextReportTarget = processed + reportInterval
				}

				return true
			}, throttler); err != nil {
				return err
			}
			if sendErr != nil {
				return sendErr
			}
			if modifiedTsNs > 0 {
				os.Chtimes(dataBaseFileName+".dat", time.Unix(0, modifiedTsNs), time.Unix(0, modifiedTsNs))
			}
		}

		if modifiedTsNs, err = vs.doCopyFileWithThrottler(client, false, req.GetCollection(), req.GetVolumeId(), volFileInfoResp.GetCompactionRevision(), volFileInfoResp.GetIdxFileSize(), indexBaseFileName, ".idx", false, false, nil, throttler); err != nil {
			return err
		}
		if modifiedTsNs > 0 {
			os.Chtimes(indexBaseFileName+".idx", time.Unix(0, modifiedTsNs), time.Unix(0, modifiedTsNs))
		}

		if modifiedTsNs, err = vs.doCopyFileWithThrottler(client, false, req.GetCollection(), req.GetVolumeId(), volFileInfoResp.GetCompactionRevision(), 1024*1024, dataBaseFileName, ".vif", false, true, nil, throttler); err != nil {
			return err
		}
		if modifiedTsNs > 0 {
			os.Chtimes(dataBaseFileName+".vif", time.Unix(0, modifiedTsNs), time.Unix(0, modifiedTsNs))
		}

		os.Remove(dataBaseFileName + ".note")

		return nil
	})

	if err != nil {
		return err
	}
	if dataBaseFileName == "" {
		return fmt.Errorf("not found volume %d file", req.GetVolumeId())
	}

	idxFileName = indexBaseFileName + ".idx"
	datFileName = dataBaseFileName + ".dat"

	defer func() {
		if err != nil && dataBaseFileName != "" {
			os.Remove(idxFileName)
			os.Remove(datFileName)
			os.Remove(dataBaseFileName + ".vif")
		}
	}()

	if err = checkCopyFiles(volFileInfoResp, hasRemoteDatFile, idxFileName, datFileName); err != nil { // added by panyc16
		return err
	}

	// mount the volume
	err = vs.store.MountVolume(needle.VolumeId(req.GetVolumeId()))
	if err != nil {
		return fmt.Errorf("failed to mount volume %d: %w", req.GetVolumeId(), err)
	}

	if err = stream.Send(&volume_server_pb.VolumeCopyResponse{
		LastAppendAtNs: volFileInfoResp.GetDatFileTimestampSeconds() * uint64(time.Second),
	}); err != nil {
		glog.Errorf("send response: %v", err)
	}

	return err
}

func (vs *VolumeServer) doCopyFile(client volume_server_pb.VolumeServerClient, isEcVolume bool, collection string, vid, compactRevision uint32, stopOffset uint64, baseFileName, ext string, isAppend, ignoreSourceFileNotFound bool, progressFn storage.ProgressFunc) (modifiedTsNs int64, err error) {
	return vs.doCopyFileWithThrottler(client, isEcVolume, collection, vid, compactRevision, stopOffset, baseFileName, ext, isAppend, ignoreSourceFileNotFound, progressFn, util.NewWriteThrottler(vs.maintenanceBytePerSecond))
}

func (vs *VolumeServer) doCopyFileWithThrottler(client volume_server_pb.VolumeServerClient, isEcVolume bool, collection string, vid, compactRevision uint32, stopOffset uint64, baseFileName, ext string, isAppend, ignoreSourceFileNotFound bool, progressFn storage.ProgressFunc, throttler *util.WriteThrottler) (modifiedTsNs int64, err error) {
	copyFileClient, err := client.CopyFile(context.Background(), &volume_server_pb.CopyFileRequest{
		VolumeId:                 vid,
		Ext:                      ext,
		CompactionRevision:       compactRevision,
		StopOffset:               stopOffset,
		Collection:               collection,
		IsEcVolume:               isEcVolume,
		IgnoreSourceFileNotFound: ignoreSourceFileNotFound,
	})
	if err != nil {
		return modifiedTsNs, fmt.Errorf("failed to start copying volume %d %s file: %w", vid, ext, err)
	}

	modifiedTsNs, err = writeToFile(copyFileClient, baseFileName+ext, throttler, isAppend, progressFn)
	if err != nil {
		return modifiedTsNs, fmt.Errorf("failed to copy %s file: %w", baseFileName+ext, err)
	}

	return modifiedTsNs, nil
}

/*
*
only check the differ of the file size
todo: maybe should check the received count and deleted count of the volume
*/
func checkCopyFiles(originFileInf *volume_server_pb.ReadVolumeFileStatusResponse, hasRemoteDatFile bool, idxFileName, datFileName string) error {
	stat, err := os.Stat(idxFileName)
	if err != nil {
		// If the idx file doesn't exist but the expected size is 0, that's OK (empty volume)
		if os.IsNotExist(err) && originFileInf.GetIdxFileSize() == 0 {
			// empty volume, idx file not needed
		} else {
			return fmt.Errorf("stat idx file %s failed: %w", idxFileName, err)
		}
	} else if originFileInf.GetIdxFileSize() != uint64(stat.Size()) {
		return fmt.Errorf("idx file %s size [%v] is not same as origin file size [%v]",
			idxFileName, stat.Size(), originFileInf.GetIdxFileSize())
	}

	if hasRemoteDatFile {
		return nil
	}

	stat, err = os.Stat(datFileName)
	if err != nil {
		return fmt.Errorf("get dat file info failed, %w", err)
	}
	if originFileInf.GetDatFileSize() != uint64(stat.Size()) {
		return fmt.Errorf("the dat file size [%v] is not same as origin file size [%v]",
			stat.Size(), originFileInf.GetDatFileSize())
	}

	return nil
}

func writeToFile(client volume_server_pb.VolumeServer_CopyFileClient, fileName string, wt *util.WriteThrottler, isAppend bool, progressFn storage.ProgressFunc) (modifiedTsNs int64, err error) {
	glog.V(4).Infof("writing to %s", fileName)
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if isAppend {
		flags = os.O_WRONLY | os.O_CREATE
	}
	dst, err := os.OpenFile(fileName, flags, 0644)
	if err != nil {
		return modifiedTsNs, fmt.Errorf("open file %s: %w", fileName, err)
	}
	defer dst.Close()

	var progressedBytes int64
	for {
		resp, receiveErr := client.Recv()
		if errors.Is(receiveErr, io.EOF) {
			break
		}
		if resp != nil && resp.GetModifiedTsNs() != 0 {
			modifiedTsNs = resp.GetModifiedTsNs()
		}
		if receiveErr != nil {
			return modifiedTsNs, fmt.Errorf("receiving %s: %w", fileName, receiveErr)
		}
		if _, writeErr := dst.Write(resp.GetFileContent()); writeErr != nil {
			return modifiedTsNs, fmt.Errorf("write file %s: %w", fileName, writeErr)
		}
		progressedBytes += int64(len(resp.GetFileContent()))
		if progressFn != nil {
			if !progressFn(progressedBytes) {
				return modifiedTsNs, errors.New("interrupted copy operation")
			}
		}
		wt.MaybeSlowdown(int64(len(resp.GetFileContent())))
	}
	// If we never received a modifiedTsNs, it means the source file did not exist.
	// Remove the empty file we created to avoid leaving corrupted empty files.
	// Note: We check modifiedTsNs (not progressedBytes) because an empty source file
	// is valid and should result in an empty destination file.
	if modifiedTsNs == 0 && !isAppend {
		if removeErr := os.Remove(fileName); removeErr != nil {
			glog.V(1).Infof("failed to remove empty file %s: %v", fileName, removeErr)
		} else {
			glog.V(1).Infof("removed empty file %s (source file not found)", fileName)
		}
	}

	return modifiedTsNs, nil
}

func (vs *VolumeServer) ReadVolumeFileStatus(ctx context.Context, req *volume_server_pb.ReadVolumeFileStatusRequest) (*volume_server_pb.ReadVolumeFileStatusResponse, error) {
	resp := &volume_server_pb.ReadVolumeFileStatusResponse{}
	v := vs.store.GetVolume(needle.VolumeId(req.GetVolumeId()))
	if v == nil {
		return nil, fmt.Errorf("not found volume id %d", req.GetVolumeId())
	}

	resp.VolumeId = req.GetVolumeId()
	datSize, idxSize, modTime := v.FileStat()
	resp.DatFileSize = datSize
	resp.IdxFileSize = idxSize
	resp.DatFileTimestampSeconds = uint64(modTime.Unix())
	resp.IdxFileTimestampSeconds = uint64(modTime.Unix())
	resp.FileCount = v.FileCount()
	resp.CompactionRevision = uint32(v.CompactionRevision)
	resp.Collection = v.Collection
	resp.DiskType = string(v.DiskType())
	resp.VolumeInfo = v.GetVolumeInfo()
	resp.Version = uint32(v.Version())

	return resp, nil
}

// CopyFile client pulls the volume related file from the source server.
// if req.CompactionRevision != math.MaxUint32, it ensures the compact revision is as expected
// The copying still stop at req.StopOffset, but you can set it to math.MaxUint64 in order to read all data.
func (vs *VolumeServer) CopyFile(req *volume_server_pb.CopyFileRequest, stream volume_server_pb.VolumeServer_CopyFileServer) error {
	var fileName string
	if !req.GetIsEcVolume() {
		v := vs.store.GetVolume(needle.VolumeId(req.GetVolumeId()))
		if v == nil {
			return fmt.Errorf("not found volume id %d", req.GetVolumeId())
		}

		if uint32(v.CompactionRevision) != req.GetCompactionRevision() && req.GetCompactionRevision() != math.MaxUint32 {
			return fmt.Errorf("volume %d is compacted", req.GetVolumeId())
		}
		v.SyncToDisk()
		fileName = v.FileName(req.GetExt())
	} else {
		// Sync EC volume files to disk before copying to ensure deletions are visible
		// This fixes issue #7751 where deleted files in encoded volumes were not
		// properly marked as deleted when decoded.
		if ecVolume, found := vs.store.FindEcVolume(needle.VolumeId(req.GetVolumeId())); found {
			ecVolume.Sync()
		}

		baseFileName := erasure_coding.EcShardBaseFileName(req.GetCollection(), int(req.GetVolumeId())) + req.GetExt()
		for _, location := range vs.store.Locations {
			tName := util.Join(location.Directory, baseFileName)
			if util.FileExists(tName) {
				fileName = tName
			}
			tName = util.Join(location.IdxDirectory, baseFileName)
			if util.FileExists(tName) {
				fileName = tName
			}
		}
		if fileName == "" {
			if req.GetIgnoreSourceFileNotFound() {
				return nil
			}

			return fmt.Errorf("CopyFile not found ec volume id %d", req.GetVolumeId())
		}
	}

	bytesToRead := int64(req.GetStopOffset())

	file, err := os.Open(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			// If file doesn't exist and we're asked to copy 0 bytes (empty file),
			// or if IgnoreSourceFileNotFound is set, treat as success
			if req.GetIgnoreSourceFileNotFound() || req.GetStopOffset() == 0 {
				return nil
			}
		}

		return err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	fileModTsNs := fileInfo.ModTime().UnixNano()

	buffer := make([]byte, BufferSizeLimit)

	for bytesToRead > 0 {
		bytesread, err := file.Read(buffer)

		// println(fileName, "read", bytesread, "bytes, with target", bytesToRead)

		if err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}
			// println(fileName, "read", bytesread, "bytes, with target", bytesToRead, "err", err.Error())
			break
		}

		if int64(bytesread) > bytesToRead {
			bytesread = int(bytesToRead)
		}
		err = stream.Send(&volume_server_pb.CopyFileResponse{
			FileContent:  buffer[:bytesread],
			ModifiedTsNs: fileModTsNs,
		})
		if err != nil {
			// println("sending", bytesread, "bytes err", err.Error())
			return err
		}
		fileModTsNs = 0 // only send once

		bytesToRead -= int64(bytesread)
	}

	// If no data has been sent in the loop (e.g. for an empty file, or when stopOffset is 0),
	// we still need to send the ModifiedTsNs so the client knows the source file exists.
	// fileModTsNs is set to 0 after the first send, so if it's still non-zero,
	// we haven't sent anything yet.
	if fileModTsNs != 0 {
		err = stream.Send(&volume_server_pb.CopyFileResponse{
			ModifiedTsNs: fileModTsNs,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// ReceiveFile receives a file stream from client and writes it to storage
func (vs *VolumeServer) ReceiveFile(stream volume_server_pb.VolumeServer_ReceiveFileServer) error {
	if err := vs.CheckMaintenanceMode(); err != nil {
		return err
	}

	var fileInfo *volume_server_pb.ReceiveFileInfo
	var targetFile *os.File
	var filePath string
	var bytesWritten uint64

	defer func() {
		if targetFile != nil {
			targetFile.Close()
		}
	}()

	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			// Stream completed successfully
			if targetFile != nil {
				targetFile.Sync()
				glog.V(1).Infof("Successfully received file %s (%d bytes)", filePath, bytesWritten)
			}

			return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
				BytesWritten: bytesWritten,
			})
		}
		if err != nil {
			// Clean up on error
			if targetFile != nil {
				targetFile.Close()
				os.Remove(filePath)
			}
			glog.Errorf("Failed to receive stream: %v", err)

			return fmt.Errorf("failed to receive stream: %w", err)
		}

		switch data := req.GetData().(type) {
		case *volume_server_pb.ReceiveFileRequest_Info:
			// First message contains file info
			fileInfo = data.Info
			glog.V(1).Infof("ReceiveFile: volume %d, ext %s, collection %s, shard %d, size %d",
				fileInfo.GetVolumeId(), fileInfo.GetExt(), fileInfo.GetCollection(), fileInfo.GetShardId(), fileInfo.GetFileSize())

			// Create file path based on file info
			if fileInfo.GetIsEcVolume() {
				// Find storage location for EC shard
				var targetLocation *storage.DiskLocation
				for _, location := range vs.store.Locations {
					if location.DiskType == types.HardDriveType {
						targetLocation = location

						break
					}
				}
				if targetLocation == nil && len(vs.store.Locations) > 0 {
					targetLocation = vs.store.Locations[0] // Fall back to first available location
				}
				if targetLocation == nil {
					glog.Errorf("ReceiveFile: no storage location available")

					return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
						Error: "no storage location available",
					})
				}

				// Create EC shard file path
				baseFileName := erasure_coding.EcShardBaseFileName(fileInfo.GetCollection(), int(fileInfo.GetVolumeId()))
				filePath = util.Join(targetLocation.Directory, baseFileName+fileInfo.GetExt())
			} else {
				// Regular volume file
				v := vs.store.GetVolume(needle.VolumeId(fileInfo.GetVolumeId()))
				if v == nil {
					glog.Errorf("ReceiveFile: volume %d not found", fileInfo.GetVolumeId())

					return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
						Error: fmt.Sprintf("volume %d not found", fileInfo.GetVolumeId()),
					})
				}
				filePath = v.FileName(fileInfo.GetExt())
			}

			// Create target file
			targetFile, err = os.Create(filePath)
			if err != nil {
				glog.Errorf("ReceiveFile: failed to create file %s: %v", filePath, err)

				return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
					Error: fmt.Sprintf("failed to create file: %v", err),
				})
			}
			glog.V(1).Infof("ReceiveFile: created target file %s", filePath)

		case *volume_server_pb.ReceiveFileRequest_FileContent:
			// Subsequent messages contain file content
			if targetFile == nil {
				glog.Errorf("ReceiveFile: file info must be sent first")

				return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
					Error: "file info must be sent first",
				})
			}

			n, err := targetFile.Write(data.FileContent)
			if err != nil {
				targetFile.Close()
				os.Remove(filePath)
				glog.Errorf("ReceiveFile: failed to write to file %s: %v", filePath, err)

				return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
					Error: fmt.Sprintf("failed to write file: %v", err),
				})
			}
			bytesWritten += uint64(n)
			glog.V(2).Infof("ReceiveFile: wrote %d bytes to %s (total: %d)", n, filePath, bytesWritten)

		default:
			glog.Errorf("ReceiveFile: unknown message type")

			return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
				Error: "unknown message type",
			})
		}
	}
}
