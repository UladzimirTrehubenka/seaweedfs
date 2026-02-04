package volume_info

import (
	"fmt"
	"os"

	jsonpb "google.golang.org/protobuf/encoding/protojson"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	_ "github.com/seaweedfs/seaweedfs/weed/storage/backend/rclone_backend"
	_ "github.com/seaweedfs/seaweedfs/weed/storage/backend/s3_backend"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// MaybeLoadVolumeInfo load the file data as *volume_server_pb.VolumeInfo, the returned volumeInfo will not be nil
func MaybeLoadVolumeInfo(fileName string) (volumeInfo *volume_server_pb.VolumeInfo, hasRemoteFile bool, hasVolumeInfoFile bool, err error) {
	volumeInfo = &volume_server_pb.VolumeInfo{}

	glog.V(1).Infof("maybeLoadVolumeInfo checks %s", fileName)
	if exists, canRead, _, _, _ := util.CheckFile(fileName); !exists || !canRead {
		if !exists {
			return volumeInfo, hasRemoteFile, hasVolumeInfoFile, err
		}
		hasVolumeInfoFile = true
		if !canRead {
			glog.Warningf("can not read %s", fileName)
			err = fmt.Errorf("can not read %s", fileName)

			return volumeInfo, hasRemoteFile, hasVolumeInfoFile, err
		}

		return volumeInfo, hasRemoteFile, hasVolumeInfoFile, err
	}

	hasVolumeInfoFile = true

	glog.V(1).Infof("maybeLoadVolumeInfo reads %s", fileName)
	fileData, readErr := os.ReadFile(fileName)
	if readErr != nil {
		glog.Warningf("fail to read %s : %v", fileName, readErr)
		err = fmt.Errorf("fail to read %s : %w", fileName, readErr)

		return volumeInfo, hasRemoteFile, hasVolumeInfoFile, err
	}

	// Handle empty .vif files gracefully - treat as if file doesn't exist
	// This can happen when ec.decode copies from a source that doesn't have a .vif file
	if len(fileData) == 0 {
		glog.Warningf("empty volume info file %s, treating as non-existent", fileName)
		hasVolumeInfoFile = false

		return volumeInfo, hasRemoteFile, hasVolumeInfoFile, err
	}

	glog.V(1).Infof("maybeLoadVolumeInfo Unmarshal volume info %v", fileName)
	if err = jsonpb.Unmarshal(fileData, volumeInfo); err != nil {
		if oldVersionErr := tryOldVersionVolumeInfo(fileData, volumeInfo); oldVersionErr != nil {
			glog.Warningf("unmarshal error: %v oldFormat: %v", err, oldVersionErr)
			err = fmt.Errorf("unmarshal error: %w oldFormat: %w", err, oldVersionErr)

			return volumeInfo, hasRemoteFile, hasVolumeInfoFile, err
		} else {
			err = nil
		}
	}

	if len(volumeInfo.GetFiles()) == 0 {
		return volumeInfo, hasRemoteFile, hasVolumeInfoFile, err
	}

	hasRemoteFile = true

	return volumeInfo, hasRemoteFile, hasVolumeInfoFile, err
}

func SaveVolumeInfo(fileName string, volumeInfo *volume_server_pb.VolumeInfo) error {
	if exists, _, canWrite, _, _ := util.CheckFile(fileName); exists && !canWrite {
		return fmt.Errorf("failed to check %s not writable", fileName)
	}

	m := jsonpb.MarshalOptions{
		AllowPartial:    true,
		EmitUnpopulated: true,
		Indent:          "  ",
	}

	text, marshalErr := m.Marshal(volumeInfo)
	if marshalErr != nil {
		return fmt.Errorf("failed to marshal %s: %w", fileName, marshalErr)
	}

	if err := util.WriteFile(fileName, text, 0644); err != nil {
		return fmt.Errorf("failed to write %s: %w", fileName, err)
	}

	return nil
}

func tryOldVersionVolumeInfo(data []byte, volumeInfo *volume_server_pb.VolumeInfo) error {
	oldVersionVolumeInfo := &volume_server_pb.OldVersionVolumeInfo{}
	if err := jsonpb.Unmarshal(data, oldVersionVolumeInfo); err != nil {
		return fmt.Errorf("failed to unmarshal old version volume info: %w", err)
	}
	volumeInfo.Files = oldVersionVolumeInfo.GetFiles()
	volumeInfo.Version = oldVersionVolumeInfo.GetVersion()
	volumeInfo.Replication = oldVersionVolumeInfo.GetReplication()
	volumeInfo.BytesOffset = oldVersionVolumeInfo.GetBytesOffset()
	volumeInfo.DatFileSize = oldVersionVolumeInfo.GetDatFileSize()
	volumeInfo.ExpireAtSec = oldVersionVolumeInfo.GetDestroyTime()
	volumeInfo.ReadOnly = oldVersionVolumeInfo.GetReadOnly()

	return nil
}
