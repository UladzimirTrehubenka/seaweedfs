package stats

import "github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"

func calculateDiskRemaining(disk *volume_server_pb.DiskStatus) {
	disk.Used = disk.GetAll() - disk.GetFree()

	if disk.GetAll() > 0 {
		disk.PercentFree = float32((float64(disk.GetFree()) / float64(disk.GetAll())) * 100)
		disk.PercentUsed = float32((float64(disk.GetUsed()) / float64(disk.GetAll())) * 100)
	} else {
		disk.PercentFree = 0
		disk.PercentUsed = 0
	}

	return
}
