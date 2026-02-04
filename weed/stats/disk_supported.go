//go:build !windows && !openbsd && !netbsd && !plan9 && !solaris

package stats

import (
	"syscall"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func fillInDiskStatus(disk *volume_server_pb.DiskStatus) {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(disk.GetDir(), &fs)
	if err != nil {
		return
	}
	disk.All = fs.Blocks * uint64(fs.Bsize)
	// https://man7.org/linux/man-pages/man3/statvfs.3.html
	// fs.Bfree: Number of free blocks
	// fs.Bavail: Number of free blocks for unprivileged users
	// disk.Free = fs.Bfree * uint64(fs.Bsize)
	disk.Free = uint64(fs.Bavail) * uint64(fs.Bsize)
	disk.Used = disk.GetAll() - disk.GetFree()
	disk.PercentFree = float32((float64(disk.GetFree()) / float64(disk.GetAll())) * 100)
	disk.PercentUsed = float32((float64(disk.GetUsed()) / float64(disk.GetAll())) * 100)

	return
}
