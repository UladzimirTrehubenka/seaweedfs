package types

import (
	"strings"
)

type DiskType string

const (
	hardDriveType = ""
	hddType       = "hdd"
	ssdType       = "ssd"

	HardDriveType DiskType = hardDriveType
	HddType       DiskType = hddType
	SsdType       DiskType = ssdType
)

func ToDiskType(vt string) (diskType DiskType) {
	vt = strings.ToLower(vt)
	diskType = HardDriveType
	switch vt {
	case hardDriveType, hddType:
		diskType = HardDriveType
	case ssdType:
		diskType = SsdType
	default:
		diskType = DiskType(vt)
	}

	return
}

func (diskType DiskType) String() string {
	if diskType == HardDriveType {
		return hardDriveType
	}

	return string(diskType)
}

func (diskType DiskType) ReadableString() string {
	if diskType == HardDriveType {
		return hddType
	}

	return string(diskType)
}
