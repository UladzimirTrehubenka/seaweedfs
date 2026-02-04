package mount

import (
	"os"
	"syscall"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (wfs *WFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	glog.V(4).Infof("GetAttr %v", input.NodeId)
	if input.NodeId == 1 {
		wfs.setRootAttr(out)

		return fuse.OK
	}

	inode := input.NodeId
	_, _, entry, status := wfs.maybeReadEntry(inode)
	if status == fuse.OK {
		out.AttrValid = 1
		wfs.setAttrByPbEntry(&out.Attr, inode, entry, true)

		return status
	} else {
		if fh, found := wfs.fhMap.FindFileHandle(inode); found {
			out.AttrValid = 1
			// Use shared lock to prevent race with Write operations
			fhActiveLock := wfs.fhLockTable.AcquireLock("GetAttr", fh.fh, util.SharedLock)
			wfs.setAttrByPbEntry(&out.Attr, inode, fh.entry.GetEntry(), true)
			wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)
			out.Nlink = 0

			return fuse.OK
		}
	}

	return status
}

func (wfs *WFS) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	// Check quota including uncommitted writes for real-time enforcement
	if wfs.IsOverQuotaWithUncommitted() {
		return fuse.Status(syscall.ENOSPC)
	}

	path, fh, entry, status := wfs.maybeReadEntry(input.NodeId)
	if status != fuse.OK || entry == nil {
		return status
	}
	if fh != nil {
		fh.entryLock.Lock()
		defer fh.entryLock.Unlock()
	}

	wormEnforced, wormEnabled := wfs.wormEnforcedForEntry(path, entry)
	if wormEnforced {
		return fuse.EPERM
	}

	if size, ok := input.GetSize(); ok {
		glog.V(4).Infof("%v setattr set size=%v chunks=%d", path, size, len(entry.GetChunks()))
		if size < filer.FileSize(entry) {
			// fmt.Printf("truncate %v \n", fullPath)
			var chunks []*filer_pb.FileChunk
			var truncatedChunks []*filer_pb.FileChunk
			for _, chunk := range entry.GetChunks() {
				int64Size := int64(chunk.GetSize())
				if chunk.GetOffset()+int64Size > int64(size) {
					// this chunk is truncated
					int64Size = int64(size) - chunk.GetOffset()
					if int64Size > 0 {
						chunks = append(chunks, chunk)
						glog.V(4).Infof("truncated chunk %+v from %d to %d\n", chunk.GetFileIdString(), chunk.GetSize(), int64Size)
						chunk.Size = uint64(int64Size)
					} else {
						glog.V(4).Infof("truncated whole chunk %+v\n", chunk.GetFileIdString())
						truncatedChunks = append(truncatedChunks, chunk)
					}
				} else {
					chunks = append(chunks, chunk)
				}
			}
			// set the new chunks and reset entry cache
			entry.Chunks = chunks
			if fh != nil {
				fh.entryChunkGroup.SetChunks(chunks)
			}
		}
		entry.Attributes.Mtime = time.Now().Unix()
		entry.Attributes.FileSize = size
	}

	if mode, ok := input.GetMode(); ok {
		// commit the file to worm when it is set to readonly at the first time
		if entry.GetWormEnforcedAtTsNs() == 0 && wormEnabled && !hasWritePermission(mode) {
			entry.WormEnforcedAtTsNs = time.Now().UnixNano()
		}

		// glog.V(4).Infof("setAttr mode %o", mode)
		entry.Attributes.FileMode = chmod(entry.GetAttributes().GetFileMode(), mode)
		if input.NodeId == 1 {
			wfs.option.MountMode = os.FileMode(chmod(uint32(wfs.option.MountMode), mode))
		}
	}

	if uid, ok := input.GetUID(); ok {
		entry.Attributes.Uid = uid
		if input.NodeId == 1 {
			wfs.option.MountUid = uid
		}
	}

	if gid, ok := input.GetGID(); ok {
		entry.Attributes.Gid = gid
		if input.NodeId == 1 {
			wfs.option.MountGid = gid
		}
	}

	if atime, ok := input.GetATime(); ok {
		entry.Attributes.Mtime = atime.Unix()
	}

	if mtime, ok := input.GetMTime(); ok {
		entry.Attributes.Mtime = mtime.Unix()
	}

	out.AttrValid = 1
	size, includeSize := input.GetSize()
	if includeSize {
		out.Size = size
	}
	wfs.setAttrByPbEntry(&out.Attr, input.NodeId, entry, !includeSize)

	if fh != nil {
		fh.dirtyMetadata = true

		return fuse.OK
	}

	return wfs.saveEntry(path, entry)
}

func (wfs *WFS) setRootAttr(out *fuse.AttrOut) {
	now := uint64(time.Now().Unix())
	out.AttrValid = 119
	out.Ino = 1
	setBlksize(&out.Attr, blockSize)
	out.Uid = wfs.option.MountUid
	out.Gid = wfs.option.MountGid
	out.Mtime = now
	out.Ctime = now
	out.Atime = now
	out.Mode = toSyscallType(os.ModeDir) | uint32(wfs.option.MountMode)
	out.Nlink = 1
}

func (wfs *WFS) setAttrByPbEntry(out *fuse.Attr, inode uint64, entry *filer_pb.Entry, calculateSize bool) {
	out.Ino = inode
	setBlksize(out, blockSize)
	if entry == nil {
		return
	}
	if entry.GetAttributes() != nil && entry.GetAttributes().GetInode() != 0 {
		out.Ino = entry.GetAttributes().GetInode()
	}
	if calculateSize {
		out.Size = filer.FileSize(entry)
	}
	if entry.FileMode()&os.ModeSymlink != 0 {
		out.Size = uint64(len(entry.GetAttributes().GetSymlinkTarget()))
	}
	out.Blocks = (out.Size + blockSize - 1) / blockSize
	out.Mtime = uint64(entry.GetAttributes().GetMtime())
	out.Ctime = uint64(entry.GetAttributes().GetMtime())
	out.Atime = uint64(entry.GetAttributes().GetMtime())
	out.Mode = toSyscallMode(os.FileMode(entry.GetAttributes().GetFileMode()))
	if entry.GetHardLinkCounter() > 0 {
		out.Nlink = uint32(entry.GetHardLinkCounter())
	} else {
		out.Nlink = 1
	}
	out.Uid = entry.GetAttributes().GetUid()
	out.Gid = entry.GetAttributes().GetGid()
	out.Rdev = entry.GetAttributes().GetRdev()
}

func (wfs *WFS) setAttrByFilerEntry(out *fuse.Attr, inode uint64, entry *filer.Entry) {
	out.Ino = inode
	out.Size = entry.FileSize
	if entry.Mode&os.ModeSymlink != 0 {
		out.Size = uint64(len(entry.SymlinkTarget))
	}
	out.Blocks = (out.Size + blockSize - 1) / blockSize
	setBlksize(out, blockSize)
	out.Atime = uint64(entry.Mtime.Unix())
	out.Mtime = uint64(entry.Mtime.Unix())
	out.Ctime = uint64(entry.Mtime.Unix())
	out.Mode = toSyscallMode(entry.Mode)
	if entry.HardLinkCounter > 0 {
		out.Nlink = uint32(entry.HardLinkCounter)
	} else {
		out.Nlink = 1
	}
	out.Uid = entry.Uid
	out.Gid = entry.Gid
	out.Rdev = entry.Rdev
}

func (wfs *WFS) outputPbEntry(out *fuse.EntryOut, inode uint64, entry *filer_pb.Entry) {
	out.NodeId = inode
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	wfs.setAttrByPbEntry(&out.Attr, inode, entry, true)
}

func (wfs *WFS) outputFilerEntry(out *fuse.EntryOut, inode uint64, entry *filer.Entry) {
	out.NodeId = inode
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	wfs.setAttrByFilerEntry(&out.Attr, inode, entry)
}

func chmod(existing uint32, mode uint32) uint32 {
	return existing&^07777 | mode&07777
}

const ownerWrite = 0o200
const groupWrite = 0o020
const otherWrite = 0o002

func hasWritePermission(mode uint32) bool {
	return (mode&ownerWrite != 0) || (mode&groupWrite != 0) || (mode&otherWrite != 0)
}

func toSyscallMode(mode os.FileMode) uint32 {
	return toSyscallType(mode) | uint32(mode)
}

func toSyscallType(mode os.FileMode) uint32 {
	switch mode & os.ModeType {
	case os.ModeDir:
		return syscall.S_IFDIR
	case os.ModeSymlink:
		return syscall.S_IFLNK
	case os.ModeNamedPipe:
		return syscall.S_IFIFO
	case os.ModeSocket:
		return syscall.S_IFSOCK
	case os.ModeDevice:
		return syscall.S_IFBLK
	case os.ModeCharDevice:
		return syscall.S_IFCHR
	default:
		return syscall.S_IFREG
	}
}

func toOsFileType(mode uint32) os.FileMode {
	switch mode & (syscall.S_IFMT & 0xffff) {
	case syscall.S_IFDIR:
		return os.ModeDir
	case syscall.S_IFLNK:
		return os.ModeSymlink
	case syscall.S_IFIFO:
		return os.ModeNamedPipe
	case syscall.S_IFSOCK:
		return os.ModeSocket
	case syscall.S_IFBLK:
		return os.ModeDevice
	case syscall.S_IFCHR:
		return os.ModeCharDevice
	default:
		return 0
	}
}

func toOsFileMode(mode uint32) os.FileMode {
	return toOsFileType(mode) | os.FileMode(mode&07777)
}
