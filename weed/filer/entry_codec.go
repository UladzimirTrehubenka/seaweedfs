package filer

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// pbEntryPool reduces allocations in EncodeAttributesAndChunks and DecodeAttributesAndChunks
// which are called on every filer store operation
var pbEntryPool = sync.Pool{
	New: func() any {
		return &filer_pb.Entry{
			Attributes: &filer_pb.FuseAttributes{}, // Pre-allocate attributes
		}
	},
}

// resetPbEntry clears a protobuf Entry for reuse
func resetPbEntry(e *filer_pb.Entry) {
	// Use struct assignment to clear all fields including protobuf internal fields
	// (unknownFields, sizeCache) that field-by-field reset would miss
	attrs := e.GetAttributes()
	*e = filer_pb.Entry{}
	if attrs == nil {
		attrs = &filer_pb.FuseAttributes{}
	} else {
		resetFuseAttributes(attrs)
	}
	e.Attributes = attrs
}

// resetFuseAttributes clears FuseAttributes for reuse
func resetFuseAttributes(a *filer_pb.FuseAttributes) {
	// Use struct assignment to clear all fields including protobuf internal fields
	*a = filer_pb.FuseAttributes{}
}

func (entry *Entry) EncodeAttributesAndChunks() ([]byte, error) {
	message := pbEntryPool.Get().(*filer_pb.Entry)
	defer func() {
		resetPbEntry(message)
		pbEntryPool.Put(message)
	}()

	entry.ToExistingProtoEntry(message)

	data, err := proto.Marshal(message)
	if err != nil {
		return nil, err
	}

	// Copy the data to a new slice since proto.Marshal may return a slice
	// that shares memory with the message (not guaranteed to be a copy)
	return append([]byte(nil), data...), nil
}

func (entry *Entry) DecodeAttributesAndChunks(blob []byte) error {
	message := pbEntryPool.Get().(*filer_pb.Entry)
	defer func() {
		resetPbEntry(message)
		pbEntryPool.Put(message)
	}()

	if err := proto.Unmarshal(blob, message); err != nil {
		return fmt.Errorf("decoding value blob for %s: %w", entry.FullPath, err)
	}

	FromPbEntryToExistingEntry(message, entry)

	return nil
}

func EntryAttributeToPb(entry *Entry) *filer_pb.FuseAttributes {
	return &filer_pb.FuseAttributes{
		Crtime:        entry.Crtime.Unix(),
		Mtime:         entry.Mtime.Unix(),
		FileMode:      uint32(entry.Mode),
		Uid:           entry.Uid,
		Gid:           entry.Gid,
		Mime:          entry.Mime,
		TtlSec:        entry.TtlSec,
		UserName:      entry.UserName,
		GroupName:     entry.GroupNames,
		SymlinkTarget: entry.SymlinkTarget,
		Md5:           entry.Md5,
		FileSize:      entry.FileSize,
		Rdev:          entry.Rdev,
		Inode:         entry.Inode,
	}
}

// EntryAttributeToExistingPb fills an existing FuseAttributes to avoid allocation.
// Safe to call with nil attr (will return early without populating).
func EntryAttributeToExistingPb(entry *Entry, attr *filer_pb.FuseAttributes) {
	if attr == nil {
		return
	}
	attr.Crtime = entry.Crtime.Unix()
	attr.Mtime = entry.Mtime.Unix()
	attr.FileMode = uint32(entry.Mode)
	attr.Uid = entry.Uid
	attr.Gid = entry.Gid
	attr.Mime = entry.Mime
	attr.TtlSec = entry.TtlSec
	attr.UserName = entry.UserName
	attr.GroupName = entry.GroupNames
	attr.SymlinkTarget = entry.SymlinkTarget
	attr.Md5 = entry.Md5
	attr.FileSize = entry.FileSize
	attr.Rdev = entry.Rdev
	attr.Inode = entry.Inode
}

func PbToEntryAttribute(attr *filer_pb.FuseAttributes) Attr {
	t := Attr{}

	if attr == nil {
		return t
	}

	t.Crtime = time.Unix(attr.GetCrtime(), 0)
	t.Mtime = time.Unix(attr.GetMtime(), 0)
	t.Mode = os.FileMode(attr.GetFileMode())
	t.Uid = attr.GetUid()
	t.Gid = attr.GetGid()
	t.Mime = attr.GetMime()
	t.TtlSec = attr.GetTtlSec()
	t.UserName = attr.GetUserName()
	t.GroupNames = attr.GetGroupName()
	t.SymlinkTarget = attr.GetSymlinkTarget()
	t.Md5 = attr.GetMd5()
	t.FileSize = attr.GetFileSize()
	t.Rdev = attr.GetRdev()
	t.Inode = attr.GetInode()

	return t
}

func EqualEntry(a, b *Entry) bool {
	if a == b {
		return true
	}
	if a == nil && b != nil || a != nil && b == nil {
		return false
	}
	if !proto.Equal(EntryAttributeToPb(a), EntryAttributeToPb(b)) {
		return false
	}
	if len(a.Chunks) != len(b.Chunks) {
		return false
	}

	if !eq(a.Extended, b.Extended) {
		return false
	}

	if !bytes.Equal(a.Md5, b.Md5) {
		return false
	}

	for i := range len(a.Chunks) {
		if !proto.Equal(a.Chunks[i], b.Chunks[i]) {
			return false
		}
	}

	if !bytes.Equal(a.HardLinkId, b.HardLinkId) {
		return false
	}
	if a.HardLinkCounter != b.HardLinkCounter {
		return false
	}
	if !bytes.Equal(a.Content, b.Content) {
		return false
	}
	if !proto.Equal(a.Remote, b.Remote) {
		return false
	}
	if a.Quota != b.Quota {
		return false
	}

	return true
}

func eq(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}

	for k, v := range a {
		if w, ok := b[k]; !ok || !bytes.Equal(v, w) {
			return false
		}
	}

	return true
}
