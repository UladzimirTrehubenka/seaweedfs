package filer_pb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/viant/ptrie"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

const cutoffTimeNewEmptyDir = 3

func (entry *Entry) IsInRemoteOnly() bool {
	return len(entry.GetChunks()) == 0 && entry.GetRemoteEntry() != nil && entry.GetRemoteEntry().GetRemoteSize() > 0
}

func (entry *Entry) IsDirectoryKeyObject() bool {
	return entry.GetIsDirectory() && entry.GetAttributes() != nil && entry.GetAttributes().GetMime() != ""
}

func (entry *Entry) GetExpiryTime() (expiryTime int64) {
	// For S3 objects with lifecycle expiration, use Mtime (modification time)
	// For regular TTL entries, use Crtime (creation time) for backward compatibility
	if entry.Extended != nil {
		if _, hasS3Expiry := entry.GetExtended()[s3_constants.SeaweedFSExpiresS3]; hasS3Expiry {
			// S3 lifecycle expiration: base TTL on modification time
			expiryTime = entry.GetAttributes().GetMtime()
			if expiryTime == 0 {
				expiryTime = entry.GetAttributes().GetCrtime()
			}
			expiryTime += int64(entry.GetAttributes().GetTtlSec())

			return expiryTime
		}
	}

	// Regular TTL expiration: base on creation time only
	expiryTime = entry.GetAttributes().GetCrtime() + int64(entry.GetAttributes().GetTtlSec())

	return expiryTime
}

func (entry *Entry) IsExpired() bool {
	return entry != nil && entry.GetAttributes() != nil && entry.GetAttributes().GetTtlSec() > 0 &&
		time.Now().Unix() >= entry.GetExpiryTime()
}

func (entry *Entry) FileMode() (fileMode os.FileMode) {
	if entry != nil && entry.GetAttributes() != nil {
		fileMode = os.FileMode(entry.GetAttributes().GetFileMode())
	}

	return
}

func (entry *Entry) IsOlderDir() bool {
	return entry.GetIsDirectory() && entry.GetAttributes() != nil && entry.GetAttributes().GetMime() == "" && entry.GetAttributes().GetCrtime() <= time.Now().Unix()-cutoffTimeNewEmptyDir
}

func ToFileIdObject(fileIdStr string) (*FileId, error) {
	t, err := needle.ParseFileIdFromString(fileIdStr)
	if err != nil {
		return nil, err
	}

	return &FileId{
		VolumeId: uint32(t.VolumeId),
		Cookie:   uint32(t.Cookie),
		FileKey:  uint64(t.Key),
	}, nil
}

func (fid *FileId) toFileIdString() string {
	return needle.NewFileId(needle.VolumeId(fid.GetVolumeId()), fid.GetFileKey(), fid.GetCookie()).String()
}

func (c *FileChunk) GetFileIdString() string {
	if c.GetFileId() != "" {
		return c.GetFileId()
	}
	if c.GetFid() != nil {
		c.FileId = c.GetFid().toFileIdString()

		return c.GetFileId()
	}

	return ""
}

func BeforeEntrySerialization(chunks []*FileChunk) {
	for _, chunk := range chunks {
		if chunk.GetFileId() != "" {
			if fid, err := ToFileIdObject(chunk.GetFileId()); err == nil {
				chunk.Fid = fid
				chunk.FileId = ""
			}
		}

		if chunk.GetSourceFileId() != "" {
			if fid, err := ToFileIdObject(chunk.GetSourceFileId()); err == nil {
				chunk.SourceFid = fid
				chunk.SourceFileId = ""
			}
		}
	}
}

func EnsureFid(chunk *FileChunk) {
	if chunk.GetFid() != nil {
		return
	}
	if fid, err := ToFileIdObject(chunk.GetFileId()); err == nil {
		chunk.Fid = fid
	}
}

func AfterEntryDeserialization(chunks []*FileChunk) {
	for _, chunk := range chunks {
		if chunk.GetFid() != nil && chunk.GetFileId() == "" {
			chunk.FileId = chunk.GetFid().toFileIdString()
		}

		if chunk.GetSourceFid() != nil && chunk.GetSourceFileId() == "" {
			chunk.SourceFileId = chunk.GetSourceFid().toFileIdString()
		}
	}
}

func CreateEntry(ctx context.Context, client SeaweedFilerClient, request *CreateEntryRequest) error {
	resp, err := client.CreateEntry(ctx, request)
	if err != nil {
		glog.V(1).InfofCtx(ctx, "create entry %s/%s %v: %v", request.GetDirectory(), request.GetEntry().GetName(), request.GetOExcl(), err)

		return fmt.Errorf("CreateEntry: %w", err)
	}
	if resp.GetError() != "" {
		glog.V(1).InfofCtx(ctx, "create entry %s/%s %v: %v", request.GetDirectory(), request.GetEntry().GetName(), request.GetOExcl(), resp.GetError())

		return fmt.Errorf("CreateEntry : %v", resp.GetError())
	}

	return nil
}

func UpdateEntry(ctx context.Context, client SeaweedFilerClient, request *UpdateEntryRequest) error {
	_, err := client.UpdateEntry(ctx, request)
	if err != nil {
		glog.V(1).InfofCtx(ctx, "update entry %s/%s :%v", request.GetDirectory(), request.GetEntry().GetName(), err)

		return fmt.Errorf("UpdateEntry: %w", err)
	}

	return nil
}

func LookupEntry(ctx context.Context, client SeaweedFilerClient, request *LookupDirectoryEntryRequest) (*LookupDirectoryEntryResponse, error) {
	resp, err := client.LookupDirectoryEntry(ctx, request)
	if err != nil {
		if errors.Is(err, ErrNotFound) || strings.Contains(err.Error(), ErrNotFound.Error()) {
			return nil, ErrNotFound
		}
		glog.V(3).InfofCtx(ctx, "read %s/%v: %v", request.GetDirectory(), request.GetName(), err)

		return nil, fmt.Errorf("LookupEntry1: %w", err)
	}
	if resp.GetEntry() == nil {
		return nil, ErrNotFound
	}

	return resp, nil
}

var ErrNotFound = errors.New("filer: no entry is found in filer store")

func IsEmpty(event *SubscribeMetadataResponse) bool {
	return event.GetEventNotification().GetNewEntry() == nil && event.GetEventNotification().GetOldEntry() == nil
}

func IsCreate(event *SubscribeMetadataResponse) bool {
	return event.GetEventNotification().GetNewEntry() != nil && event.GetEventNotification().GetOldEntry() == nil
}

func IsUpdate(event *SubscribeMetadataResponse) bool {
	return event.GetEventNotification().GetNewEntry() != nil &&
		event.GetEventNotification().GetOldEntry() != nil &&
		event.GetDirectory() == event.GetEventNotification().GetNewParentPath() &&
		event.GetEventNotification().GetNewEntry().GetName() == event.GetEventNotification().GetOldEntry().GetName()
}

func IsDelete(event *SubscribeMetadataResponse) bool {
	return event.GetEventNotification().GetNewEntry() == nil && event.GetEventNotification().GetOldEntry() != nil
}

func IsRename(event *SubscribeMetadataResponse) bool {
	return event.GetEventNotification().GetNewEntry() != nil &&
		event.GetEventNotification().GetOldEntry() != nil &&
		(event.GetDirectory() != event.GetEventNotification().GetNewParentPath() ||
			event.GetEventNotification().GetNewEntry().GetName() != event.GetEventNotification().GetOldEntry().GetName())
}

var _ = ptrie.KeyProvider(&FilerConf_PathConf{})

func (fp *FilerConf_PathConf) Key() any {
	key, _ := proto.Marshal(fp)

	return string(key)
}
