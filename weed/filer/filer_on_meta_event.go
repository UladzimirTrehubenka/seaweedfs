package filer

import (
	"bytes"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// onMetadataChangeEvent is triggered after filer processed change events from local or remote filers
func (f *Filer) onMetadataChangeEvent(event *filer_pb.SubscribeMetadataResponse) {
	f.maybeReloadFilerConfiguration(event)
	f.maybeReloadRemoteStorageConfigurationAndMapping(event)
	f.onBucketEvents(event)
	f.onEmptyFolderCleanupEvents(event)
}

func (f *Filer) onBucketEvents(event *filer_pb.SubscribeMetadataResponse) {
	message := event.GetEventNotification()

	if f.DirBucketsPath == event.GetDirectory() {
		if filer_pb.IsCreate(event) {
			if message.GetNewEntry().GetIsDirectory() {
				f.Store.OnBucketCreation(message.GetNewEntry().GetName())
			}
		}
		if filer_pb.IsDelete(event) {
			if message.GetOldEntry().GetIsDirectory() {
				f.Store.OnBucketDeletion(message.GetOldEntry().GetName())
			}
		}
	}
}

// onEmptyFolderCleanupEvents handles create/delete events for empty folder cleanup
func (f *Filer) onEmptyFolderCleanupEvents(event *filer_pb.SubscribeMetadataResponse) {
	if f.EmptyFolderCleaner == nil || !f.EmptyFolderCleaner.IsEnabled() {
		return
	}

	message := event.GetEventNotification()
	directory := event.GetDirectory()
	eventTime := time.Unix(0, event.GetTsNs())

	// Handle delete events - trigger folder cleanup check
	if filer_pb.IsDelete(event) && message.GetOldEntry() != nil {
		f.EmptyFolderCleaner.OnDeleteEvent(directory, message.GetOldEntry().GetName(), message.GetOldEntry().GetIsDirectory(), eventTime)
	}

	// Handle create events - cancel pending cleanup for the folder
	if filer_pb.IsCreate(event) && message.GetNewEntry() != nil {
		f.EmptyFolderCleaner.OnCreateEvent(directory, message.GetNewEntry().GetName(), message.GetNewEntry().GetIsDirectory())
	}

	// Handle rename/move events
	if filer_pb.IsRename(event) {
		// Treat the old location as a delete
		if message.GetOldEntry() != nil {
			f.EmptyFolderCleaner.OnDeleteEvent(directory, message.GetOldEntry().GetName(), message.GetOldEntry().GetIsDirectory(), eventTime)
		}
		// Treat the new location as a create
		if message.GetNewEntry() != nil {
			newDir := message.GetNewParentPath()
			if newDir == "" {
				newDir = directory
			}
			f.EmptyFolderCleaner.OnCreateEvent(newDir, message.GetNewEntry().GetName(), message.GetNewEntry().GetIsDirectory())
		}
	}
}

func (f *Filer) maybeReloadFilerConfiguration(event *filer_pb.SubscribeMetadataResponse) {
	if DirectoryEtcSeaweedFS != event.GetDirectory() {
		if DirectoryEtcSeaweedFS != event.GetEventNotification().GetNewParentPath() {
			return
		}
	}

	entry := event.GetEventNotification().GetNewEntry()
	if entry == nil {
		return
	}

	glog.V(0).Infof("procesing %v", event)
	if entry.GetName() == FilerConfName {
		f.reloadFilerConfiguration(entry)
	}
}

func (f *Filer) readEntry(chunks []*filer_pb.FileChunk, size uint64) ([]byte, error) {
	var buf bytes.Buffer
	err := StreamContent(f.MasterClient, &buf, chunks, 0, int64(size))
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (f *Filer) reloadFilerConfiguration(entry *filer_pb.Entry) {
	fc := NewFilerConf()
	err := fc.loadFromChunks(f, entry.GetContent(), entry.GetChunks(), FileSize(entry))
	if err != nil {
		glog.Errorf("read filer conf chunks: %v", err)

		return
	}
	f.FilerConf = fc
}

func (f *Filer) LoadFilerConf() {
	fc := NewFilerConf()
	err := util.Retry("loadFilerConf", func() error {
		return fc.loadFromFiler(f)
	})
	if err != nil {
		glog.Errorf("read filer conf: %v", err)

		return
	}
	f.FilerConf = fc
}

// //////////////////////////////////
// load and maintain remote storages
// //////////////////////////////////
func (f *Filer) LoadRemoteStorageConfAndMapping() {
	if err := f.RemoteStorage.LoadRemoteStorageConfigurationsAndMapping(f); err != nil {
		glog.Errorf("read remote conf and mapping: %v", err)

		return
	}
}
func (f *Filer) maybeReloadRemoteStorageConfigurationAndMapping(event *filer_pb.SubscribeMetadataResponse) {
	// FIXME add reloading
}
