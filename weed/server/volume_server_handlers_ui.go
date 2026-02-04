package weed_server

import (
	"net/http"
	"path/filepath"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/util/version"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	ui "github.com/seaweedfs/seaweedfs/weed/server/volume_server_ui"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
)

func (vs *VolumeServer) uiStatusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "SeaweedFS Volume "+version.VERSION)
	infos := make(map[string]any)
	infos["Up Time"] = time.Since(startTime).Truncate(time.Second).String()
	var ds []*volume_server_pb.DiskStatus
	for _, loc := range vs.store.Locations {
		if dir, e := filepath.Abs(loc.Directory); e == nil {
			newDiskStatus := stats.NewDiskStatus(dir)
			newDiskStatus.DiskType = loc.DiskType.String()
			ds = append(ds, newDiskStatus)
		}
	}
	volumeInfos := vs.store.VolumeInfos()
	var normalVolumeInfos, remoteVolumeInfos []*storage.VolumeInfo
	for _, vinfo := range volumeInfos {
		if vinfo.IsRemote() {
			remoteVolumeInfos = append(remoteVolumeInfos, vinfo)
		} else {
			normalVolumeInfos = append(normalVolumeInfos, vinfo)
		}
	}
	args := struct {
		Version       string
		Masters       []pb.ServerAddress
		Volumes       any
		EcVolumes     any
		RemoteVolumes any
		DiskStatuses  any
		Stats         any
		Counters      *stats.ServerStats
	}{
		version.Version(),
		vs.SeedMasterNodes,
		normalVolumeInfos,
		vs.store.EcVolumes(),
		remoteVolumeInfos,
		ds,
		infos,
		serverStats,
	}
	if err := ui.StatusTpl.Execute(w, args); err != nil {
		glog.Errorf("template execution error: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}
