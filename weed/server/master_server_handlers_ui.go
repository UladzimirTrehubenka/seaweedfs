package weed_server

import (
	"net/http"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util/version"

	hashicorpRaft "github.com/hashicorp/raft"
	"github.com/seaweedfs/raft"

	ui "github.com/seaweedfs/seaweedfs/weed/server/master_ui"
	"github.com/seaweedfs/seaweedfs/weed/stats"
)

func (ms *MasterServer) uiStatusHandler(w http.ResponseWriter, r *http.Request) {
	infos := make(map[string]any)
	infos["Up Time"] = time.Since(startTime).Truncate(time.Second).String()
	infos["Max Volume Id"] = ms.Topo.GetMaxVolumeId()

	ms.Topo.RaftServerAccessLock.RLock()
	defer ms.Topo.RaftServerAccessLock.RUnlock()

	if ms.Topo.RaftServer != nil {
		args := struct {
			Version           string
			Topology          any
			RaftServer        raft.Server
			Stats             map[string]any
			Counters          *stats.ServerStats
			VolumeSizeLimitMB uint32
		}{
			version.Version(),
			ms.Topo.ToInfo(),
			ms.Topo.RaftServer,
			infos,
			serverStats,
			ms.option.VolumeSizeLimitMB,
		}
		ui.StatusTpl.Execute(w, args)
	} else if ms.Topo.HashicorpRaft != nil {
		args := struct {
			Version           string
			Topology          any
			RaftServer        *hashicorpRaft.Raft
			Stats             map[string]any
			Counters          *stats.ServerStats
			VolumeSizeLimitMB uint32
		}{
			version.Version(),
			ms.Topo.ToInfo(),
			ms.Topo.HashicorpRaft,
			infos,
			serverStats,
			ms.option.VolumeSizeLimitMB,
		}
		ui.StatusNewRaftTpl.Execute(w, args)
	}
}
