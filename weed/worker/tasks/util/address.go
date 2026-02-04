package util

import (
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
)

// ResolveServerAddress resolves a server ID to its network address using the active topology
func ResolveServerAddress(serverID string, activeTopology *topology.ActiveTopology) (string, error) {
	if activeTopology == nil {
		return "", errors.New("topology not available")
	}
	allNodes := activeTopology.GetAllNodes()
	nodeInfo, exists := allNodes[serverID]
	if !exists {
		return "", fmt.Errorf("server %s not found in topology", serverID)
	}
	if nodeInfo.GetAddress() == "" {
		return "", fmt.Errorf("server %s has no address in topology", serverID)
	}

	return nodeInfo.GetAddress(), nil
}
