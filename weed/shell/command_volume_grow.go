package shell

import (
	"context"
	"errors"
	"flag"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func init() {
	Commands = append(Commands, &commandGrow{})
}

type commandGrow struct {
}

func (c *commandGrow) Name() string {
	return "volume.grow"
}

func (c *commandGrow) Help() string {
	return `grow volumes

	volume.grow [-collection=<collection name>] [-dataCenter=<data center name>]

`
}

func (c *commandGrow) HasTag(CommandTag) bool {
	return false
}

func (c *commandGrow) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	volumeVacuumCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	growCount := volumeVacuumCommand.Uint("count", 2, "")
	collection := volumeVacuumCommand.String("collection", "", "grow this collection")
	dataCenter := volumeVacuumCommand.String("dataCenter", "", "grow volumes only from the specified data center")
	rack := volumeVacuumCommand.String("rack", "", "grow volumes only from the specified rack")
	dataNode := volumeVacuumCommand.String("dataNode", "", "grow volumes only from the specified data node")
	diskType := volumeVacuumCommand.String("diskType", "", "grow volumes only from the specified disk type")

	if err = volumeVacuumCommand.Parse(args); err != nil {
		return nil
	}
	if *collection == "" {
		return errors.New("collection option is required")
	}
	t, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}
	volumeGrowRequest := &master_pb.VolumeGrowRequest{
		Collection:          *collection,
		DataCenter:          *dataCenter,
		Rack:                *rack,
		DataNode:            *dataNode,
		WritableVolumeCount: uint32(*growCount),
	}

	collectionFound := false
	dataCenterFound := *dataCenter == ""
	rackFound := *rack == ""
	dataNodeFound := *dataNode == ""
	diskTypeFound := *diskType == ""
	for _, dc := range t.GetDataCenterInfos() {
		if dc.GetId() == *dataCenter {
			dataCenterFound = true
		}
		for _, r := range dc.GetRackInfos() {
			if r.GetId() == *rack {
				rackFound = true
			}
			for _, dn := range r.GetDataNodeInfos() {
				if dn.GetId() == *dataNode {
					dataNodeFound = true
				}
				for _, di := range dn.GetDiskInfos() {
					if !diskTypeFound && di.GetType() == types.ToDiskType(*diskType).String() {
						diskTypeFound = true
					}
					for _, vi := range di.GetVolumeInfos() {
						if !collectionFound && vi.GetCollection() == *collection {
							replicaPlacement, _ := super_block.NewReplicaPlacementFromByte(byte(vi.GetReplicaPlacement()))
							volumeGrowRequest.Ttl = needle.LoadTTLFromUint32(vi.GetTtl()).String()
							volumeGrowRequest.DiskType = vi.GetDiskType()
							volumeGrowRequest.Replication = replicaPlacement.String()
							collectionFound = true
						}
						if collectionFound && dataCenterFound && rackFound && dataNodeFound && diskTypeFound {
							break
						}
					}
				}
			}
		}
	}
	if !dataCenterFound {
		return errors.New("data center not found")
	}
	if !rackFound {
		return errors.New("rack not found")
	}
	if !dataNodeFound {
		return errors.New("data node not found")
	}
	if !diskTypeFound {
		return errors.New("disk type not found")
	}
	if !collectionFound {
		return errors.New("collection not found")
	}
	if err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		if _, err := client.VolumeGrow(context.Background(), volumeGrowRequest); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}
