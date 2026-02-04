package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func init() {
	Commands = append(Commands, &commandClusterPs{})
}

type commandClusterPs struct {
}

func (c *commandClusterPs) Name() string {
	return "cluster.ps"
}

func (c *commandClusterPs) Help() string {
	return `check current cluster process status

	cluster.ps

`
}

func (c *commandClusterPs) HasTag(CommandTag) bool {
	return false
}

func (c *commandClusterPs) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	clusterPsCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	if err = clusterPsCommand.Parse(args); err != nil {
		return nil
	}

	var filerNodes []*master_pb.ListClusterNodesResponse_ClusterNode
	var mqBrokerNodes []*master_pb.ListClusterNodesResponse_ClusterNode

	// get the list of filers
	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: cluster.FilerType,
			FilerGroup: *commandEnv.option.FilerGroup,
		})
		if err != nil {
			return err
		}

		filerNodes = resp.GetClusterNodes()

		return err
	})
	if err != nil {
		return err
	}

	// get the list of message queue brokers
	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: cluster.BrokerType,
			FilerGroup: *commandEnv.option.FilerGroup,
		})
		if err != nil {
			return err
		}

		mqBrokerNodes = resp.GetClusterNodes()

		return err
	})
	if err != nil {
		return err
	}

	if len(mqBrokerNodes) > 0 {
		fmt.Fprintf(writer, "* message queue brokers %d\n", len(mqBrokerNodes))
		for _, node := range mqBrokerNodes {
			fmt.Fprintf(writer, "  * %s (%v)\n", node.GetAddress(), node.GetVersion())
			if node.GetDataCenter() != "" {
				fmt.Fprintf(writer, "    DataCenter: %v\n", node.GetDataCenter())
			}
			if node.GetRack() != "" {
				fmt.Fprintf(writer, "    Rack: %v\n", node.GetRack())
			}
		}
	}

	filerSignatures := make(map[*master_pb.ListClusterNodesResponse_ClusterNode]int32)
	fmt.Fprintf(writer, "* filers %d\n", len(filerNodes))
	for _, node := range filerNodes {
		fmt.Fprintf(writer, "  * %s (%v) %v\n", node.GetAddress(), node.GetVersion(), time.Unix(0, node.GetCreatedAtNs()).UTC())
		if node.GetDataCenter() != "" {
			fmt.Fprintf(writer, "    DataCenter: %v\n", node.GetDataCenter())
		}
		if node.GetRack() != "" {
			fmt.Fprintf(writer, "    Rack: %v\n", node.GetRack())
		}
		pb.WithFilerClient(false, 0, pb.ServerAddress(node.GetAddress()), commandEnv.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
			if err == nil {
				if resp.GetFilerGroup() != "" {
					fmt.Fprintf(writer, "    filer group: %s\n", resp.GetFilerGroup())
				}
				fmt.Fprintf(writer, "    signature: %d\n", resp.GetSignature())
				filerSignatures[node] = resp.GetSignature()
			} else {
				fmt.Fprintf(writer, "    failed to connect: %v\n", err)
			}

			return err
		})
	}
	for _, node := range filerNodes {
		pb.WithFilerClient(false, 0, pb.ServerAddress(node.GetAddress()), commandEnv.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			fmt.Fprintf(writer, "* filer %s metadata sync time\n", node.GetAddress())
			selfSignature := filerSignatures[node]
			for peer, peerSignature := range filerSignatures {
				if selfSignature == peerSignature {
					continue
				}
				if resp, err := client.KvGet(context.Background(), &filer_pb.KvGetRequest{Key: filer.GetPeerMetaOffsetKey(peerSignature)}); err == nil && len(resp.GetValue()) == 8 {
					lastTsNs := int64(util.BytesToUint64(resp.GetValue()))
					fmt.Fprintf(writer, "    %s: %v\n", peer.GetAddress(), time.Unix(0, lastTsNs).UTC())
				}
			}

			return nil
		})
	}

	// collect volume servers
	var volumeServers []pb.ServerAddress
	t, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}
	for _, dc := range t.GetDataCenterInfos() {
		for _, r := range dc.GetRackInfos() {
			for _, dn := range r.GetDataNodeInfos() {
				volumeServers = append(volumeServers, pb.NewServerAddressFromDataNode(dn))
			}
		}
	}

	fmt.Fprintf(writer, "* volume servers %d\n", len(volumeServers))
	for _, dc := range t.GetDataCenterInfos() {
		fmt.Fprintf(writer, "  * data center: %s\n", dc.GetId())
		for _, r := range dc.GetRackInfos() {
			fmt.Fprintf(writer, "    * rack: %s\n", r.GetId())
			for _, dn := range r.GetDataNodeInfos() {
				pb.WithVolumeServerClient(false, pb.NewServerAddressFromDataNode(dn), commandEnv.option.GrpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
					resp, err := client.VolumeServerStatus(context.Background(), &volume_server_pb.VolumeServerStatusRequest{})
					if err == nil {
						fmt.Fprintf(writer, "      * %s (%v)\n", dn.GetId(), resp.GetVersion())
					}

					return err
				})
			}
		}
	}

	return nil
}
