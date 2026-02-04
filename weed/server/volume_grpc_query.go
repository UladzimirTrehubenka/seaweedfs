package weed_server

import (
	"github.com/tidwall/gjson"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/query/json"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func (vs *VolumeServer) Query(req *volume_server_pb.QueryRequest, stream volume_server_pb.VolumeServer_QueryServer) error {
	for _, fid := range req.GetFromFileIds() {
		vid, id_cookie, err := operation.ParseFileId(fid)
		if err != nil {
			glog.V(0).Infof("volume query failed to parse fid %s: %v", fid, err)

			return err
		}

		n := new(needle.Needle)
		volumeId, _ := needle.NewVolumeId(vid)
		n.ParsePath(id_cookie)

		cookie := n.Cookie
		if _, err := vs.store.ReadVolumeNeedle(volumeId, n, nil, nil); err != nil {
			glog.V(0).Infof("volume query failed to read fid %s: %v", fid, err)

			return err
		}

		if n.Cookie != cookie {
			glog.V(0).Infof("volume query failed to read fid cookie %s: %v", fid, err)

			return err
		}

		if req.GetInputSerialization().GetCsvInput() != nil {

		}

		if req.GetInputSerialization().GetJsonInput() != nil {
			stripe := &volume_server_pb.QueriedStripe{
				Records: nil,
			}

			filter := json.Query{
				Field: req.GetFilter().GetField(),
				Op:    req.GetFilter().GetOperand(),
				Value: req.GetFilter().GetValue(),
			}
			gjson.ForEachLine(string(n.Data), func(line gjson.Result) bool {
				passedFilter, values := json.QueryJson(line.Raw, req.GetSelections(), filter)
				if !passedFilter {
					return true
				}
				stripe.Records = json.ToJson(stripe.GetRecords(), req.GetSelections(), values)

				return true
			})
			err = stream.Send(stripe)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
