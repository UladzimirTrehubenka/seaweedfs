package filer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"

	"github.com/viant/ptrie"
	jsonpb "google.golang.org/protobuf/encoding/protojson"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	DirectoryEtcRoot      = "/etc/"
	DirectoryEtcSeaweedFS = "/etc/seaweedfs"
	DirectoryEtcRemote    = "/etc/remote"
	FilerConfName         = "filer.conf"
	IamConfigDirectory    = "/etc/iam"
	IamIdentityFile       = "identity.json"
	IamPoliciesFile       = "policies.json"
)

type FilerConf struct {
	rules ptrie.Trie[*filer_pb.FilerConf_PathConf]
}

func ReadFilerConf(filerGrpcAddress pb.ServerAddress, grpcDialOption grpc.DialOption, masterClient *wdclient.MasterClient) (*FilerConf, error) {
	return ReadFilerConfFromFilers([]pb.ServerAddress{filerGrpcAddress}, grpcDialOption, masterClient)
}

// ReadFilerConfFromFilers reads filer configuration with multi-filer failover support
func ReadFilerConfFromFilers(filerGrpcAddresses []pb.ServerAddress, grpcDialOption grpc.DialOption, masterClient *wdclient.MasterClient) (*FilerConf, error) {
	var data []byte
	if err := pb.WithOneOfGrpcFilerClients(false, filerGrpcAddresses, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		if masterClient != nil {
			var buf bytes.Buffer
			if err := ReadEntry(masterClient, client, DirectoryEtcSeaweedFS, FilerConfName, &buf); err != nil {
				return err
			}
			data = buf.Bytes()

			return nil
		}
		content, err := ReadInsideFiler(client, DirectoryEtcSeaweedFS, FilerConfName)
		if err != nil {
			return err
		}
		data = content

		return nil
	}); err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		return nil, fmt.Errorf("read %s/%s: %w", DirectoryEtcSeaweedFS, FilerConfName, err)
	}

	fc := NewFilerConf()
	if len(data) > 0 {
		if err := fc.LoadFromBytes(data); err != nil {
			return nil, fmt.Errorf("parse %s/%s: %w", DirectoryEtcSeaweedFS, FilerConfName, err)
		}
	}

	return fc, nil
}

func NewFilerConf() (fc *FilerConf) {
	fc = &FilerConf{
		rules: ptrie.New[*filer_pb.FilerConf_PathConf](),
	}

	return fc
}

func (fc *FilerConf) loadFromFiler(filer *Filer) (err error) {
	filerConfPath := util.NewFullPath(DirectoryEtcSeaweedFS, FilerConfName)
	entry, err := filer.FindEntry(context.Background(), filerConfPath)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return nil
		}
		glog.Errorf("read filer conf entry %s: %v", filerConfPath, err)

		return
	}

	if len(entry.Content) > 0 {
		return fc.LoadFromBytes(entry.Content)
	}

	return fc.loadFromChunks(filer, entry.Content, entry.GetChunks(), entry.Size())
}

func (fc *FilerConf) loadFromChunks(filer *Filer, content []byte, chunks []*filer_pb.FileChunk, size uint64) (err error) {
	if len(content) == 0 {
		content, err = filer.readEntry(chunks, size)
		if err != nil {
			glog.Errorf("read filer conf content: %v", err)

			return
		}
	}

	return fc.LoadFromBytes(content)
}

func (fc *FilerConf) LoadFromBytes(data []byte) (err error) {
	conf := &filer_pb.FilerConf{}

	if err := jsonpb.Unmarshal(data, conf); err != nil {
		return err
	}

	return fc.doLoadConf(conf)
}

func (fc *FilerConf) doLoadConf(conf *filer_pb.FilerConf) (err error) {
	for _, location := range conf.GetLocations() {
		err = fc.SetLocationConf(location)
		if err != nil {
			// this is not recoverable
			return nil
		}
	}

	return nil
}

func (fc *FilerConf) GetLocationConf(locationPrefix string) (locConf *filer_pb.FilerConf_PathConf, found bool) {
	return fc.rules.Get([]byte(locationPrefix))
}

func (fc *FilerConf) SetLocationConf(locConf *filer_pb.FilerConf_PathConf) (err error) {
	err = fc.rules.Put([]byte(locConf.GetLocationPrefix()), locConf)
	if err != nil {
		glog.Errorf("put location prefix: %v", err)
	}

	return
}

func (fc *FilerConf) AddLocationConf(locConf *filer_pb.FilerConf_PathConf) (err error) {
	existingConf, found := fc.rules.Get([]byte(locConf.GetLocationPrefix()))
	if found {
		mergePathConf(existingConf, locConf)
		locConf = existingConf
	}
	err = fc.rules.Put([]byte(locConf.GetLocationPrefix()), locConf)
	if err != nil {
		glog.Errorf("put location prefix: %v", err)
	}

	return
}

func (fc *FilerConf) DeleteLocationConf(locationPrefix string) {
	rules := ptrie.New[*filer_pb.FilerConf_PathConf]()
	fc.rules.Walk(func(key []byte, value *filer_pb.FilerConf_PathConf) bool {
		if string(key) == locationPrefix {
			return true
		}
		key = bytes.Clone(key)
		_ = rules.Put(key, value)

		return true
	})
	fc.rules = rules
}

// emptyPathConf is a singleton for paths with no matching rules
// Callers must NOT mutate the returned value
var emptyPathConf = &filer_pb.FilerConf_PathConf{}

func (fc *FilerConf) MatchStorageRule(path string) (pathConf *filer_pb.FilerConf_PathConf) {
	// Convert once to avoid allocation in multi-match case
	pathBytes := []byte(path)

	// Fast path: check if any rules match before allocating
	// This avoids allocation for paths with no configured rules (common case)
	var firstMatch *filer_pb.FilerConf_PathConf
	matchCount := 0

	fc.rules.MatchPrefix(pathBytes, func(key []byte, value *filer_pb.FilerConf_PathConf) bool {
		matchCount++
		if matchCount == 1 {
			firstMatch = value

			return true // continue to check for more matches
		}
		// Stop after 2 matches - we only need to know if there are multiple
		return false
	})

	// No rules match - return singleton (callers must NOT mutate)
	if matchCount == 0 {
		return emptyPathConf
	}

	// Single rule matches - return directly (callers must NOT mutate)
	if matchCount == 1 {
		return firstMatch
	}

	// Multiple rules match - need to merge (allocate new)
	pathConf = &filer_pb.FilerConf_PathConf{}
	fc.rules.MatchPrefix(pathBytes, func(key []byte, value *filer_pb.FilerConf_PathConf) bool {
		mergePathConf(pathConf, value)

		return true
	})

	return pathConf
}

// ClonePathConf creates a mutable copy of an existing PathConf.
// Use this when you need to modify a config (e.g., before calling SetLocationConf).
//
// IMPORTANT: Keep in sync with filer_pb.FilerConf_PathConf fields.
// When adding new fields to the protobuf, update this function accordingly.
func ClonePathConf(src *filer_pb.FilerConf_PathConf) *filer_pb.FilerConf_PathConf {
	if src == nil {
		return &filer_pb.FilerConf_PathConf{}
	}

	return &filer_pb.FilerConf_PathConf{
		LocationPrefix:           src.GetLocationPrefix(),
		Collection:               src.GetCollection(),
		Replication:              src.GetReplication(),
		Ttl:                      src.GetTtl(),
		DiskType:                 src.GetDiskType(),
		Fsync:                    src.GetFsync(),
		VolumeGrowthCount:        src.GetVolumeGrowthCount(),
		ReadOnly:                 src.GetReadOnly(),
		MaxFileNameLength:        src.GetMaxFileNameLength(),
		DataCenter:               src.GetDataCenter(),
		Rack:                     src.GetRack(),
		DataNode:                 src.GetDataNode(),
		DisableChunkDeletion:     src.GetDisableChunkDeletion(),
		Worm:                     src.GetWorm(),
		WormGracePeriodSeconds:   src.GetWormGracePeriodSeconds(),
		WormRetentionTimeSeconds: src.GetWormRetentionTimeSeconds(),
	}
}

func (fc *FilerConf) GetCollectionTtls(collection string) (ttls map[string]string) {
	ttls = make(map[string]string)
	fc.rules.Walk(func(key []byte, value *filer_pb.FilerConf_PathConf) bool {
		if value.GetCollection() == collection {
			ttls[value.GetLocationPrefix()] = value.GetTtl()
		}

		return true
	})

	return ttls
}

// merge if values in b is not empty, merge them into a
func mergePathConf(a, b *filer_pb.FilerConf_PathConf) {
	a.Collection = util.Nvl(b.GetCollection(), a.GetCollection())
	a.Replication = util.Nvl(b.GetReplication(), a.GetReplication())
	a.Ttl = util.Nvl(b.GetTtl(), a.GetTtl())
	a.DiskType = util.Nvl(b.GetDiskType(), a.GetDiskType())
	a.Fsync = b.GetFsync() || a.GetFsync()
	if b.GetVolumeGrowthCount() > 0 {
		a.VolumeGrowthCount = b.GetVolumeGrowthCount()
	}
	a.ReadOnly = b.GetReadOnly() || a.GetReadOnly()
	if b.GetMaxFileNameLength() > 0 {
		a.MaxFileNameLength = b.GetMaxFileNameLength()
	}
	a.DataCenter = util.Nvl(b.GetDataCenter(), a.GetDataCenter())
	a.Rack = util.Nvl(b.GetRack(), a.GetRack())
	a.DataNode = util.Nvl(b.GetDataNode(), a.GetDataNode())
	a.DisableChunkDeletion = b.GetDisableChunkDeletion() || a.GetDisableChunkDeletion()
	a.Worm = b.GetWorm() || a.GetWorm()
	if b.GetWormRetentionTimeSeconds() > 0 {
		a.WormRetentionTimeSeconds = b.GetWormRetentionTimeSeconds()
	}
	if b.GetWormGracePeriodSeconds() > 0 {
		a.WormGracePeriodSeconds = b.GetWormGracePeriodSeconds()
	}
}

func (fc *FilerConf) ToProto() *filer_pb.FilerConf {
	m := &filer_pb.FilerConf{}
	fc.rules.Walk(func(key []byte, value *filer_pb.FilerConf_PathConf) bool {
		m.Locations = append(m.Locations, value)

		return true
	})

	return m
}

func (fc *FilerConf) ToText(writer io.Writer) error {
	return ProtoToText(writer, fc.ToProto())
}
