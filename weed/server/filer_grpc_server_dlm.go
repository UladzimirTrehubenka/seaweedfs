package weed_server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// DistributedLock is a grpc handler to handle FilerServer's LockRequest
func (fs *FilerServer) DistributedLock(ctx context.Context, req *filer_pb.LockRequest) (resp *filer_pb.LockResponse, err error) {
	glog.V(4).Infof("FILER LOCK: Received DistributedLock request - name=%s owner=%s renewToken=%s secondsToLock=%d isMoved=%v",
		req.GetName(), req.GetOwner(), req.GetRenewToken(), req.GetSecondsToLock(), req.GetIsMoved())

	resp = &filer_pb.LockResponse{}

	var movedTo pb.ServerAddress
	expiredAtNs := time.Now().Add(time.Duration(req.GetSecondsToLock()) * time.Second).UnixNano()
	resp.LockOwner, resp.RenewToken, movedTo, err = fs.filer.Dlm.LockWithTimeout(req.GetName(), expiredAtNs, req.GetRenewToken(), req.GetOwner())
	glog.V(4).Infof("FILER LOCK: LockWithTimeout result - name=%s lockOwner=%s renewToken=%s movedTo=%s err=%v",
		req.GetName(), resp.GetLockOwner(), resp.GetRenewToken(), movedTo, err)
	glog.V(4).Infof("lock %s %v %v %v, isMoved=%v %v", req.GetName(), req.GetSecondsToLock(), req.GetRenewToken(), req.GetOwner(), req.GetIsMoved(), movedTo)
	if movedTo != "" && movedTo != fs.option.Host && !req.GetIsMoved() {
		glog.V(0).Infof("FILER LOCK: Forwarding to correct filer - from=%s to=%s", fs.option.Host, movedTo)
		err = pb.WithFilerClient(false, 0, movedTo, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			secondResp, err := client.DistributedLock(ctx, &filer_pb.LockRequest{
				Name:          req.GetName(),
				SecondsToLock: req.GetSecondsToLock(),
				RenewToken:    req.GetRenewToken(),
				IsMoved:       true,
				Owner:         req.GetOwner(),
			})
			if err == nil {
				resp.RenewToken = secondResp.GetRenewToken()
				resp.LockOwner = secondResp.GetLockOwner()
				resp.Error = secondResp.GetError()
				glog.V(0).Infof("FILER LOCK: Forwarded lock acquired - name=%s renewToken=%s", req.GetName(), resp.GetRenewToken())
			} else {
				glog.V(0).Infof("FILER LOCK: Forward failed - name=%s err=%v", req.GetName(), err)
			}

			return err
		})
	}

	if err != nil {
		resp.Error = fmt.Sprintf("%v", err)
		if !strings.Contains(resp.GetError(), "lock already owned") {
			glog.V(0).Infof("FILER LOCK: Error - name=%s error=%s", req.GetName(), resp.GetError())
		}
	}
	if movedTo != "" {
		resp.LockHostMovedTo = string(movedTo)
	}

	glog.V(4).Infof("FILER LOCK: Returning response - name=%s renewToken=%s lockOwner=%s error=%s movedTo=%s",
		req.GetName(), resp.GetRenewToken(), resp.GetLockOwner(), resp.GetError(), resp.GetLockHostMovedTo())

	return resp, nil
}

// Unlock is a grpc handler to handle FilerServer's UnlockRequest
func (fs *FilerServer) DistributedUnlock(ctx context.Context, req *filer_pb.UnlockRequest) (resp *filer_pb.UnlockResponse, err error) {
	resp = &filer_pb.UnlockResponse{}

	var movedTo pb.ServerAddress
	movedTo, err = fs.filer.Dlm.Unlock(req.GetName(), req.GetRenewToken())

	if !req.GetIsMoved() && movedTo != "" {
		err = pb.WithFilerClient(false, 0, movedTo, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			secondResp, err := client.DistributedUnlock(ctx, &filer_pb.UnlockRequest{
				Name:       req.GetName(),
				RenewToken: req.GetRenewToken(),
				IsMoved:    true,
			})
			resp.Error = secondResp.GetError()

			return err
		})
	}

	if err != nil {
		resp.Error = fmt.Sprintf("%v", err)
	}
	if movedTo != "" {
		resp.MovedTo = string(movedTo)
	}

	return resp, nil
}

func (fs *FilerServer) FindLockOwner(ctx context.Context, req *filer_pb.FindLockOwnerRequest) (*filer_pb.FindLockOwnerResponse, error) {
	owner, movedTo, err := fs.filer.Dlm.FindLockOwner(req.GetName())
	if !req.GetIsMoved() && movedTo != "" || errors.Is(err, lock_manager.LockNotFound) {
		err = pb.WithFilerClient(false, 0, movedTo, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			secondResp, err := client.FindLockOwner(ctx, &filer_pb.FindLockOwnerRequest{
				Name:    req.GetName(),
				IsMoved: true,
			})
			if err != nil {
				return err
			}
			owner = secondResp.GetOwner()

			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	if owner == "" {
		glog.V(0).Infof("find lock %s moved to %v: %v", req.GetName(), movedTo, err)

		return nil, status.Error(codes.NotFound, fmt.Sprintf("lock %s not found", req.GetName()))
	}
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &filer_pb.FindLockOwnerResponse{
		Owner: owner,
	}, nil
}

// TransferLocks is a grpc handler to handle FilerServer's TransferLocksRequest
func (fs *FilerServer) TransferLocks(ctx context.Context, req *filer_pb.TransferLocksRequest) (*filer_pb.TransferLocksResponse, error) {
	for _, lock := range req.GetLocks() {
		fs.filer.Dlm.InsertLock(lock.GetName(), lock.GetExpiredAtNs(), lock.GetRenewToken(), lock.GetOwner())
	}

	return &filer_pb.TransferLocksResponse{}, nil
}

func (fs *FilerServer) OnDlmChangeSnapshot(snapshot []pb.ServerAddress) {
	locks := fs.filer.Dlm.SelectNotOwnedLocks(snapshot)
	if len(locks) == 0 {
		return
	}

	for _, lock := range locks {
		server := fs.filer.Dlm.CalculateTargetServer(lock.Key, snapshot)
		// Use a context with timeout for lock transfer to avoid hanging indefinitely
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := pb.WithFilerClient(false, 0, server, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			_, err := client.TransferLocks(ctx, &filer_pb.TransferLocksRequest{
				Locks: []*filer_pb.Lock{
					{
						Name:        lock.Key,
						RenewToken:  lock.Token,
						ExpiredAtNs: lock.ExpiredAtNs,
						Owner:       lock.Owner,
					},
				},
			})

			return err
		})
		cancel()
		if err != nil {
			// it may not be worth retrying, since the lock may have expired
			glog.Errorf("transfer lock %v to %v: %v", lock.Key, server, err)
		}
	}
}
