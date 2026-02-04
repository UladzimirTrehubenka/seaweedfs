package s3api

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

// SeaweedS3IamCacheServer Implementation
// This interface is dedicated to UNIDIRECTIONAL updates from Filer to S3 Server.
// S3 Server acts purely as a cache.

func (s3a *S3ApiServer) PutIdentity(ctx context.Context, req *iam_pb.PutIdentityRequest) (*iam_pb.PutIdentityResponse, error) {
	if req.GetIdentity() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "identity is required")
	}
	// Direct in-memory cache update
	glog.V(1).Infof("IAM: received identity update for %s", req.GetIdentity().GetName())
	if err := s3a.iam.UpsertIdentity(req.GetIdentity()); err != nil {
		glog.Errorf("failed to update identity cache for %s: %v", req.GetIdentity().GetName(), err)

		return nil, status.Errorf(codes.Internal, "failed to update identity cache: %v", err)
	}

	return &iam_pb.PutIdentityResponse{}, nil
}

func (s3a *S3ApiServer) RemoveIdentity(ctx context.Context, req *iam_pb.RemoveIdentityRequest) (*iam_pb.RemoveIdentityResponse, error) {
	if req.GetUsername() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "username is required")
	}
	// Direct in-memory cache update
	glog.V(1).Infof("IAM: received identity removal for %s", req.GetUsername())
	s3a.iam.RemoveIdentity(req.GetUsername())

	return &iam_pb.RemoveIdentityResponse{}, nil
}

func (s3a *S3ApiServer) PutPolicy(ctx context.Context, req *iam_pb.PutPolicyRequest) (*iam_pb.PutPolicyResponse, error) {
	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "policy name is required")
	}
	if req.GetContent() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "policy content is required")
	}

	// Update IAM policy cache
	glog.V(1).Infof("IAM: received policy update for %s", req.GetName())
	if s3a.iam == nil {
		return nil, status.Errorf(codes.Internal, "IAM not initialized")
	}

	if err := s3a.iam.PutPolicy(req.GetName(), req.GetContent()); err != nil {
		glog.Errorf("failed to update policy cache for %s: %v", req.GetName(), err)

		return nil, status.Errorf(codes.Internal, "failed to update policy cache: %v", err)
	}

	return &iam_pb.PutPolicyResponse{}, nil
}

func (s3a *S3ApiServer) DeletePolicy(ctx context.Context, req *iam_pb.DeletePolicyRequest) (*iam_pb.DeletePolicyResponse, error) {
	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "policy name is required")
	}

	// Delete from IAM policy cache
	glog.V(1).Infof("IAM: received policy removal for %s", req.GetName())
	if s3a.iam == nil {
		return nil, status.Errorf(codes.Internal, "IAM not initialized")
	}

	if err := s3a.iam.DeletePolicy(req.GetName()); err != nil {
		glog.Errorf("failed to delete policy cache for %s: %v", req.GetName(), err)

		return nil, status.Errorf(codes.Internal, "failed to delete policy cache: %v", err)
	}

	return &iam_pb.DeletePolicyResponse{}, nil
}

func (s3a *S3ApiServer) GetPolicy(ctx context.Context, req *iam_pb.GetPolicyRequest) (*iam_pb.GetPolicyResponse, error) {
	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "policy name is required")
	}
	if s3a.iam == nil {
		return nil, status.Errorf(codes.Internal, "IAM not initialized")
	}
	policy, err := s3a.iam.GetPolicy(req.GetName())
	if err != nil {
		return &iam_pb.GetPolicyResponse{}, nil // Not found is fine for cache
	}

	return &iam_pb.GetPolicyResponse{
		Name:    policy.GetName(),
		Content: policy.GetContent(),
	}, nil
}

func (s3a *S3ApiServer) ListPolicies(ctx context.Context, req *iam_pb.ListPoliciesRequest) (*iam_pb.ListPoliciesResponse, error) {
	resp := &iam_pb.ListPoliciesResponse{}
	if s3a.iam == nil {
		return nil, status.Errorf(codes.Internal, "IAM not initialized")
	}
	policies := s3a.iam.ListPolicies()
	resp.Policies = append(resp.Policies, policies...)

	return resp, nil
}
