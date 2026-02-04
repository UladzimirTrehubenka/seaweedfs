package weed_server

import (
	"context"
	"encoding/json"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
)

// IamGrpcServer implements the IAM gRPC service on the filer
type IamGrpcServer struct {
	iam_pb.UnimplementedSeaweedIdentityAccessManagementServer

	credentialManager *credential.CredentialManager
}

// NewIamGrpcServer creates a new IAM gRPC server
func NewIamGrpcServer(credentialManager *credential.CredentialManager) *IamGrpcServer {
	return &IamGrpcServer{
		credentialManager: credentialManager,
	}
}

//////////////////////////////////////////////////
// Configuration Management

func (s *IamGrpcServer) GetConfiguration(ctx context.Context, req *iam_pb.GetConfigurationRequest) (*iam_pb.GetConfigurationResponse, error) {
	glog.V(4).Infof("GetConfiguration")

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	config, err := s.credentialManager.LoadConfiguration(ctx)
	if err != nil {
		glog.Errorf("Failed to load configuration: %v", err)

		return nil, err
	}

	return &iam_pb.GetConfigurationResponse{
		Configuration: config,
	}, nil
}

func (s *IamGrpcServer) PutConfiguration(ctx context.Context, req *iam_pb.PutConfigurationRequest) (*iam_pb.PutConfigurationResponse, error) {
	glog.V(4).Infof("PutConfiguration")

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	if req.GetConfiguration() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "configuration is nil")
	}

	err := s.credentialManager.SaveConfiguration(ctx, req.GetConfiguration())
	if err != nil {
		glog.Errorf("Failed to save configuration: %v", err)

		return nil, err
	}

	return &iam_pb.PutConfigurationResponse{}, nil
}

//////////////////////////////////////////////////
// User Management

func (s *IamGrpcServer) CreateUser(ctx context.Context, req *iam_pb.CreateUserRequest) (*iam_pb.CreateUserResponse, error) {
	if req == nil || req.GetIdentity() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "identity is required")
	}
	glog.V(4).Infof("IAM: Filer.CreateUser %s", req.GetIdentity().GetName())

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	err := s.credentialManager.CreateUser(ctx, req.GetIdentity())
	if err != nil {
		if errors.Is(err, credential.ErrUserAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "user %s already exists", req.GetIdentity().GetName())
		}
		glog.Errorf("Failed to create user %s: %v", req.GetIdentity().GetName(), err)

		return nil, status.Errorf(codes.Internal, "failed to create user: %v", err)
	}

	return &iam_pb.CreateUserResponse{}, nil
}

func (s *IamGrpcServer) GetUser(ctx context.Context, req *iam_pb.GetUserRequest) (*iam_pb.GetUserResponse, error) {
	glog.V(4).Infof("GetUser: %s", req.GetUsername())

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	identity, err := s.credentialManager.GetUser(ctx, req.GetUsername())
	if err != nil {
		if errors.Is(err, credential.ErrUserNotFound) {
			return nil, status.Errorf(codes.NotFound, "user %s not found", req.GetUsername())
		}
		glog.Errorf("Failed to get user %s: %v", req.GetUsername(), err)

		return nil, status.Errorf(codes.Internal, "failed to get user: %v", err)
	}

	return &iam_pb.GetUserResponse{
		Identity: identity,
	}, nil
}

func (s *IamGrpcServer) UpdateUser(ctx context.Context, req *iam_pb.UpdateUserRequest) (*iam_pb.UpdateUserResponse, error) {
	if req == nil || req.GetIdentity() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "identity is required")
	}
	glog.V(4).Infof("IAM: Filer.UpdateUser %s", req.GetUsername())

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	err := s.credentialManager.UpdateUser(ctx, req.GetUsername(), req.GetIdentity())
	if err != nil {
		if errors.Is(err, credential.ErrUserNotFound) {
			return nil, status.Errorf(codes.NotFound, "user %s not found", req.GetUsername())
		}
		glog.Errorf("Failed to update user %s: %v", req.GetUsername(), err)

		return nil, status.Errorf(codes.Internal, "failed to update user: %v", err)
	}

	return &iam_pb.UpdateUserResponse{}, nil
}

func (s *IamGrpcServer) DeleteUser(ctx context.Context, req *iam_pb.DeleteUserRequest) (*iam_pb.DeleteUserResponse, error) {
	glog.V(4).Infof("IAM: Filer.DeleteUser %s", req.GetUsername())

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	err := s.credentialManager.DeleteUser(ctx, req.GetUsername())
	if err != nil {
		if errors.Is(err, credential.ErrUserNotFound) {
			return nil, status.Errorf(codes.NotFound, "user %s not found", req.GetUsername())
		}
		glog.Errorf("Failed to delete user %s: %v", req.GetUsername(), err)

		return nil, status.Errorf(codes.Internal, "failed to delete user: %v", err)
	}

	return &iam_pb.DeleteUserResponse{}, nil
}

func (s *IamGrpcServer) ListUsers(ctx context.Context, req *iam_pb.ListUsersRequest) (*iam_pb.ListUsersResponse, error) {
	glog.V(4).Infof("ListUsers")

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	usernames, err := s.credentialManager.ListUsers(ctx)
	if err != nil {
		glog.Errorf("Failed to list users: %v", err)

		return nil, err
	}

	return &iam_pb.ListUsersResponse{
		Usernames: usernames,
	}, nil
}

//////////////////////////////////////////////////
// Access Key Management

func (s *IamGrpcServer) CreateAccessKey(ctx context.Context, req *iam_pb.CreateAccessKeyRequest) (*iam_pb.CreateAccessKeyResponse, error) {
	if req == nil || req.GetCredential() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "credential is required")
	}
	glog.V(4).Infof("CreateAccessKey for user: %s", req.GetUsername())

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	err := s.credentialManager.CreateAccessKey(ctx, req.GetUsername(), req.GetCredential())
	if err != nil {
		if errors.Is(err, credential.ErrUserNotFound) {
			return nil, status.Errorf(codes.NotFound, "user %s not found", req.GetUsername())
		}
		glog.Errorf("Failed to create access key for user %s: %v", req.GetUsername(), err)

		return nil, status.Errorf(codes.Internal, "failed to create access key: %v", err)
	}

	return &iam_pb.CreateAccessKeyResponse{}, nil
}

func (s *IamGrpcServer) DeleteAccessKey(ctx context.Context, req *iam_pb.DeleteAccessKeyRequest) (*iam_pb.DeleteAccessKeyResponse, error) {
	glog.V(4).Infof("DeleteAccessKey: %s for user: %s", req.GetAccessKey(), req.GetUsername())

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	err := s.credentialManager.DeleteAccessKey(ctx, req.GetUsername(), req.GetAccessKey())
	if err != nil {
		if errors.Is(err, credential.ErrUserNotFound) {
			return nil, status.Errorf(codes.NotFound, "user %s not found", req.GetUsername())
		}
		if errors.Is(err, credential.ErrAccessKeyNotFound) {
			return nil, status.Errorf(codes.NotFound, "access key %s not found", req.GetAccessKey())
		}
		glog.Errorf("Failed to delete access key %s for user %s: %v", req.GetAccessKey(), req.GetUsername(), err)

		return nil, status.Errorf(codes.Internal, "failed to delete access key: %v", err)
	}

	return &iam_pb.DeleteAccessKeyResponse{}, nil
}

func (s *IamGrpcServer) GetUserByAccessKey(ctx context.Context, req *iam_pb.GetUserByAccessKeyRequest) (*iam_pb.GetUserByAccessKeyResponse, error) {
	glog.V(4).Infof("GetUserByAccessKey: %s", req.GetAccessKey())

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	identity, err := s.credentialManager.GetUserByAccessKey(ctx, req.GetAccessKey())
	if err != nil {
		if errors.Is(err, credential.ErrAccessKeyNotFound) {
			return nil, status.Errorf(codes.NotFound, "access key %s not found", req.GetAccessKey())
		}
		glog.Errorf("Failed to get user by access key %s: %v", req.GetAccessKey(), err)

		return nil, status.Errorf(codes.Internal, "failed to get user: %v", err)
	}

	return &iam_pb.GetUserByAccessKeyResponse{
		Identity: identity,
	}, nil
}

//////////////////////////////////////////////////
// Policy Management

func (s *IamGrpcServer) PutPolicy(ctx context.Context, req *iam_pb.PutPolicyRequest) (*iam_pb.PutPolicyResponse, error) {
	glog.V(4).Infof("IAM: Filer.PutPolicy %s", req.GetName())

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "policy name is required")
	}
	if err := credential.ValidatePolicyName(req.GetName()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if req.GetContent() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "policy content is required")
	}

	var policy policy_engine.PolicyDocument
	if err := json.Unmarshal([]byte(req.GetContent()), &policy); err != nil {
		glog.Errorf("Failed to unmarshal policy %s: %v", req.GetName(), err)

		return nil, err
	}

	err := s.credentialManager.PutPolicy(ctx, req.GetName(), policy)
	if err != nil {
		glog.Errorf("Failed to put policy %s: %v", req.GetName(), err)

		return nil, err
	}

	return &iam_pb.PutPolicyResponse{}, nil
}

func (s *IamGrpcServer) GetPolicy(ctx context.Context, req *iam_pb.GetPolicyRequest) (*iam_pb.GetPolicyResponse, error) {
	glog.V(4).Infof("GetPolicy: %s", req.GetName())

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	policy, err := s.credentialManager.GetPolicy(ctx, req.GetName())
	if err != nil {
		glog.Errorf("Failed to get policy %s: %v", req.GetName(), err)

		return nil, err
	}

	if policy == nil {
		return nil, status.Errorf(codes.NotFound, "policy %s not found", req.GetName())
	}

	jsonBytes, err := json.Marshal(policy)
	if err != nil {
		glog.Errorf("Failed to marshal policy %s: %v", req.GetName(), err)

		return nil, err
	}

	return &iam_pb.GetPolicyResponse{
		Name:    req.GetName(),
		Content: string(jsonBytes),
	}, nil
}

func (s *IamGrpcServer) ListPolicies(ctx context.Context, req *iam_pb.ListPoliciesRequest) (*iam_pb.ListPoliciesResponse, error) {
	glog.V(4).Infof("ListPolicies")

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	policiesData, err := s.credentialManager.GetPolicies(ctx)
	if err != nil {
		glog.Errorf("Failed to list policies: %v", err)

		return nil, err
	}

	var policies []*iam_pb.Policy
	for name, policy := range policiesData {
		jsonBytes, err := json.Marshal(policy)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to marshal policy %s: %v", name, err)
		}
		policies = append(policies, &iam_pb.Policy{
			Name:    name,
			Content: string(jsonBytes),
		})
	}

	return &iam_pb.ListPoliciesResponse{
		Policies: policies,
	}, nil
}

func (s *IamGrpcServer) DeletePolicy(ctx context.Context, req *iam_pb.DeletePolicyRequest) (*iam_pb.DeletePolicyResponse, error) {
	glog.V(4).Infof("DeletePolicy: %s", req.GetName())

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	err := s.credentialManager.DeletePolicy(ctx, req.GetName())
	if err != nil {
		glog.Errorf("Failed to delete policy %s: %v", req.GetName(), err)

		return nil, err
	}

	return &iam_pb.DeletePolicyResponse{}, nil
}

//////////////////////////////////////////////////
// Service Account Management

func (s *IamGrpcServer) CreateServiceAccount(ctx context.Context, req *iam_pb.CreateServiceAccountRequest) (*iam_pb.CreateServiceAccountResponse, error) {
	if req == nil || req.GetServiceAccount() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "service account is required")
	}
	if err := credential.ValidateServiceAccountId(req.GetServiceAccount().GetId()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	glog.V(4).Infof("CreateServiceAccount: %s", req.GetServiceAccount().GetId())

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	err := s.credentialManager.CreateServiceAccount(ctx, req.GetServiceAccount())
	if err != nil {
		glog.Errorf("Failed to create service account %s: %v", req.GetServiceAccount().GetId(), err)

		return nil, status.Errorf(codes.Internal, "failed to create service account: %v", err)
	}

	return &iam_pb.CreateServiceAccountResponse{}, nil
}

func (s *IamGrpcServer) UpdateServiceAccount(ctx context.Context, req *iam_pb.UpdateServiceAccountRequest) (*iam_pb.UpdateServiceAccountResponse, error) {
	if req == nil || req.GetServiceAccount() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "service account is required")
	}
	glog.V(4).Infof("UpdateServiceAccount: %s", req.GetId())

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	err := s.credentialManager.UpdateServiceAccount(ctx, req.GetId(), req.GetServiceAccount())
	if err != nil {
		glog.Errorf("Failed to update service account %s: %v", req.GetId(), err)

		return nil, status.Errorf(codes.Internal, "failed to update service account: %v", err)
	}

	return &iam_pb.UpdateServiceAccountResponse{}, nil
}

func (s *IamGrpcServer) DeleteServiceAccount(ctx context.Context, req *iam_pb.DeleteServiceAccountRequest) (*iam_pb.DeleteServiceAccountResponse, error) {
	glog.V(4).Infof("DeleteServiceAccount: %s", req.GetId())

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	err := s.credentialManager.DeleteServiceAccount(ctx, req.GetId())
	if err != nil {
		if errors.Is(err, credential.ErrServiceAccountNotFound) {
			return nil, status.Errorf(codes.NotFound, "service account %s not found", req.GetId())
		}
		glog.Errorf("Failed to delete service account %s: %v", req.GetId(), err)

		return nil, status.Errorf(codes.Internal, "failed to delete service account: %v", err)
	}

	return &iam_pb.DeleteServiceAccountResponse{}, nil
}

func (s *IamGrpcServer) GetServiceAccount(ctx context.Context, req *iam_pb.GetServiceAccountRequest) (*iam_pb.GetServiceAccountResponse, error) {
	glog.V(4).Infof("GetServiceAccount: %s", req.GetId())

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	sa, err := s.credentialManager.GetServiceAccount(ctx, req.GetId())
	if err != nil {
		glog.Errorf("Failed to get service account %s: %v", req.GetId(), err)

		return nil, status.Errorf(codes.Internal, "failed to get service account: %v", err)
	}

	if sa == nil {
		return nil, status.Errorf(codes.NotFound, "service account %s not found", req.GetId())
	}

	return &iam_pb.GetServiceAccountResponse{
		ServiceAccount: sa,
	}, nil
}

func (s *IamGrpcServer) ListServiceAccounts(ctx context.Context, req *iam_pb.ListServiceAccountsRequest) (*iam_pb.ListServiceAccountsResponse, error) {
	glog.V(4).Infof("ListServiceAccounts")

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	accounts, err := s.credentialManager.ListServiceAccounts(ctx)
	if err != nil {
		glog.Errorf("Failed to list service accounts: %v", err)

		return nil, status.Errorf(codes.Internal, "failed to list service accounts: %v", err)
	}

	return &iam_pb.ListServiceAccountsResponse{
		ServiceAccounts: accounts,
	}, nil
}

func (s *IamGrpcServer) GetServiceAccountByAccessKey(ctx context.Context, req *iam_pb.GetServiceAccountByAccessKeyRequest) (*iam_pb.GetServiceAccountByAccessKeyResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request is required")
	}
	glog.V(4).Infof("GetServiceAccountByAccessKey: %s", req.GetAccessKey())
	if req.GetAccessKey() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "access key is required")
	}

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	sa, err := s.credentialManager.GetStore().GetServiceAccountByAccessKey(ctx, req.GetAccessKey())
	if err != nil {
		if errors.Is(err, credential.ErrAccessKeyNotFound) {
			return nil, status.Errorf(codes.NotFound, "access key %s not found", req.GetAccessKey())
		}
		glog.Errorf("Failed to get service account by access key %s: %v", req.GetAccessKey(), err)

		return nil, status.Errorf(codes.Internal, "failed to get service account: %v", err)
	}

	return &iam_pb.GetServiceAccountByAccessKeyResponse{
		ServiceAccount: sa,
	}, nil
}
