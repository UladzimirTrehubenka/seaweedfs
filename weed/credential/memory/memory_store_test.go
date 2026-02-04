package memory

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestMemoryStore(t *testing.T) {
	store := &MemoryStore{}

	// Test initialization
	config := util.GetViper()
	if err := store.Initialize(config, "credential."); err != nil {
		t.Fatalf("Failed to initialize store: %v", err)
	}

	ctx := context.Background()

	// Test creating a user
	identity := &iam_pb.Identity{
		Name: "testuser",
		Credentials: []*iam_pb.Credential{
			{
				AccessKey: "access123",
				SecretKey: "secret123",
			},
		},
	}

	if err := store.CreateUser(ctx, identity); err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// Test getting user
	retrievedUser, err := store.GetUser(ctx, "testuser")
	if err != nil {
		t.Fatalf("Failed to get user: %v", err)
	}

	if retrievedUser.GetName() != "testuser" {
		t.Errorf("Expected username 'testuser', got '%s'", retrievedUser.GetName())
	}

	if len(retrievedUser.GetCredentials()) != 1 {
		t.Errorf("Expected 1 credential, got %d", len(retrievedUser.GetCredentials()))
	}

	// Test getting user by access key
	userByAccessKey, err := store.GetUserByAccessKey(ctx, "access123")
	if err != nil {
		t.Fatalf("Failed to get user by access key: %v", err)
	}

	if userByAccessKey.GetName() != "testuser" {
		t.Errorf("Expected username 'testuser', got '%s'", userByAccessKey.GetName())
	}

	// Test listing users
	users, err := store.ListUsers(ctx)
	if err != nil {
		t.Fatalf("Failed to list users: %v", err)
	}

	if len(users) != 1 || users[0] != "testuser" {
		t.Errorf("Expected ['testuser'], got %v", users)
	}

	// Test creating access key
	newCred := &iam_pb.Credential{
		AccessKey: "access456",
		SecretKey: "secret456",
	}

	if err := store.CreateAccessKey(ctx, "testuser", newCred); err != nil {
		t.Fatalf("Failed to create access key: %v", err)
	}

	// Verify user now has 2 credentials
	updatedUser, err := store.GetUser(ctx, "testuser")
	if err != nil {
		t.Fatalf("Failed to get updated user: %v", err)
	}

	if len(updatedUser.GetCredentials()) != 2 {
		t.Errorf("Expected 2 credentials, got %d", len(updatedUser.GetCredentials()))
	}

	// Test deleting access key
	if err := store.DeleteAccessKey(ctx, "testuser", "access456"); err != nil {
		t.Fatalf("Failed to delete access key: %v", err)
	}

	// Verify user now has 1 credential again
	finalUser, err := store.GetUser(ctx, "testuser")
	if err != nil {
		t.Fatalf("Failed to get final user: %v", err)
	}

	if len(finalUser.GetCredentials()) != 1 {
		t.Errorf("Expected 1 credential, got %d", len(finalUser.GetCredentials()))
	}

	// Test deleting user
	if err := store.DeleteUser(ctx, "testuser"); err != nil {
		t.Fatalf("Failed to delete user: %v", err)
	}

	// Verify user is gone
	_, err = store.GetUser(ctx, "testuser")
	if !errors.Is(err, credential.ErrUserNotFound) {
		t.Errorf("Expected ErrUserNotFound, got %v", err)
	}

	// Test error cases
	if err := store.CreateUser(ctx, identity); err != nil {
		t.Fatalf("Failed to create user for error tests: %v", err)
	}

	// Try to create duplicate user
	if err := store.CreateUser(ctx, identity); !errors.Is(err, credential.ErrUserAlreadyExists) {
		t.Errorf("Expected ErrUserAlreadyExists, got %v", err)
	}

	// Try to get non-existent user
	_, err = store.GetUser(ctx, "nonexistent")
	if !errors.Is(err, credential.ErrUserNotFound) {
		t.Errorf("Expected ErrUserNotFound, got %v", err)
	}

	// Try to get user by non-existent access key
	_, err = store.GetUserByAccessKey(ctx, "nonexistent")
	if !errors.Is(err, credential.ErrAccessKeyNotFound) {
		t.Errorf("Expected ErrAccessKeyNotFound, got %v", err)
	}
}

func TestMemoryStoreConcurrency(t *testing.T) {
	store := &MemoryStore{}
	config := util.GetViper()
	if err := store.Initialize(config, "credential."); err != nil {
		t.Fatalf("Failed to initialize store: %v", err)
	}

	ctx := context.Background()

	// Test concurrent access
	done := make(chan bool, 10)
	for i := range 10 {
		go func(i int) {
			defer func() { done <- true }()

			username := fmt.Sprintf("user%d", i)
			identity := &iam_pb.Identity{
				Name: username,
				Credentials: []*iam_pb.Credential{
					{
						AccessKey: fmt.Sprintf("access%d", i),
						SecretKey: fmt.Sprintf("secret%d", i),
					},
				},
			}

			if err := store.CreateUser(ctx, identity); err != nil {
				t.Errorf("Failed to create user %s: %v", username, err)

				return
			}

			if _, err := store.GetUser(ctx, username); err != nil {
				t.Errorf("Failed to get user %s: %v", username, err)

				return
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for range 10 {
		<-done
	}

	// Verify all users were created
	users, err := store.ListUsers(ctx)
	if err != nil {
		t.Fatalf("Failed to list users: %v", err)
	}

	if len(users) != 10 {
		t.Errorf("Expected 10 users, got %d", len(users))
	}
}

func TestMemoryStoreReset(t *testing.T) {
	store := &MemoryStore{}
	config := util.GetViper()
	if err := store.Initialize(config, "credential."); err != nil {
		t.Fatalf("Failed to initialize store: %v", err)
	}

	ctx := context.Background()

	// Create a user
	identity := &iam_pb.Identity{
		Name: "testuser",
		Credentials: []*iam_pb.Credential{
			{
				AccessKey: "access123",
				SecretKey: "secret123",
			},
		},
	}

	if err := store.CreateUser(ctx, identity); err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// Verify user exists
	if store.GetUserCount() != 1 {
		t.Errorf("Expected 1 user, got %d", store.GetUserCount())
	}

	if store.GetAccessKeyCount() != 1 {
		t.Errorf("Expected 1 access key, got %d", store.GetAccessKeyCount())
	}

	// Reset the store
	store.Reset()

	// Verify store is empty
	if store.GetUserCount() != 0 {
		t.Errorf("Expected 0 users after reset, got %d", store.GetUserCount())
	}

	if store.GetAccessKeyCount() != 0 {
		t.Errorf("Expected 0 access keys after reset, got %d", store.GetAccessKeyCount())
	}

	// Verify user is gone
	_, err := store.GetUser(ctx, "testuser")
	if !errors.Is(err, credential.ErrUserNotFound) {
		t.Errorf("Expected ErrUserNotFound after reset, got %v", err)
	}
}

func TestMemoryStoreConfigurationSaveLoad(t *testing.T) {
	store := &MemoryStore{}
	config := util.GetViper()
	if err := store.Initialize(config, "credential."); err != nil {
		t.Fatalf("Failed to initialize store: %v", err)
	}

	ctx := context.Background()

	// Create initial configuration
	originalConfig := &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "user1",
				Credentials: []*iam_pb.Credential{
					{
						AccessKey: "access1",
						SecretKey: "secret1",
					},
				},
			},
			{
				Name: "user2",
				Credentials: []*iam_pb.Credential{
					{
						AccessKey: "access2",
						SecretKey: "secret2",
					},
				},
			},
		},
	}

	// Save configuration
	if err := store.SaveConfiguration(ctx, originalConfig); err != nil {
		t.Fatalf("Failed to save configuration: %v", err)
	}

	// Load configuration
	loadedConfig, err := store.LoadConfiguration(ctx)
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}

	// Verify configuration matches
	if len(loadedConfig.GetIdentities()) != 2 {
		t.Errorf("Expected 2 identities, got %d", len(loadedConfig.GetIdentities()))
	}

	// Check users exist
	user1, err := store.GetUser(ctx, "user1")
	if err != nil {
		t.Fatalf("Failed to get user1: %v", err)
	}

	if len(user1.GetCredentials()) != 1 || user1.GetCredentials()[0].GetAccessKey() != "access1" {
		t.Errorf("User1 credentials not correct: %+v", user1.GetCredentials())
	}

	user2, err := store.GetUser(ctx, "user2")
	if err != nil {
		t.Fatalf("Failed to get user2: %v", err)
	}

	if len(user2.GetCredentials()) != 1 || user2.GetCredentials()[0].GetAccessKey() != "access2" {
		t.Errorf("User2 credentials not correct: %+v", user2.GetCredentials())
	}
}

func TestMemoryStoreServiceAccountByAccessKey(t *testing.T) {
	store := &MemoryStore{}
	config := util.GetViper()
	if err := store.Initialize(config, "credential."); err != nil {
		t.Fatalf("Failed to initialize store: %v", err)
	}

	ctx := context.Background()

	// 1. Create service account
	sa := &iam_pb.ServiceAccount{
		Id:         "sa-test-1",
		ParentUser: "user1",
		Credential: &iam_pb.Credential{
			AccessKey: "ACCESS-KEY-1",
			SecretKey: "SECRET-KEY-1",
		},
	}

	if err := store.CreateServiceAccount(ctx, sa); err != nil {
		t.Fatalf("Failed to create service account: %v", err)
	}

	// 2. Lookup by access key
	found, err := store.GetServiceAccountByAccessKey(ctx, "ACCESS-KEY-1")
	if err != nil {
		t.Fatalf("Failed to lookup by access key: %v", err)
	}
	if found.GetId() != "sa-test-1" {
		t.Errorf("Expected sa-test-1, got %s", found.GetId())
	}

	// 3. Update with new access key
	sa.Credential.AccessKey = "ACCESS-KEY-2"
	if err := store.UpdateServiceAccount(ctx, sa.GetId(), sa); err != nil {
		t.Fatalf("Failed to update service account: %v", err)
	}

	// Verify old key is gone
	_, err = store.GetServiceAccountByAccessKey(ctx, "ACCESS-KEY-1")
	if !errors.Is(err, credential.ErrAccessKeyNotFound) {
		t.Errorf("Expected ErrAccessKeyNotFound for old key, got %v", err)
	}

	// Verify new key works
	found, err = store.GetServiceAccountByAccessKey(ctx, "ACCESS-KEY-2")
	if err != nil {
		t.Fatalf("Failed to lookup by new access key: %v", err)
	}
	if found.GetId() != "sa-test-1" {
		t.Errorf("Expected sa-test-1, got %s", found.GetId())
	}

	// 4. Delete service account
	if err := store.DeleteServiceAccount(ctx, sa.GetId()); err != nil {
		t.Fatalf("Failed to delete service account: %v", err)
	}

	// Verify key is gone
	_, err = store.GetServiceAccountByAccessKey(ctx, "ACCESS-KEY-2")
	if !errors.Is(err, credential.ErrAccessKeyNotFound) {
		t.Errorf("Expected ErrAccessKeyNotFound after delete, got %v", err)
	}
}
