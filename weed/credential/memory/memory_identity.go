package memory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

func (store *MemoryStore) LoadConfiguration(ctx context.Context) (*iam_pb.S3ApiConfiguration, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	if !store.initialized {
		return nil, errors.New("store not initialized")
	}

	config := &iam_pb.S3ApiConfiguration{}

	// Convert all users to identities
	for _, user := range store.users {
		// Deep copy the identity to avoid mutation issues
		identityCopy := store.deepCopyIdentity(user)
		config.Identities = append(config.Identities, identityCopy)
	}

	return config, nil
}

func (store *MemoryStore) SaveConfiguration(ctx context.Context, config *iam_pb.S3ApiConfiguration) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if !store.initialized {
		return errors.New("store not initialized")
	}

	// Clear existing data
	store.users = make(map[string]*iam_pb.Identity)
	store.accessKeys = make(map[string]string)

	// Add all identities
	for _, identity := range config.GetIdentities() {
		// Deep copy to avoid mutation issues
		identityCopy := store.deepCopyIdentity(identity)
		store.users[identity.GetName()] = identityCopy

		// Index access keys
		for _, credential := range identity.GetCredentials() {
			store.accessKeys[credential.GetAccessKey()] = identity.GetName()
		}
	}

	return nil
}

func (store *MemoryStore) CreateUser(ctx context.Context, identity *iam_pb.Identity) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if !store.initialized {
		return errors.New("store not initialized")
	}

	if _, exists := store.users[identity.GetName()]; exists {
		return credential.ErrUserAlreadyExists
	}

	// Check for duplicate access keys
	for _, cred := range identity.GetCredentials() {
		if _, exists := store.accessKeys[cred.GetAccessKey()]; exists {
			return fmt.Errorf("access key %s already exists", cred.GetAccessKey())
		}
	}

	// Deep copy to avoid mutation issues
	identityCopy := store.deepCopyIdentity(identity)
	store.users[identity.GetName()] = identityCopy

	// Index access keys
	for _, cred := range identity.GetCredentials() {
		store.accessKeys[cred.GetAccessKey()] = identity.GetName()
	}

	return nil
}

func (store *MemoryStore) GetUser(ctx context.Context, username string) (*iam_pb.Identity, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	if !store.initialized {
		return nil, errors.New("store not initialized")
	}

	user, exists := store.users[username]
	if !exists {
		return nil, credential.ErrUserNotFound
	}

	// Return a deep copy to avoid mutation issues
	return store.deepCopyIdentity(user), nil
}

func (store *MemoryStore) UpdateUser(ctx context.Context, username string, identity *iam_pb.Identity) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if !store.initialized {
		return errors.New("store not initialized")
	}

	existingUser, exists := store.users[username]
	if !exists {
		return credential.ErrUserNotFound
	}

	// Remove old access keys from index
	for _, cred := range existingUser.GetCredentials() {
		delete(store.accessKeys, cred.GetAccessKey())
	}

	// Check for duplicate access keys (excluding current user)
	for _, cred := range identity.GetCredentials() {
		if existingUsername, exists := store.accessKeys[cred.GetAccessKey()]; exists && existingUsername != username {
			return fmt.Errorf("access key %s already exists", cred.GetAccessKey())
		}
	}

	// Deep copy to avoid mutation issues
	identityCopy := store.deepCopyIdentity(identity)
	store.users[username] = identityCopy

	// Re-index access keys
	for _, cred := range identity.GetCredentials() {
		store.accessKeys[cred.GetAccessKey()] = username
	}

	return nil
}

func (store *MemoryStore) DeleteUser(ctx context.Context, username string) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if !store.initialized {
		return errors.New("store not initialized")
	}

	user, exists := store.users[username]
	if !exists {
		return credential.ErrUserNotFound
	}

	// Remove access keys from index
	for _, cred := range user.GetCredentials() {
		delete(store.accessKeys, cred.GetAccessKey())
	}

	// Remove user
	delete(store.users, username)

	return nil
}

func (store *MemoryStore) ListUsers(ctx context.Context) ([]string, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	if !store.initialized {
		return nil, errors.New("store not initialized")
	}

	var usernames []string
	for username := range store.users {
		usernames = append(usernames, username)
	}

	return usernames, nil
}

func (store *MemoryStore) GetUserByAccessKey(ctx context.Context, accessKey string) (*iam_pb.Identity, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	if !store.initialized {
		return nil, errors.New("store not initialized")
	}

	username, exists := store.accessKeys[accessKey]
	if !exists {
		return nil, credential.ErrAccessKeyNotFound
	}

	user, exists := store.users[username]
	if !exists {
		// This should not happen, but handle it gracefully
		return nil, credential.ErrUserNotFound
	}

	// Return a deep copy to avoid mutation issues
	return store.deepCopyIdentity(user), nil
}

func (store *MemoryStore) CreateAccessKey(ctx context.Context, username string, cred *iam_pb.Credential) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if !store.initialized {
		return errors.New("store not initialized")
	}

	user, exists := store.users[username]
	if !exists {
		return credential.ErrUserNotFound
	}

	// Check if access key already exists
	if _, exists := store.accessKeys[cred.GetAccessKey()]; exists {
		return fmt.Errorf("access key %s already exists", cred.GetAccessKey())
	}

	// Add credential to user
	user.Credentials = append(user.Credentials, &iam_pb.Credential{
		AccessKey: cred.GetAccessKey(),
		SecretKey: cred.GetSecretKey(),
	})

	// Index the access key
	store.accessKeys[cred.GetAccessKey()] = username

	return nil
}

func (store *MemoryStore) DeleteAccessKey(ctx context.Context, username string, accessKey string) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if !store.initialized {
		return errors.New("store not initialized")
	}

	user, exists := store.users[username]
	if !exists {
		return credential.ErrUserNotFound
	}

	// Find and remove the credential
	var newCredentials []*iam_pb.Credential
	found := false
	for _, cred := range user.GetCredentials() {
		if cred.GetAccessKey() == accessKey {
			found = true
			// Remove from access key index
			delete(store.accessKeys, accessKey)
		} else {
			newCredentials = append(newCredentials, cred)
		}
	}

	if !found {
		return credential.ErrAccessKeyNotFound
	}

	user.Credentials = newCredentials

	return nil
}

// deepCopyIdentity creates a deep copy of an identity to avoid mutation issues
func (store *MemoryStore) deepCopyIdentity(identity *iam_pb.Identity) *iam_pb.Identity {
	if identity == nil {
		return nil
	}

	// Use JSON marshaling/unmarshaling for deep copy
	// This is simple and safe for protobuf messages
	data, err := json.Marshal(identity)
	if err != nil {
		// Fallback to shallow copy if JSON fails
		return &iam_pb.Identity{
			Name:        identity.GetName(),
			Account:     identity.GetAccount(),
			Credentials: identity.GetCredentials(),
			Actions:     identity.GetActions(),
		}
	}

	var copy iam_pb.Identity
	if err := json.Unmarshal(data, &copy); err != nil {
		// Fallback to shallow copy if JSON fails
		return &iam_pb.Identity{
			Name:        identity.GetName(),
			Account:     identity.GetAccount(),
			Credentials: identity.GetCredentials(),
			Actions:     identity.GetActions(),
		}
	}

	return &copy
}
