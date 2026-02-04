package filer_etc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

func validateServiceAccountId(id string) error {
	return credential.ValidateServiceAccountId(id)
}

func (store *FilerEtcStore) loadServiceAccountsFromMultiFile(ctx context.Context, s3cfg *iam_pb.S3ApiConfiguration) error {
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		dir := filer.IamConfigDirectory + "/" + IamServiceAccountsDirectory
		entries, err := listEntries(ctx, client, dir)
		if err != nil {
			if errors.Is(err, filer_pb.ErrNotFound) {
				return nil
			}

			return err
		}

		for _, entry := range entries {
			if entry.GetIsDirectory() {
				continue
			}

			var content []byte
			if len(entry.GetContent()) > 0 {
				content = entry.GetContent()
			} else {
				c, err := filer.ReadInsideFiler(client, dir, entry.GetName())
				if err != nil {
					glog.Warningf("Failed to read service account file %s: %v", entry.GetName(), err)

					continue
				}
				content = c
			}

			if len(content) > 0 {
				sa := &iam_pb.ServiceAccount{}
				if err := json.Unmarshal(content, sa); err != nil {
					glog.Warningf("Failed to unmarshal service account %s: %v", entry.GetName(), err)

					continue
				}
				s3cfg.ServiceAccounts = append(s3cfg.ServiceAccounts, sa)
			}
		}

		return nil
	})
}

func (store *FilerEtcStore) saveServiceAccount(ctx context.Context, sa *iam_pb.ServiceAccount) error {
	if sa == nil {
		return errors.New("service account is nil")
	}
	if err := validateServiceAccountId(sa.GetId()); err != nil {
		return err
	}

	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		data, err := json.MarshalIndent(sa, "", "  ")
		if err != nil {
			return err
		}

		return filer.SaveInsideFiler(client, filer.IamConfigDirectory+"/"+IamServiceAccountsDirectory, sa.GetId()+".json", data)
	})
}

func (store *FilerEtcStore) deleteServiceAccount(ctx context.Context, saId string) error {
	if err := validateServiceAccountId(saId); err != nil {
		return err
	}

	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
			Directory: filer.IamConfigDirectory + "/" + IamServiceAccountsDirectory,
			Name:      saId + ".json",
		})
		if err != nil {
			if strings.Contains(err.Error(), filer_pb.ErrNotFound.Error()) {
				return credential.ErrServiceAccountNotFound
			}

			return err
		}
		if resp != nil && resp.GetError() != "" {
			if strings.Contains(resp.GetError(), filer_pb.ErrNotFound.Error()) {
				return credential.ErrServiceAccountNotFound
			}

			return fmt.Errorf("delete service account %s: %s", saId, resp.GetError())
		}

		return nil
	})
}

func (store *FilerEtcStore) CreateServiceAccount(ctx context.Context, sa *iam_pb.ServiceAccount) error {
	existing, err := store.GetServiceAccount(ctx, sa.GetId())
	if err != nil {
		if !errors.Is(err, credential.ErrServiceAccountNotFound) {
			return err
		}
	} else if existing != nil {
		return fmt.Errorf("service account %s already exists", sa.GetId())
	}

	return store.saveServiceAccount(ctx, sa)
}

func (store *FilerEtcStore) UpdateServiceAccount(ctx context.Context, id string, sa *iam_pb.ServiceAccount) error {
	if sa.GetId() != id {
		return errors.New("service account ID mismatch")
	}
	_, err := store.GetServiceAccount(ctx, id)
	if err != nil {
		return err
	}

	return store.saveServiceAccount(ctx, sa)
}

func (store *FilerEtcStore) DeleteServiceAccount(ctx context.Context, id string) error {
	return store.deleteServiceAccount(ctx, id)
}

func (store *FilerEtcStore) GetServiceAccount(ctx context.Context, id string) (*iam_pb.ServiceAccount, error) {
	if err := validateServiceAccountId(id); err != nil {
		return nil, err
	}
	var sa *iam_pb.ServiceAccount
	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		data, err := filer.ReadInsideFiler(client, filer.IamConfigDirectory+"/"+IamServiceAccountsDirectory, id+".json")
		if err != nil {
			if errors.Is(err, filer_pb.ErrNotFound) {
				return credential.ErrServiceAccountNotFound
			}

			return err
		}
		if len(data) == 0 {
			return credential.ErrServiceAccountNotFound
		}
		sa = &iam_pb.ServiceAccount{}

		return json.Unmarshal(data, sa)
	})

	return sa, err
}

func (store *FilerEtcStore) ListServiceAccounts(ctx context.Context) ([]*iam_pb.ServiceAccount, error) {
	var accounts []*iam_pb.ServiceAccount
	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		dir := filer.IamConfigDirectory + "/" + IamServiceAccountsDirectory
		entries, err := listEntries(ctx, client, dir)
		if err != nil {
			if errors.Is(err, filer_pb.ErrNotFound) {
				return nil
			}

			return err
		}

		for _, entry := range entries {
			if entry.GetIsDirectory() {
				continue
			}

			var content []byte
			if len(entry.GetContent()) > 0 {
				content = entry.GetContent()
			} else {
				c, err := filer.ReadInsideFiler(client, dir, entry.GetName())
				if err != nil {
					glog.Warningf("Failed to read service account file %s: %v", entry.GetName(), err)

					continue
				}
				content = c
			}

			if len(content) > 0 {
				sa := &iam_pb.ServiceAccount{}
				if err := json.Unmarshal(content, sa); err != nil {
					glog.Warningf("Failed to unmarshal service account %s: %v", entry.GetName(), err)

					continue
				}
				accounts = append(accounts, sa)
			}
		}

		return nil
	})

	return accounts, err
}

func (store *FilerEtcStore) GetServiceAccountByAccessKey(ctx context.Context, accessKey string) (*iam_pb.ServiceAccount, error) {
	accounts, err := store.ListServiceAccounts(ctx)
	if err != nil {
		return nil, err
	}
	for _, sa := range accounts {
		if sa.GetCredential() != nil && sa.GetCredential().GetAccessKey() == accessKey {
			return sa, nil
		}
	}

	return nil, credential.ErrAccessKeyNotFound
}
