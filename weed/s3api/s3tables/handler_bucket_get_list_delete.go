package s3tables

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// handleGetTableBucket gets details of a table bucket
func (h *S3TablesHandler) handleGetTableBucket(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	// Check permission

	var req GetTableBucketRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())

		return err
	}

	if req.TableBucketARN == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN is required")

		return errors.New("tableBucketARN is required")
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())

		return err
	}

	bucketPath := GetTableBucketPath(bucketName)

	var metadata tableBucketMetadata
	var bucketPolicy string
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &metadata); err != nil {
			return fmt.Errorf("failed to unmarshal metadata: %w", err)
		}

		// Fetch bucket policy if it exists
		policyData, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err == nil {
			bucketPolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket policy: %w", err)
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to get table bucket: %v", err))
		}

		return err
	}

	bucketARN := h.generateTableBucketARN(metadata.OwnerAccountID, bucketName)
	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	if !CheckPermissionWithContext("GetTableBucket", principal, metadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
		TableBucketName: bucketName,
		IdentityActions: identityActions,
	}) {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to get table bucket details")

		return ErrAccessDenied
	}

	resp := &GetTableBucketResponse{
		ARN:            h.generateTableBucketARN(metadata.OwnerAccountID, bucketName),
		Name:           metadata.Name,
		OwnerAccountID: metadata.OwnerAccountID,
		CreatedAt:      metadata.CreatedAt,
	}

	h.writeJSON(w, http.StatusOK, resp)

	return nil
}

// handleListTableBuckets lists all table buckets
func (h *S3TablesHandler) handleListTableBuckets(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req ListTableBucketsRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())

		return err
	}

	principal := h.getAccountID(r)
	accountID := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	if !CheckPermissionWithContext("ListTableBuckets", principal, accountID, "", "", &PolicyContext{
		IdentityActions: identityActions,
	}) {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to list table buckets")

		return NewAuthError("ListTableBuckets", principal, "not authorized to list table buckets")
	}

	maxBuckets := req.MaxBuckets
	if maxBuckets <= 0 {
		maxBuckets = 100
	}
	// Cap to prevent uint32 overflow when used in uint32(maxBuckets*2)
	const maxBucketsLimit = 1000
	if maxBuckets > maxBucketsLimit {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "MaxBuckets exceeds maximum allowed value")

		return fmt.Errorf("invalid maxBuckets value: %d", maxBuckets)
	}

	var buckets []TableBucketSummary

	lastFileName := req.ContinuationToken
	err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		for len(buckets) < maxBuckets {
			resp, err := client.ListEntries(r.Context(), &filer_pb.ListEntriesRequest{
				Directory:          TablesPath,
				Limit:              uint32(maxBuckets * 2), // Fetch more than needed to account for filtering
				StartFromFileName:  lastFileName,
				InclusiveStartFrom: lastFileName == "" || lastFileName == req.ContinuationToken,
			})
			if err != nil {
				return err
			}

			hasMore := false
			for {
				entry, respErr := resp.Recv()
				if respErr != nil {
					if errors.Is(respErr, io.EOF) {
						break
					}

					return respErr
				}
				if entry.GetEntry() == nil {
					continue
				}

				// Skip the start item if it was included in the previous page
				if len(buckets) == 0 && req.ContinuationToken != "" && entry.GetEntry().GetName() == req.ContinuationToken {
					continue
				}

				hasMore = true
				lastFileName = entry.GetEntry().GetName()

				if !entry.GetEntry().GetIsDirectory() {
					continue
				}

				// Skip entries starting with "."
				if strings.HasPrefix(entry.GetEntry().GetName(), ".") {
					continue
				}

				// Apply prefix filter
				if req.Prefix != "" && !strings.HasPrefix(entry.GetEntry().GetName(), req.Prefix) {
					continue
				}

				// Read metadata from extended attribute
				data, ok := entry.GetEntry().GetExtended()[ExtendedKeyMetadata]
				if !ok {
					continue
				}

				var metadata tableBucketMetadata
				if err := json.Unmarshal(data, &metadata); err != nil {
					continue
				}

				bucketPath := GetTableBucketPath(entry.GetEntry().GetName())
				bucketPolicy := ""
				policyData, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
				if err != nil {
					if !errors.Is(err, ErrAttributeNotFound) {
						continue
					}
				} else {
					bucketPolicy = string(policyData)
				}

				bucketARN := h.generateTableBucketARN(metadata.OwnerAccountID, entry.GetEntry().GetName())
				identityActions := getIdentityActions(r)
				if !CheckPermissionWithContext("GetTableBucket", accountID, metadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
					TableBucketName: entry.GetEntry().GetName(),
					IdentityActions: identityActions,
				}) {
					continue
				}

				buckets = append(buckets, TableBucketSummary{
					ARN:       bucketARN,
					Name:      entry.GetEntry().GetName(),
					CreatedAt: metadata.CreatedAt,
				})

				if len(buckets) >= maxBuckets {
					return nil
				}
			}

			if !hasMore {
				break
			}
		}

		return nil
	})

	if err != nil {
		// Check if it's a "not found" error - return empty list in that case
		if errors.Is(err, filer_pb.ErrNotFound) {
			buckets = []TableBucketSummary{}
		} else {
			// For other errors, return error response
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to list table buckets: %v", err))

			return err
		}
	}

	paginationToken := ""
	if len(buckets) >= maxBuckets {
		paginationToken = lastFileName
	}

	resp := &ListTableBucketsResponse{
		TableBuckets:      buckets,
		ContinuationToken: paginationToken,
	}

	h.writeJSON(w, http.StatusOK, resp)

	return nil
}

// handleDeleteTableBucket deletes a table bucket
func (h *S3TablesHandler) handleDeleteTableBucket(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req DeleteTableBucketRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())

		return err
	}

	if req.TableBucketARN == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN is required")

		return errors.New("tableBucketARN is required")
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())

		return err
	}

	bucketPath := GetTableBucketPath(bucketName)

	// Check if bucket exists and perform ownership + emptiness check in one block
	var metadata tableBucketMetadata
	var bucketPolicy string
	hasChildren := false
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// 1. Get metadata for ownership check
		data, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &metadata); err != nil {
			return fmt.Errorf("failed to unmarshal metadata: %w", err)
		}

		// Fetch bucket policy if it exists
		policyData, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err != nil {
			if errors.Is(err, ErrAttributeNotFound) {
				// No bucket policy set; proceed with empty bucketPolicy
			} else {
				return fmt.Errorf("failed to fetch bucket policy: %w", err)
			}
		} else {
			bucketPolicy = string(policyData)
		}

		bucketARN := h.generateTableBucketARN(metadata.OwnerAccountID, bucketName)
		principal := h.getAccountID(r)
		identityActions := getIdentityActions(r)
		if !CheckPermissionWithContext("DeleteTableBucket", principal, metadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
			TableBucketName: bucketName,
			IdentityActions: identityActions,
		}) {
			return NewAuthError("DeleteTableBucket", principal, "not authorized to delete bucket "+bucketName)
		}

		// 3. Check if bucket is empty
		resp, err := client.ListEntries(r.Context(), &filer_pb.ListEntriesRequest{
			Directory: bucketPath,
			Limit:     10,
		})
		if err != nil {
			return err
		}

		for {
			entry, err := resp.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				return err
			}
			if entry.GetEntry() != nil && !strings.HasPrefix(entry.GetEntry().GetName(), ".") {
				hasChildren = true

				break
			}
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		} else if isAuthError(err) {
			h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, err.Error())
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to delete table bucket: %v", err))
		}

		return err
	}

	if hasChildren {
		h.writeError(w, http.StatusConflict, ErrCodeBucketNotEmpty, "table bucket is not empty")

		return errors.New("bucket not empty")
	}

	// Delete the bucket
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.deleteDirectory(r.Context(), client, bucketPath)
	})

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to delete table bucket")

		return err
	}

	h.writeJSON(w, http.StatusOK, nil)

	return nil
}
