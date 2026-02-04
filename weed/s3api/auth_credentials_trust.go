package s3api

import (
	"context"
	"errors"
)

// ValidateTrustPolicyForPrincipal validates if a principal is allowed to assume a role
// Delegates to the IAM integration if available
func (iam *IdentityAccessManagement) ValidateTrustPolicyForPrincipal(ctx context.Context, roleArn, principalArn string) error {
	if iam.iamIntegration != nil {
		return iam.iamIntegration.ValidateTrustPolicyForPrincipal(ctx, roleArn, principalArn)
	}

	return errors.New("IAM integration not available")
}
