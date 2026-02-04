package integration

import (
	"context"
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/utils"
)

// ValidateTrustPolicyForPrincipal validates if a principal is allowed to assume a role
func (m *IAMManager) ValidateTrustPolicyForPrincipal(ctx context.Context, roleArn, principalArn string) error {
	if !m.initialized {
		return errors.New("IAM manager not initialized")
	}

	// Extract role name from ARN
	roleName := utils.ExtractRoleNameFromArn(roleArn)

	// Get role definition
	roleDef, err := m.roleStore.GetRole(ctx, m.getFilerAddress(), roleName)
	if err != nil {
		return fmt.Errorf("failed to get role %s: %w", roleName, err)
	}

	if roleDef.TrustPolicy == nil {
		return errors.New("role has no trust policy")
	}

	// Create evaluation context
	evalCtx := &policy.EvaluationContext{
		Principal: principalArn,
		Action:    "sts:AssumeRole",
		Resource:  roleArn,
	}

	// Evaluate the trust policy
	if !m.evaluateTrustPolicy(roleDef.TrustPolicy, evalCtx) {
		return fmt.Errorf("trust policy denies access to principal: %s", principalArn)
	}

	return nil
}
