package maintenance

import (
	"errors"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
)

// VerifyProtobufConfig demonstrates that the protobuf configuration system is working
func VerifyProtobufConfig() error {
	// Create configuration manager
	configManager := NewMaintenanceConfigManager()
	config := configManager.GetConfig()

	// Verify basic configuration
	if !config.GetEnabled() {
		return errors.New("expected config to be enabled by default")
	}

	if config.GetScanIntervalSeconds() != 30*60 {
		return fmt.Errorf("expected scan interval to be 1800 seconds, got %d", config.GetScanIntervalSeconds())
	}

	// Verify policy configuration
	if config.GetPolicy() == nil {
		return errors.New("expected policy to be configured")
	}

	if config.GetPolicy().GetGlobalMaxConcurrent() != 4 {
		return fmt.Errorf("expected global max concurrent to be 4, got %d", config.GetPolicy().GetGlobalMaxConcurrent())
	}

	// Verify task policies
	vacuumPolicy := config.GetPolicy().GetTaskPolicies()["vacuum"]
	if vacuumPolicy == nil {
		return errors.New("expected vacuum policy to be configured")
	}

	if !vacuumPolicy.GetEnabled() {
		return errors.New("expected vacuum policy to be enabled")
	}

	// Verify typed configuration access
	vacuumConfig := vacuumPolicy.GetVacuumConfig()
	if vacuumConfig == nil {
		return errors.New("expected vacuum config to be accessible")
	}

	if vacuumConfig.GetGarbageThreshold() != 0.3 {
		return fmt.Errorf("expected garbage threshold to be 0.3, got %f", vacuumConfig.GetGarbageThreshold())
	}

	// Verify helper functions work
	if !IsTaskEnabled(config.GetPolicy(), "vacuum") {
		return errors.New("expected vacuum task to be enabled via helper function")
	}

	maxConcurrent := GetMaxConcurrent(config.GetPolicy(), "vacuum")
	if maxConcurrent != 2 {
		return fmt.Errorf("expected vacuum max concurrent to be 2, got %d", maxConcurrent)
	}

	// Verify erasure coding configuration
	ecPolicy := config.GetPolicy().GetTaskPolicies()["erasure_coding"]
	if ecPolicy == nil {
		return errors.New("expected EC policy to be configured")
	}

	ecConfig := ecPolicy.GetErasureCodingConfig()
	if ecConfig == nil {
		return errors.New("expected EC config to be accessible")
	}

	// Verify configurable EC fields only
	if ecConfig.GetFullnessRatio() <= 0 || ecConfig.GetFullnessRatio() > 1 {
		return fmt.Errorf("expected EC config to have valid fullness ratio (0-1), got %f", ecConfig.GetFullnessRatio())
	}

	return nil
}

// GetProtobufConfigSummary returns a summary of the current protobuf configuration
func GetProtobufConfigSummary() string {
	configManager := NewMaintenanceConfigManager()
	config := configManager.GetConfig()

	summary := "SeaweedFS Protobuf Maintenance Configuration:\n"
	summary += fmt.Sprintf("  Enabled: %v\n", config.GetEnabled())
	summary += fmt.Sprintf("  Scan Interval: %d seconds\n", config.GetScanIntervalSeconds())
	summary += fmt.Sprintf("  Max Retries: %d\n", config.GetMaxRetries())
	summary += fmt.Sprintf("  Global Max Concurrent: %d\n", config.GetPolicy().GetGlobalMaxConcurrent())
	summary += fmt.Sprintf("  Task Policies: %d configured\n", len(config.GetPolicy().GetTaskPolicies()))

	var summarySb94 strings.Builder
	for taskType, policy := range config.GetPolicy().GetTaskPolicies() {
		summarySb94.WriteString(fmt.Sprintf("    %s: enabled=%v, max_concurrent=%d\n",
			taskType, policy.GetEnabled(), policy.GetMaxConcurrent()))
	}
	summary += summarySb94.String()

	return summary
}

// CreateCustomConfig demonstrates creating a custom protobuf configuration
func CreateCustomConfig() *worker_pb.MaintenanceConfig {
	return &worker_pb.MaintenanceConfig{
		Enabled:             true,
		ScanIntervalSeconds: 60 * 60, // 1 hour
		MaxRetries:          5,
		Policy: &worker_pb.MaintenancePolicy{
			GlobalMaxConcurrent: 8,
			TaskPolicies: map[string]*worker_pb.TaskPolicy{
				"custom_vacuum": {
					Enabled:       true,
					MaxConcurrent: 4,
					TaskConfig: &worker_pb.TaskPolicy_VacuumConfig{
						VacuumConfig: &worker_pb.VacuumTaskConfig{
							GarbageThreshold:  0.5,
							MinVolumeAgeHours: 48,
						},
					},
				},
			},
		},
	}
}
