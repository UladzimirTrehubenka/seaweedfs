package maintenance

import (
	"errors"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
)

// MaintenanceConfigManager handles protobuf-based configuration
type MaintenanceConfigManager struct {
	config *worker_pb.MaintenanceConfig
}

// NewMaintenanceConfigManager creates a new config manager with defaults
func NewMaintenanceConfigManager() *MaintenanceConfigManager {
	return &MaintenanceConfigManager{
		config: DefaultMaintenanceConfigProto(),
	}
}

// DefaultMaintenanceConfigProto returns default configuration as protobuf
func DefaultMaintenanceConfigProto() *worker_pb.MaintenanceConfig {
	return &worker_pb.MaintenanceConfig{
		Enabled:                true,
		ScanIntervalSeconds:    30 * 60,     // 30 minutes
		WorkerTimeoutSeconds:   5 * 60,      // 5 minutes
		TaskTimeoutSeconds:     2 * 60 * 60, // 2 hours
		RetryDelaySeconds:      15 * 60,     // 15 minutes
		MaxRetries:             3,
		CleanupIntervalSeconds: 24 * 60 * 60,     // 24 hours
		TaskRetentionSeconds:   7 * 24 * 60 * 60, // 7 days
		// Policy field will be populated dynamically from separate task configuration files
		Policy: nil,
	}
}

// GetConfig returns the current configuration
func (mcm *MaintenanceConfigManager) GetConfig() *worker_pb.MaintenanceConfig {
	return mcm.config
}

// Type-safe configuration accessors

// GetVacuumConfig returns vacuum-specific configuration for a task type
func (mcm *MaintenanceConfigManager) GetVacuumConfig(taskType string) *worker_pb.VacuumTaskConfig {
	if policy := mcm.getTaskPolicy(taskType); policy != nil {
		if vacuumConfig := policy.GetVacuumConfig(); vacuumConfig != nil {
			return vacuumConfig
		}
	}
	// Return defaults if not configured
	return &worker_pb.VacuumTaskConfig{
		GarbageThreshold:   0.3,
		MinVolumeAgeHours:  24,
		MinIntervalSeconds: 7 * 24 * 60 * 60, // 7 days
	}
}

// GetErasureCodingConfig returns EC-specific configuration for a task type
func (mcm *MaintenanceConfigManager) GetErasureCodingConfig(taskType string) *worker_pb.ErasureCodingTaskConfig {
	if policy := mcm.getTaskPolicy(taskType); policy != nil {
		if ecConfig := policy.GetErasureCodingConfig(); ecConfig != nil {
			return ecConfig
		}
	}
	// Return defaults if not configured
	return &worker_pb.ErasureCodingTaskConfig{
		FullnessRatio:    0.95,
		QuietForSeconds:  3600,
		MinVolumeSizeMb:  100,
		CollectionFilter: "",
	}
}

// GetBalanceConfig returns balance-specific configuration for a task type
func (mcm *MaintenanceConfigManager) GetBalanceConfig(taskType string) *worker_pb.BalanceTaskConfig {
	if policy := mcm.getTaskPolicy(taskType); policy != nil {
		if balanceConfig := policy.GetBalanceConfig(); balanceConfig != nil {
			return balanceConfig
		}
	}
	// Return defaults if not configured
	return &worker_pb.BalanceTaskConfig{
		ImbalanceThreshold: 0.2,
		MinServerCount:     2,
	}
}

// GetReplicationConfig returns replication-specific configuration for a task type
func (mcm *MaintenanceConfigManager) GetReplicationConfig(taskType string) *worker_pb.ReplicationTaskConfig {
	if policy := mcm.getTaskPolicy(taskType); policy != nil {
		if replicationConfig := policy.GetReplicationConfig(); replicationConfig != nil {
			return replicationConfig
		}
	}
	// Return defaults if not configured
	return &worker_pb.ReplicationTaskConfig{
		TargetReplicaCount: 2,
	}
}

// Typed convenience methods for getting task configurations

// GetVacuumTaskConfigForType returns vacuum configuration for a specific task type
func (mcm *MaintenanceConfigManager) GetVacuumTaskConfigForType(taskType string) *worker_pb.VacuumTaskConfig {
	return GetVacuumTaskConfig(mcm.config.GetPolicy(), MaintenanceTaskType(taskType))
}

// GetErasureCodingTaskConfigForType returns erasure coding configuration for a specific task type
func (mcm *MaintenanceConfigManager) GetErasureCodingTaskConfigForType(taskType string) *worker_pb.ErasureCodingTaskConfig {
	return GetErasureCodingTaskConfig(mcm.config.GetPolicy(), MaintenanceTaskType(taskType))
}

// GetBalanceTaskConfigForType returns balance configuration for a specific task type
func (mcm *MaintenanceConfigManager) GetBalanceTaskConfigForType(taskType string) *worker_pb.BalanceTaskConfig {
	return GetBalanceTaskConfig(mcm.config.GetPolicy(), MaintenanceTaskType(taskType))
}

// GetReplicationTaskConfigForType returns replication configuration for a specific task type
func (mcm *MaintenanceConfigManager) GetReplicationTaskConfigForType(taskType string) *worker_pb.ReplicationTaskConfig {
	return GetReplicationTaskConfig(mcm.config.GetPolicy(), MaintenanceTaskType(taskType))
}

// Helper methods

func (mcm *MaintenanceConfigManager) getTaskPolicy(taskType string) *worker_pb.TaskPolicy {
	if mcm.config.GetPolicy() != nil && mcm.config.Policy.TaskPolicies != nil {
		return mcm.config.GetPolicy().GetTaskPolicies()[taskType]
	}

	return nil
}

// IsTaskEnabled returns whether a task type is enabled
func (mcm *MaintenanceConfigManager) IsTaskEnabled(taskType string) bool {
	if policy := mcm.getTaskPolicy(taskType); policy != nil {
		return policy.GetEnabled()
	}

	return false
}

// GetMaxConcurrent returns the max concurrent limit for a task type
func (mcm *MaintenanceConfigManager) GetMaxConcurrent(taskType string) int32 {
	if policy := mcm.getTaskPolicy(taskType); policy != nil {
		return policy.GetMaxConcurrent()
	}

	return 1 // Default
}

// GetRepeatInterval returns the repeat interval for a task type in seconds
func (mcm *MaintenanceConfigManager) GetRepeatInterval(taskType string) int32 {
	if policy := mcm.getTaskPolicy(taskType); policy != nil {
		return policy.GetRepeatIntervalSeconds()
	}

	return mcm.config.GetPolicy().GetDefaultRepeatIntervalSeconds()
}

// GetCheckInterval returns the check interval for a task type in seconds
func (mcm *MaintenanceConfigManager) GetCheckInterval(taskType string) int32 {
	if policy := mcm.getTaskPolicy(taskType); policy != nil {
		return policy.GetCheckIntervalSeconds()
	}

	return mcm.config.GetPolicy().GetDefaultCheckIntervalSeconds()
}

// Duration accessor methods

// GetScanInterval returns the scan interval as a time.Duration
func (mcm *MaintenanceConfigManager) GetScanInterval() time.Duration {
	return time.Duration(mcm.config.GetScanIntervalSeconds()) * time.Second
}

// GetWorkerTimeout returns the worker timeout as a time.Duration
func (mcm *MaintenanceConfigManager) GetWorkerTimeout() time.Duration {
	return time.Duration(mcm.config.GetWorkerTimeoutSeconds()) * time.Second
}

// GetTaskTimeout returns the task timeout as a time.Duration
func (mcm *MaintenanceConfigManager) GetTaskTimeout() time.Duration {
	return time.Duration(mcm.config.GetTaskTimeoutSeconds()) * time.Second
}

// GetRetryDelay returns the retry delay as a time.Duration
func (mcm *MaintenanceConfigManager) GetRetryDelay() time.Duration {
	return time.Duration(mcm.config.GetRetryDelaySeconds()) * time.Second
}

// GetCleanupInterval returns the cleanup interval as a time.Duration
func (mcm *MaintenanceConfigManager) GetCleanupInterval() time.Duration {
	return time.Duration(mcm.config.GetCleanupIntervalSeconds()) * time.Second
}

// GetTaskRetention returns the task retention period as a time.Duration
func (mcm *MaintenanceConfigManager) GetTaskRetention() time.Duration {
	return time.Duration(mcm.config.GetTaskRetentionSeconds()) * time.Second
}

// ValidateMaintenanceConfigWithSchema validates protobuf maintenance configuration using ConfigField rules
func ValidateMaintenanceConfigWithSchema(config *worker_pb.MaintenanceConfig) error {
	if config == nil {
		return errors.New("configuration cannot be nil")
	}

	// Get the schema to access field validation rules
	schema := GetMaintenanceConfigSchema()

	// Validate each field individually using the ConfigField rules
	if err := validateFieldWithSchema(schema, "enabled", config.GetEnabled()); err != nil {
		return err
	}

	if err := validateFieldWithSchema(schema, "scan_interval_seconds", int(config.GetScanIntervalSeconds())); err != nil {
		return err
	}

	if err := validateFieldWithSchema(schema, "worker_timeout_seconds", int(config.GetWorkerTimeoutSeconds())); err != nil {
		return err
	}

	if err := validateFieldWithSchema(schema, "task_timeout_seconds", int(config.GetTaskTimeoutSeconds())); err != nil {
		return err
	}

	if err := validateFieldWithSchema(schema, "retry_delay_seconds", int(config.GetRetryDelaySeconds())); err != nil {
		return err
	}

	if err := validateFieldWithSchema(schema, "max_retries", int(config.GetMaxRetries())); err != nil {
		return err
	}

	if err := validateFieldWithSchema(schema, "cleanup_interval_seconds", int(config.GetCleanupIntervalSeconds())); err != nil {
		return err
	}

	if err := validateFieldWithSchema(schema, "task_retention_seconds", int(config.GetTaskRetentionSeconds())); err != nil {
		return err
	}

	// Validate policy fields if present
	if config.GetPolicy() != nil {
		// Note: These field names might need to be adjusted based on the actual schema
		if err := validatePolicyField("global_max_concurrent", int(config.GetPolicy().GetGlobalMaxConcurrent())); err != nil {
			return err
		}

		if err := validatePolicyField("default_repeat_interval_seconds", int(config.GetPolicy().GetDefaultRepeatIntervalSeconds())); err != nil {
			return err
		}

		if err := validatePolicyField("default_check_interval_seconds", int(config.GetPolicy().GetDefaultCheckIntervalSeconds())); err != nil {
			return err
		}
	}

	return nil
}

// validateFieldWithSchema validates a single field using its ConfigField definition
func validateFieldWithSchema(schema *MaintenanceConfigSchema, fieldName string, value any) error {
	field := schema.GetFieldByName(fieldName)
	if field == nil {
		// Field not in schema, skip validation
		return nil
	}

	return field.ValidateValue(value)
}

// validatePolicyField validates policy fields (simplified validation for now)
func validatePolicyField(fieldName string, value int) error {
	switch fieldName {
	case "global_max_concurrent":
		if value < 1 || value > 20 {
			return fmt.Errorf("Global Max Concurrent must be between 1 and 20, got %d", value)
		}
	case "default_repeat_interval":
		if value < 1 || value > 168 {
			return fmt.Errorf("Default Repeat Interval must be between 1 and 168 hours, got %d", value)
		}
	case "default_check_interval":
		if value < 1 || value > 168 {
			return fmt.Errorf("Default Check Interval must be between 1 and 168 hours, got %d", value)
		}
	}

	return nil
}
