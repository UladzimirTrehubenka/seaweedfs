package balance

import (
	"slices"

	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Scheduling implements the scheduling logic for balance tasks
func Scheduling(task *types.TaskInput, runningTasks []*types.TaskInput, availableWorkers []*types.WorkerData, config base.TaskConfig) bool {
	balanceConfig := config.(*Config)

	// Count running balance tasks
	runningBalanceCount := 0
	for _, runningTask := range runningTasks {
		if runningTask.Type == types.TaskTypeBalance {
			runningBalanceCount++
		}
	}

	// Check concurrency limit
	if runningBalanceCount >= balanceConfig.MaxConcurrent {
		return false
	}

	// Check if we have available workers
	availableWorkerCount := 0
	for _, worker := range availableWorkers {
		if slices.Contains(worker.Capabilities, types.TaskTypeBalance) {
			availableWorkerCount++
		}
	}

	return availableWorkerCount > 0
}
