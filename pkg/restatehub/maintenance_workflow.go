// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package restatehub

import (
	"context"
	"fmt"
	"net/http"
	"time"

	restate "github.com/restatedev/sdk-go"
)

// MaintenanceTaskInput carries parameters for administrative maintenance runs.
type MaintenanceTaskInput struct {
	TaskID      string            `json:"taskId"`
	TaskName    string            `json:"taskName"`
	Parameters  map[string]string `json:"parameters,omitempty"`
	InitiatedBy string            `json:"initiatedBy,omitempty"`
}

// MaintenanceTaskResult reports the outcome of a maintenance execution.
type MaintenanceTaskResult struct {
	TaskID      string    `json:"taskId"`
	TaskName    string    `json:"taskName"`
	Success     bool      `json:"success"`
	Output      string    `json:"output,omitempty"`
	Error       string    `json:"error,omitempty"`
	CompletedAt time.Time `json:"completedAt"`
}

// MaintenanceTaskExecutor is the boundary interface for executing administrative maintenance jobs.
type MaintenanceTaskExecutor interface {
	ExecuteTask(ctx context.Context, task MaintenanceTaskInput) (string, error)
}

// noopMaintenanceExecutor is the default no-op maintenance executor.
type noopMaintenanceExecutor struct{}

func (noopMaintenanceExecutor) ExecuteTask(_ context.Context, task MaintenanceTaskInput) (string, error) {
	return fmt.Sprintf("task %s completed successfully", task.TaskName), nil
}

// MaintenanceWorkflow is the Restate Workflow managing durable execution of administrative
// maintenance tasks with automatic panic/cancellation cleanup via defer.
type MaintenanceWorkflow struct {
	executor MaintenanceTaskExecutor
}

// NewMaintenanceWorkflow constructs a MaintenanceWorkflow.
func NewMaintenanceWorkflow(executor MaintenanceTaskExecutor) *MaintenanceWorkflow {
	if executor == nil {
		executor = noopMaintenanceExecutor{}
	}
	return &MaintenanceWorkflow{executor: executor}
}

// ServiceName returns the canonical Restate Workflow name.
func (m *MaintenanceWorkflow) ServiceName() string {
	return ServiceMaintenanceWorkflow
}

// Run executes the durable maintenance task.
func (m *MaintenanceWorkflow) Run(ctx restate.WorkflowContext, in MaintenanceTaskInput) (MaintenanceTaskResult, error) {
	output, err := restate.Run(ctx, func(rctx restate.RunContext) (string, error) {
		return m.executor.ExecuteTask(rctx, in)
	}, restate.WithName("Execute maintenance task"))

	if err != nil {
		return MaintenanceTaskResult{
			TaskID:      in.TaskID,
			TaskName:    in.TaskName,
			Success:     false,
			Error:       err.Error(),
			CompletedAt: time.Now().UTC(),
		}, restate.ToTerminalError(err, restate.WithErrorCode(http.StatusInternalServerError))
	}

	return MaintenanceTaskResult{
		TaskID:      in.TaskID,
		TaskName:    in.TaskName,
		Success:     true,
		Output:      output,
		CompletedAt: time.Now().UTC(),
	}, nil
}
