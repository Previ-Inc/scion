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
	"slices"
	"strings"
	"sync"
	"time"

	restate "github.com/restatedev/sdk-go"
)

// DecryptedSecret is materialized only inside the ephemeral memory of a restate.Run
// broker dispatch closure and is never persisted in Restate state or journal entries (Invariant 4).
type DecryptedSecret struct {
	Name   string `json:"name"`
	Type   string `json:"type,omitempty"`
	Target string `json:"target,omitempty"`
	Value  string `json:"value,omitempty"`
	Ref    string `json:"ref,omitempty"`
	Source string `json:"source,omitempty"`
}

// BrokerStartCommand carries the idempotent container start parameters sent to the Runtime Broker.
type BrokerStartCommand struct {
	AgentID          string            `json:"agentId"`
	RunID            string            `json:"runId"`
	OpID             string            `json:"opId"`
	Generation       uint64            `json:"generation"`
	AwakeableID      string            `json:"awakeableId,omitempty"`
	Spec             *AgentSpec        `json:"spec"`
	DecryptedSecrets []DecryptedSecret `json:"decryptedSecrets,omitempty"`
}

// BrokerLaunchResult is the outcome of a broker launch (either returned synchronously or resolved via Awakeable).
type BrokerLaunchResult struct {
	ContainerID string `json:"containerId"`
	RunID       string `json:"runId"`
	AsyncWait   bool   `json:"asyncWait,omitempty"` // When true, the workflow awaits Awakeable resolution
	Phase       string `json:"phase,omitempty"`
}

// BrokerStopCommand requests stopping a container for a specific agent/run/generation.
type BrokerStopCommand struct {
	BrokerID    string `json:"brokerId"`
	AgentID     string `json:"agentId"`
	RunID       string `json:"runId"`
	OpID        string `json:"opId"`
	Generation  uint64 `json:"generation"`
	ContainerID string `json:"containerId,omitempty"`
}

// BrokerDeleteCommand requests idempotent container and workspace removal.
type BrokerDeleteCommand struct {
	BrokerID     string `json:"brokerId"`
	AgentID      string `json:"agentId"`
	RunID        string `json:"runId"`
	OpID         string `json:"opId"`
	Generation   uint64 `json:"generation"`
	DeleteFiles  bool   `json:"deleteFiles"`
	RemoveBranch bool   `json:"removeBranch"`
}

// BrokerRuntimeExecutor is the boundary interface implemented by the Runtime Broker client
// (or NAT BrokerObject dispatcher) for executing idempotent container operations.
type BrokerRuntimeExecutor interface {
	StartContainerIdempotent(ctx context.Context, cmd BrokerStartCommand) (BrokerLaunchResult, error)
	StopContainerIdempotent(ctx context.Context, cmd BrokerStopCommand) error
	DeleteContainerIdempotent(ctx context.Context, cmd BrokerDeleteCommand) error
}

// noopBrokerExecutor is the default executor when none is injected, returning deterministic IDs.
type noopBrokerExecutor struct{}

func (noopBrokerExecutor) StartContainerIdempotent(_ context.Context, cmd BrokerStartCommand) (BrokerLaunchResult, error) {
	return BrokerLaunchResult{
		ContainerID: "ctr-" + cmd.RunID,
		RunID:       cmd.RunID,
		Phase:       PhaseRunning,
	}, nil
}

func (noopBrokerExecutor) StopContainerIdempotent(_ context.Context, _ BrokerStopCommand) error {
	return nil
}

func (noopBrokerExecutor) DeleteContainerIdempotent(_ context.Context, _ BrokerDeleteCommand) error {
	return nil
}

// WorkflowInput is the input to AgentOpWorkflow.Run.
type WorkflowInput struct {
	AgentID             string     `json:"agentId"`
	OpID                string     `json:"opId"`
	OpKind              string     `json:"opKind"`
	Generation          uint64     `json:"generation"`
	PreemptedOpID       string     `json:"preemptedOpId,omitempty"`
	PreemptedInvocation string     `json:"preemptedInvocation,omitempty"`
	PriorRunID          string     `json:"priorRunId,omitempty"`
	PriorContainerID    string     `json:"priorContainerId,omitempty"`
	Spec                *AgentSpec `json:"spec,omitempty"`
}

// WorkflowOutput is the result of AgentOpWorkflow.Run.
type WorkflowOutput struct {
	AgentID     string `json:"agentId"`
	OpID        string `json:"opId"`
	OpKind      string `json:"opKind"`
	Generation  uint64 `json:"generation"`
	RunID       string `json:"runId,omitempty"`
	ContainerID string `json:"containerId,omitempty"`
	Phase       string `json:"phase"`
}

// AgentOpWorkflow executes long-running multi-step agent lifecycle sagas (Create, Start, Stop, Delete)
// without holding the AgentObject lock across external I/O or Awakeables (Invariant 1).
type AgentOpWorkflow struct {
	mu     sync.RWMutex
	broker BrokerRuntimeExecutor
	cipher *ValueCipher
}

// NewAgentOpWorkflow constructs an AgentOpWorkflow.
func NewAgentOpWorkflow(broker BrokerRuntimeExecutor, cipher *ValueCipher) *AgentOpWorkflow {
	if broker == nil {
		broker = noopBrokerExecutor{}
	}
	return &AgentOpWorkflow{
		broker: broker,
		cipher: cipher,
	}
}

// SetBrokerExecutor sets the BrokerRuntimeExecutor on the workflow instance.
func (w *AgentOpWorkflow) SetBrokerExecutor(broker BrokerRuntimeExecutor) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if broker == nil {
		broker = noopBrokerExecutor{}
	}
	w.broker = broker
}

func (w *AgentOpWorkflow) getBroker() BrokerRuntimeExecutor {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.broker
}

// ServiceName returns the canonical Restate Workflow name.
func (w *AgentOpWorkflow) ServiceName() string {
	return ServiceAgentOpWorkflow
}

// Run executes the deterministic lifecycle saga for Create, Start, Restart, Stop, or Delete.
func (w *AgentOpWorkflow) Run(ctx restate.WorkflowContext, in WorkflowInput) (WorkflowOutput, error) {
	if in.Spec == nil {
		in.Spec = &AgentSpec{AgentID: in.AgentID}
	}
	switch in.OpKind {
	case OpKindCreate:
		return w.runCreateOrStart(ctx, in, true)
	case OpKindStart, OpKindRestart:
		return w.runCreateOrStart(ctx, in, false)
	case OpKindStop:
		return w.runStop(ctx, in)
	case OpKindDelete:
		return w.runDelete(ctx, in)
	default:
		return WorkflowOutput{}, restate.ToTerminalError(
			fmt.Errorf("unsupported operation kind %q", in.OpKind),
			restate.WithErrorCode(http.StatusBadRequest),
		)
	}
}

func (w *AgentOpWorkflow) runCreateOrStart(ctx restate.WorkflowContext, in WorkflowInput, isCreate bool) (out WorkflowOutput, err error) {
	runID := in.OpID
	if runID == "" {
		runID = restate.UUID(ctx).String()
	}

	var compensations []func() error
	defer func() {
		if err != nil {
			for _, comp := range slices.Backward(compensations) {
				_ = comp()
			}
			_, _ = restate.Object[restate.Void](ctx, ServiceAgentObject, in.AgentID, "FailOp").
				Request(FailOpRequest{
					OpID:       in.OpID,
					Generation: in.Generation,
					Reason:     err.Error(),
					RemoveRow:  isCreate,
				})
		}
	}()

	// Step 1: For Create, claim unique slug in ProjectObject and register initial agent summary.
	if isCreate && in.Spec.ProjectID != "" && in.Spec.Slug != "" {
		compensations = append(compensations, func() error {
			_, cErr := restate.Object[restate.Void](ctx, ServiceProjectObject, in.Spec.ProjectID, "ReleaseSlug").
				Request(SlugClaim{Slug: in.Spec.Slug, AgentID: in.AgentID})
			_, _ = restate.Object[restate.Void](ctx, ServiceProjectObject, in.Spec.ProjectID, "RemoveAgentSummary").
				Request(in.AgentID)
			return cErr
		})
		if _, err = restate.Object[restate.Void](ctx, ServiceProjectObject, in.Spec.ProjectID, "ClaimSlug").
			Request(SlugClaim{Slug: in.Spec.Slug, AgentID: in.AgentID}); err != nil {
			return WorkflowOutput{}, err
		}
		if _, err = restate.Object[restate.Void](ctx, ServiceProjectObject, in.Spec.ProjectID, "UpsertAgentSummary").
			Request(ProjectAgentSummary{
				AgentID:    in.AgentID,
				Slug:       in.Spec.Slug,
				Name:       in.Spec.Name,
				Phase:      PhaseProvisioning,
				BrokerID:   in.Spec.BrokerID,
				OwnerID:    in.Spec.OwnerID,
				Generation: in.Generation,
			}); err != nil {
			return WorkflowOutput{}, err
		}
	}

	// Step 2: Reserve Broker Quota (idempotent on OpID, key "res:<op_id>").
	if in.Spec.BrokerID != "" {
		quotaScope := "broker:" + in.Spec.BrokerID
		compensations = append(compensations, func() error {
			_, cErr := restate.Object[restate.Void](ctx, ServiceQuotaObject, quotaScope, "Release").
				Request(QuotaReleaseRequest{OpID: in.OpID, AgentID: in.AgentID})
			return cErr
		})
		if _, err = restate.Object[QuotaReservation](ctx, ServiceQuotaObject, quotaScope, "Reserve").
			Request(QuotaReserveRequest{
				OpID:    in.OpID,
				AgentID: in.AgentID,
				Amount:  1,
			}); err != nil {
			return WorkflowOutput{}, err
		}
	}

	// Step 3: Reserve Project Quota if ProjectID is set.
	if in.Spec.ProjectID != "" {
		projQuotaScope := "project:" + in.Spec.ProjectID
		compensations = append(compensations, func() error {
			_, cErr := restate.Object[restate.Void](ctx, ServiceQuotaObject, projQuotaScope, "Release").
				Request(QuotaReleaseRequest{OpID: in.OpID, AgentID: in.AgentID})
			return cErr
		})
		if _, err = restate.Object[QuotaReservation](ctx, ServiceQuotaObject, projQuotaScope, "Reserve").
			Request(QuotaReserveRequest{
				OpID:    in.OpID,
				AgentID: in.AgentID,
				Amount:  1,
			}); err != nil {
			return WorkflowOutput{}, err
		}
	}

	// Step 4: Dispatch idempotent container start to Broker (decrypting enc:v2: secrets only inside RunContext).
	launchAwakeable := restate.Awakeable[BrokerLaunchResult](ctx)
	awkID := launchAwakeable.Id()
	restate.Set(ctx, "launch_awakeable_id", awkID)

	compensations = append(compensations, func() error {
		_, cErr := restate.Run(ctx, func(rctx restate.RunContext) (restate.Void, error) {
			return restate.Void{}, w.getBroker().DeleteContainerIdempotent(rctx, BrokerDeleteCommand{
				BrokerID:     in.Spec.BrokerID,
				AgentID:      in.AgentID,
				RunID:        runID,
				OpID:         in.OpID,
				Generation:   in.Generation,
				DeleteFiles:  isCreate,
				RemoveBranch: false,
			})
		}, restate.WithName("Compensate: broker delete container"))
		return cErr
	})

	launchRes, err := restate.Run(ctx, func(rctx restate.RunContext) (BrokerLaunchResult, error) {
		decrypted, dErr := w.decryptSecretsInMemory(in.Spec.Secrets)
		if dErr != nil {
			return BrokerLaunchResult{}, restate.ToTerminalError(dErr, restate.WithErrorCode(http.StatusBadRequest))
		}
		return w.getBroker().StartContainerIdempotent(rctx, BrokerStartCommand{
			AgentID:          in.AgentID,
			RunID:            runID,
			OpID:             in.OpID,
			Generation:       in.Generation,
			AwakeableID:      awkID,
			Spec:             in.Spec,
			DecryptedSecrets: decrypted,
		})
	}, restate.WithName("Dispatch broker start"))
	if err != nil {
		return WorkflowOutput{}, err
	}

	// Step 5: If the broker accepted an asynchronous launch, wait on the Awakeable raced with LaunchTimeout.
	if launchRes.AsyncWait {
		timeout := in.Spec.LaunchTimeout
		if timeout <= 0 {
			timeout = 5 * time.Minute
		}
		timeoutFuture := restate.After(ctx, timeout)
		first, wErr := restate.WaitFirst(ctx, launchAwakeable, timeoutFuture)
		if wErr != nil {
			return WorkflowOutput{}, wErr
		}
		if first == timeoutFuture {
			return WorkflowOutput{}, restate.TerminalErrorf("launch timed out after %v", timeout)
		}
		launchRes, err = launchAwakeable.Result()
		if err != nil {
			return WorkflowOutput{}, err
		}
	}

	if launchRes.RunID == "" {
		launchRes.RunID = runID
	}
	if launchRes.Phase == "" {
		if in.Spec.ProvisionOnly {
			launchRes.Phase = PhaseCreated
		} else {
			launchRes.Phase = PhaseRunning
		}
	}

	// Step 6: Commit running state on AgentObject (fenced by Generation).
	if _, err = restate.Object[AgentRecord](ctx, ServiceAgentObject, in.AgentID, "CommitLaunch").
		Request(CommitLaunchRequest{
			OpID:        in.OpID,
			Generation:  in.Generation,
			RunID:       launchRes.RunID,
			ContainerID: launchRes.ContainerID,
			Phase:       launchRes.Phase,
		}); err != nil {
		return WorkflowOutput{}, err
	}

	return WorkflowOutput{
		AgentID:     in.AgentID,
		OpID:        in.OpID,
		OpKind:      in.OpKind,
		Generation:  in.Generation,
		RunID:       launchRes.RunID,
		ContainerID: launchRes.ContainerID,
		Phase:       launchRes.Phase,
	}, nil
}

func (w *AgentOpWorkflow) runStop(ctx restate.WorkflowContext, in WorkflowInput) (WorkflowOutput, error) {
	if _, err := restate.Run(ctx, func(rctx restate.RunContext) (restate.Void, error) {
		return restate.Void{}, w.getBroker().StopContainerIdempotent(rctx, BrokerStopCommand{
			BrokerID:    in.Spec.BrokerID,
			AgentID:     in.AgentID,
			RunID:       in.PriorRunID,
			OpID:        in.OpID,
			Generation:  in.Generation,
			ContainerID: in.PriorContainerID,
		})
	}, restate.WithName("Dispatch broker stop")); err != nil {
		return WorkflowOutput{}, err
	}

	// Release active broker quota reservation if any.
	if in.Spec.BrokerID != "" {
		_, _ = restate.Object[restate.Void](ctx, ServiceQuotaObject, "broker:"+in.Spec.BrokerID, "ReleaseByAgent").
			Request(in.AgentID)
	}

	if _, err := restate.Object[AgentRecord](ctx, ServiceAgentObject, in.AgentID, "CommitLaunch").
		Request(CommitLaunchRequest{
			OpID:       in.OpID,
			Generation: in.Generation,
			Phase:      PhaseStopped,
		}); err != nil {
		return WorkflowOutput{}, err
	}

	return WorkflowOutput{
		AgentID:    in.AgentID,
		OpID:       in.OpID,
		OpKind:     OpKindStop,
		Generation: in.Generation,
		Phase:      PhaseStopped,
	}, nil
}

func (w *AgentOpWorkflow) runDelete(ctx restate.WorkflowContext, in WorkflowInput) (WorkflowOutput, error) {
	// Step 1: Delete container on Broker (both prior run and any preempted run).
	runIDToClean := in.PriorRunID
	if runIDToClean == "" {
		runIDToClean = in.PreemptedOpID
	}
	if _, err := restate.Run(ctx, func(rctx restate.RunContext) (restate.Void, error) {
		return restate.Void{}, w.getBroker().DeleteContainerIdempotent(rctx, BrokerDeleteCommand{
			BrokerID:     in.Spec.BrokerID,
			AgentID:      in.AgentID,
			RunID:        runIDToClean,
			OpID:         in.OpID,
			Generation:   in.Generation,
			DeleteFiles:  in.Spec.DeleteFiles,
			RemoveBranch: in.Spec.RemoveBranch,
		})
	}, restate.WithName("Dispatch broker delete")); err != nil && !in.Spec.Force {
		return WorkflowOutput{}, err
	}

	// Step 2: Release Broker and Project Quotas (idempotent).
	if in.Spec.BrokerID != "" {
		if in.PreemptedOpID != "" {
			_, _ = restate.Object[restate.Void](ctx, ServiceQuotaObject, "broker:"+in.Spec.BrokerID, "Release").
				Request(QuotaReleaseRequest{OpID: in.PreemptedOpID, AgentID: in.AgentID})
		}
		_, _ = restate.Object[restate.Void](ctx, ServiceQuotaObject, "broker:"+in.Spec.BrokerID, "ReleaseByAgent").
			Request(in.AgentID)
	}
	if in.Spec.ProjectID != "" {
		if in.PreemptedOpID != "" {
			_, _ = restate.Object[restate.Void](ctx, ServiceQuotaObject, "project:"+in.Spec.ProjectID, "Release").
				Request(QuotaReleaseRequest{OpID: in.PreemptedOpID, AgentID: in.AgentID})
		}
		_, _ = restate.Object[restate.Void](ctx, ServiceQuotaObject, "project:"+in.Spec.ProjectID, "ReleaseByAgent").
			Request(in.AgentID)
	}

	// Step 3: Release slug and remove summary from ProjectObject.
	if in.Spec.ProjectID != "" {
		if in.Spec.Slug != "" && !in.Spec.SoftDelete {
			_, _ = restate.Object[restate.Void](ctx, ServiceProjectObject, in.Spec.ProjectID, "ReleaseSlug").
				Request(SlugClaim{Slug: in.Spec.Slug, AgentID: in.AgentID})
		}
		if !in.Spec.SoftDelete {
			_, _ = restate.Object[restate.Void](ctx, ServiceProjectObject, in.Spec.ProjectID, "RemoveAgentSummary").
				Request(in.AgentID)
		}
	}

	// Step 4: Finalize deletion on AgentObject (retaining tombstone + generation fence).
	if _, err := restate.Object[restate.Void](ctx, ServiceAgentObject, in.AgentID, "FinalizeDelete").
		Request(FinalizeDeleteRequest{
			OpID:       in.OpID,
			Generation: in.Generation,
			SoftDelete: in.Spec.SoftDelete,
		}); err != nil {
		return WorkflowOutput{}, err
	}

	return WorkflowOutput{
		AgentID:    in.AgentID,
		OpID:       in.OpID,
		OpKind:     OpKindDelete,
		Generation: in.Generation,
		Phase:      PhaseDeleted,
	}, nil
}

// GetLaunchAwakeableID returns the Awakeable ID registered by this workflow for async launch completion.
func (w *AgentOpWorkflow) GetLaunchAwakeableID(ctx restate.WorkflowSharedContext, _ restate.Void) (string, error) {
	return restate.Get[string](ctx, "launch_awakeable_id")
}

func (w *AgentOpWorkflow) decryptSecretsInMemory(encSecrets []EncryptedSecretVar) ([]DecryptedSecret, error) {
	if len(encSecrets) == 0 {
		return nil, nil
	}
	out := make([]DecryptedSecret, 0, len(encSecrets))
	for _, s := range encSecrets {
		val := ""
		if s.Ciphertext != "" {
			if !strings.HasPrefix(s.Ciphertext, "enc:v2:") {
				return nil, fmt.Errorf("secret %q is not enc:v2: encrypted", s.Name)
			}
			if w.cipher != nil {
				pt, err := w.cipher.Decrypt(s.Ciphertext)
				if err != nil {
					return nil, fmt.Errorf("decrypt secret %q: %w", s.Name, err)
				}
				val = pt
			}
		}
		out = append(out, DecryptedSecret{
			Name:   s.Name,
			Type:   s.Type,
			Target: s.Target,
			Value:  val,
			Ref:    s.Ref,
			Source: s.Source,
		})
	}
	return out, nil
}
