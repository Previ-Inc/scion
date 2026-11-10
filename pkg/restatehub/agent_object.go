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
	"fmt"
	"net/http"
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"
)

// Lifecycle operation kinds managed by AgentObject and AgentOpWorkflow.
const (
	OpKindCreate      = "create"
	OpKindStart       = "start"
	OpKindStop        = "stop"
	OpKindRestart     = "restart"
	OpKindReprovision = "reprovision"
	OpKindDelete      = "delete"
)

// Standard agent phases.
const (
	PhaseCreated      = "created"
	PhaseProvisioning = "provisioning"
	PhaseStarting     = "starting"
	PhaseRunning      = "running"
	PhaseStopping     = "stopping"
	PhaseStopped      = "stopped"
	PhaseSuspended    = "suspended"
	PhaseError        = "error"
	PhaseDeleted      = "deleted"
)

// EncryptedSecretVar carries an enc:v2: encrypted secret value or external secret reference.
type EncryptedSecretVar struct {
	Name       string `json:"name"`
	Type       string `json:"type,omitempty"`
	Target     string `json:"target,omitempty"`
	Ciphertext string `json:"ciphertext,omitempty"`
	Ref        string `json:"ref,omitempty"`
	Source     string `json:"source,omitempty"`
}

// AgentSpec describes the desired configuration and launch parameters for an agent operation.
type AgentSpec struct {
	AgentID        string               `json:"agentId"`
	Slug           string               `json:"slug"`
	Name           string               `json:"name"`
	ProjectID      string               `json:"projectId"`
	BrokerID       string               `json:"brokerId"`
	Template       string               `json:"template,omitempty"`
	Image          string               `json:"image,omitempty"`
	Runtime        string               `json:"runtime,omitempty"`
	Task           string               `json:"task,omitempty"`
	OwnerID        string               `json:"ownerId,omitempty"`
	CreatedBy      string               `json:"createdBy,omitempty"`
	DelegatorID    string               `json:"delegatorId,omitempty"`
	DelegatorType  string               `json:"delegatorType,omitempty"`
	Ancestry       []string             `json:"ancestry,omitempty"`
	Labels         map[string]string    `json:"labels,omitempty"`
	Annotations    map[string]string    `json:"annotations,omitempty"`
	Env            map[string]string    `json:"env,omitempty"`
	Secrets        []EncryptedSecretVar `json:"secrets,omitempty"`
	LaunchTimeout  time.Duration        `json:"launchTimeout,omitempty"`
	ProvisionOnly  bool                 `json:"provisionOnly,omitempty"`
	DeleteFiles    bool                 `json:"deleteFiles,omitempty"`
	RemoveBranch   bool                 `json:"removeBranch,omitempty"`
	SoftDelete     bool                 `json:"softDelete,omitempty"`
	Force          bool                 `json:"force,omitempty"`
	ServiceAccount string               `json:"serviceAccount,omitempty"`
}

// Granular state keys inside AgentObject[agent_id] (Invariant 1 & Invariant 2).
const (
	agentKeyInfo       = "info"
	agentKeyGen        = "gen"
	agentKeyActiveOp   = "active_op"
	agentKeyCred       = "cred"
	agentKeySAAssign   = "sa_assign"
	agentKeyHoldPrefix = "hold:"
	agentKeyRunPrefix  = "run:"
)

// AgentRecord is the authoritative state stored under key "info" in AgentObject[agent_id].
type AgentRecord struct {
	ID              string            `json:"id"`
	Slug            string            `json:"slug"`
	Name            string            `json:"name"`
	ProjectID       string            `json:"projectId"`
	BrokerID        string            `json:"brokerId"`
	Template        string            `json:"template,omitempty"`
	Image           string            `json:"image,omitempty"`
	Runtime         string            `json:"runtime,omitempty"`
	Phase           string            `json:"phase"`
	Activity        string            `json:"activity,omitempty"`
	ContainerID     string            `json:"containerId,omitempty"`
	ContainerStatus string            `json:"containerStatus,omitempty"`
	RunID           string            `json:"runId,omitempty"`
	Generation      uint64            `json:"generation"`
	StateVersion    int64             `json:"stateVersion"`
	OwnerID         string            `json:"ownerId,omitempty"`
	CreatedBy       string            `json:"createdBy,omitempty"`
	DelegatorID     string            `json:"delegatorId,omitempty"`
	Ancestry        []string          `json:"ancestry,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
	Annotations     map[string]string `json:"annotations,omitempty"`
	Message         string            `json:"message,omitempty"`
	Created         time.Time         `json:"created"`
	Updated         time.Time         `json:"updated"`
	LastSeen        time.Time         `json:"lastSeen,omitempty"`
	DeletedAt       *time.Time        `json:"deletedAt,omitempty"`
	Tombstoned      bool              `json:"tombstoned,omitempty"`
}

// ActiveOp tracks the currently executing AgentOpWorkflow on AgentObject.
type ActiveOp struct {
	OpID                 string    `json:"opId"`
	OpKind               string    `json:"opKind"`
	Generation           uint64    `json:"generation"`
	WorkflowInvocationID string    `json:"workflowInvocationId,omitempty"`
	StartedAt            time.Time `json:"startedAt"`
}

// AgentHold represents an administrative or legal hold preventing hard deletion.
type AgentHold struct {
	ID        string    `json:"id"`
	AgentID   string    `json:"agentId"`
	Reason    string    `json:"reason"`
	CreatedBy string    `json:"createdBy"`
	CreatedAt time.Time `json:"createdAt"`
}

// AgentCredentialRecord tracks active credentials issued to an agent run.
type AgentCredentialRecord struct {
	CredentialID string    `json:"credentialId"`
	RunID        string    `json:"runId"`
	Generation   uint64    `json:"generation"`
	Revoked      bool      `json:"revoked"`
	RevokeReason string    `json:"revokeReason,omitempty"`
	IssuedAt     time.Time `json:"issuedAt"`
}

// AgentSAAssignment tracks GCP Service Account assignment for an agent.
type AgentSAAssignment struct {
	AgentID          string    `json:"agentId"`
	ProjectID        string    `json:"projectId"`
	ServiceAccountID string    `json:"serviceAccountId"`
	Origin           string    `json:"origin"`
	Active           bool      `json:"active"`
	AssignedAt       time.Time `json:"assignedAt"`
}

// ClaimOpRequest requests exclusive operation ownership on AgentObject.
type ClaimOpRequest struct {
	OpID    string     `json:"opId"`
	OpKind  string     `json:"opKind"`
	Preempt bool       `json:"preempt"`
	Spec    *AgentSpec `json:"spec,omitempty"`
	Now     time.Time  `json:"now,omitempty"`
}

// ClaimOpResponse returns the generation fence and any cancelled workflow ID.
type ClaimOpResponse struct {
	Generation           uint64      `json:"generation"`
	CancelledWorkflow    string      `json:"cancelledWorkflow,omitempty"`
	CancelledOpID        string      `json:"cancelledOpId,omitempty"`
	WorkflowInvocationID string      `json:"workflowInvocationId,omitempty"`
	Agent                AgentRecord `json:"agent"`
}

// CommitLaunchRequest commits a successful container launch or start on AgentObject.
type CommitLaunchRequest struct {
	OpID        string    `json:"opId"`
	Generation  uint64    `json:"generation"`
	RunID       string    `json:"runId"`
	ContainerID string    `json:"containerId"`
	Phase       string    `json:"phase,omitempty"`
	Now         time.Time `json:"now,omitempty"`
}

// FailOpRequest records that an operation failed and its compensations ran.
type FailOpRequest struct {
	OpID       string    `json:"opId"`
	Generation uint64    `json:"generation"`
	Reason     string    `json:"reason"`
	RemoveRow  bool      `json:"removeRow,omitempty"`
	Now        time.Time `json:"now,omitempty"`
}

// FinalizeDeleteRequest marks an agent as soft-deleted or tombstoned after teardown.
type FinalizeDeleteRequest struct {
	OpID       string    `json:"opId"`
	Generation uint64    `json:"generation"`
	SoftDelete bool      `json:"softDelete"`
	Now        time.Time `json:"now,omitempty"`
}

// BrokerStatusReport carries a status/heartbeat update or timer event fenced by Generation.
type BrokerStatusReport struct {
	Generation      uint64            `json:"generation"`
	RunID           string            `json:"runId,omitempty"`
	Phase           string            `json:"phase,omitempty"`
	Activity        string            `json:"activity,omitempty"`
	ContainerStatus string            `json:"containerStatus,omitempty"`
	ContainerID     string            `json:"containerId,omitempty"`
	Message         string            `json:"message,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
	Now             time.Time         `json:"now,omitempty"`
}

// BrokerStatusReportResponse indicates whether a status report was accepted or dropped as stale.
type BrokerStatusReportResponse struct {
	Accepted          bool   `json:"accepted"`
	CurrentGeneration uint64 `json:"currentGeneration"`
	Reason            string `json:"reason,omitempty"`
}

// EvaluateStandingRequest triggers an immediate standing evaluation (e.g. when a user loses project membership).
type EvaluateStandingRequest struct {
	RevokedPrincipalID string    `json:"revokedPrincipalId"`
	ProjectID          string    `json:"projectId"`
	Reason             string    `json:"reason"`
	Now                time.Time `json:"now,omitempty"`
}

// AutoSuspendTimerRequest is fired by a delayed timer to auto-suspend an idle agent.
type AutoSuspendTimerRequest struct {
	ExpectedGeneration uint64    `json:"expectedGeneration"`
	Reason             string    `json:"reason"`
	Now                time.Time `json:"now,omitempty"`
}

// AgentObject is the Restate Virtual Object owning the serialized state and generation fence of a single agent.
type AgentObject struct{}

// NewAgentObject constructs an AgentObject.
func NewAgentObject() *AgentObject {
	return &AgentObject{}
}

// ServiceName returns the canonical Restate Virtual Object name.
func (a *AgentObject) ServiceName() string {
	return ServiceAgentObject
}

// ClaimOp is a fast, non-blocking exclusive handler that claims operation ownership.
func (a *AgentObject) ClaimOp(ctx restate.ObjectContext, req ClaimOpRequest) (ClaimOpResponse, error) {
	now := req.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	agentID := restate.Key(ctx)

	if req.Spec != nil {
		for _, sec := range req.Spec.Secrets {
			if sec.Ciphertext != "" && !strings.HasPrefix(sec.Ciphertext, "enc:v2:") {
				return ClaimOpResponse{}, restate.ToTerminalError(
					fmt.Errorf("secret %q violates Invariant 4: ciphertext must use enc:v2: envelope encryption", sec.Name),
					restate.WithErrorCode(http.StatusBadRequest),
				)
			}
		}
	}

	gen, err := restate.Get[uint64](ctx, agentKeyGen)
	if err != nil {
		return ClaimOpResponse{}, err
	}
	activeOp, err := restate.Get[ActiveOp](ctx, agentKeyActiveOp)
	if err != nil {
		return ClaimOpResponse{}, err
	}
	info, err := restate.Get[AgentRecord](ctx, agentKeyInfo)
	if err != nil {
		return ClaimOpResponse{}, err
	}

	if info.Tombstoned && req.OpKind != OpKindDelete {
		return ClaimOpResponse{}, restate.ToTerminalError(
			fmt.Errorf("agent %s has been deleted", agentID),
			restate.WithErrorCode(http.StatusNotFound),
		)
	}

	if activeOp.OpID != "" && activeOp.OpID == req.OpID {
		return ClaimOpResponse{
			Generation:           activeOp.Generation,
			WorkflowInvocationID: activeOp.WorkflowInvocationID,
			Agent:                info,
		}, nil
	}

	var cancelledWf, cancelledOpID string
	if activeOp.OpID != "" && activeOp.OpID != req.OpID {
		if !req.Preempt && req.OpKind != OpKindDelete && req.OpKind != OpKindStop {
			return ClaimOpResponse{}, restate.ToTerminalError(
				fmt.Errorf("conflict: operation %s (%s) already in progress on agent %s", activeOp.OpKind, activeOp.OpID, agentID),
				restate.WithErrorCode(http.StatusConflict),
			)
		}
		cancelledWf = activeOp.WorkflowInvocationID
		cancelledOpID = activeOp.OpID
		if cancelledWf != "" {
			restate.CancelInvocation(ctx, cancelledWf)
		}
	}

	gen++
	restate.Set(ctx, agentKeyGen, gen)

	if info.ID == "" {
		info.ID = agentID
		info.Created = now
	}
	if req.Spec != nil {
		if req.Spec.Slug != "" {
			info.Slug = req.Spec.Slug
		}
		if req.Spec.Name != "" {
			info.Name = req.Spec.Name
		}
		if req.Spec.ProjectID != "" {
			info.ProjectID = req.Spec.ProjectID
		}
		if req.Spec.BrokerID != "" {
			info.BrokerID = req.Spec.BrokerID
		}
		if req.Spec.Template != "" {
			info.Template = req.Spec.Template
		}
		if req.Spec.Image != "" {
			info.Image = req.Spec.Image
		}
		if req.Spec.Runtime != "" {
			info.Runtime = req.Spec.Runtime
		}
		if req.Spec.OwnerID != "" {
			info.OwnerID = req.Spec.OwnerID
		}
		if req.Spec.CreatedBy != "" {
			info.CreatedBy = req.Spec.CreatedBy
		}
		if req.Spec.DelegatorID != "" {
			info.DelegatorID = req.Spec.DelegatorID
		}
		if len(req.Spec.Ancestry) > 0 {
			info.Ancestry = req.Spec.Ancestry
		}
		if len(req.Spec.Labels) > 0 {
			info.Labels = req.Spec.Labels
		}
		if len(req.Spec.Annotations) > 0 {
			info.Annotations = req.Spec.Annotations
		}
	}

	switch req.OpKind {
	case OpKindCreate:
		info.Phase = PhaseProvisioning
	case OpKindStart, OpKindRestart:
		info.Phase = PhaseStarting
	case OpKindStop, OpKindDelete:
		info.Phase = PhaseStopping
	}
	info.Generation = gen
	info.StateVersion++
	info.Updated = now
	restate.Set(ctx, agentKeyInfo, info)

	spec := req.Spec
	if spec == nil {
		spec = &AgentSpec{}
	}
	if spec.AgentID == "" {
		spec.AgentID = agentID
	}
	if spec.Slug == "" {
		spec.Slug = info.Slug
	}
	if spec.Name == "" {
		spec.Name = info.Name
	}
	if spec.ProjectID == "" {
		spec.ProjectID = info.ProjectID
	}
	if spec.BrokerID == "" {
		spec.BrokerID = info.BrokerID
	}

	wfHandle := restate.WorkflowSend(ctx, ServiceAgentOpWorkflow, req.OpID, "Run").Send(WorkflowInput{
		AgentID:             agentID,
		OpID:                req.OpID,
		OpKind:              req.OpKind,
		Generation:          gen,
		PreemptedOpID:       cancelledOpID,
		PreemptedInvocation: cancelledWf,
		PriorRunID:          info.RunID,
		PriorContainerID:    info.ContainerID,
		Spec:                spec,
	})
	invID := ExtractInvocationID(wfHandle)

	restate.Set(ctx, agentKeyActiveOp, ActiveOp{
		OpID:                 req.OpID,
		OpKind:               req.OpKind,
		Generation:           gen,
		WorkflowInvocationID: invID,
		StartedAt:            now,
	})

	return ClaimOpResponse{
		Generation:           gen,
		CancelledWorkflow:    cancelledWf,
		CancelledOpID:        cancelledOpID,
		WorkflowInvocationID: invID,
		Agent:                info,
	}, nil
}

// CommitLaunch finalizes a Create/Start/Restart/Stop workflow.
func (a *AgentObject) CommitLaunch(ctx restate.ObjectContext, req CommitLaunchRequest) (AgentRecord, error) {
	gen, _ := restate.Get[uint64](ctx, agentKeyGen)
	info, _ := restate.Get[AgentRecord](ctx, agentKeyInfo)
	if req.Generation < gen {
		return info, restate.ToTerminalError(
			fmt.Errorf("stale generation %d < current generation %d", req.Generation, gen),
			restate.WithErrorCode(http.StatusConflict),
		)
	}

	now := req.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}

	activeOp, _ := restate.Get[ActiveOp](ctx, agentKeyActiveOp)
	if activeOp.OpID == req.OpID {
		restate.Clear(ctx, agentKeyActiveOp)
	}

	if req.RunID != "" {
		info.RunID = req.RunID
		restate.Set(ctx, fmt.Sprintf("%s%08d", agentKeyRunPrefix, gen), req.RunID)
	}
	if req.ContainerID != "" {
		info.ContainerID = req.ContainerID
	}
	if req.Phase != "" {
		info.Phase = req.Phase
	} else {
		info.Phase = PhaseRunning
	}
	info.Message = ""
	info.StateVersion++
	info.Updated = now
	restate.Set(ctx, agentKeyInfo, info)

	if info.ProjectID != "" {
		restate.ObjectSend(ctx, ServiceProjectObject, info.ProjectID, "UpsertAgentSummary").Send(ProjectAgentSummary{
			AgentID:    info.ID,
			Slug:       info.Slug,
			Name:       info.Name,
			Phase:      info.Phase,
			BrokerID:   info.BrokerID,
			OwnerID:    info.OwnerID,
			Generation: info.Generation,
			Updated:    now,
		})
	}

	return info, nil
}

// FailOp records that an operation failed.
func (a *AgentObject) FailOp(ctx restate.ObjectContext, req FailOpRequest) (restate.Void, error) {
	gen, _ := restate.Get[uint64](ctx, agentKeyGen)
	if req.Generation < gen {
		return restate.Void{}, nil
	}
	now := req.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	activeOp, _ := restate.Get[ActiveOp](ctx, agentKeyActiveOp)
	if activeOp.OpID == req.OpID {
		restate.Clear(ctx, agentKeyActiveOp)
	}

	info, _ := restate.Get[AgentRecord](ctx, agentKeyInfo)
	if req.RemoveRow {
		info.Phase = PhaseDeleted
		info.Tombstoned = true
		info.DeletedAt = &now
	} else {
		info.Phase = PhaseError
		info.Message = req.Reason
	}
	info.StateVersion++
	info.Updated = now
	restate.Set(ctx, agentKeyInfo, info)
	return restate.Void{}, nil
}

// FinalizeDelete marks the agent as soft-deleted or tombstoned.
func (a *AgentObject) FinalizeDelete(ctx restate.ObjectContext, req FinalizeDeleteRequest) (restate.Void, error) {
	gen, _ := restate.Get[uint64](ctx, agentKeyGen)
	if req.Generation < gen {
		return restate.Void{}, nil
	}
	now := req.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	restate.Clear(ctx, agentKeyActiveOp)

	info, _ := restate.Get[AgentRecord](ctx, agentKeyInfo)
	info.Phase = PhaseDeleted
	info.ContainerID = ""
	info.ContainerStatus = "deleted"
	info.DeletedAt = &now
	info.Tombstoned = !req.SoftDelete
	info.StateVersion++
	info.Updated = now
	restate.Set(ctx, agentKeyInfo, info)
	return restate.Void{}, nil
}

// ReportStatus processes a broker heartbeat or status report.
func (a *AgentObject) ReportStatus(ctx restate.ObjectContext, req BrokerStatusReport) (BrokerStatusReportResponse, error) {
	gen, _ := restate.Get[uint64](ctx, agentKeyGen)
	if req.Generation < gen {
		return BrokerStatusReportResponse{
			Accepted:          false,
			CurrentGeneration: gen,
			Reason:            "stale_generation",
		}, nil
	}
	info, _ := restate.Get[AgentRecord](ctx, agentKeyInfo)
	if info.Tombstoned {
		return BrokerStatusReportResponse{
			Accepted:          false,
			CurrentGeneration: gen,
			Reason:            "agent_tombstoned",
		}, nil
	}

	now := req.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if req.Phase != "" {
		info.Phase = req.Phase
	}
	if req.Activity != "" {
		info.Activity = req.Activity
	}
	if req.ContainerStatus != "" {
		info.ContainerStatus = req.ContainerStatus
	}
	if req.ContainerID != "" {
		info.ContainerID = req.ContainerID
	}
	if req.Message != "" {
		info.Message = req.Message
	}
	if len(req.Labels) > 0 {
		if info.Labels == nil {
			info.Labels = make(map[string]string, len(req.Labels))
		}
		for k, v := range req.Labels {
			info.Labels[k] = v
		}
	}
	info.LastSeen = now
	info.Updated = now
	info.StateVersion++
	restate.Set(ctx, agentKeyInfo, info)

	if info.ProjectID != "" {
		restate.ObjectSend(ctx, ServiceProjectObject, info.ProjectID, "UpsertAgentSummary").Send(ProjectAgentSummary{
			AgentID:    info.ID,
			Slug:       info.Slug,
			Name:       info.Name,
			Phase:      info.Phase,
			BrokerID:   info.BrokerID,
			OwnerID:    info.OwnerID,
			Generation: info.Generation,
			Updated:    now,
		})
	}

	return BrokerStatusReportResponse{
		Accepted:          true,
		CurrentGeneration: gen,
	}, nil
}

// EvaluateStanding triggers an immediate standing evaluation.
func (a *AgentObject) EvaluateStanding(ctx restate.ObjectContext, req EvaluateStandingRequest) (bool, error) {
	info, _ := restate.Get[AgentRecord](ctx, agentKeyInfo)
	if info.ID == "" || info.Tombstoned || info.Phase == PhaseStopped || info.Phase == PhaseDeleted {
		return false, nil
	}
	affected := info.OwnerID == req.RevokedPrincipalID || info.DelegatorID == req.RevokedPrincipalID
	if !affected {
		for _, anc := range info.Ancestry {
			if anc == req.RevokedPrincipalID {
				affected = true
				break
			}
		}
	}
	if !affected {
		return false, nil
	}

	opID := fmt.Sprintf("revoke-stop-%s-%s", info.ID, restate.UUID(ctx).String())
	_, err := a.ClaimOp(ctx, ClaimOpRequest{
		OpID:    opID,
		OpKind:  OpKindStop,
		Preempt: true,
		Now:     req.Now,
	})
	return err == nil, err
}

// HandleAutoSuspendTimer processes an auto-suspend timer.
func (a *AgentObject) HandleAutoSuspendTimer(ctx restate.ObjectContext, req AutoSuspendTimerRequest) (bool, error) {
	gen, _ := restate.Get[uint64](ctx, agentKeyGen)
	if req.ExpectedGeneration != gen {
		return false, nil
	}
	info, _ := restate.Get[AgentRecord](ctx, agentKeyInfo)
	if info.ID == "" || info.Tombstoned || info.Phase != PhaseRunning {
		return false, nil
	}
	now := req.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	info.Phase = PhaseSuspended
	info.Message = req.Reason
	info.StateVersion++
	info.Updated = now
	restate.Set(ctx, agentKeyInfo, info)
	return true, nil
}

// PlaceHold records a hold.
func (a *AgentObject) PlaceHold(ctx restate.ObjectContext, hold AgentHold) (AgentHold, error) {
	if hold.ID == "" {
		hold.ID = restate.UUID(ctx).String()
	}
	hold.AgentID = restate.Key(ctx)
	if hold.CreatedAt.IsZero() {
		hold.CreatedAt = time.Now().UTC()
	}
	restate.Set(ctx, agentKeyHoldPrefix+hold.ID, hold)
	return hold, nil
}

// ReleaseHold removes a hold key.
func (a *AgentObject) ReleaseHold(ctx restate.ObjectContext, holdID string) (restate.Void, error) {
	restate.Clear(ctx, agentKeyHoldPrefix+holdID)
	return restate.Void{}, nil
}

// SetCredential stores credential metadata.
func (a *AgentObject) SetCredential(ctx restate.ObjectContext, cred AgentCredentialRecord) (restate.Void, error) {
	restate.Set(ctx, agentKeyCred, cred)
	return restate.Void{}, nil
}

// AssignServiceAccount stores service account assignment.
func (a *AgentObject) AssignServiceAccount(ctx restate.ObjectContext, sa AgentSAAssignment) (restate.Void, error) {
	restate.Set(ctx, agentKeySAAssign, sa)
	return restate.Void{}, nil
}

// Get reads the agent record.
func (a *AgentObject) Get(ctx restate.ObjectSharedContext, _ restate.Void) (AgentRecord, error) {
	info, err := restate.Get[AgentRecord](ctx, agentKeyInfo)
	if err != nil {
		return AgentRecord{}, err
	}
	if info.ID == "" {
		return AgentRecord{}, restate.ToTerminalError(
			fmt.Errorf("agent %s not found", restate.Key(ctx)),
			restate.WithErrorCode(http.StatusNotFound),
		)
	}
	return info, nil
}

// GetActiveOp reads active op.
func (a *AgentObject) GetActiveOp(ctx restate.ObjectSharedContext, _ restate.Void) (ActiveOp, error) {
	return restate.Get[ActiveOp](ctx, agentKeyActiveOp)
}

// ListHolds enumerates holds.
func (a *AgentObject) ListHolds(ctx restate.ObjectSharedContext, _ restate.Void) ([]AgentHold, error) {
	keys, err := restate.Keys(ctx)
	if err != nil {
		return nil, err
	}
	var holds []AgentHold
	for _, k := range keys {
		if strings.HasPrefix(k, agentKeyHoldPrefix) {
			h, err := restate.Get[AgentHold](ctx, k)
			if err != nil {
				return nil, err
			}
			holds = append(holds, h)
		}
	}
	return holds, nil
}
