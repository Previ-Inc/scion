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
	"sort"
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"
)

// Granular state key prefixes inside ProjectObject[project_id] (Invariant 2).
const (
	projectKeyMeta         = "meta"
	projectKeySyncState    = "sync_state"
	projectKeyAgentPrefix  = "agent:"
	projectKeySlugPrefix   = "slug:"
	projectKeyBrokerPrefix = "broker:"
	projectKeyEnvPrefix    = "env:"
	projectKeySecretPrefix = "secret:"
	projectKeyMemberPrefix = "member:"
)

// ProjectMeta stores the metadata of a project under key "meta".
type ProjectMeta struct {
	ID              string            `json:"id"`
	Slug            string            `json:"slug"`
	Name            string            `json:"name"`
	GitRemote       string            `json:"gitRemote,omitempty"`
	DefaultBrokerID string            `json:"defaultBrokerId,omitempty"`
	OwnerID         string            `json:"ownerId,omitempty"`
	WorkspaceMode   string            `json:"workspaceMode,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
	Annotations     map[string]string `json:"annotations,omitempty"`
	Created         time.Time         `json:"created"`
	Updated         time.Time         `json:"updated"`
}

// ProjectAgentSummary is stored under key "agent:<agent_id>" in ProjectObject[project_id]
// so listing project agents never requires DataFusion table scans (Invariant 5) or O(N^2) slice rewrites (Invariant 2).
type ProjectAgentSummary struct {
	AgentID    string    `json:"agentId"`
	Slug       string    `json:"slug"`
	Name       string    `json:"name"`
	Phase      string    `json:"phase"`
	BrokerID   string    `json:"brokerId,omitempty"`
	OwnerID    string    `json:"ownerId,omitempty"`
	Generation uint64    `json:"generation"`
	Updated    time.Time `json:"updated"`
}

// SlugClaim requests claiming or releasing a unique slug within a project.
type SlugClaim struct {
	Slug    string `json:"slug"`
	AgentID string `json:"agentId"`
}

// ProjectEnvVar represents a single environment variable stored under "env:<key>".
type ProjectEnvVar struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	UpdatedBy string    `json:"updatedBy,omitempty"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// ProjectSecretRecord stores an encrypted secret under "secret:<name>" (Invariant 4: enc:v2: ciphertext only).
type ProjectSecretRecord struct {
	Name        string    `json:"name"`
	Type        string    `json:"type,omitempty"`
	Target      string    `json:"target,omitempty"`
	Ciphertext  string    `json:"ciphertext"` // Always "enc:v2:..."
	Ref         string    `json:"ref,omitempty"`
	Description string    `json:"description,omitempty"`
	Version     int       `json:"version"`
	UpdatedBy   string    `json:"updatedBy,omitempty"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

// SetProjectSecretRequest writes a secret to ProjectObject. If Plaintext is supplied,
// ProjectObject encrypts it with its ValueCipher before persisting; otherwise Ciphertext must start with "enc:v2:".
type SetProjectSecretRequest struct {
	Name        string `json:"name"`
	Type        string `json:"type,omitempty"`
	Target      string `json:"target,omitempty"`
	Ciphertext  string `json:"ciphertext,omitempty"`
	Ref         string `json:"ref,omitempty"`
	Description string `json:"description,omitempty"`
	UpdatedBy   string `json:"updatedBy,omitempty"`
}

// ProjectBrokerLink links a Runtime Broker provider to a project under "broker:<broker_id>".
type ProjectBrokerLink struct {
	BrokerID    string    `json:"brokerId"`
	ProjectPath string    `json:"projectPath,omitempty"`
	LinkedAt    time.Time `json:"linkedAt"`
}

// ProjectMemberBinding stores a principal's membership in a project under "member:<principal_id>".
type ProjectMemberBinding struct {
	PrincipalID   string    `json:"principalId"`
	PrincipalType string    `json:"principalType"` // "user" | "group" | "agent"
	Role          string    `json:"role"`
	AddedBy       string    `json:"addedBy,omitempty"`
	AddedAt       time.Time `json:"addedAt"`
}

// ProjectObject is the Restate Virtual Object owning a project's metadata, granular agent index,
// slug uniqueness claims, environment variables, and enc:v2: secrets.
type ProjectObject struct {
	cipher *ValueCipher
}

// NewProjectObject constructs a ProjectObject.
func NewProjectObject(cipher *ValueCipher) *ProjectObject {
	return &ProjectObject{cipher: cipher}
}

// ServiceName returns the canonical Restate Virtual Object name.
func (p *ProjectObject) ServiceName() string {
	return ServiceProjectObject
}

// CreateOrUpdateMeta creates or updates the project metadata under "meta".
func (p *ProjectObject) CreateOrUpdateMeta(ctx restate.ObjectContext, req ProjectMeta) (ProjectMeta, error) {
	projectID := restate.Key(ctx)
	existing, err := restate.Get[ProjectMeta](ctx, projectKeyMeta)
	if err != nil {
		return ProjectMeta{}, err
	}
	now := time.Now().UTC()
	if existing.ID == "" {
		req.ID = projectID
		if req.Created.IsZero() {
			req.Created = now
		}
	} else {
		req.ID = existing.ID
		req.Created = existing.Created
	}
	req.Updated = now
	restate.Set(ctx, projectKeyMeta, req)
	return req, nil
}

// GetMeta reads the project metadata.
func (p *ProjectObject) GetMeta(ctx restate.ObjectSharedContext, _ restate.Void) (ProjectMeta, error) {
	meta, err := restate.Get[ProjectMeta](ctx, projectKeyMeta)
	if err != nil {
		return ProjectMeta{}, err
	}
	if meta.ID == "" {
		return ProjectMeta{}, restate.ToTerminalError(
			fmt.Errorf("project %s not found", restate.Key(ctx)),
			restate.WithErrorCode(http.StatusNotFound),
		)
	}
	return meta, nil
}

// ClaimSlug reserves "slug:<slug>" for AgentID (idempotent if already held by the same AgentID).
func (p *ProjectObject) ClaimSlug(ctx restate.ObjectContext, req SlugClaim) (restate.Void, error) {
	if req.Slug == "" || req.AgentID == "" {
		return restate.Void{}, restate.ToTerminalError(
			fmt.Errorf("slug and agentId are required"),
			restate.WithErrorCode(http.StatusBadRequest),
		)
	}
	key := projectKeySlugPrefix + req.Slug
	holder, err := restate.Get[string](ctx, key)
	if err != nil {
		return restate.Void{}, err
	}
	if holder != "" && holder != req.AgentID {
		return restate.Void{}, restate.ToTerminalError(
			fmt.Errorf("slug %q already claimed by agent %s in project %s", req.Slug, holder, restate.Key(ctx)),
			restate.WithErrorCode(http.StatusConflict),
		)
	}
	restate.Set(ctx, key, req.AgentID)
	return restate.Void{}, nil
}

// ReleaseSlug releases "slug:<slug>" only if it is currently held by req.AgentID.
func (p *ProjectObject) ReleaseSlug(ctx restate.ObjectContext, req SlugClaim) (restate.Void, error) {
	key := projectKeySlugPrefix + req.Slug
	holder, err := restate.Get[string](ctx, key)
	if err != nil {
		return restate.Void{}, err
	}
	if holder == req.AgentID {
		restate.Clear(ctx, key)
	}
	return restate.Void{}, nil
}

// ResolveSlug returns the AgentID owning the given slug in this project.
func (p *ProjectObject) ResolveSlug(ctx restate.ObjectSharedContext, slug string) (string, error) {
	holder, err := restate.Get[string](ctx, projectKeySlugPrefix+slug)
	if err != nil {
		return "", err
	}
	if holder == "" {
		return "", restate.ToTerminalError(
			fmt.Errorf("agent slug %q not found in project %s", slug, restate.Key(ctx)),
			restate.WithErrorCode(http.StatusNotFound),
		)
	}
	return holder, nil
}

// UpsertAgentSummary writes a single granular key "agent:<agent_id>" (Invariant 2).
func (p *ProjectObject) UpsertAgentSummary(ctx restate.ObjectContext, summary ProjectAgentSummary) (restate.Void, error) {
	if summary.AgentID == "" {
		return restate.Void{}, restate.ToTerminalError(
			fmt.Errorf("agentId is required"),
			restate.WithErrorCode(http.StatusBadRequest),
		)
	}
	if summary.Updated.IsZero() {
		summary.Updated = time.Now().UTC()
	}
	restate.Set(ctx, projectKeyAgentPrefix+summary.AgentID, summary)
	return restate.Void{}, nil
}

// RemoveAgentSummary deletes the granular key "agent:<agent_id>".
func (p *ProjectObject) RemoveAgentSummary(ctx restate.ObjectContext, agentID string) (restate.Void, error) {
	restate.Clear(ctx, projectKeyAgentPrefix+agentID)
	return restate.Void{}, nil
}

// ListAgents enumerates all "agent:<id>" keys in deterministic order (Invariant 2 & Invariant 5).
func (p *ProjectObject) ListAgents(ctx restate.ObjectSharedContext, _ restate.Void) ([]ProjectAgentSummary, error) {
	keys, err := restate.Keys(ctx)
	if err != nil {
		return nil, err
	}
	var out []ProjectAgentSummary
	for _, k := range keys {
		if strings.HasPrefix(k, projectKeyAgentPrefix) {
			item, err := restate.Get[ProjectAgentSummary](ctx, k)
			if err != nil {
				return nil, err
			}
			out = append(out, item)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].AgentID < out[j].AgentID
	})
	return out, nil
}

// SetEnvVar sets a granular environment variable under "env:<key>".
func (p *ProjectObject) SetEnvVar(ctx restate.ObjectContext, ev ProjectEnvVar) (ProjectEnvVar, error) {
	if ev.Key == "" {
		return ProjectEnvVar{}, restate.ToTerminalError(
			fmt.Errorf("env var key is required"),
			restate.WithErrorCode(http.StatusBadRequest),
		)
	}
	if ev.UpdatedAt.IsZero() {
		ev.UpdatedAt = time.Now().UTC()
	}
	restate.Set(ctx, projectKeyEnvPrefix+ev.Key, ev)
	return ev, nil
}

// DeleteEnvVar deletes "env:<key>".
func (p *ProjectObject) DeleteEnvVar(ctx restate.ObjectContext, key string) (restate.Void, error) {
	restate.Clear(ctx, projectKeyEnvPrefix+key)
	return restate.Void{}, nil
}

// ListEnvVars returns all environment variables for the project.
func (p *ProjectObject) ListEnvVars(ctx restate.ObjectSharedContext, _ restate.Void) ([]ProjectEnvVar, error) {
	keys, err := restate.Keys(ctx)
	if err != nil {
		return nil, err
	}
	var out []ProjectEnvVar
	for _, k := range keys {
		if strings.HasPrefix(k, projectKeyEnvPrefix) {
			ev, err := restate.Get[ProjectEnvVar](ctx, k)
			if err != nil {
				return nil, err
			}
			out = append(out, ev)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// SetSecret stores a secret under "secret:<name>". Enforces Invariant 4: Ciphertext must have "enc:v2:" prefix.
func (p *ProjectObject) SetSecret(ctx restate.ObjectContext, req SetProjectSecretRequest) (ProjectSecretRecord, error) {
	if req.Name == "" {
		return ProjectSecretRecord{}, restate.ToTerminalError(
			fmt.Errorf("secret name is required"),
			restate.WithErrorCode(http.StatusBadRequest),
		)
	}
	if req.Ciphertext == "" && req.Ref == "" {
		return ProjectSecretRecord{}, restate.ToTerminalError(
			fmt.Errorf("secret %q requires enc:v2: ciphertext or external ref", req.Name),
			restate.WithErrorCode(http.StatusBadRequest),
		)
	}
	if req.Ciphertext != "" && !strings.HasPrefix(req.Ciphertext, "enc:v2:") {
		return ProjectSecretRecord{}, restate.ToTerminalError(
			fmt.Errorf("Invariant 4 violation: secret %q must be envelope-encrypted (enc:v2:) before entering Restate", req.Name),
			restate.WithErrorCode(http.StatusBadRequest),
		)
	}

	key := projectKeySecretPrefix + req.Name
	existing, _ := restate.Get[ProjectSecretRecord](ctx, key)
	rec := ProjectSecretRecord{
		Name:        req.Name,
		Type:        req.Type,
		Target:      req.Target,
		Ciphertext:  req.Ciphertext,
		Ref:         req.Ref,
		Description: req.Description,
		Version:     existing.Version + 1,
		UpdatedBy:   req.UpdatedBy,
		UpdatedAt:   time.Now().UTC(),
	}
	restate.Set(ctx, key, rec)
	return rec, nil
}

// GetSecret retrieves a secret record by name.
func (p *ProjectObject) GetSecret(ctx restate.ObjectSharedContext, name string) (ProjectSecretRecord, error) {
	rec, err := restate.Get[ProjectSecretRecord](ctx, projectKeySecretPrefix+name)
	if err != nil {
		return ProjectSecretRecord{}, err
	}
	if rec.Name == "" {
		return ProjectSecretRecord{}, restate.ToTerminalError(
			fmt.Errorf("secret %q not found", name),
			restate.WithErrorCode(http.StatusNotFound),
		)
	}
	return rec, nil
}

// DeleteSecret removes "secret:<name>".
func (p *ProjectObject) DeleteSecret(ctx restate.ObjectContext, name string) (restate.Void, error) {
	restate.Clear(ctx, projectKeySecretPrefix+name)
	return restate.Void{}, nil
}

// ListSecrets enumerates all "secret:<name>" keys in the project.
func (p *ProjectObject) ListSecrets(ctx restate.ObjectSharedContext, _ restate.Void) ([]ProjectSecretRecord, error) {
	keys, err := restate.Keys(ctx)
	if err != nil {
		return nil, err
	}
	var out []ProjectSecretRecord
	for _, k := range keys {
		if strings.HasPrefix(k, projectKeySecretPrefix) {
			sec, err := restate.Get[ProjectSecretRecord](ctx, k)
			if err != nil {
				return nil, err
			}
			out = append(out, sec)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// LinkBroker associates a Runtime Broker with this project under "broker:<broker_id>".
func (p *ProjectObject) LinkBroker(ctx restate.ObjectContext, link ProjectBrokerLink) (restate.Void, error) {
	if link.LinkedAt.IsZero() {
		link.LinkedAt = time.Now().UTC()
	}
	restate.Set(ctx, projectKeyBrokerPrefix+link.BrokerID, link)
	return restate.Void{}, nil
}

// UnlinkBroker removes "broker:<broker_id>".
func (p *ProjectObject) UnlinkBroker(ctx restate.ObjectContext, brokerID string) (restate.Void, error) {
	restate.Clear(ctx, projectKeyBrokerPrefix+brokerID)
	return restate.Void{}, nil
}

// SetMember adds or updates a project member under "member:<principal_id>".
func (p *ProjectObject) SetMember(ctx restate.ObjectContext, member ProjectMemberBinding) (restate.Void, error) {
	if member.AddedAt.IsZero() {
		member.AddedAt = time.Now().UTC()
	}
	restate.Set(ctx, projectKeyMemberPrefix+member.PrincipalID, member)
	return restate.Void{}, nil
}

// RemoveMember removes a project member and immediately cascades standing evaluation
// to every agent in the project via restate.ObjectSend (replacing MembershipLossCheckStore polling, Scenario 8).
func (p *ProjectObject) RemoveMember(ctx restate.ObjectContext, principalID string) (int, error) {
	restate.Clear(ctx, projectKeyMemberPrefix+principalID)
	projectID := restate.Key(ctx)

	keys, err := restate.Keys(ctx)
	if err != nil {
		return 0, err
	}
	notified := 0
	now := time.Now().UTC()
	for _, k := range keys {
		if strings.HasPrefix(k, projectKeyAgentPrefix) {
			agentID := strings.TrimPrefix(k, projectKeyAgentPrefix)
			restate.ObjectSend(ctx, ServiceAgentObject, agentID, "EvaluateStanding").Send(EvaluateStandingRequest{
				RevokedPrincipalID: principalID,
				ProjectID:          projectID,
				Reason:             "project_membership_removed",
				Now:                now,
			})
			notified++
		}
	}
	return notified, nil
}
