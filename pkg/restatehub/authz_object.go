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
	"sort"
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"
)

// Granular state key prefixes inside AuthzObject[scope_id].
const (
	authzKeyGroupPrefix  = "group:"
	authzKeyRolePrefix   = "role:"
	authzKeyBindPrefix   = "bind:"
	authzKeyACPrefix     = "ac:"
	authzKeyEdgePrefix   = "edge:"
	authzKeyGrantPrefix  = "grant:"
	authzKeyAuditPrefix  = "audit:"
)

// RoleBindingRecord stores a role binding under "bind:<id>".
type RoleBindingRecord struct {
	ID               string    `json:"id"`
	ScopeID          string    `json:"scopeId"`
	RoleDefinitionID string    `json:"roleDefinitionId"`
	PrincipalID      string    `json:"principalId"`
	PrincipalType    string    `json:"principalType"` // "user" | "group" | "agent"
	CreatedBy        string    `json:"createdBy,omitempty"`
	CreatedAt        time.Time `json:"createdAt"`
}

// DelegationEdgeRecord stores a delegation edge under "edge:<id>".
type DelegationEdgeRecord struct {
	ID          string    `json:"id"`
	ProjectID   string    `json:"projectId"`
	DelegatorID string    `json:"delegatorId"`
	DelegateID  string    `json:"delegateId"`
	Active      bool      `json:"active"`
	CreatedAt   time.Time `json:"createdAt"`
}

// AuthzObject is the Restate Virtual Object managing roles, role bindings, access constraints,
// and delegation edges for a scope ("system" or "project:<id>").
type AuthzObject struct{}

// NewAuthzObject constructs an AuthzObject.
func NewAuthzObject() *AuthzObject {
	return &AuthzObject{}
}

// ServiceName returns the canonical Restate Virtual Object name.
func (a *AuthzObject) ServiceName() string {
	return ServiceAuthzObject
}

// CreateRoleBinding creates or updates a role binding under "bind:<id>".
func (a *AuthzObject) CreateRoleBinding(ctx restate.ObjectContext, binding RoleBindingRecord) (RoleBindingRecord, error) {
	if binding.ID == "" {
		binding.ID = restate.UUID(ctx).String()
	}
	binding.ScopeID = restate.Key(ctx)
	if binding.CreatedAt.IsZero() {
		binding.CreatedAt = time.Now().UTC()
	}
	restate.Set(ctx, authzKeyBindPrefix+binding.ID, binding)
	return binding, nil
}

// DeleteRoleBinding removes a role binding and cascades membership loss notifications via restate.ObjectSend (Scenario 8).
func (a *AuthzObject) DeleteRoleBinding(ctx restate.ObjectContext, bindingID string) (restate.Void, error) {
	key := authzKeyBindPrefix + bindingID
	binding, err := restate.Get[RoleBindingRecord](ctx, key)
	if err != nil {
		return restate.Void{}, err
	}
	restate.Clear(ctx, key)

	if binding.PrincipalID != "" && strings.HasPrefix(binding.ScopeID, "project:") {
		projectID := strings.TrimPrefix(binding.ScopeID, "project:")
		restate.ObjectSend(ctx, ServiceProjectObject, projectID, "RemoveMember").Send(binding.PrincipalID)
	}
	return restate.Void{}, nil
}

// ListRoleBindings enumerates all role bindings in this scope.
func (a *AuthzObject) ListRoleBindings(ctx restate.ObjectSharedContext, _ restate.Void) ([]RoleBindingRecord, error) {
	keys, err := restate.Keys(ctx)
	if err != nil {
		return nil, err
	}
	var out []RoleBindingRecord
	for _, k := range keys {
		if strings.HasPrefix(k, authzKeyBindPrefix) {
			b, err := restate.Get[RoleBindingRecord](ctx, k)
			if err != nil {
				return nil, err
			}
			out = append(out, b)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
