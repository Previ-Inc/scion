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
	"slices"
	"strings"
	"time"

	"github.com/GoogleCloudPlatform/scion/pkg/store"
	"github.com/google/uuid"
)

// --- AgentServiceAccountAssignmentStore, DelegationEdgeStore, DelegationAdoptionStore, AgentHoldStore, MembershipLossCheckStore, AgentCredentialStore, AgentDelegationStore, AgentIdentityKeyStore, AgentReincarnationStore, MutationAuditStore, QuotaStore ---

func (s *RestateStore) ReplaceAgentServiceAccountAssignment(_ context.Context, a *store.AgentServiceAccountAssignment) error {
	if a == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.agentSAAssignments[a.AgentID]
	for i := range list {
		list[i].Active = false
	}
	cp := *a
	if cp.ID == "" {
		cp.ID = uuid.NewString()
	}
	cp.Active = true
	s.agentSAAssignments[a.AgentID] = append(list, &cp)
	return nil
}

func (s *RestateStore) ReplaceAgentServiceAccountAssignmentWith(ctx context.Context, a *store.AgentServiceAccountAssignment, _ store.Deactivation) error {
	return s.ReplaceAgentServiceAccountAssignment(ctx, a)
}

func (s *RestateStore) GetActiveAgentServiceAccountAssignments(_ context.Context, agentID string) ([]store.AgentServiceAccountAssignment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.AgentServiceAccountAssignment
	for _, a := range s.agentSAAssignments[agentID] {
		if a.Active {
			out = append(out, *a)
		}
	}
	return out, nil
}

func (s *RestateStore) DeactivateAgentServiceAccountAssignments(_ context.Context, agentID string, _ store.Deactivation) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, a := range s.agentSAAssignments[agentID] {
		if a.Active {
			a.Active = false
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) DeactivateAgentServiceAccountAssignment(_ context.Context, agentID, rowID string, _ store.Deactivation) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, a := range s.agentSAAssignments[agentID] {
		if a.ID == rowID && a.Active {
			a.Active = false
			return 1, nil
		}
	}
	return 0, nil
}

func (s *RestateStore) ReactivateAgentServiceAccountAssignments(_ context.Context, agentID string, _ store.EdgeDeactivationCause, _ string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, a := range s.agentSAAssignments[agentID] {
		if !a.Active {
			a.Active = true
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) RemarkDeactivatedAgentServiceAccountAssignments(_ context.Context, _, _ string, _ store.EdgeDeactivationCause, _ string, _ store.Deactivation) (int, error) {
	return 0, nil
}

func (s *RestateStore) CreateDelegationEdge(_ context.Context, edge *store.DelegationEdge) error {
	if edge == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if edge.ID == "" {
		edge.ID = uuid.NewString()
	}
	now := time.Now().UTC()
	if edge.CreatedAt.IsZero() {
		edge.CreatedAt = now
	}
	edge.UpdatedAt = now
	cp := *edge
	s.delegationEdges[edge.ID] = &cp
	return nil
}

func (s *RestateStore) GetDelegationEdgesForDelegate(_ context.Context, delegateType, delegateID string) ([]*store.DelegationEdge, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.DelegationEdge
	for _, e := range s.delegationEdges {
		if e.Active && e.DelegateType == delegateType && e.DelegateID == delegateID {
			cp := *e
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *RestateStore) GetDelegationEdgesForDelegator(_ context.Context, delegatorType, delegatorID string) ([]*store.DelegationEdge, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.DelegationEdge
	for _, e := range s.delegationEdges {
		if e.Active && e.DelegatorType == delegatorType && e.DelegatorID == delegatorID {
			cp := *e
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *RestateStore) DeactivateDelegationEdgesForDelegate(_ context.Context, delegateType, delegateID string, d store.Deactivation) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, e := range s.delegationEdges {
		if e.Active && e.DelegateType == delegateType && e.DelegateID == delegateID {
			e.Active = false
			e.Deactivation = d
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) DeactivateDelegationEdgesForDelegator(_ context.Context, delegatorType, delegatorID string, d store.Deactivation) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, e := range s.delegationEdges {
		if e.Active && e.DelegatorType == delegatorType && e.DelegatorID == delegatorID {
			e.Active = false
			e.Deactivation = d
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) ReactivateDelegationEdgesForDelegate(_ context.Context, delegateType, delegateID string, cause store.EdgeDeactivationCause, opID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, e := range s.delegationEdges {
		if !e.Active && e.DelegateType == delegateType && e.DelegateID == delegateID && e.Cause == cause && e.OpID == opID {
			e.Active = true
			e.Deactivation = store.Deactivation{}
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) GetDeactivatedDelegationEdgesForDelegate(_ context.Context, delegateType, delegateID string, cause store.EdgeDeactivationCause, opID string) ([]*store.DelegationEdge, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.DelegationEdge
	for _, e := range s.delegationEdges {
		if !e.Active && e.DelegateType == delegateType && e.DelegateID == delegateID && e.Cause == cause && e.OpID == opID {
			cp := *e
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *RestateStore) GetDelegationEdge(_ context.Context, edgeID string) (*store.DelegationEdge, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.delegationEdges[edgeID]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *e
	return &cp, nil
}

func (s *RestateStore) ListAllDelegationEdgesForDelegate(_ context.Context, delegateType, delegateID string) ([]*store.DelegationEdge, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.DelegationEdge
	for _, e := range s.delegationEdges {
		if e.DelegateType == delegateType && e.DelegateID == delegateID {
			cp := *e
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *RestateStore) DeactivateDelegationEdgeGuarded(_ context.Context, edgeID string, _ store.DelegationEdgeDeactivateGuard, cause store.EdgeDeactivationCause, opID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.delegationEdges[edgeID]
	if !ok {
		return false, store.ErrNotFound
	}
	if !e.Active {
		return false, nil
	}
	e.Active = false
	e.Cause = cause
	e.OpID = opID
	return true, nil
}

func (s *RestateStore) ReactivateDelegationEdge(_ context.Context, edgeID string, expectCause store.EdgeDeactivationCause) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.delegationEdges[edgeID]
	if !ok {
		return store.ErrNotFound
	}
	if e.Active || e.Cause != expectCause {
		return store.ErrRevisionConflict
	}
	e.Active = true
	e.Deactivation = store.Deactivation{}
	return nil
}

func (s *RestateStore) ListDelegationDescendants(_ context.Context, _ store.DescendantQuery) (store.DescendantResult, error) {
	return store.DescendantResult{}, nil
}

func (s *RestateStore) CreateDelegationAdoption(_ context.Context, rec *store.DelegationAdoption) error {
	if rec == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec.ID == "" {
		rec.ID = uuid.NewString()
	}
	cp := *rec
	s.delegationAdoptions[rec.ID] = &cp
	return nil
}

func (s *RestateStore) UpdateDelegationAdoption(_ context.Context, rec *store.DelegationAdoption) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.delegationAdoptions[rec.ID]; !ok {
		return store.ErrNotFound
	}
	cp := *rec
	s.delegationAdoptions[rec.ID] = &cp
	return nil
}

func (s *RestateStore) GetDelegationAdoption(_ context.Context, id string) (*store.DelegationAdoption, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.delegationAdoptions[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *rec
	return &cp, nil
}

func (s *RestateStore) ListDelegationAdoptions(_ context.Context, _ store.DelegationAdoptionFilter) ([]*store.DelegationAdoption, int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.DelegationAdoption
	for _, r := range s.delegationAdoptions {
		cp := *r
		out = append(out, &cp)
	}
	return out, len(out), nil
}

func (s *RestateStore) CreateAgentHolds(_ context.Context, holds []*store.AgentHold) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	inserted := 0
	for _, h := range holds {
		h.ID = uuid.NewString()
		h.CreatedAt = now
		cp := *h
		s.agentHolds[h.AgentID] = append(s.agentHolds[h.AgentID], &cp)
		inserted++
	}
	return inserted, nil
}

func (s *RestateStore) HasActiveAgentHold(_ context.Context, agentID string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, h := range s.agentHolds[agentID] {
		if h.ClearedAt == nil {
			return true, nil
		}
	}
	return false, nil
}

func (s *RestateStore) ListActiveAgentHolds(_ context.Context, agentID string) ([]*store.AgentHold, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.AgentHold
	for _, h := range s.agentHolds[agentID] {
		if h.ClearedAt == nil {
			cp := *h
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *RestateStore) ListActiveAgentHoldsByProject(_ context.Context, projectID string, _ store.ListOptions) (*store.ListResult[store.AgentHold], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var items []store.AgentHold
	for _, list := range s.agentHolds {
		for _, h := range list {
			if h.ProjectID == projectID && h.ClearedAt == nil {
				items = append(items, *h)
			}
		}
	}
	return &store.ListResult[store.AgentHold]{Items: items, TotalCount: len(items)}, nil
}

func (s *RestateStore) ClearAgentHolds(_ context.Context, agentID string, by store.ClearActor, reason string) (int, error) {
	if by.Kind != store.ClearActorUser {
		return 0, store.ErrInvalidActor
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	cleared := 0
	for _, h := range s.agentHolds[agentID] {
		if h.ClearedAt == nil {
			h.ClearedAt = &now
			h.ClearedByKind = by.Kind
			h.ClearedByID = by.ID
			h.ClearReason = reason
			cleared++
		}
	}
	return cleared, nil
}

func (s *RestateStore) EnqueueMembershipLossCheck(ctx context.Context, check *store.MembershipLossCheck) error {
	if check == nil {
		return store.ErrInvalidInput
	}
	if check.ID == "" {
		check.ID = uuid.NewString()
	}
	if check.CreatedAt.IsZero() {
		check.CreatedAt = time.Now().UTC()
	}
	// Per Plan Argon §4: MembershipLossCheckStore polling is replaced by direct async ObjectSend
	// to AgentObject when membership is removed.
	if s.ingressActive() && check.ProjectID != "" {
		_, _ = SendObject[string](ctx, s.host, ServiceProjectObject, check.ProjectID, "RemoveMember", check.UserID)
	}
	return nil
}

func (s *RestateStore) ClaimMembershipLossChecks(_ context.Context, _ int, _ time.Duration) ([]*store.MembershipLossCheck, error) {
	return nil, nil
}

func (s *RestateStore) CompleteMembershipLossCheck(_ context.Context, _ string, _ int) error {
	return nil
}

func (s *RestateStore) FailMembershipLossCheck(_ context.Context, _ string, _ int, _ string) error {
	return nil
}

func (s *RestateStore) CreateAgentCredential(_ context.Context, cred *store.AgentCredential) error {
	if cred == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if cred.ID == "" {
		cred.ID = uuid.NewString()
	}
	if cred.IssuedAt.IsZero() {
		cred.IssuedAt = time.Now().UTC()
	}
	cp := *cred
	s.agentCreds[cred.ID] = &cp
	return nil
}

func (s *RestateStore) GetAgentCredentialByJTIHash(_ context.Context, jtiHash string) (*store.AgentCredential, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, c := range s.agentCreds {
		if c.TokenJTIHash == jtiHash {
			cp := *c
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) GetAgentCredentialByID(_ context.Context, id string) (*store.AgentCredential, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.agentCreds[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *c
	return &cp, nil
}

func (s *RestateStore) RevokeAgentCredential(_ context.Context, id, revokedBy, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.agentCreds[id]
	if !ok {
		return store.ErrNotFound
	}
	now := time.Now().UTC()
	c.RevokedAt = &now
	c.RevokedBy = &revokedBy
	c.RevokeReason = &reason
	return nil
}

func (s *RestateStore) RevokeAgentCredentialsByAgent(_ context.Context, agentID, revokedBy, reason string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	n := 0
	for _, c := range s.agentCreds {
		if c.AgentID == agentID && c.RevokedAt == nil {
			c.RevokedAt = &now
			c.RevokedBy = &revokedBy
			c.RevokeReason = &reason
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) UpdateAgentCredentialLastSeen(_ context.Context, id string, lastSeen time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.agentCreds[id]
	if !ok {
		return store.ErrNotFound
	}
	c.LastSeenAt = &lastSeen
	return nil
}

func (s *RestateStore) PurgeExpiredAgentCredentials(_ context.Context, cutoff time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, c := range s.agentCreds {
		if c.ExpiresAt.Before(cutoff) {
			delete(s.agentCreds, id)
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) HasAgentCredentialForRun(_ context.Context, agentID, runID string) (bool, error) {
	if runID == "" {
		return false, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, c := range s.agentCreds {
		if c.AgentID == agentID && c.RunID == runID {
			return true, nil
		}
	}
	return false, nil
}

func (s *RestateStore) DeleteAgentCredentialsByProject(_ context.Context, projectID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, c := range s.agentCreds {
		if c.ProjectID == projectID {
			delete(s.agentCreds, id)
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) CreateAgentDelegationGrant(_ context.Context, grant *store.AgentDelegationGrant) error {
	if err := grant.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if grant.ID == "" {
		grant.ID = uuid.NewString()
	}
	if grant.Created.IsZero() {
		grant.Created = time.Now().UTC()
	}
	cp := *grant
	s.agentDelegationGrants[grant.ID] = &cp
	return nil
}

func (s *RestateStore) GetAgentDelegationGrant(_ context.Context, id string) (*store.AgentDelegationGrant, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	g, ok := s.agentDelegationGrants[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *g
	return &cp, nil
}

func (s *RestateStore) MarkAgentDelegationGrantExchanged(_ context.Context, id string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.agentDelegationGrants[id]
	if !ok {
		return store.ErrNotFound
	}
	g.LastExchangedAt = &at
	return nil
}

func (s *RestateStore) RevokeAgentDelegationGrant(_ context.Context, id, revokedBy, reason, auditID string, at time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.agentDelegationGrants[id]
	if !ok {
		return false, store.ErrNotFound
	}
	if g.RevokedAt != nil {
		return false, nil
	}
	g.RevokedAt = &at
	g.RevokedBy = revokedBy
	g.RevokeReason = reason
	g.RevocationAuditID = auditID
	for _, c := range s.agentDelegatedCreds {
		if c.GrantID == id && c.RevokedAt == nil {
			c.RevokedAt = &at
			c.RevokeReason = reason
		}
	}
	return true, nil
}

func (s *RestateStore) CreateAgentDelegatedCredential(_ context.Context, cred *store.AgentDelegatedCredential) error {
	if cred == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if cred.ID == "" {
		cred.ID = uuid.NewString()
	}
	cp := *cred
	s.agentDelegatedCreds[cred.ID] = &cp
	return nil
}

func (s *RestateStore) GetAgentDelegatedCredentialByKeyHash(_ context.Context, keyHash string) (*store.AgentDelegatedCredential, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, c := range s.agentDelegatedCreds {
		if c.KeyHash == keyHash {
			cp := *c
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) UpdateAgentDelegatedCredentialLastSeen(_ context.Context, id string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.agentDelegatedCreds[id]; ok {
		c.LastSeenAt = &at
	}
	return nil
}

func (s *RestateStore) ListAgentDelegationGrants(_ context.Context, filter store.AgentDelegationGrantFilter) ([]*store.AgentDelegationGrant, error) {
	if filter.AgentID == "" && filter.IssuerUserID == "" {
		return nil, store.ErrInvalidInput
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.AgentDelegationGrant
	for _, g := range s.agentDelegationGrants {
		if (filter.AgentID == "" || g.AgentID == filter.AgentID) && (filter.IssuerUserID == "" || g.IssuerUserID == filter.IssuerUserID) {
			cp := *g
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *RestateStore) CountActiveAgentDelegationGrants(_ context.Context, filter store.AgentDelegationGrantFilter, now time.Time) (int, error) {
	list, err := s.ListAgentDelegationGrants(context.Background(), filter)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, g := range list {
		if g.RevokedAt == nil && g.ExpiresAt.After(now) {
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) LockAgentDelegationIssuer(_ context.Context, userID string) error {
	return s.LockUserRow(context.Background(), userID, true)
}

func (s *RestateStore) LockAgentDelegationGrant(_ context.Context, id string) (*store.AgentDelegationGrant, error) {
	return s.GetAgentDelegationGrant(context.Background(), id)
}

func (s *RestateStore) GetAgentDelegatedCredential(_ context.Context, id string) (*store.AgentDelegatedCredential, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.agentDelegatedCreds[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *c
	return &cp, nil
}

func (s *RestateStore) ListActiveAgentDelegatedCredentials(_ context.Context, grantIDs []string, now time.Time) ([]*store.AgentDelegatedCredential, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.AgentDelegatedCredential
	for _, c := range s.agentDelegatedCreds {
		if slices.Contains(grantIDs, c.GrantID) && c.RevokedAt == nil && c.ExpiresAt.After(now) {
			cp := *c
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *RestateStore) RevokeAgentDelegatedCredential(_ context.Context, id, reason string, at time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.agentDelegatedCreds[id]
	if !ok {
		return false, store.ErrNotFound
	}
	if c.RevokedAt != nil {
		return false, nil
	}
	c.RevokedAt = &at
	c.RevokeReason = reason
	return true, nil
}

func (s *RestateStore) PurgeAgentDelegatedCredentials(_ context.Context, expiredBefore time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, c := range s.agentDelegatedCreds {
		if c.ExpiresAt.Before(expiredBefore) {
			delete(s.agentDelegatedCreds, id)
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) ReplaceAgentIdentityKeys(ctx context.Context, agentID, projectID string, keys []string) error {
	s.mu.Lock()
	list := s.agentKeys[projectID]
	for _, existing := range list {
		if existing.AgentID != agentID && slices.Contains(keys, existing.Key) {
			s.mu.Unlock()
			return store.ErrIdentityKeyConflict
		}
	}
	var filtered []*store.AgentIdentityKey
	for _, existing := range list {
		if existing.AgentID != agentID {
			filtered = append(filtered, existing)
		}
	}
	for _, k := range keys {
		filtered = append(filtered, &store.AgentIdentityKey{
			ID:        uuid.NewString(),
			ProjectID: projectID,
			Key:       k,
			AgentID:   agentID,
		})
	}
	s.agentKeys[projectID] = filtered
	s.mu.Unlock()

	if s.ingressActive() && len(keys) > 0 {
		_, _ = InvokeObject[SlugClaim, any](ctx, s.host, ServiceProjectObject, projectID, "ClaimSlug", SlugClaim{
			Slug:    keys[0],
			AgentID: agentID,
		})
	}
	return nil
}

func (s *RestateStore) DeleteAgentIdentityKeys(_ context.Context, agentID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for pid, list := range s.agentKeys {
		var filtered []*store.AgentIdentityKey
		for _, k := range list {
			if k.AgentID != agentID {
				filtered = append(filtered, k)
			}
		}
		s.agentKeys[pid] = filtered
	}
	return nil
}

func (s *RestateStore) ListAgentIdentityKeys(_ context.Context, projectID string) ([]*store.AgentIdentityKey, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.AgentIdentityKey
	for _, k := range s.agentKeys[projectID] {
		cp := *k
		out = append(out, &cp)
	}
	return out, nil
}

func (s *RestateStore) CreateAgentReincarnation(_ context.Context, r *store.AgentReincarnation) error {
	if r == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.ID == "" {
		r.ID = uuid.NewString()
	}
	if _, exists := s.agentReincarnations[r.ID]; exists {
		return store.ErrAlreadyExists
	}
	cp := *r
	s.agentReincarnations[r.ID] = &cp
	return nil
}

func (s *RestateStore) GetAgentReincarnation(_ context.Context, id string) (*store.AgentReincarnation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.agentReincarnations[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *r
	return &cp, nil
}

func (s *RestateStore) UpdateAgentReincarnation(_ context.Context, r *store.AgentReincarnation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.agentReincarnations[r.ID]; !ok {
		return store.ErrNotFound
	}
	cp := *r
	cp.UpdatedAt = time.Now().UTC()
	s.agentReincarnations[r.ID] = &cp
	return nil
}

func (s *RestateStore) ListAgentReincarnations(_ context.Context, agentID string) ([]*store.AgentReincarnation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.AgentReincarnation
	for _, r := range s.agentReincarnations {
		if r.AgentID == agentID {
			cp := *r
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *RestateStore) GetPendingAgentReincarnation(_ context.Context, agentID string) (*store.AgentReincarnation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, r := range s.agentReincarnations {
		if r.AgentID == agentID && slices.Contains(store.AgentReincarnationNonTerminalStates, r.State) {
			cp := *r
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) DeleteAgentReincarnationsForAgent(_ context.Context, agentID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, r := range s.agentReincarnations {
		if r.AgentID == agentID {
			delete(s.agentReincarnations, id)
		}
	}
	return nil
}

func (s *RestateStore) ListStaleNonTerminalAgentReincarnations(_ context.Context, _ time.Time) ([]*store.AgentReincarnation, error) {
	return nil, nil
}

func (s *RestateStore) TryAdvanceAgentReincarnation(_ context.Context, r *store.AgentReincarnation, expectState string, _ time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.agentReincarnations[r.ID]
	if !ok || cur.State != expectState {
		return false, nil
	}
	cp := *r
	cp.UpdatedAt = time.Now().UTC()
	s.agentReincarnations[r.ID] = &cp
	return true, nil
}

func (s *RestateStore) ListAgentReincarnationsPage(_ context.Context, afterID string, limit int) ([]*store.AgentReincarnation, error) {
	return nil, nil
}

func (s *RestateStore) UpdateAgentReincarnationSnapshots(_ context.Context, r *store.AgentReincarnation, expectState string) (bool, error) {
	return s.TryAdvanceAgentReincarnation(context.Background(), r, expectState, time.Time{})
}

func (s *RestateStore) SetAgentReincarnationSourceCleanup(_ context.Context, id, outcome string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.agentReincarnations[id]
	if !ok {
		return store.ErrNotFound
	}
	r.SourceCleanup = outcome
	return nil
}

func (s *RestateStore) CreateMutationAudit(_ context.Context, record *store.MutationAuditRecord) error {
	if record == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if record.ID == "" {
		record.ID = uuid.NewString()
	}
	if record.Timestamp.IsZero() {
		record.Timestamp = time.Now().UTC()
	}
	cp := *record
	s.mutationAudits = append(s.mutationAudits, &cp)
	return nil
}

func (s *RestateStore) ListMutationAudits(_ context.Context, filter store.MutationAuditFilter) ([]*store.MutationAuditRecord, int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.MutationAuditRecord
	for _, r := range s.mutationAudits {
		if filter.MutationType != "" && r.MutationType != filter.MutationType {
			continue
		}
		if filter.TargetType != "" && r.TargetType != filter.TargetType {
			continue
		}
		if filter.TargetID != "" && r.TargetID != filter.TargetID {
			continue
		}
		cp := *r
		out = append(out, &cp)
	}
	total := len(out)
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, total, nil
}

func (s *RestateStore) DeleteMutationAuditsBefore(_ context.Context, before time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var kept []*store.MutationAuditRecord
	removed := 0
	for _, r := range s.mutationAudits {
		if r.Timestamp.Before(before) {
			removed++
		} else {
			kept = append(kept, r)
		}
	}
	s.mutationAudits = kept
	return removed, nil
}

// --- QuotaStore ---

func (s *RestateStore) CreateLimitDefinition(_ context.Context, def *store.LimitDefinition) (*store.LimitDefinition, error) {
	if def == nil {
		return nil, store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cur := range s.limitDefinitions {
		if strings.EqualFold(cur.Name, def.Name) {
			return nil, store.ErrAlreadyExists
		}
	}
	cp := *def
	if cp.ID == "" {
		cp.ID = uuid.NewString()
	}
	now := time.Now().UTC()
	cp.CreatedAt = now
	cp.UpdatedAt = now
	s.limitDefinitions[cp.ID] = &cp
	out := cp
	return &out, nil
}

func (s *RestateStore) GetLimitDefinition(_ context.Context, id string) (*store.LimitDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.limitDefinitions[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *d
	return &cp, nil
}

func (s *RestateStore) GetLimitDefinitionByName(_ context.Context, name string) (*store.LimitDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, d := range s.limitDefinitions {
		if strings.EqualFold(d.Name, name) {
			cp := *d
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) ListLimitDefinitions(_ context.Context) ([]*store.LimitDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.LimitDefinition
	for _, d := range s.limitDefinitions {
		cp := *d
		out = append(out, &cp)
	}
	return out, nil
}

func (s *RestateStore) UpdateLimitDefinition(_ context.Context, def *store.LimitDefinition) (*store.LimitDefinition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.limitDefinitions[def.ID]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *def
	cp.CreatedAt = cur.CreatedAt
	cp.UpdatedAt = time.Now().UTC()
	s.limitDefinitions[def.ID] = &cp
	out := cp
	return &out, nil
}

func (s *RestateStore) DeleteLimitDefinition(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.limitDefinitions[id]; !ok {
		return store.ErrNotFound
	}
	delete(s.limitDefinitions, id)
	return nil
}

func (s *RestateStore) CreateEntitlementBinding(_ context.Context, b *store.EntitlementBinding) (*store.EntitlementBinding, error) {
	if b == nil {
		return nil, store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *b
	if cp.ID == "" {
		cp.ID = uuid.NewString()
	}
	now := time.Now().UTC()
	cp.CreatedAt = now
	cp.UpdatedAt = now
	s.entitlementBindings[cp.ID] = &cp
	out := cp
	return &out, nil
}

func (s *RestateStore) GetEntitlementBinding(_ context.Context, id string) (*store.EntitlementBinding, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.entitlementBindings[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *b
	return &cp, nil
}

func (s *RestateStore) ListEntitlementBindings(_ context.Context, limitDefinitionID string) ([]*store.EntitlementBinding, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.EntitlementBinding
	for _, b := range s.entitlementBindings {
		if limitDefinitionID == "" || b.LimitDefinitionID == limitDefinitionID {
			cp := *b
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *RestateStore) ListEntitlementBindingsForSubject(_ context.Context, subjectType, subjectID string) ([]*store.EntitlementBinding, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.EntitlementBinding
	for _, b := range s.entitlementBindings {
		if b.SubjectType == subjectType && b.SubjectID == subjectID {
			cp := *b
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *RestateStore) UpdateEntitlementBinding(_ context.Context, b *store.EntitlementBinding) (*store.EntitlementBinding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.entitlementBindings[b.ID]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *b
	cp.CreatedAt = cur.CreatedAt
	cp.UpdatedAt = time.Now().UTC()
	s.entitlementBindings[b.ID] = &cp
	out := cp
	return &out, nil
}

func (s *RestateStore) DeleteEntitlementBinding(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.entitlementBindings[id]; !ok {
		return store.ErrNotFound
	}
	delete(s.entitlementBindings, id)
	return nil
}

func (s *RestateStore) CreateUsageReservation(ctx context.Context, res *store.UsageReservation) (*store.UsageReservation, error) {
	if res == nil {
		return nil, store.ErrInvalidInput
	}
	s.mu.Lock()
	for _, cur := range s.usageReservations {
		if cur.ReleasedAt == nil && cur.LimitDefinitionID == res.LimitDefinitionID && cur.ResourceID == res.ResourceID {
			s.mu.Unlock()
			return nil, store.ErrActiveReservationExists
		}
	}
	cp := *res
	if cp.ID == "" {
		cp.ID = uuid.NewString()
	}
	if cp.CreatedAt.IsZero() {
		cp.CreatedAt = time.Now().UTC()
	}
	if cp.Reserved <= 0 {
		cp.Reserved = 1
	}
	s.usageReservations[cp.ID] = &cp
	s.mu.Unlock()

	if s.ingressActive() && cp.ScopeID != "" {
		_, _ = InvokeObject[QuotaReserveRequest, QuotaReservation](ctx, s.host, ServiceQuotaObject, cp.ScopeType+":"+cp.ScopeID, "Reserve", QuotaReserveRequest{
			OpID:    cp.ID,
			AgentID: cp.ResourceID,
			Amount:  int(cp.Reserved),
			Now:     cp.CreatedAt,
		})
	}
	out := cp
	return &out, nil
}

func (s *RestateStore) CountActiveReservations(_ context.Context, limitDefinitionID, subjectID, scopeType, scopeID string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var n int64
	for _, r := range s.usageReservations {
		if r.ReleasedAt == nil && r.LimitDefinitionID == limitDefinitionID &&
			(subjectID == "" || r.SubjectID == subjectID) &&
			(scopeType == "" || r.ScopeType == scopeType) &&
			(scopeID == "" || r.ScopeID == scopeID) {
			n += r.Reserved
		}
	}
	return n, nil
}

func (s *RestateStore) ReleaseReservation(ctx context.Context, limitDefinitionID, resourceID string) error {
	s.mu.Lock()
	now := time.Now().UTC()
	var released []*store.UsageReservation
	for _, r := range s.usageReservations {
		if r.ReleasedAt == nil && r.LimitDefinitionID == limitDefinitionID && r.ResourceID == resourceID {
			r.ReleasedAt = &now
			cp := *r
			released = append(released, &cp)
		}
	}
	s.mu.Unlock()

	if s.ingressActive() {
		for _, r := range released {
			_, _ = InvokeObject[QuotaReleaseRequest, any](ctx, s.host, ServiceQuotaObject, r.ScopeType+":"+r.ScopeID, "Release", QuotaReleaseRequest{
				OpID:    r.ID,
				AgentID: r.ResourceID,
			})
		}
	}
	return nil
}

func (s *RestateStore) ReleaseReservationByID(_ context.Context, reservationID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.usageReservations[reservationID]
	if !ok || r.ReleasedAt != nil {
		return store.ErrNotFound
	}
	now := time.Now().UTC()
	r.ReleasedAt = &now
	return nil
}

func (s *RestateStore) ReleaseReservationIfAgent(ctx context.Context, limitDefinitionID, agentID string, _ store.AgentReleaseCond) (bool, error) {
	err := s.ReleaseReservation(ctx, limitDefinitionID, agentID)
	return err == nil, err
}

func (s *RestateStore) ReleaseReservationIfScopeBrokerAbsent(_ context.Context, reservationID, scopeID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.brokers[scopeID]; exists {
		return false, nil
	}
	r, ok := s.usageReservations[reservationID]
	if !ok || r.ReleasedAt != nil {
		return false, nil
	}
	now := time.Now().UTC()
	r.ReleasedAt = &now
	return true, nil
}

func (s *RestateStore) ReleaseReservationsByResource(_ context.Context, resourceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	for _, r := range s.usageReservations {
		if r.ReleasedAt == nil && r.ResourceID == resourceID {
			r.ReleasedAt = &now
		}
	}
	return nil
}

func (s *RestateStore) ListActiveReservations(_ context.Context, limitDefinitionID, scopeType, scopeID string) ([]*store.UsageReservation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.UsageReservation
	for _, r := range s.usageReservations {
		if r.ReleasedAt == nil && r.LimitDefinitionID == limitDefinitionID && r.ScopeType == scopeType && r.ScopeID == scopeID {
			cp := *r
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *RestateStore) ListActiveReservationsByScopeType(_ context.Context, limitDefinitionID, scopeType string) ([]*store.UsageReservation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.UsageReservation
	for _, r := range s.usageReservations {
		if r.ReleasedAt == nil && r.LimitDefinitionID == limitDefinitionID && r.ScopeType == scopeType {
			cp := *r
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *RestateStore) CountActiveReservationsByScope(_ context.Context, limitDefinitionID, scopeType string) (map[string]int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]int64)
	for _, r := range s.usageReservations {
		if r.ReleasedAt == nil && r.LimitDefinitionID == limitDefinitionID && r.ScopeType == scopeType {
			out[r.ScopeID] += r.Reserved
		}
	}
	return out, nil
}

func (s *RestateStore) HasActiveReservation(_ context.Context, limitDefinitionID, resourceID string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, r := range s.usageReservations {
		if r.ReleasedAt == nil && r.LimitDefinitionID == limitDefinitionID && r.ResourceID == resourceID {
			return true, nil
		}
	}
	return false, nil
}

func (s *RestateStore) GetActiveReservation(_ context.Context, limitDefinitionID, resourceID string) (*store.UsageReservation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, r := range s.usageReservations {
		if r.ReleasedAt == nil && r.LimitDefinitionID == limitDefinitionID && r.ResourceID == resourceID {
			cp := *r
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) TransferBrokerReservation(_ context.Context, req store.BrokerReservationTransfer) (store.BrokerReservationTransferResult, error) {
	if req.CreateIfAbsent && req.FromReservationID != "" {
		return store.BrokerReservationTransferResult{}, store.ErrInvalidTransfer
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[req.AgentID]
	if !ok || !a.DeletedAt.IsZero() {
		return store.BrokerReservationTransferResult{}, store.ErrNotFound
	}
	if req.RequireAgentOnTo && a.RuntimeBrokerID != req.ToBrokerID {
		return store.BrokerReservationTransferResult{Outcome: store.TransferAgentElsewhere}, nil
	}
	if req.Cond != nil && !req.Cond.Holds(a) {
		return store.BrokerReservationTransferResult{Outcome: store.TransferStale}, nil
	}
	var active *store.UsageReservation
	for _, r := range s.usageReservations {
		if r.ReleasedAt == nil && r.LimitDefinitionID == req.LimitDefinitionID && r.ResourceID == req.AgentID {
			active = r
			break
		}
	}
	if req.FromReservationID != "" && (active == nil || active.ID != req.FromReservationID) {
		return store.BrokerReservationTransferResult{Outcome: store.TransferStale}, nil
	}
	if active != nil && active.ScopeType == store.QuotaScopeBroker && active.ScopeID == req.ToBrokerID {
		return store.BrokerReservationTransferResult{Outcome: store.TransferAlreadyOn, ReservationID: active.ID}, nil
	}
	if active == nil && !req.CreateIfAbsent {
		return store.BrokerReservationTransferResult{Outcome: store.TransferAbsent}, nil
	}
	if req.Cap > 0 {
		var count int64
		for _, r := range s.usageReservations {
			if r.ReleasedAt == nil && r.LimitDefinitionID == req.LimitDefinitionID && r.ScopeType == store.QuotaScopeBroker && r.ScopeID == req.ToBrokerID {
				count += r.Reserved
			}
		}
		if count >= req.Cap {
			return store.BrokerReservationTransferResult{}, store.ErrQuotaExceeded
		}
	}
	now := time.Now().UTC()
	var res store.BrokerReservationTransferResult
	if active != nil {
		active.ReleasedAt = &now
		res.Outcome = store.TransferMoved
		res.FromScopeType = active.ScopeType
		res.FromScopeID = active.ScopeID
		res.ReleasedID = active.ID
	} else {
		res.Outcome = store.TransferCreated
	}
	newRes := &store.UsageReservation{
		ID:                uuid.NewString(),
		LimitDefinitionID: req.LimitDefinitionID,
		SubjectID:         req.ToBrokerID,
		ScopeType:         store.QuotaScopeBroker,
		ScopeID:           req.ToBrokerID,
		ResourceID:        req.AgentID,
		Reserved:          1,
		CreatedAt:         now,
	}
	s.usageReservations[newRes.ID] = newRes
	res.ReservationID = newRes.ID
	return res, nil
}
