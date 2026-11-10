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
	"sort"
	"strings"
	"time"

	"github.com/GoogleCloudPlatform/scion/pkg/api"
	"github.com/GoogleCloudPlatform/scion/pkg/store"
	"github.com/google/uuid"
)

// --- UserStore, UserDeletionStore, UserAccessTokenStore, UserTerminalWorkspaceStore, ExternalIdentityStore, AllowListStore, InviteCodeStore, TestIdentityDelegationStore ---

func (s *RestateStore) CreateUser(_ context.Context, user *store.User) error {
	if user == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if user.ID == "" {
		user.ID = uuid.NewString()
	}
	for _, u := range s.users {
		if u.ID == user.ID || (user.Email != "" && strings.EqualFold(u.Email, user.Email)) {
			return store.ErrAlreadyExists
		}
	}
	now := time.Now().UTC()
	if user.Created.IsZero() {
		user.Created = now
	}
	if user.Status == "" {
		user.Status = store.UserStatusActive
	}
	cp := *user
	s.users[user.ID] = &cp
	return nil
}

func (s *RestateStore) GetUser(_ context.Context, id string) (*store.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.users[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *u
	return &cp, nil
}

func (s *RestateStore) GetUsersByIDs(_ context.Context, ids []string) (map[string]*store.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]*store.User, len(ids))
	for _, id := range ids {
		if u, ok := s.users[id]; ok {
			cp := *u
			out[id] = &cp
		}
	}
	return out, nil
}

func (s *RestateStore) GetUserByEmail(_ context.Context, email string) (*store.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, u := range s.users {
		if u.Kind != store.UserKindFederated && strings.EqualFold(u.Email, email) {
			cp := *u
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) UpdateUser(_ context.Context, user *store.User) error {
	if user == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.users[user.ID]
	if !ok {
		return store.ErrNotFound
	}
	if cur.Status == store.UserStatusDeleting || user.Status == store.UserStatusDeleting {
		return store.ErrUserDeleting
	}
	cp := *user
	s.users[user.ID] = &cp
	return nil
}

func (s *RestateStore) UpdateUserLastLogin(_ context.Context, id string, t time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[id]
	if !ok {
		return store.ErrNotFound
	}
	u.LastLogin = t
	return nil
}

func (s *RestateStore) UpdateUserLastSeen(_ context.Context, id string, t time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[id]
	if !ok {
		return store.ErrNotFound
	}
	u.LastSeen = t
	return nil
}

func (s *RestateStore) DeleteUser(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.users[id]; !ok {
		return store.ErrNotFound
	}
	delete(s.users, id)
	return nil
}

func (s *RestateStore) MarkUserDeleting(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[id]
	if !ok {
		return store.ErrNotFound
	}
	u.Status = store.UserStatusDeleting
	return nil
}

func (s *RestateStore) LockUserRow(_ context.Context, id string, _ bool) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.users[id]; !ok {
		return store.ErrNotFound
	}
	return nil
}

func (s *RestateStore) ListUsers(_ context.Context, filter store.UserFilter, opts store.ListOptions) (*store.ListResult[store.User], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var items []store.User
	for _, u := range s.users {
		if u.Kind == store.UserKindFederated {
			continue
		}
		if filter.Role != "" && u.Role != filter.Role {
			continue
		}
		if filter.Status != "" && u.Status != filter.Status {
			continue
		}
		if filter.Search != "" {
			q := strings.ToLower(filter.Search)
			if !strings.Contains(strings.ToLower(u.Email), q) && !strings.Contains(strings.ToLower(u.DisplayName), q) {
				continue
			}
		}
		items = append(items, *u)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Email < items[j].Email })
	total := len(items)
	if opts.Limit > 0 && len(items) > opts.Limit {
		items = items[:opts.Limit]
	}
	return &store.ListResult[store.User]{Items: items, TotalCount: total}, nil
}

func (s *RestateStore) IsUserInvitedOrActive(_ context.Context, email string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, u := range s.users {
		if u.Kind != store.UserKindFederated && strings.EqualFold(u.Email, email) && (u.Status == "invited" || u.Status == "active") {
			return true, nil
		}
	}
	return false, nil
}

func (s *RestateStore) IncrementSessionGeneration(_ context.Context, userID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[userID]
	if !ok {
		return store.ErrNotFound
	}
	u.SessionGeneration++
	return nil
}

func (s *RestateStore) CreateTestFixtureUser(_ context.Context, user *store.User) error {
	if user == nil || user.Kind != store.UserKindTestFixture || user.ExpiresAt == nil || user.IssuedBy == nil || *user.IssuedBy == "" {
		return store.ErrInvalidInput
	}
	return s.CreateUser(context.Background(), user)
}

func (s *RestateStore) CountLiveTestFixtureUsers(_ context.Context, issuedBy string, now time.Time) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, u := range s.users {
		if u.Kind == store.UserKindTestFixture && u.ExpiresAt != nil && u.ExpiresAt.After(now) && (issuedBy == "" || (u.IssuedBy != nil && *u.IssuedBy == issuedBy)) {
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) ListTestFixtureUsers(_ context.Context, issuedBy string, liveAt time.Time, limit int) ([]store.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.User
	for _, u := range s.users {
		if u.Kind != store.UserKindTestFixture {
			continue
		}
		if issuedBy != "" && (u.IssuedBy == nil || *u.IssuedBy != issuedBy) {
			continue
		}
		if !liveAt.IsZero() && (u.ExpiresAt == nil || !u.ExpiresAt.After(liveAt)) {
			continue
		}
		out = append(out, *u)
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *RestateStore) ListExpiredTestFixtureUsers(_ context.Context, _ store.ExpiredTestFixtureQuery) ([]store.User, error) {
	return nil, nil
}

func (s *RestateStore) LockTestFixtureIssuance(_ context.Context) error { return nil }

func (s *RestateStore) ReplaceTestIdentityLoginCode(_ context.Context, code *store.TestIdentityLoginCode) error {
	if code == nil || code.UserID == "" || code.CodeHash == "" {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if code.ID == "" {
		code.ID = uuid.NewString()
	}
	cp := *code
	s.testLoginCodes[code.UserID] = &cp
	return nil
}

func (s *RestateStore) ConsumeTestIdentityLoginCode(_ context.Context, codeHash string, now time.Time) (*store.TestIdentityLoginCode, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.testLoginCodes {
		if c.CodeHash == codeHash {
			if c.ConsumedAt != nil {
				return nil, store.ErrTestIdentityLoginCodeConsumed
			}
			if !c.ExpiresAt.After(now) {
				return nil, store.ErrTestIdentityLoginCodeExpired
			}
			c.ConsumedAt = &now
			cp := *c
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) CountLiveTestAdminUsers(_ context.Context, kind, issuedBy string, now time.Time) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, u := range s.users {
		if (u.Kind == store.UserKindTestHubAdmin || u.Kind == store.UserKindTestSuperAdmin) &&
			(kind == "" || u.Kind == kind) && (issuedBy == "" || (u.IssuedBy != nil && *u.IssuedBy == issuedBy)) &&
			u.ExpiresAt != nil && u.ExpiresAt.After(now) {
		}
	}
	return n, nil
}

func (s *RestateStore) ListTestAdminUsers(_ context.Context) ([]store.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.User
	for _, u := range s.users {
		if u.Kind == store.UserKindTestHubAdmin || u.Kind == store.UserKindTestSuperAdmin {
			out = append(out, *u)
		}
	}
	return out, nil
}

func (s *RestateStore) CreateTestIdentityDelegation(_ context.Context, d *store.TestIdentityDelegation) error {
	if d == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if d.ID == "" {
		d.ID = uuid.NewString()
	}
	if d.Created.IsZero() {
		d.Created = time.Now().UTC()
	}
	cp := *d
	s.testDelegations[d.ID] = &cp
	return nil
}

func (s *RestateStore) GetTestIdentityDelegation(_ context.Context, id string) (*store.TestIdentityDelegation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.testDelegations[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *d
	return &cp, nil
}

func (s *RestateStore) ListTestIdentityDelegations(_ context.Context, grantorID string, limit int) ([]store.TestIdentityDelegation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.TestIdentityDelegation
	for _, d := range s.testDelegations {
		if grantorID == "" || d.GrantorID == grantorID {
			out = append(out, *d)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *RestateStore) RevokeTestIdentityDelegation(_ context.Context, id, revokedBy string, at time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.testDelegations[id]
	if !ok {
		return false, store.ErrNotFound
	}
	if d.RevokedAt != nil {
		return false, nil
	}
	d.RevokedAt = &at
	d.RevokedBy = &revokedBy
	return true, nil
}

func (s *RestateStore) CountLiveTestFixtureUsersByDelegation(_ context.Context, delegationID string, now time.Time) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, u := range s.users {
		if u.Kind == store.UserKindTestFixture && u.IssuerDelegationID != nil && *u.IssuerDelegationID == delegationID && u.ExpiresAt != nil && u.ExpiresAt.After(now) {
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) CountLiveTestAdminUsersByDelegation(_ context.Context, delegationID string, now time.Time) (int, error) {
	if delegationID == "" {
		return 0, store.ErrInvalidInput
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, u := range s.users {
		if (u.Kind == store.UserKindTestHubAdmin || u.Kind == store.UserKindTestSuperAdmin) &&
			u.IssuerDelegationID != nil && *u.IssuerDelegationID == delegationID && u.ExpiresAt != nil && u.ExpiresAt.After(now) {
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) ListTestFixtureUsersByDelegation(_ context.Context, delegationID string, liveAt time.Time, limit int) ([]store.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.User
	for _, u := range s.users {
		if u.Kind == store.UserKindTestFixture && u.IssuerDelegationID != nil && *u.IssuerDelegationID == delegationID {
			if !liveAt.IsZero() && (u.ExpiresAt == nil || !u.ExpiresAt.After(liveAt)) {
				continue
			}
			out = append(out, *u)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *RestateStore) GetTestInfraBinding(_ context.Context) (*store.TestInfraBinding, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.testInfraBinding == nil {
		return nil, store.ErrNotFound
	}
	cp := *s.testInfraBinding
	return &cp, nil
}

func (s *RestateStore) CreateUserDeletion(_ context.Context, d *store.UserDeletion) error {
	if d == nil || d.UserID == "" {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if d.ID == "" {
		d.ID = uuid.NewString()
	}
	cp := *d
	s.userDeletions[d.ID] = &cp
	return nil
}

func (s *RestateStore) GetUserDeletion(_ context.Context, id string) (*store.UserDeletion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.userDeletions[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *d
	return &cp, nil
}

func (s *RestateStore) GetUserDeletionByUser(_ context.Context, userID string) (*store.UserDeletion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, d := range s.userDeletions {
		if d.UserID == userID {
			cp := *d
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) ListUserDeletionsByState(_ context.Context, state string, limit int) ([]store.UserDeletion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.UserDeletion
	for _, d := range s.userDeletions {
		if d.State == state {
			out = append(out, *d)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *RestateStore) CompleteUserDeletion(_ context.Context, id string, at time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.userDeletions[id]
	if !ok {
		return false, store.ErrNotFound
	}
	if d.State != store.UserDeletionStateStopping {
		return false, nil
	}
	d.State = store.UserDeletionStateCompleted
	d.CompletedAt = &at
	return true, nil
}

func (s *RestateStore) TouchUserDeletionReconciled(_ context.Context, id string, at time.Time, lastErr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.userDeletions[id]
	if !ok {
		return store.ErrNotFound
	}
	d.LastReconciledAt = &at
	d.LastError = lastErr
	return nil
}

func (s *RestateStore) UpsertUserDeletionAgent(_ context.Context, a *store.UserDeletionAgent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.userDeletionAgents[a.DeletionID]
	for i := range list {
		if list[i].AgentID == a.AgentID {
			list[i] = *a
			s.userDeletionAgents[a.DeletionID] = list
			return nil
		}
	}
	s.userDeletionAgents[a.DeletionID] = append(list, *a)
	return nil
}

func (s *RestateStore) ReopenUserDeletionAgent(ctx context.Context, a *store.UserDeletionAgent) error {
	return s.UpsertUserDeletionAgent(ctx, a)
}

func (s *RestateStore) ListUserDeletionAgents(_ context.Context, deletionID string) ([]store.UserDeletionAgent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Clone(s.userDeletionAgents[deletionID]), nil
}

func (s *RestateStore) ListUserDeletionAgentsForAgent(_ context.Context, agentID string) ([]store.UserDeletionAgent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.UserDeletionAgent
	for _, list := range s.userDeletionAgents {
		for _, item := range list {
			if item.AgentID == agentID {
				out = append(out, item)
			}
		}
	}
	return out, nil
}

func (s *RestateStore) CreateUserAccessToken(_ context.Context, token *store.UserAccessToken) error {
	if token == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if token.ID == "" {
		token.ID = uuid.NewString()
	}
	if token.Created.IsZero() {
		token.Created = time.Now().UTC()
	}
	cp := *token
	s.userAccessTokens[token.ID] = &cp
	return nil
}

func (s *RestateStore) GetUserAccessToken(_ context.Context, id string) (*store.UserAccessToken, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.userAccessTokens[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *t
	return &cp, nil
}

func (s *RestateStore) GetUserAccessTokenByHash(_ context.Context, hash string) (*store.UserAccessToken, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, t := range s.userAccessTokens {
		if t.KeyHash == hash {
			cp := *t
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) UpdateUserAccessTokenLastUsed(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.userAccessTokens[id]
	if !ok {
		return store.ErrNotFound
	}
	now := time.Now().UTC()
	t.LastUsed = &now
	return nil
}

func (s *RestateStore) RevokeUserAccessToken(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.userAccessTokens[id]
	if !ok {
		return store.ErrNotFound
	}
	t.Revoked = true
	return nil
}

func (s *RestateStore) DeleteUserAccessToken(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.userAccessTokens[id]; !ok {
		return store.ErrNotFound
	}
	delete(s.userAccessTokens, id)
	return nil
}

func (s *RestateStore) ListUserAccessTokens(_ context.Context, userID string) ([]store.UserAccessToken, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.UserAccessToken
	for _, t := range s.userAccessTokens {
		if t.UserID == userID {
			out = append(out, *t)
		}
	}
	return out, nil
}

func (s *RestateStore) CountUserAccessTokens(_ context.Context, userID string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, t := range s.userAccessTokens {
		if t.UserID == userID && !t.Revoked {
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) RevokeUserAccessTokensForUser(_ context.Context, userID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, t := range s.userAccessTokens {
		if t.UserID == userID && !t.Revoked {
			t.Revoked = true
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) DeleteUserAccessTokensByProject(_ context.Context, projectID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, t := range s.userAccessTokens {
		if t.ProjectID == projectID {
			delete(s.userAccessTokens, id)
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) LockUserForTokens(_ context.Context, userID string) error {
	return s.LockUserRow(context.Background(), userID, true)
}

func (s *RestateStore) GetUserTerminalWorkspace(_ context.Context, userID string) (*store.UserTerminalWorkspace, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	w, ok := s.userTerminalWorkspaces[userID]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *w
	cp.AgentIDs = slices.Clone(w.AgentIDs)
	return &cp, nil
}

func (s *RestateStore) PutUserTerminalWorkspace(_ context.Context, userID string, agentIDs []string, frontmostAgentID string) (*store.UserTerminalWorkspace, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var rev int64 = 1
	if cur, ok := s.userTerminalWorkspaces[userID]; ok {
		rev = cur.Revision + 1
	}
	w := &store.UserTerminalWorkspace{
		UserID:           userID,
		AgentIDs:         slices.Clone(agentIDs),
		FrontmostAgentID: frontmostAgentID,
		Revision:         rev,
		Updated:          time.Now().UTC(),
	}
	s.userTerminalWorkspaces[userID] = w
	cp := *w
	return &cp, nil
}

func (s *RestateStore) GetExternalIdentity(_ context.Context, provider, issuer, subject string) (*store.ExternalIdentityBinding, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.externalIdentities[provider+":"+issuer+":"+subject]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *b
	return &cp, nil
}

func (s *RestateStore) CreateExternalIdentity(_ context.Context, binding *store.ExternalIdentityBinding) error {
	if binding == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := binding.Provider + ":" + binding.Issuer + ":" + binding.Subject
	if _, exists := s.externalIdentities[key]; exists {
		return store.ErrAlreadyExists
	}
	if binding.ID == "" {
		binding.ID = uuid.NewString()
	}
	now := time.Now().UTC()
	binding.CreatedAt = now
	binding.UpdatedAt = now
	cp := *binding
	s.externalIdentities[key] = &cp
	return nil
}

func (s *RestateStore) UpdateExternalIdentityEmail(_ context.Context, id, email string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.externalIdentities {
		if b.ID == id {
			b.Email = email
			b.UpdatedAt = time.Now().UTC()
			return nil
		}
	}
	return store.ErrNotFound
}

func (s *RestateStore) GetExternalIdentitiesByUserID(_ context.Context, userID string) ([]*store.ExternalIdentityBinding, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.ExternalIdentityBinding
	for _, b := range s.externalIdentities {
		if b.UserID == userID {
			cp := *b
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *RestateStore) AddAllowListEntry(_ context.Context, entry *store.AllowListEntry) error {
	if entry == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := strings.ToLower(entry.Email)
	if _, exists := s.allowList[k]; exists {
		return store.ErrAlreadyExists
	}
	if entry.ID == "" {
		entry.ID = uuid.NewString()
	}
	entry.Created = time.Now().UTC()
	cp := *entry
	s.allowList[k] = &cp
	return nil
}

func (s *RestateStore) RemoveAllowListEntry(_ context.Context, email string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := strings.ToLower(email)
	if _, ok := s.allowList[k]; !ok {
		return store.ErrNotFound
	}
	delete(s.allowList, k)
	return nil
}

func (s *RestateStore) GetAllowListEntry(_ context.Context, email string) (*store.AllowListEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.allowList[strings.ToLower(email)]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *e
	return &cp, nil
}

func (s *RestateStore) ListAllowListEntries(_ context.Context, _ store.ListOptions) (*store.ListResult[store.AllowListEntry], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var items []store.AllowListEntry
	for _, e := range s.allowList {
		items = append(items, *e)
	}
	return &store.ListResult[store.AllowListEntry]{Items: items, TotalCount: len(items)}, nil
}

func (s *RestateStore) IsEmailAllowListed(_ context.Context, email string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.allowList[strings.ToLower(email)]
	return ok, nil
}

func (s *RestateStore) BulkAddAllowListEntries(_ context.Context, entries []*store.AllowListEntry) (int, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	added, skipped := 0, 0
	for _, e := range entries {
		k := strings.ToLower(e.Email)
		if _, exists := s.allowList[k]; exists {
			skipped++
			continue
		}
		cp := *e
		if cp.ID == "" {
			cp.ID = uuid.NewString()
		}
		cp.Created = time.Now().UTC()
		s.allowList[k] = &cp
		added++
	}
	return added, skipped, nil
}

func (s *RestateStore) ListEmailDomains(_ context.Context) ([]string, error) {
	return nil, nil
}

func (s *RestateStore) UpdateAllowListEntryInviteID(_ context.Context, email, inviteID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.allowList[strings.ToLower(email)]
	if !ok {
		return store.ErrNotFound
	}
	e.InviteID = inviteID
	return nil
}

func (s *RestateStore) ListAllowListEntriesWithInvites(_ context.Context, _ store.ListOptions) (*store.ListResult[store.AllowListEntryWithInvite], error) {
	return &store.ListResult[store.AllowListEntryWithInvite]{}, nil
}

func (s *RestateStore) CreateInviteCode(_ context.Context, invite *store.InviteCode) error {
	if invite == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if invite.ID == "" {
		invite.ID = uuid.NewString()
	}
	invite.Created = time.Now().UTC()
	cp := *invite
	s.inviteCodes[invite.ID] = &cp
	return nil
}

func (s *RestateStore) GetInviteCodeByHash(_ context.Context, codeHash string) (*store.InviteCode, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, inv := range s.inviteCodes {
		if inv.CodeHash == codeHash {
			cp := *inv
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) GetInviteCode(_ context.Context, id string) (*store.InviteCode, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	inv, ok := s.inviteCodes[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *inv
	return &cp, nil
}

func (s *RestateStore) ListInviteCodes(_ context.Context, _ store.ListOptions) (*store.ListResult[store.InviteCode], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var items []store.InviteCode
	for _, inv := range s.inviteCodes {
		items = append(items, *inv)
	}
	return &store.ListResult[store.InviteCode]{Items: items, TotalCount: len(items)}, nil
}

func (s *RestateStore) IncrementInviteUseCount(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	inv, ok := s.inviteCodes[id]
	if !ok {
		return store.ErrNotFound
	}
	inv.UseCount++
	return nil
}

func (s *RestateStore) RevokeInviteCode(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	inv, ok := s.inviteCodes[id]
	if !ok {
		return store.ErrNotFound
	}
	inv.Revoked = true
	return nil
}

func (s *RestateStore) DeleteInviteCode(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.inviteCodes[id]; !ok {
		return store.ErrNotFound
	}
	delete(s.inviteCodes, id)
	return nil
}

func (s *RestateStore) GetInviteStats(_ context.Context) (*store.InviteStats, error) {
	return &store.InviteStats{}, nil
}

// --- GroupStore & RoleStore & AccessConstraintStore ---

func (s *RestateStore) CreateGroup(_ context.Context, g *store.Group) error {
	if g == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if g.ID == "" {
		g.ID = uuid.NewString()
	}
	if g.Slug == "" {
		g.Slug = api.Slugify(g.Name)
	}
	for _, cur := range s.groups {
		if cur.ID == g.ID || strings.EqualFold(cur.Slug, g.Slug) {
			return store.ErrAlreadyExists
		}
	}
	now := time.Now().UTC()
	g.Created = now
	g.Updated = now
	cp := *g
	s.groups[g.ID] = &cp
	return nil
}

func (s *RestateStore) GetGroup(_ context.Context, id string) (*store.Group, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	g, ok := s.groups[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *g
	return &cp, nil
}

func (s *RestateStore) GetGroupBySlug(_ context.Context, slug string) (*store.Group, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, g := range s.groups {
		if strings.EqualFold(g.Slug, slug) {
			cp := *g
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) UpdateGroup(_ context.Context, g *store.Group) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.groups[g.ID]; !ok {
		return store.ErrNotFound
	}
	cp := *g
	cp.Updated = time.Now().UTC()
	s.groups[g.ID] = &cp
	return nil
}

func (s *RestateStore) DeleteGroup(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.groups[id]; !ok {
		return store.ErrNotFound
	}
	delete(s.groups, id)
	delete(s.groupMembers, id)
	return nil
}

func (s *RestateStore) ListGroups(_ context.Context, filter store.GroupFilter, opts store.ListOptions) (*store.ListResult[store.Group], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var items []store.Group
	for _, g := range s.groups {
		if filter.GroupType != "" && g.GroupType != filter.GroupType {
			continue
		}
		if filter.ProjectID != "" && g.ProjectID != filter.ProjectID {
			continue
		}
		items = append(items, *g)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	total := len(items)
	if opts.Limit > 0 && len(items) > opts.Limit {
		items = items[:opts.Limit]
	}
	return &store.ListResult[store.Group]{Items: items, TotalCount: total}, nil
}

func (s *RestateStore) AddGroupMember(_ context.Context, m *store.GroupMember) error {
	if m == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.groupMembers[m.GroupID]
	for _, cur := range list {
		if cur.MemberType == m.MemberType && cur.MemberID == m.MemberID {
			return store.ErrAlreadyExists
		}
	}
	if m.AddedAt.IsZero() {
		m.AddedAt = time.Now().UTC()
	}
	s.groupMembers[m.GroupID] = append(list, *m)
	return nil
}

func (s *RestateStore) RemoveGroupMember(_ context.Context, groupID, memberType, memberID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.groupMembers[groupID]
	for i := range list {
		if list[i].MemberType == memberType && list[i].MemberID == memberID {
			s.groupMembers[groupID] = slices.Delete(list, i, i+1)
			return nil
		}
	}
	return store.ErrNotFound
}

func (s *RestateStore) UpdateGroupMemberRole(_ context.Context, groupID, memberType, memberID, newRole string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.groupMembers[groupID]
	for i := range list {
		if list[i].MemberType == memberType && list[i].MemberID == memberID {
			list[i].Role = newRole
			return nil
		}
	}
	return store.ErrNotFound
}

func (s *RestateStore) GetGroupMembers(_ context.Context, groupID string) ([]store.GroupMember, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Clone(s.groupMembers[groupID]), nil
}

func (s *RestateStore) GetUserGroups(_ context.Context, userID string) ([]store.GroupMember, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.GroupMember
	for _, list := range s.groupMembers {
		for _, m := range list {
			if m.MemberType == "user" && m.MemberID == userID {
				out = append(out, m)
			}
		}
	}
	return out, nil
}

func (s *RestateStore) GetGroupMembership(_ context.Context, groupID, memberType, memberID string) (*store.GroupMember, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, m := range s.groupMembers[groupID] {
		if m.MemberType == memberType && m.MemberID == memberID {
			cp := m
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) WouldCreateCycle(_ context.Context, groupID, memberGroupID string) (bool, error) {
	return groupID == memberGroupID, nil
}

func (s *RestateStore) GetGroupByProjectID(_ context.Context, projectID string) (*store.Group, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, g := range s.groups {
		if g.ProjectID == projectID && g.GroupType == store.GroupTypeProjectAgents {
			cp := *g
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) GetEffectiveGroups(_ context.Context, userID string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []string
	for gid, list := range s.groupMembers {
		for _, m := range list {
			if m.MemberType == "user" && m.MemberID == userID {
				out = append(out, gid)
			}
		}
	}
	return out, nil
}

func (s *RestateStore) GetEffectiveGroupsForAgent(_ context.Context, agentID string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []string
	for gid, list := range s.groupMembers {
		for _, m := range list {
			if m.MemberType == "agent" && m.MemberID == agentID {
				out = append(out, gid)
			}
		}
	}
	return out, nil
}

func (s *RestateStore) GetParentGroups(_ context.Context, _ string) ([]string, error) {
	return nil, nil
}

func (s *RestateStore) RemoveChildGroupEdge(_ context.Context, parentGroupID, childGroupID string) error {
	return s.RemoveGroupMember(context.Background(), parentGroupID, "group", childGroupID)
}

func (s *RestateStore) GetDirectParentGroupIDs(_ context.Context, _ string) ([]string, error) {
	return nil, nil
}

func (s *RestateStore) CountGroupMembersByRole(_ context.Context, groupID, role string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, m := range s.groupMembers[groupID] {
		if m.Role == role {
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) DeleteGroupMembershipsForUser(_ context.Context, userID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for gid, list := range s.groupMembers {
		filtered := list[:0]
		for _, m := range list {
			if m.MemberType == "user" && m.MemberID == userID {
				n++
				continue
			}
			filtered = append(filtered, m)
		}
		s.groupMembers[gid] = filtered
	}
	return n, nil
}

func (s *RestateStore) DeleteGroupMembershipsForAgents(_ context.Context, ids []string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for gid, list := range s.groupMembers {
		filtered := list[:0]
		for _, m := range list {
			if m.MemberType == "agent" && slices.Contains(ids, m.MemberID) {
				n++
				continue
			}
			filtered = append(filtered, m)
		}
		s.groupMembers[gid] = filtered
	}
	return n, nil
}

func (s *RestateStore) DeleteOrphanedGroupMemberships(_ context.Context) (int, error) {
	return 0, nil
}

func (s *RestateStore) GetGroupsByIDs(_ context.Context, ids []string) ([]store.Group, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.Group
	for _, id := range ids {
		if g, ok := s.groups[id]; ok {
			out = append(out, *g)
		}
	}
	return out, nil
}

func (s *RestateStore) GetRoleDefinition(_ context.Context, id string) (*store.RoleDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rd, ok := s.roleDefinitions[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *rd
	return &cp, nil
}

func (s *RestateStore) GetRoleDefinitionsByIDs(_ context.Context, ids []string) (map[string]*store.RoleDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]*store.RoleDefinition, len(ids))
	for _, id := range ids {
		if rd, ok := s.roleDefinitions[id]; ok {
			cp := *rd
			out[id] = &cp
		}
	}
	return out, nil
}

func (s *RestateStore) GetRoleDefinitionByName(_ context.Context, name, scopeType string) (*store.RoleDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, rd := range s.roleDefinitions {
		if rd.Name == name && (scopeType == "" || rd.ScopeType == scopeType) {
			cp := *rd
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) ListRoleDefinitions(_ context.Context) ([]*store.RoleDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.RoleDefinition
	for _, rd := range s.roleDefinitions {
		cp := *rd
		out = append(out, &cp)
	}
	return out, nil
}

func (s *RestateStore) CreateRoleDefinition(_ context.Context, rd *store.RoleDefinition) (*store.RoleDefinition, error) {
	if rd == nil {
		return nil, store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cur := range s.roleDefinitions {
		if cur.Name == rd.Name && cur.ScopeType == rd.ScopeType {
			return nil, store.ErrAlreadyExists
		}
	}
	cp := *rd
	if cp.ID == "" {
		cp.ID = uuid.NewString()
	}
	now := time.Now().UTC()
	if cp.CreatedAt.IsZero() {
		cp.CreatedAt = now
	}
	cp.UpdatedAt = now
	if cp.ConfersMembership == nil {
		t := cp.Name != "project-visitor"
		cp.ConfersMembership = &t
	}
	s.roleDefinitions[cp.ID] = &cp
	out := cp
	return &out, nil
}

func (s *RestateStore) GetRoleBinding(_ context.Context, id string) (*store.RoleBinding, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rb, ok := s.roleBindings[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *rb
	return &cp, nil
}

func (s *RestateStore) ListRoleBindingsForPrincipal(_ context.Context, principalType, principalID string) ([]*store.RoleBinding, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.RoleBinding
	for _, rb := range s.roleBindings {
		if rb.PrincipalType == principalType && rb.PrincipalID == principalID {
			cp := *rb
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *RestateStore) ListRoleBindingsForPrincipals(_ context.Context, principals []store.PrincipalRef, scopeTypes, scopeIDs []string) ([]*store.RoleBinding, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.RoleBinding
	for _, rb := range s.roleBindings {
		matchedPrincipal := false
		for _, p := range principals {
			if rb.PrincipalType == p.Type && rb.PrincipalID == p.ID {
				matchedPrincipal = true
				break
			}
		}
		if !matchedPrincipal {
			continue
		}
		if len(scopeTypes) > 0 && !slices.Contains(scopeTypes, rb.ScopeType) {
			continue
		}
		if len(scopeIDs) > 0 && rb.ScopeType != "system" && !slices.Contains(scopeIDs, rb.ScopeID) {
			continue
		}
		cp := *rb
		out = append(out, &cp)
	}
	return out, nil
}

func (s *RestateStore) ListRoleBindingsForScope(_ context.Context, scopeType, scopeID string) ([]*store.RoleBinding, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.RoleBinding
	for _, rb := range s.roleBindings {
		if rb.ScopeType == scopeType && (scopeID == "" || rb.ScopeID == scopeID) {
			cp := *rb
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *RestateStore) CreateRoleBinding(_ context.Context, rb *store.RoleBinding) (*store.RoleBinding, error) {
	if rb == nil {
		return nil, store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cur := range s.roleBindings {
		if cur.RoleDefinitionID == rb.RoleDefinitionID && cur.PrincipalType == rb.PrincipalType &&
			cur.PrincipalID == rb.PrincipalID && cur.ScopeType == rb.ScopeType && cur.ScopeID == rb.ScopeID {
			return nil, store.ErrAlreadyExists
		}
	}
	cp := *rb
	if cp.ID == "" {
		cp.ID = uuid.NewString()
	}
	if cp.CreatedAt.IsZero() {
		cp.CreatedAt = time.Now().UTC()
	}
	s.roleBindings[cp.ID] = &cp
	out := cp
	return &out, nil
}

func (s *RestateStore) CreateVisitorRoleBinding(ctx context.Context, rb *store.RoleBinding) (*store.RoleBinding, error) {
	return s.CreateRoleBinding(ctx, rb)
}

func (s *RestateStore) ListVisitorRoleBindings(_ context.Context, projectID string) ([]*store.RoleBinding, error) {
	return s.ListRoleBindingsForScope(context.Background(), "project", projectID)
}

func (s *RestateStore) UpdateVisitorRoleBindingWindow(_ context.Context, id string, notBefore, expiresAt *time.Time) (*store.RoleBinding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rb, ok := s.roleBindings[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	rb.NotBefore = notBefore
	rb.ExpiresAt = expiresAt
	cp := *rb
	return &cp, nil
}

func (s *RestateStore) DeleteRoleBinding(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.roleBindings[id]; !ok {
		return store.ErrNotFound
	}
	delete(s.roleBindings, id)
	return nil
}

func (s *RestateStore) DeleteRoleBindingsForPrincipal(_ context.Context, principalType, principalID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, rb := range s.roleBindings {
		if rb.PrincipalType == principalType && rb.PrincipalID == principalID {
			delete(s.roleBindings, id)
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) DeleteRoleBindingsForScope(_ context.Context, scopeType, scopeID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, rb := range s.roleBindings {
		if rb.ScopeType == scopeType && rb.ScopeID == scopeID {
			delete(s.roleBindings, id)
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) UpdateRoleDefinition(_ context.Context, rd *store.RoleDefinition) (*store.RoleDefinition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.roleDefinitions[rd.ID]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *rd
	cp.CreatedAt = cur.CreatedAt
	cp.UpdatedAt = time.Now().UTC()
	s.roleDefinitions[rd.ID] = &cp
	out := cp
	return &out, nil
}

func (s *RestateStore) UpdateSystemRoleDefinitionPermissions(_ context.Context, id string, permissions []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.roleDefinitions[id]
	if !ok {
		return store.ErrNotFound
	}
	cur.Permissions = slices.Clone(permissions)
	cur.UpdatedAt = time.Now().UTC()
	return nil
}

func (s *RestateStore) DeleteRoleDefinition(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.roleDefinitions[id]; !ok {
		return store.ErrNotFound
	}
	delete(s.roleDefinitions, id)
	return nil
}

func (s *RestateStore) LockRoleDefinitionForAdminGuard(_ context.Context, id string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.roleDefinitions[id]; !ok {
		return store.ErrNotFound
	}
	return nil
}

func (s *RestateStore) ListAllRoleBindings(_ context.Context, _ store.RoleBindingListOptions) ([]*store.RoleBinding, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.RoleBinding
	for _, rb := range s.roleBindings {
		cp := *rb
		out = append(out, &cp)
	}
	return out, nil
}

func (s *RestateStore) CountAllRoleBindings(_ context.Context) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.roleBindings), nil
}

func (s *RestateStore) GetProjectMembership(_ context.Context, projectID, userID string) (*store.ProjectMembership, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, rb := range s.roleBindings {
		if rb.ScopeType == "project" && rb.ScopeID == projectID && rb.PrincipalType == "user" && rb.PrincipalID == userID {
			rd := s.roleDefinitions[rb.RoleDefinitionID]
			roleName := rb.RoleDefinitionID
			if rd != nil {
				if !rd.ConfersProjectMembership() {
					continue
				}
				roleName = rd.Name
			}
			return &store.ProjectMembership{
				ProjectID:     projectID,
				UserID:        userID,
				Role:          roleName,
				RoleBindingID: rb.ID,
			}, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) ListProjectMembers(_ context.Context, projectID string) ([]*store.ProjectMembership, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.ProjectMembership
	for _, rb := range s.roleBindings {
		if rb.ScopeType == "project" && rb.ScopeID == projectID && rb.PrincipalType == "user" {
			rd := s.roleDefinitions[rb.RoleDefinitionID]
			roleName := rb.RoleDefinitionID
			if rd != nil {
				if !rd.ConfersProjectMembership() {
					continue
				}
				roleName = rd.Name
			}
			out = append(out, &store.ProjectMembership{
				ProjectID:     projectID,
				UserID:        rb.PrincipalID,
				Role:          roleName,
				RoleBindingID: rb.ID,
			})
		}
	}
	return out, nil
}

func (s *RestateStore) IsProjectMember(ctx context.Context, projectID, userID string) (bool, error) {
	_, err := s.GetProjectMembership(ctx, projectID, userID)
	if err == nil {
		return true, nil
	}
	return false, nil
}

func (s *RestateStore) CreateAccessConstraint(_ context.Context, c *store.AccessConstraint) (*store.AccessConstraint, error) {
	if c == nil {
		return nil, store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *c
	if cp.ID == "" {
		cp.ID = uuid.NewString()
	}
	cp.Revision = 1
	now := time.Now().UTC()
	cp.CreatedAt = now
	cp.UpdatedAt = now
	s.accessConstraints[cp.ID] = &cp
	out := cp
	return &out, nil
}

func (s *RestateStore) GetAccessConstraint(_ context.Context, id string) (*store.AccessConstraint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.accessConstraints[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *c
	return &cp, nil
}

func (s *RestateStore) UpdateAccessConstraint(_ context.Context, c *store.AccessConstraint, expectedRevision int64) (*store.AccessConstraint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.accessConstraints[c.ID]
	if !ok {
		return nil, store.ErrNotFound
	}
	if expectedRevision > 0 && cur.Revision != expectedRevision {
		return nil, store.ErrRevisionConflict
	}
	cp := *c
	cp.Revision = cur.Revision + 1
	cp.UpdatedAt = time.Now().UTC()
	s.accessConstraints[c.ID] = &cp
	out := cp
	return &out, nil
}

func (s *RestateStore) DeleteAccessConstraint(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.accessConstraints[id]; !ok {
		return store.ErrNotFound
	}
	delete(s.accessConstraints, id)
	return nil
}

func (s *RestateStore) ListAccessConstraints(_ context.Context, limit, offset int) ([]*store.AccessConstraint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.AccessConstraint
	for _, c := range s.accessConstraints {
		cp := *c
		out = append(out, &cp)
	}
	if offset > 0 && offset < len(out) {
		out = out[offset:]
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *RestateStore) ListAccessConstraintsFiltered(_ context.Context, _ store.AccessConstraintListOptions) ([]*store.AccessConstraint, string, int, error) {
	list, err := s.ListAccessConstraints(context.Background(), 100, 0)
	return list, "", len(list), err
}

func (s *RestateStore) CountAccessConstraints(_ context.Context) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.accessConstraints), nil
}

func (s *RestateStore) ResolveApplicableConstraints(_ context.Context, _ []store.PrincipalRef, _, _ []string) ([]*store.AccessConstraint, error) {
	return s.ListAccessConstraints(context.Background(), 1000, 0)
}

func (s *RestateStore) ListConstraintsForScope(_ context.Context, scopeType, scopeID string) ([]*store.AccessConstraint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.AccessConstraint
	for _, c := range s.accessConstraints {
		if c.ScopeType == scopeType && c.ScopeID == scopeID {
			cp := *c
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *RestateStore) DisableAccessConstraint(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.accessConstraints[id]
	if !ok {
		return store.ErrNotFound
	}
	c.Disabled = true
	c.Revision++
	c.UpdatedAt = time.Now().UTC()
	return nil
}

func (s *RestateStore) AppendConstraintHistoryTx(_ context.Context, entry *store.AccessConstraintHistory) error {
	if entry == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *entry
	s.accessConstraintHistory[entry.ConstraintID] = append(s.accessConstraintHistory[entry.ConstraintID], &cp)
	return nil
}

func (s *RestateStore) ListConstraintHistory(_ context.Context, constraintID string) ([]*store.AccessConstraintHistory, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Clone(s.accessConstraintHistory[constraintID]), nil
}
