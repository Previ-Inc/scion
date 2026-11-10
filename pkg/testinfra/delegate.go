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

package testinfra

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/GoogleCloudPlatform/scion/pkg/config"
	"github.com/GoogleCloudPlatform/scion/pkg/store"
	"github.com/google/uuid"
)

const DelegateMutationType = "test_identity_delegation_create_offline"
const RevokeDelegationMutationType = "test_identity_delegation_revoke"
const delegationLabelMaxRunes = 64

var (
	ErrDelegateHubIDRequired     = errors.New("test-infra delegate: hub id must be non-empty and carry no surrounding whitespace")
	ErrDelegateMarkerHubMismatch = errors.New("test-infra delegate: the test infrastructure marker names a different hub id")
	ErrDelegateMarkerClass       = errors.New(`test-infra delegate: the test infrastructure marker class is not "test"`)
	ErrDelegateNotAllowlisted    = errors.New("test-infra delegate: the workload subject is not on server.hub.test_identities.workload_issuers with a max_tier covering member")
	ErrDelegateScope             = errors.New("test-infra delegate: an offline delegation holds only the member scope " + store.TestInfraOfflineDelegationScope)
	ErrDelegateTTL               = errors.New("test-infra delegate: the lifetime is out of bounds")
	ErrDelegateMemberCap         = errors.New("test-infra delegate: the member cap is out of bounds")
	ErrDelegateLabel             = errors.New("test-infra delegate: the label is too long or has control characters")
	ErrDelegateTooManyLive       = errors.New("test-infra delegate: this hub already holds the maximum number of live offline delegations")
	ErrDelegationNotFound        = errors.New("test-infra delegate revoke: no such delegation")
	ErrDelegationNotOffline      = errors.New("test-infra delegate revoke: the delegation is not an offline delegation; revoke it through the hub API")
	ErrDelegationOtherHub        = errors.New("test-infra delegate revoke: the delegation is bound to a different hub id")
)

type DelegateInput struct {
	HubID           string
	WorkloadSub     string
	Scope           string
	WorkloadIssuers []config.TestIdentityWorkloadIssuer
	TTL             time.Duration
	MemberCap       int
	Label           string
	Operator        string
	Host            string
	Now             time.Time
}

func subCoversMember(issuers []config.TestIdentityWorkloadIssuer, sub string) bool {
	for _, e := range issuers {
		if e.Sub != sub {
			continue
		}
		return e.MaxTier == config.WorkloadIssuerTierMember || e.MaxTier == config.WorkloadIssuerTierAdmin
	}
	return false
}

var (
	delegationMu sync.Mutex
	delegations  = make(map[string]*store.TestIdentityDelegation)
)

func Delegate(ctx context.Context, client any, in DelegateInput) (*store.TestIdentityDelegation, error) {
	if client == nil {
		return nil, errors.New("test-infra delegate: no database client")
	}
	hubID := strings.TrimSpace(in.HubID)
	if hubID == "" || hubID != in.HubID {
		return nil, ErrDelegateHubIDRequired
	}
	scope := in.Scope
	if scope == "" {
		scope = store.TestInfraOfflineDelegationScope
	}
	if scope != store.TestInfraOfflineDelegationScope {
		return nil, fmt.Errorf("%w (requested %q)", ErrDelegateScope, scope)
	}
	sub := in.WorkloadSub
	if sub == "" || strings.TrimSpace(sub) != sub || !subCoversMember(in.WorkloadIssuers, sub) {
		return nil, ErrDelegateNotAllowlisted
	}
	ttl := in.TTL
	if ttl == 0 {
		ttl = store.TestInfraOfflineDelegationDefaultTTL
	}
	if ttl < store.TestInfraOfflineDelegationMinTTL || ttl > store.TestInfraOfflineDelegationMaxTTL {
		return nil, fmt.Errorf("%w: must be between %s and %s", ErrDelegateTTL, store.TestInfraOfflineDelegationMinTTL, store.TestInfraOfflineDelegationMaxTTL)
	}
	memberCap := in.MemberCap
	if memberCap == 0 {
		memberCap = store.TestInfraOfflineDelegationMaxMemberCap
	}
	if memberCap < 1 || memberCap > store.TestInfraOfflineDelegationMaxMemberCap {
		return nil, fmt.Errorf("%w: must be between 1 and %d", ErrDelegateMemberCap, store.TestInfraOfflineDelegationMaxMemberCap)
	}
	label := strings.TrimSpace(in.Label)
	if utf8.RuneCountInString(label) > delegationLabelMaxRunes || !utf8.ValidString(label) || strings.ContainsFunc(label, isControl) {
		return nil, ErrDelegateLabel
	}
	operator := strings.TrimSpace(in.Operator)
	if operator == "" {
		operator = "unknown"
	}
	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}
	now = now.UTC()
	expiresAt := now.Add(ttl)

	marker, err := Read(ctx, client)
	if err != nil {
		return nil, err
	}
	if marker.HubID != hubID {
		return nil, fmt.Errorf("%w: marker %q, --hub-id %q", ErrDelegateMarkerHubMismatch, marker.HubID, hubID)
	}
	if !config.IsTestHubClass(marker.Class) {
		return nil, ErrDelegateMarkerClass
	}

	delegationMu.Lock()
	defer delegationMu.Unlock()
	live := 0
	for _, d := range delegations {
		if d.GrantorKind == store.DelegationGrantorTestInfraOffline && d.HubID == hubID && d.RevokedAt == nil && d.ExpiresAt.After(now) {
			live++
		}
	}
	if live >= store.TestInfraOfflineMaxLiveDelegations {
		return nil, fmt.Errorf("%w (%d)", ErrDelegateTooManyLive, store.TestInfraOfflineMaxLiveDelegations)
	}

	markerAt := marker.CreatedAt
	rec := &store.TestIdentityDelegation{
		ID:              uuid.NewString(),
		GrantorID:       store.TestInfraOfflineGrantorID(hubID),
		WorkloadSub:     sub,
		Scope:           scope,
		MemberCap:       memberCap,
		Label:           label,
		Created:         now,
		ExpiresAt:       expiresAt,
		GrantorKind:     store.DelegationGrantorTestInfraOffline,
		HubID:           hubID,
		MarkerCreatedAt: &markerAt,
	}
	cp := *rec
	delegations[rec.ID] = &cp

	if us, ok := client.(store.TestIdentityDelegationStore); ok {
		_ = us.CreateTestIdentityDelegation(ctx, rec)
	}
	if as, ok := client.(store.MutationAuditStore); ok {
		_ = as.CreateMutationAudit(ctx, &store.MutationAuditRecord{
			Timestamp:          now,
			MutationType:       DelegateMutationType,
			ActorPrincipalKind: "operator",
			ActorPrincipalID:   operator,
			TargetType:         "test_identity_delegation",
			TargetID:           rec.ID,
			AfterSummary: fmt.Sprintf("offline issuer delegation created: grantor_kind=%s hub_id=%s marker_id=%s marker_created_at=%s workload_sub=%s scope=%s member_cap=%d expires_at=%s host=%s operator=%s",
				store.DelegationGrantorTestInfraOffline, hubID, MarkerID, markerAt.UTC().Format(time.RFC3339Nano), sub, scope, memberCap,
				expiresAt.Format(time.RFC3339), in.Host, operator),
		})
	}
	return rec, nil
}

func isControl(r rune) bool {
	return r < 0x20 || r == 0x7f || (r >= 0x80 && r < 0xa0)
}

type RevokeDelegationInput struct {
	HubID    string
	ID       string
	Operator string
	Host     string
	Now      time.Time
}

func RevokeDelegation(ctx context.Context, client any, in RevokeDelegationInput) (*store.TestIdentityDelegation, bool, error) {
	if client == nil {
		return nil, false, errors.New("test-infra delegate revoke: no database client")
	}
	hubID := strings.TrimSpace(in.HubID)
	if hubID == "" || hubID != in.HubID {
		return nil, false, ErrDelegateHubIDRequired
	}
	id, err := uuid.Parse(strings.TrimSpace(in.ID))
	if err != nil {
		return nil, false, ErrDelegationNotFound
	}
	operator := strings.TrimSpace(in.Operator)
	if operator == "" {
		operator = "unknown"
	}
	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}
	now = now.UTC()

	delegationMu.Lock()
	defer delegationMu.Unlock()
	row, ok := delegations[id.String()]
	if !ok {
		return nil, false, ErrDelegationNotFound
	}
	if row.GrantorKind != store.DelegationGrantorTestInfraOffline {
		return nil, false, ErrDelegationNotOffline
	}
	if row.HubID != hubID {
		return nil, false, fmt.Errorf("%w: delegation %q, --hub-id %q", ErrDelegationOtherHub, row.HubID, hubID)
	}
	if row.RevokedAt != nil {
		cp := *row
		return &cp, false, nil
	}
	revokedBy := "operator:" + operator
	row.RevokedAt = &now
	row.RevokedBy = &revokedBy
	cp := *row

	if us, ok := client.(store.TestIdentityDelegationStore); ok {
		_, _ = us.RevokeTestIdentityDelegation(ctx, row.ID, revokedBy, now)
	}
	if as, ok := client.(store.MutationAuditStore); ok {
		_ = as.CreateMutationAudit(ctx, &store.MutationAuditRecord{
			Timestamp:          now,
			MutationType:       RevokeDelegationMutationType,
			ActorPrincipalKind: "operator",
			ActorPrincipalID:   operator,
			TargetType:         "test_identity_delegation",
			TargetID:           row.ID,
			BeforeSummary: fmt.Sprintf("offline issuer delegation revoked: grantor_kind=%s hub_id=%s workload_sub=%s scope=%s host=%s operator=%s",
				store.DelegationGrantorTestInfraOffline, row.HubID, row.WorkloadSub, row.Scope, in.Host, operator),
		})
	}
	return &cp, true, nil
}
