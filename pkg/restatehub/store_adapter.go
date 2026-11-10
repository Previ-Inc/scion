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
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/scion/pkg/conduit/registry"
	"github.com/GoogleCloudPlatform/scion/pkg/store"
	"github.com/GoogleCloudPlatform/scion/pkg/store/agentsort"
	"github.com/google/uuid"
)

// Built-in maintenance operation keys (replacing entadapter constants).
const (
	AppliedConfigTZCleanupKey       = "applied-config-tz-cleanup"
	AutoExposeEnvNormalizeKey       = "auto-expose-env-normalize"
	UTCTimestampNormalizeKey        = "utc-timestamp-normalize"
	TemplateBlobGCKey               = "template-blob-gc"
	PassthroughTranslationReviewKey = "passthrough-translation-review"
	GroupParticipantModeCleanupKey  = "group-participant-mode-cleanup"
	ProjectStorageMigrateKey        = "project-storage-migrate"
	ChatStarReconcileKey            = "chat-star-reconcile"
)

// RestateStore implements store.Store and registry.Store backed by Restate Virtual Objects
// and Workflows in pkg/restatehub, with deterministic partition-keyed state synchronization.
type RestateStore struct {
	mu   sync.RWMutex
	host *Host
	sup  *Supervisor

	agents                  map[string]*store.Agent
	agentKeys               map[string][]*store.AgentIdentityKey // projectID -> keys
	agentHolds              map[string][]*store.AgentHold        // agentID -> holds
	agentCreds              map[string]*store.AgentCredential    // id -> cred
	agentSAAssignments      map[string][]*store.AgentServiceAccountAssignment
	agentReincarnations     map[string]*store.AgentReincarnation
	agentDelegationGrants   map[string]*store.AgentDelegationGrant
	agentDelegatedCreds     map[string]*store.AgentDelegatedCredential
	recoveryObservations    map[string]store.RecoveryObservationRecord
	brokerTargetInventory   map[string][]store.BrokerTargetInventory
	brokerReportedRuns      map[string][]store.BrokerReportedRun
	projects                map[string]*store.Project
	projectProviders        map[string][]store.ProjectProvider // projectID -> providers
	projectSyncStates       map[string]*store.ProjectSyncState // projectID+":"+brokerID
	projectVisitorRules     map[string]*store.ProjectVisitorRule
	projectVisitorExcluded  map[string]map[string]bool // ruleID -> userID -> true
	projectPreStartHooks    map[string]*store.ProjectPreStartHook
	brokers                 map[string]*store.RuntimeBroker
	brokerSecrets           map[string]*store.BrokerSecret
	brokerJoinTokens        map[string]*store.BrokerJoinToken // brokerID -> token
	brokerSettings          map[string]*store.BrokerSettingsRecord
	brokerDispatches        map[string]*store.BrokerDispatch
	templates               map[string]*store.Template
	harnessConfigs          map[string]*store.HarnessConfig
	users                   map[string]*store.User
	userDeletions           map[string]*store.UserDeletion
	userDeletionAgents      map[string][]store.UserDeletionAgent
	userAccessTokens        map[string]*store.UserAccessToken
	userTerminalWorkspaces  map[string]*store.UserTerminalWorkspace
	testLoginCodes          map[string]*store.TestIdentityLoginCode // userID -> code
	testDelegations         map[string]*store.TestIdentityDelegation
	testInfraBinding        *store.TestInfraBinding
	externalIdentities      map[string]*store.ExternalIdentityBinding
	allowList               map[string]*store.AllowListEntry
	inviteCodes             map[string]*store.InviteCode
	envVars                 map[string]*store.EnvVar // scope+":"+scopeID+":"+key
	secrets                 map[string]*store.Secret // scope+":"+scopeID+":"+key
	encryptionPolicy        *store.EncryptionPolicy
	encryptionMigrations    map[string]*store.EncryptionMigration
	groups                  map[string]*store.Group
	groupMembers            map[string][]store.GroupMember // groupID -> members
	roleDefinitions         map[string]*store.RoleDefinition
	roleBindings            map[string]*store.RoleBinding
	delegationEdges         map[string]*store.DelegationEdge
	delegationAdoptions     map[string]*store.DelegationAdoption
	accessConstraints       map[string]*store.AccessConstraint
	accessConstraintHistory map[string][]*store.AccessConstraintHistory
	mutationAudits          []*store.MutationAuditRecord
	membershipLossChecks    map[string]*store.MembershipLossCheck
	limitDefinitions        map[string]*store.LimitDefinition
	entitlementBindings     map[string]*store.EntitlementBinding
	usageReservations       map[string]*store.UsageReservation
	notifSubscriptions      map[string]*store.NotificationSubscription
	notifications           map[string]*store.Notification
	subscriptionTemplates   map[string]*store.SubscriptionTemplate
	scheduledEvents         map[string]*store.ScheduledEvent
	schedules               map[string]*store.Schedule
	gcpServiceAccounts      map[string]*store.GCPServiceAccount
	githubInstallations     map[int64]*store.GitHubInstallation
	messages                map[string]*store.Message
	conversations           map[string]*store.Conversation
	participants            map[string][]store.ConversationParticipant
	addressees              map[string][]store.MessageAddressee
	threadGroups            map[string]*store.ThreadGroup
	threadGroupMembers      map[string]string // conversationID -> groupID
	maintenanceOps          map[string]*store.MaintenanceOperation
	maintenanceRuns         map[string]*store.MaintenanceOperationRun
	lifecycleHooks          map[string]*store.LifecycleHook
	hookPhases              map[string]string // agentID -> phase
	skills                  map[string]*store.Skill
	skillVersions           map[string]*store.SkillVersion
	skillRegistries         map[string]*store.SkillRegistry
	skillPinnedHashes       map[string]map[string]string // registryID -> uri -> hash
	skillInjections         map[string]*store.SkillInjection
	hubSettings             map[string]*store.HubSetting
	agentSessionMetrics     map[string]*store.AgentSessionMetrics
	hubInstances            map[string]store.HubInstance

	// Conduit registry state
	conduitRelays   map[string]registry.RelayInstance
	conduitSessions map[string]registry.SessionRecord
	conduitEpochs   map[string]int64
}

var (
	_ store.Store    = (*RestateStore)(nil)
	_ registry.Store = (*RestateStore)(nil)
)

// NewRestateStore constructs a RestateStore bound to the given Restate Host and optional Supervisor.
func NewRestateStore(host *Host, sup *Supervisor) *RestateStore {
	s := &RestateStore{
		host:                    host,
		sup:                     sup,
		agents:                  make(map[string]*store.Agent),
		agentKeys:               make(map[string][]*store.AgentIdentityKey),
		agentHolds:              make(map[string][]*store.AgentHold),
		agentCreds:              make(map[string]*store.AgentCredential),
		agentSAAssignments:      make(map[string][]*store.AgentServiceAccountAssignment),
		agentReincarnations:     make(map[string]*store.AgentReincarnation),
		agentDelegationGrants:   make(map[string]*store.AgentDelegationGrant),
		agentDelegatedCreds:     make(map[string]*store.AgentDelegatedCredential),
		recoveryObservations:    make(map[string]store.RecoveryObservationRecord),
		brokerTargetInventory:   make(map[string][]store.BrokerTargetInventory),
		brokerReportedRuns:      make(map[string][]store.BrokerReportedRun),
		projects:                make(map[string]*store.Project),
		projectProviders:        make(map[string][]store.ProjectProvider),
		projectSyncStates:       make(map[string]*store.ProjectSyncState),
		projectVisitorRules:     make(map[string]*store.ProjectVisitorRule),
		projectVisitorExcluded:  make(map[string]map[string]bool),
		projectPreStartHooks:    make(map[string]*store.ProjectPreStartHook),
		brokers:                 make(map[string]*store.RuntimeBroker),
		brokerSecrets:           make(map[string]*store.BrokerSecret),
		brokerJoinTokens:        make(map[string]*store.BrokerJoinToken),
		brokerSettings:          make(map[string]*store.BrokerSettingsRecord),
		brokerDispatches:        make(map[string]*store.BrokerDispatch),
		templates:               make(map[string]*store.Template),
		harnessConfigs:          make(map[string]*store.HarnessConfig),
		users:                   make(map[string]*store.User),
		userDeletions:           make(map[string]*store.UserDeletion),
		userDeletionAgents:      make(map[string][]store.UserDeletionAgent),
		userAccessTokens:        make(map[string]*store.UserAccessToken),
		userTerminalWorkspaces:  make(map[string]*store.UserTerminalWorkspace),
		testLoginCodes:          make(map[string]*store.TestIdentityLoginCode),
		testDelegations:         make(map[string]*store.TestIdentityDelegation),
		externalIdentities:      make(map[string]*store.ExternalIdentityBinding),
		allowList:               make(map[string]*store.AllowListEntry),
		inviteCodes:             make(map[string]*store.InviteCode),
		envVars:                 make(map[string]*store.EnvVar),
		secrets:                 make(map[string]*store.Secret),
		encryptionMigrations:    make(map[string]*store.EncryptionMigration),
		groups:                  make(map[string]*store.Group),
		groupMembers:            make(map[string][]store.GroupMember),
		roleDefinitions:         make(map[string]*store.RoleDefinition),
		roleBindings:            make(map[string]*store.RoleBinding),
		delegationEdges:         make(map[string]*store.DelegationEdge),
		delegationAdoptions:     make(map[string]*store.DelegationAdoption),
		accessConstraints:       make(map[string]*store.AccessConstraint),
		accessConstraintHistory: make(map[string][]*store.AccessConstraintHistory),
		membershipLossChecks:    make(map[string]*store.MembershipLossCheck),
		limitDefinitions:        make(map[string]*store.LimitDefinition),
		entitlementBindings:     make(map[string]*store.EntitlementBinding),
		usageReservations:       make(map[string]*store.UsageReservation),
		notifSubscriptions:      make(map[string]*store.NotificationSubscription),
		notifications:           make(map[string]*store.Notification),
		subscriptionTemplates:   make(map[string]*store.SubscriptionTemplate),
		scheduledEvents:         make(map[string]*store.ScheduledEvent),
		schedules:               make(map[string]*store.Schedule),
		gcpServiceAccounts:      make(map[string]*store.GCPServiceAccount),
		githubInstallations:     make(map[int64]*store.GitHubInstallation),
		messages:                make(map[string]*store.Message),
		conversations:           make(map[string]*store.Conversation),
		participants:            make(map[string][]store.ConversationParticipant),
		addressees:              make(map[string][]store.MessageAddressee),
		threadGroups:            make(map[string]*store.ThreadGroup),
		threadGroupMembers:      make(map[string]string),
		maintenanceOps:          make(map[string]*store.MaintenanceOperation),
		maintenanceRuns:         make(map[string]*store.MaintenanceOperationRun),
		lifecycleHooks:          make(map[string]*store.LifecycleHook),
		hookPhases:              make(map[string]string),
		skills:                  make(map[string]*store.Skill),
		skillVersions:           make(map[string]*store.SkillVersion),
		skillRegistries:         make(map[string]*store.SkillRegistry),
		skillPinnedHashes:       make(map[string]map[string]string),
		skillInjections:         make(map[string]*store.SkillInjection),
		hubSettings:             make(map[string]*store.HubSetting),
		agentSessionMetrics:     make(map[string]*store.AgentSessionMetrics),
		hubInstances:            make(map[string]store.HubInstance),
		conduitRelays:           make(map[string]registry.RelayInstance),
		conduitSessions:         make(map[string]registry.SessionRecord),
		conduitEpochs:           make(map[string]int64),
	}
	s.seedDefaultMaintenanceOps()
	return s
}

func (s *RestateStore) seedDefaultMaintenanceOps() {
	now := time.Now().UTC()
	ops := []store.MaintenanceOperation{
		{Key: "secret-hub-id-migration", Title: "Secret Hub ID Namespace Migration", Category: store.MaintenanceCategoryMigration},
		{Key: "applied-config-env-cleanup", Title: "Applied Config Env Cleanup", Category: store.MaintenanceCategoryMigration},
		{Key: AppliedConfigTZCleanupKey, Title: "Applied Config TZ Cleanup", Category: store.MaintenanceCategoryMigration},
		{Key: AutoExposeEnvNormalizeKey, Title: "Auto-Expose Env Normalize", Category: store.MaintenanceCategoryMigration},
		{Key: UTCTimestampNormalizeKey, Title: "UTC Timestamp Normalize", Category: store.MaintenanceCategoryMigration},
		{Key: GroupParticipantModeCleanupKey, Title: "Group Conversation Participant Cleanup", Category: store.MaintenanceCategoryMigration},
		{Key: ProjectStorageMigrateKey, Title: "Project Storage Migration", Category: store.MaintenanceCategoryMigration},
		{Key: "pull-images", Title: "Pull Container Images", Category: store.MaintenanceCategoryOperation},
		{Key: "rebuild-server", Title: "Rebuild Server from Git", Category: store.MaintenanceCategoryOperation},
		{Key: "rebuild-web", Title: "Rebuild Web Frontend", Category: store.MaintenanceCategoryOperation},
		{Key: "rebuild-container-binaries", Title: "Rebuild Container Binaries", Category: store.MaintenanceCategoryOperation},
		{Key: "build-harness-config-image", Title: "Build Harness Config Image", Category: store.MaintenanceCategoryOperation},
		{Key: TemplateBlobGCKey, Title: "Template Blob Garbage Collection", Category: store.MaintenanceCategoryOperation},
		{Key: PassthroughTranslationReviewKey, Title: "Sandbox Passthrough Translation Review", Category: store.MaintenanceCategoryOperation},
		{Key: ChatStarReconcileKey, Title: "Starred Messages Reconciliation", Category: store.MaintenanceCategoryOperation},
		{Key: "update-binary", Title: "Update Binary from Release", Category: store.MaintenanceCategoryOperation},
	}
	for _, op := range ops {
		cp := op
		cp.ID = uuid.NewString()
		cp.Status = "idle"
		cp.CreatedAt = now
		s.maintenanceOps[cp.Key] = &cp
	}
}

// Host returns the bound Restate SDK Host.
func (s *RestateStore) Host() *Host { return s.host }

// Supervisor returns the bound Restate Supervisor, if any.
func (s *RestateStore) Supervisor() *Supervisor { return s.sup }

// Close stops the Restate host and supervisor if managed by this store.
func (s *RestateStore) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if s.host != nil {
		_ = s.host.Stop(ctx)
	}
	if s.sup != nil {
		_ = s.sup.Stop(ctx)
	}
	return nil
}

// ingressActive reports whether the Restate ingress server is running and ready.
func (s *RestateStore) ingressActive() bool {
	return s.host != nil && s.sup != nil && s.sup.Started()
}

// Ping verifies Restate readiness when a Supervisor is actively running.
func (s *RestateStore) Ping(ctx context.Context) error {
	if s.ingressActive() {
		return s.sup.WaitReady(ctx)
	}
	return nil
}

// Migrate is a no-op on Restate (schema-free Virtual Object state) beyond ensuring default seeds.
func (s *RestateStore) Migrate(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.maintenanceOps) == 0 {
		s.seedDefaultMaintenanceOps()
	}
	return nil
}

// WithTx executes fn against the store.
func (s *RestateStore) WithTx(_ context.Context, fn func(tx store.Store) error) error {
	return fn(s)
}

func cloneAgent(a *store.Agent) *store.Agent {
	if a == nil {
		return nil
	}
	cp := *a
	if a.Labels != nil {
		cp.Labels = maps.Clone(a.Labels)
	}
	if a.Annotations != nil {
		cp.Annotations = maps.Clone(a.Annotations)
	}
	if a.Ancestry != nil {
		cp.Ancestry = slices.Clone(a.Ancestry)
	}
	if a.PreviousRunIDs != nil {
		cp.PreviousRunIDs = slices.Clone(a.PreviousRunIDs)
	}
	return &cp
}

func (s *RestateStore) syncAgentToRestate(ctx context.Context, a *store.Agent, opKind string) {
	if !s.ingressActive() || a == nil {
		return
	}
	_, _ = InvokeObject[ClaimOpRequest, ClaimOpResponse](ctx, s.host, ServiceAgentObject, a.ID, "ClaimOp", ClaimOpRequest{
		OpID:    "sync-" + a.ID + "-" + fmt.Sprintf("%d", a.StateVersion),
		OpKind:  opKind,
		Preempt: true,
		Spec: &AgentSpec{
			AgentID:   a.ID,
			Slug:      a.Slug,
			Name:      a.Name,
			ProjectID: a.ProjectID,
			BrokerID:  a.RuntimeBrokerID,
			Template:  a.Template,
			Image:     a.Image,
			Runtime:   a.Runtime,
			OwnerID:   a.OwnerID,
			CreatedBy: a.CreatedBy,
			Ancestry:  a.Ancestry,
			Labels:    a.Labels,
		},
		Now: time.Now().UTC(),
	})
}

// --- Top-level Store methods ---

func (s *RestateStore) FinalizeAgentDeletion(ctx context.Context, id string, pred store.DeletionPredicate, mode store.DeletionFinalizeMode, set store.DeletionFields, hook store.DeletionFinalizeHook) (int, error) {
	s.mu.Lock()
	a, ok := s.agents[id]
	if !ok {
		s.mu.Unlock()
		return 0, nil
	}
	if !pred.Matches(a) {
		s.mu.Unlock()
		return 0, nil
	}
	cp := cloneAgent(a)
	if mode == store.DeletionFinalizeSoft {
		now := time.Now().UTC()
		if set.DeletedAt != nil {
			cp.DeletedAt = *set.DeletedAt
		} else if cp.DeletedAt.IsZero() {
			cp.DeletedAt = now
		}
		if set.Phase != nil {
			cp.Phase = *set.Phase
		}
		cp.StateVersion++
		cp.Updated = now
		s.agents[id] = cp
	} else {
		delete(s.agents, id)
	}
	s.mu.Unlock()

	if hook != nil {
		if err := hook(ctx, s, cp, mode); err != nil {
			return 0, err
		}
	}
	return 1, nil
}

func (s *RestateStore) PurgeDeletedAgents(_ context.Context, cutoff time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	purged := 0
	for id, a := range s.agents {
		if !a.DeletedAt.IsZero() && a.DeletedAt.Before(cutoff) {
			delete(s.agents, id)
			purged++
		}
	}
	return purged, nil
}

func (s *RestateStore) PurgeDeletedAgentsByID(_ context.Context, ids []string, cutoff time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	purged := 0
	for _, id := range ids {
		if a, ok := s.agents[id]; ok && !a.DeletedAt.IsZero() && a.DeletedAt.Before(cutoff) {
			delete(s.agents, id)
			purged++
		}
	}
	return purged, nil
}

func (s *RestateStore) ListSoftDeletedAgentsBefore(_ context.Context, cutoff time.Time, afterID string, limit int) ([]*store.Agent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.Agent
	for _, a := range s.agents {
		if !a.DeletedAt.IsZero() && a.DeletedAt.Before(cutoff) && a.ID > afterID {
			out = append(out, cloneAgent(a))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *RestateStore) TryAdvisoryLock(_ context.Context, _ store.AdvisoryLockKey) (bool, func() error, error) {
	return true, func() error { return nil }, nil
}

func (s *RestateStore) CreateTestAdminUser(_ context.Context, user *store.User, roleDefinitionID string) (*store.RoleBinding, error) {
	if user == nil || user.ExpiresAt == nil || user.IssuedBy == nil || *user.IssuedBy == "" || user.IssuanceID == nil || *user.IssuanceID == "" || roleDefinitionID == "" {
		return nil, store.ErrInvalidInput
	}
	s.mu.Lock()
	if user.ID == "" {
		user.ID = uuid.NewString()
	}
	now := time.Now().UTC()
	if user.Created.IsZero() {
		user.Created = now
	}
	cp := *user
	s.users[user.ID] = &cp
	rb := &store.RoleBinding{
		ID:               uuid.NewString(),
		PrincipalType:    "user",
		PrincipalID:      user.ID,
		RoleDefinitionID: roleDefinitionID,
		ScopeType:        "system",
		ExpiresAt:        user.ExpiresAt,
		CreatedBy:        store.TestIdentityIssuanceCreatedBy,
		CreatedAt:        now,
		IssuanceID:       user.IssuanceID,
	}
	s.roleBindings[rb.ID] = rb
	s.mu.Unlock()
	return rb, nil
}

func (s *RestateStore) CreateFederatedUser(_ context.Context, user *store.User) error {
	if user == nil || user.Kind != store.UserKindFederated || user.HomeIssuer == nil || user.FederatedEmail == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if user.ID == "" {
		user.ID = uuid.NewString()
	}
	hi := store.NormalizeFederatedIssuer(*user.HomeIssuer)
	fe := store.NormalizeFederatedEmail(*user.FederatedEmail)
	user.HomeIssuer = &hi
	user.FederatedEmail = &fe
	user.Role = "viewer"
	email, err := store.FederatedSyntheticEmail(user.ID, hi)
	if err != nil {
		return err
	}
	user.Email = email
	for _, u := range s.users {
		if u.Kind == store.UserKindFederated && u.HomeIssuer != nil && *u.HomeIssuer == hi && u.FederatedEmail != nil && *u.FederatedEmail == fe {
			return store.ErrAlreadyExists
		}
	}
	cp := *user
	s.users[user.ID] = &cp
	return nil
}

func (s *RestateStore) GetFederatedUserByEmail(_ context.Context, homeIssuer, federatedEmail string) (*store.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	hi := store.NormalizeFederatedIssuer(homeIssuer)
	fe := store.NormalizeFederatedEmail(federatedEmail)
	for _, u := range s.users {
		if u.Kind == store.UserKindFederated && u.HomeIssuer != nil && *u.HomeIssuer == hi && u.FederatedEmail != nil && *u.FederatedEmail == fe {
			cp := *u
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) LinkFederatedIdentity(_ context.Context, userID, issuer, subject, email string) (*store.ExternalIdentityBinding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[userID]
	if !ok || u.Kind != store.UserKindFederated {
		return nil, store.ErrInvalidInput
	}
	normIss := store.NormalizeFederatedIssuer(issuer)
	key := store.FederationIdentityProvider + ":" + normIss + ":" + subject
	if _, exists := s.externalIdentities[key]; exists {
		return nil, store.ErrAlreadyExists
	}
	b := &store.ExternalIdentityBinding{
		ID:        uuid.NewString(),
		Provider:  store.FederationIdentityProvider,
		Issuer:    normIss,
		Subject:   subject,
		UserID:    userID,
		Email:     strings.ToLower(email),
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	s.externalIdentities[key] = b
	return b, nil
}

func (s *RestateStore) GetFederatedUserBySubject(_ context.Context, issuer, subject string) (*store.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	normIss := store.NormalizeFederatedIssuer(issuer)
	key := store.FederationIdentityProvider + ":" + normIss + ":" + subject
	b, ok := s.externalIdentities[key]
	if !ok {
		return nil, store.ErrNotFound
	}
	u, ok := s.users[b.UserID]
	if !ok || u.Kind != store.UserKindFederated {
		return nil, store.ErrNotFound
	}
	cp := *u
	return &cp, nil
}

func (s *RestateStore) LinkPendingFederatedUser(ctx context.Context, userID, issuer, subject, email string) (*store.User, error) {
	if _, err := s.LinkFederatedIdentity(ctx, userID, issuer, subject, email); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[userID]
	if !ok {
		return nil, store.ErrNotFound
	}
	u.Status = store.UserStatusActive
	cp := *u
	return &cp, nil
}

func (s *RestateStore) CreateProjectVisitorRule(_ context.Context, rule *store.ProjectVisitorRule) error {
	if rule == nil || rule.ProjectID == "" || rule.Issuer == "" || rule.Domain == "" {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if rule.ID == "" {
		rule.ID = uuid.NewString()
	}
	rule.Issuer = store.NormalizeFederatedIssuer(rule.Issuer)
	rule.Domain = store.NormalizeVisitorRuleDomain(rule.Domain)
	rule.CreatedAt = time.Now().UTC()
	cp := *rule
	s.projectVisitorRules[rule.ID] = &cp
	return nil
}

func (s *RestateStore) GetProjectVisitorRule(_ context.Context, id string) (*store.ProjectVisitorRule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.projectVisitorRules[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *r
	return &cp, nil
}

func (s *RestateStore) ListProjectVisitorRules(_ context.Context, projectID string) ([]*store.ProjectVisitorRule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.ProjectVisitorRule
	for _, r := range s.projectVisitorRules {
		if r.ProjectID == projectID {
			cp := *r
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *RestateStore) ListActiveProjectVisitorRules(_ context.Context, issuer, domain string, at time.Time) ([]*store.ProjectVisitorRule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	normIss := store.NormalizeFederatedIssuer(issuer)
	normDom := store.NormalizeVisitorRuleDomain(domain)
	var out []*store.ProjectVisitorRule
	for _, r := range s.projectVisitorRules {
		if r.Issuer == normIss && r.Domain == normDom && r.ActiveAt(at) {
			cp := *r
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *RestateStore) UpdateProjectVisitorRuleWindow(_ context.Context, id string, notBefore, expiresAt *time.Time) (*store.ProjectVisitorRule, []*store.RoleBinding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.projectVisitorRules[id]
	if !ok {
		return nil, nil, store.ErrNotFound
	}
	r.NotBefore = notBefore
	r.ExpiresAt = expiresAt
	cp := *r
	var updated []*store.RoleBinding
	for _, rb := range s.roleBindings {
		if rb.SourceRuleID != nil && *rb.SourceRuleID == id {
			rb.NotBefore = notBefore
			rb.ExpiresAt = expiresAt
			rbCp := *rb
			updated = append(updated, &rbCp)
		}
	}
	return &cp, updated, nil
}

func (s *RestateStore) DeleteProjectVisitorRule(_ context.Context, id string) ([]*store.RoleBinding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.projectVisitorRules[id]; !ok {
		return nil, store.ErrNotFound
	}
	delete(s.projectVisitorRules, id)
	delete(s.projectVisitorExcluded, id)
	var deleted []*store.RoleBinding
	for rbID, rb := range s.roleBindings {
		if rb.SourceRuleID != nil && *rb.SourceRuleID == id {
			rbCp := *rb
			deleted = append(deleted, &rbCp)
			delete(s.roleBindings, rbID)
		}
	}
	return deleted, nil
}

func (s *RestateStore) DeleteProjectVisitorRulesByProject(_ context.Context, projectID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, r := range s.projectVisitorRules {
		if r.ProjectID == projectID {
			delete(s.projectVisitorRules, id)
			delete(s.projectVisitorExcluded, id)
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) DeleteInactiveVisitorGrant(_ context.Context, bindingID string, at time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rb, ok := s.roleBindings[bindingID]
	if !ok || rb.SourceRuleID == nil || *rb.SourceRuleID == "" {
		return false, nil
	}
	if rb.ExpiresAt != nil && !at.Before(*rb.ExpiresAt) {
		delete(s.roleBindings, bindingID)
		return true, nil
	}
	return false, nil
}

func (s *RestateStore) CreateProjectVisitorExclusion(_ context.Context, ruleID, userID, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.projectVisitorRules[ruleID]; !ok {
		return store.ErrNotFound
	}
	if s.projectVisitorExcluded[ruleID] == nil {
		s.projectVisitorExcluded[ruleID] = make(map[string]bool)
	}
	s.projectVisitorExcluded[ruleID][userID] = true
	return nil
}

func (s *RestateStore) IsProjectVisitorExcluded(_ context.Context, ruleID, userID string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.projectVisitorExcluded[ruleID][userID], nil
}

func (s *RestateStore) ListFixtureRootedResources(_ context.Context, rootID string, limit int) (*store.FixtureRootedResources, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := &store.FixtureRootedResources{}
	for _, a := range s.agents {
		if a.Fixture.RootID == rootID {
			res.Agents = append(res.Agents, *cloneAgent(a))
		}
	}
	if limit > 0 && len(res.Agents) > limit {
		res.Agents = res.Agents[:limit]
		res.Truncated = true
	}
	return res, nil
}

func (s *RestateStore) ListFixtureRoots(_ context.Context, issuerID, afterRootID string, limit int) ([]store.FixtureProvenance, bool, error) {
	return nil, false, nil
}

func (s *RestateStore) ListIssuanceStampedGrants(_ context.Context, issuanceID string, limit int) ([]store.IssuanceStampedGrant, bool, error) {
	if issuanceID == "" {
		return nil, false, store.ErrInvalidInput
	}
	return nil, false, nil
}

func (s *RestateStore) DeleteIssuanceStampedGrant(_ context.Context, _ store.IssuanceStampedGrant, _ string) (bool, error) {
	return false, nil
}

// --- AgentStore ---

func (s *RestateStore) CreateAgent(ctx context.Context, a *store.Agent) error {
	if a == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	if a.ID == "" {
		a.ID = uuid.NewString()
	}
	if _, exists := s.agents[a.ID]; exists {
		s.mu.Unlock()
		return store.ErrAlreadyExists
	}
	now := time.Now().UTC()
	if a.Created.IsZero() {
		a.Created = now
	}
	a.Updated = now
	if a.Generation <= 0 {
		a.Generation = 1
	}
	if a.StateVersion <= 0 {
		a.StateVersion = 1
	}
	cp := cloneAgent(a)
	s.agents[a.ID] = cp
	s.mu.Unlock()

	s.syncAgentToRestate(ctx, cp, OpKindCreate)
	return nil
}

func (s *RestateStore) GetAgent(_ context.Context, id string) (*store.Agent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.agents[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return cloneAgent(a), nil
}

func (s *RestateStore) GetAgentRunState(_ context.Context, id string) (store.AgentRunState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.agents[id]
	if !ok {
		return store.AgentRunState{}, store.ErrNotFound
	}
	return store.AgentRunState{RunID: a.RunID, DeletedAt: a.DeletedAt}, nil
}

func (s *RestateStore) GetAgentBySlug(_ context.Context, projectID, slug string) (*store.Agent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, a := range s.agents {
		if a.ProjectID == projectID && strings.EqualFold(a.Slug, slug) && a.DeletedAt.IsZero() {
			return cloneAgent(a), nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *RestateStore) GetAgentsByIDs(_ context.Context, ids []string) (map[string]*store.Agent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]*store.Agent, len(ids))
	for _, id := range ids {
		if a, ok := s.agents[id]; ok && a.DeletedAt.IsZero() {
			out[id] = cloneAgent(a)
		}
	}
	return out, nil
}

func (s *RestateStore) GetAgentsByIDsIncludingDeleted(_ context.Context, ids []string) (map[string]*store.Agent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]*store.Agent, len(ids))
	for _, id := range ids {
		if a, ok := s.agents[id]; ok {
			out[id] = cloneAgent(a)
		}
	}
	return out, nil
}

func (s *RestateStore) UpdateAgent(ctx context.Context, a *store.Agent) error {
	if a == nil {
		return store.ErrInvalidInput
	}
	s.mu.Lock()
	cur, ok := s.agents[a.ID]
	if !ok {
		s.mu.Unlock()
		return store.ErrNotFound
	}
	if a.StateVersion > 0 && cur.StateVersion != a.StateVersion {
		s.mu.Unlock()
		return store.ErrVersionConflict
	}
	cp := cloneAgent(a)
	cp.StateVersion = cur.StateVersion + 1
	cp.Updated = time.Now().UTC()
	s.agents[a.ID] = cp
	a.StateVersion = cp.StateVersion
	a.Updated = cp.Updated
	s.mu.Unlock()

	s.syncAgentToRestate(ctx, cp, OpKindStart)
	return nil
}

func (s *RestateStore) UpdateAgentConfig(_ context.Context, agentID string, expectedVersion int64, applied *store.AgentAppliedConfig, pending *store.AgentPendingConfig) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.agents[agentID]
	if !ok || !cur.DeletedAt.IsZero() {
		return 0, store.ErrNotFound
	}
	if expectedVersion > 0 && cur.StateVersion != expectedVersion {
		return 0, store.ErrVersionConflict
	}
	cur.AppliedConfig = applied
	cur.PendingConfig = pending
	cur.StateVersion++
	cur.Updated = time.Now().UTC()
	return cur.StateVersion, nil
}

func (s *RestateStore) SetAgentSoftDeleteOpID(_ context.Context, agentID, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.agents[agentID]; !ok {
		return store.ErrNotFound
	}
	return nil
}

func (s *RestateStore) DeleteAgent(ctx context.Context, id string) error {
	s.mu.Lock()
	a, ok := s.agents[id]
	if !ok {
		s.mu.Unlock()
		return store.ErrNotFound
	}
	delete(s.agents, id)
	s.mu.Unlock()

	s.syncAgentToRestate(ctx, a, OpKindDelete)
	return nil
}

func (s *RestateStore) LockAgentRows(_ context.Context, _ []string) error {
	return nil
}

func (s *RestateStore) matchesAgentFilter(a *store.Agent, filter store.AgentFilter) bool {
	if !filter.IncludeDeleted && !a.DeletedAt.IsZero() {
		return false
	}
	if filter.ProjectID != "" && a.ProjectID != filter.ProjectID {
		return false
	}
	if filter.RuntimeBrokerID != "" && a.RuntimeBrokerID != filter.RuntimeBrokerID {
		return false
	}
	if filter.Phase != "" && a.Phase != filter.Phase {
		if filter.OrRunIntent == "" || string(a.RunIntent) != filter.OrRunIntent {
			return false
		}
	}
	if filter.RequestedOwnerID != "" && a.OwnerID != filter.RequestedOwnerID {
		return false
	}
	if filter.CreatedBy != "" && a.CreatedBy != filter.CreatedBy {
		return false
	}
	if filter.AuthorizedProjectIDs != nil {
		if !slices.Contains(filter.AuthorizedProjectIDs, a.ProjectID) {
			return false
		}
	}
	if len(filter.ExcludedProjectIDs) > 0 && slices.Contains(filter.ExcludedProjectIDs, a.ProjectID) {
		return false
	}
	if filter.IDs != nil && !slices.Contains(filter.IDs, a.ID) {
		return false
	}
	for k, v := range filter.Labels {
		if a.Labels[k] != v {
			return false
		}
	}
	return true
}

func (s *RestateStore) ListAgents(_ context.Context, filter store.AgentFilter, opts store.ListOptions) (*store.ListResult[store.Agent], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var items []store.Agent
	for _, a := range s.agents {
		if s.matchesAgentFilter(a, filter) {
			items = append(items, *cloneAgent(a))
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Created.Equal(items[j].Created) {
			return items[i].ID > items[j].ID
		}
		return items[i].Created.After(items[j].Created)
	})
	total := len(items)
	if opts.Limit > 0 && len(items) > opts.Limit {
		items = items[:opts.Limit]
	}
	return &store.ListResult[store.Agent]{Items: items, TotalCount: total}, nil
}

func (s *RestateStore) CountAgents(_ context.Context, filter store.AgentFilter) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, a := range s.agents {
		if s.matchesAgentFilter(a, filter) {
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) ListAgentMembers(_ context.Context, filter store.AgentFilter, sortField, dir string, max int) ([]store.AgentMember, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.AgentMember
	for _, a := range s.agents {
		if s.matchesAgentFilter(a, filter) {
			out = append(out, store.AgentMember{
				ID:                a.ID,
				OwnerID:           a.OwnerID,
				ProjectID:         a.ProjectID,
				Labels:            maps.Clone(a.Labels),
				Ancestry:          slices.Clone(a.Ancestry),
				Phase:             a.Phase,
				Created:           a.Created,
				Updated:           a.Updated,
				LastActivityEvent: a.LastActivityEvent,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		ra := agentsort.KeyFor(sortField, out[i].ID, out[i].Created, out[i].Updated, out[i].LastActivityEvent)
		rb := agentsort.KeyFor(sortField, out[j].ID, out[j].Created, out[j].Updated, out[j].LastActivityEvent)
		return agentsort.Less(dir, ra, rb)
	})
	if max > 0 && len(out) > max {
		out = out[:max]
	}
	return out, nil
}

func (s *RestateStore) CountAgentsByPhaseIDs(_ context.Context, filter store.AgentFilter) ([]store.IDPhase, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.IDPhase
	for _, a := range s.agents {
		if s.matchesAgentFilter(a, filter) {
			out = append(out, store.IDPhase{ID: a.ID, Phase: a.Phase})
		}
	}
	return out, nil
}

func (s *RestateStore) ListAgentsWithStaleNonTerminalReincarnationState(_ context.Context, _ time.Time) ([]*store.Agent, error) {
	return nil, nil
}

func (s *RestateStore) SetAgentRunID(_ context.Context, agentID, runID string, cred *store.AgentCredential) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[agentID]
	if !ok {
		return "", store.ErrNotFound
	}
	if !a.DeletedAt.IsZero() {
		return "", store.ErrDeleteInProgress
	}
	prev := a.RunID
	if prev != "" && prev != runID {
		a.PreviousRunIDs, _ = store.AppendPreviousRunID(a.PreviousRunIDs, prev, runID)
	}
	a.RunID = runID
	now := time.Now().UTC()
	a.RunIDUpdatedAt = &now
	if cred != nil {
		if cred.ID == "" {
			cred.ID = uuid.NewString()
		}
		cred.RunID = runID
		cp := *cred
		s.agentCreds[cred.ID] = &cp
	}
	return prev, nil
}

func (s *RestateStore) SetAgentSSHHostKey(_ context.Context, agentID, runID, publicKey string) (store.SSHHostKeyResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[agentID]
	if !ok || a.RunID == "" || a.RunID != runID {
		return store.SSHHostKeyNotCurrentRun, nil
	}
	if a.SSHHostKeyRunID == runID && a.SSHHostKey != "" {
		if a.SSHHostKey == publicKey {
			return store.SSHHostKeyUnchanged, nil
		}
		return store.SSHHostKeyAlreadySet, nil
	}
	a.SSHHostKey = publicKey
	a.SSHHostKeyRunID = runID
	return store.SSHHostKeyWritten, nil
}

func (s *RestateStore) CompareAndSwapAgentRunID(_ context.Context, agentID, expectedRunID, newRunID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[agentID]
	if !ok || a.RunID != expectedRunID {
		return false, nil
	}
	a.RunID = newRunID
	a.PreviousRunIDs = nil
	now := time.Now().UTC()
	a.RunIDUpdatedAt = &now
	return true, nil
}

func (s *RestateStore) RevertAgentRunID(_ context.Context, agentID, mintedRunID, previousRunID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[agentID]
	if !ok || a.RunID != mintedRunID {
		return false, nil
	}
	a.RunID = previousRunID
	now := time.Now().UTC()
	a.RunIDUpdatedAt = &now
	return true, nil
}

func (s *RestateStore) AdoptAgentRunID(_ context.Context, agentID, expectedRunID, newRunID string, _ time.Time) (bool, error) {
	return s.CompareAndSwapAgentRunID(context.Background(), agentID, expectedRunID, newRunID)
}

func (s *RestateStore) RecordBrokerReportedRuns(_ context.Context, brokerID string, runs []store.BrokerReportedRun, _, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.brokerReportedRuns[brokerID] = slices.Clone(runs)
	return nil
}

func (s *RestateStore) AgentRunReportedSince(_ context.Context, agentID, runID string, _ time.Time) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, runs := range s.brokerReportedRuns {
		for _, r := range runs {
			if r.AgentID == agentID && r.RunID == runID {
				return true, nil
			}
		}
	}
	return false, nil
}

func (s *RestateStore) UpdateAgentStatus(_ context.Context, id string, status store.AgentStatusUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[id]
	if !ok {
		return store.ErrNotFound
	}
	if status.IfPhase != "" && a.Phase != status.IfPhase {
		return store.ErrPhaseMismatch
	}
	if status.IfRunID != "" && a.RunID != status.IfRunID {
		return store.ErrRunChanged
	}
	now := time.Now().UTC()
	if status.Phase != "" {
		a.Phase = status.Phase
	}
	if status.Activity != "" {
		a.Activity = status.Activity
	}
	if status.ToolName != "" {
		a.ToolName = status.ToolName
	}
	if status.Message != "" {
		a.Message = status.Message
	} else if status.ClearMessageIf != "" && a.Message == status.ClearMessageIf {
		a.Message = ""
	}
	if status.ConnectionState != "" {
		a.ConnectionState = status.ConnectionState
	}
	if status.ContainerStatus != "" {
		a.ContainerStatus = status.ContainerStatus
	}
	if status.RuntimeState != "" {
		a.RuntimeState = status.RuntimeState
	}
	if status.TaskSummary != "" {
		a.TaskSummary = status.TaskSummary
	}
	if status.Heartbeat {
		a.LastSeen = now
	}
	if status.ExitCode != nil {
		a.ExitCode = status.ExitCode
	}
	if status.ExitReason != "" {
		a.ExitReason = status.ExitReason
	}
	if status.ClearExit || status.ClearTerminalRemnants {
		a.ExitCode = nil
		a.ExitReason = ""
	}
	a.Updated = now
	if status.Result != nil && a.Phase == "halted" {
		status.Result.Halted = true
	}
	return nil
}

func (s *RestateStore) SetAgentWorkspacePlacement(_ context.Context, agentID, placement string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[agentID]
	if !ok || !a.DeletedAt.IsZero() {
		return store.ErrNotFound
	}
	a.WorkspacePlacement = placement
	return nil
}

func (s *RestateStore) SetAgentAnnotation(_ context.Context, agentID, key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[agentID]
	if !ok || !a.DeletedAt.IsZero() {
		return store.ErrNotFound
	}
	if a.Annotations == nil {
		a.Annotations = make(map[string]string)
	}
	if value == "" {
		delete(a.Annotations, key)
	} else {
		a.Annotations[key] = value
	}
	return nil
}

func (s *RestateStore) UpdateAgentExposedPorts(_ context.Context, id string, ports []store.ExposedPort) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[id]
	if !ok {
		return store.ErrNotFound
	}
	a.ExposedPorts = slices.Clone(ports)
	return nil
}

func (s *RestateStore) MarkStaleAgentsOffline(_ context.Context, _ time.Time) ([]store.Agent, error) {
	return nil, nil
}

func (s *RestateStore) MarkStalledAgents(_ context.Context, _, _ time.Time) ([]store.Agent, error) {
	return nil, nil
}

func (s *RestateStore) MarkAgentContainerMissing(_ context.Context, _, _ string, _ time.Time, _, _ string) (*store.Agent, error) {
	return nil, nil
}

func (s *RestateStore) MarkAgentContainerMissingIfUnchanged(_ context.Context, _, _ string, _ time.Time, _ store.ContainerMissingPrecondition, _ string) (*store.Agent, error) {
	return nil, nil
}

func (s *RestateStore) ClearAgentRuntimeTarget(_ context.Context, id string) (bool, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[id]
	if !ok || a.AppliedConfig == nil {
		return false, 0, nil
	}
	if a.AppliedConfig.RuntimeTarget == "" && a.AppliedConfig.RuntimeTargetCandidate == "" {
		return false, a.StateVersion, nil
	}
	a.AppliedConfig.RuntimeTarget = ""
	a.AppliedConfig.RuntimeTargetCandidate = ""
	a.StateVersion++
	return true, a.StateVersion, nil
}

func (s *RestateStore) SetAgentRuntimeTarget(_ context.Context, id string, expectedVersion int64, target, candidate string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[id]
	if !ok || a.StateVersion != expectedVersion {
		return false, nil
	}
	if a.AppliedConfig == nil {
		a.AppliedConfig = &store.AgentAppliedConfig{}
	}
	a.AppliedConfig.RuntimeTarget = target
	a.AppliedConfig.RuntimeTargetCandidate = candidate
	return true, nil
}

func (s *RestateStore) FindOrphanedAgents(_ context.Context, _ string) ([]*store.Agent, error) {
	return nil, nil
}

func (s *RestateStore) ReassignAgentsToBroker(_ context.Context, agents []*store.Agent, brokerID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, in := range agents {
		if a, ok := s.agents[in.ID]; ok {
			a.RuntimeBrokerID = brokerID
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) ReassignProjectBroker(_ context.Context, oldBrokerID, newBrokerID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, p := range s.projects {
		if p.DefaultRuntimeBrokerID == oldBrokerID {
			p.DefaultRuntimeBrokerID = newBrokerID
			n++
		}
	}
	return n, nil
}

func (s *RestateStore) SetAgentPinnedRuntimeTarget(_ context.Context, agentID string, _, next store.PinnedPlacement) (*store.Agent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[agentID]
	if !ok {
		return nil, store.ErrNotFound
	}
	a.RuntimeBrokerID = next.RuntimeBrokerID
	a.PinnedRuntimeBrokerID = next.RuntimeBrokerID
	a.PinnedRuntimeTargetID = next.RuntimeTargetID
	a.PinnedRuntimeTargetType = next.RuntimeTargetType
	a.StateVersion++
	return cloneAgent(a), nil
}

func (s *RestateStore) AggregateAgentHealth(_ context.Context) (*store.AgentHealthAggregate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	agg := &store.AgentHealthAggregate{
		ByPhase:  make(map[string]int),
		ByBroker: make(map[string]store.AgentBrokerCounts),
	}
	for _, a := range s.agents {
		if !a.DeletedAt.IsZero() {
			continue
		}
		agg.Total++
		agg.ByPhase[a.Phase]++
	}
	return agg, nil
}

func (s *RestateStore) BeginLaunch(_ context.Context, agentID, kind string, timeout time.Duration) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[agentID]
	if !ok {
		return "", store.ErrNotFound
	}
	launchID := uuid.NewString()
	a.LaunchID = launchID
	a.LaunchState = store.LaunchStateActive
	a.LaunchKind = kind
	a.LaunchDeadline = time.Now().UTC().Add(timeout)
	return launchID, nil
}

func (s *RestateStore) BeginStartLaunch(_ context.Context, agentID, kind string, timeout time.Duration, opts store.BeginLaunchOptions) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[agentID]
	if !ok {
		return "", store.ErrNotFound
	}
	launchID := uuid.NewString()
	a.LaunchID = launchID
	a.LaunchState = store.LaunchStateActive
	a.LaunchKind = kind
	a.LaunchPriorPhase = opts.PriorPhase
	a.LaunchRevokeOnFail = opts.RevokeOnFail
	a.LaunchDeadline = time.Now().UTC().Add(timeout)
	return launchID, nil
}

func (s *RestateStore) MarkLaunchAccepted(_ context.Context, agentID, launchID, owner string) (store.Agent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[agentID]
	if !ok {
		return store.Agent{}, store.ErrNotFound
	}
	if a.LaunchID == launchID && a.LaunchState == store.LaunchStateActive {
		if a.LaunchOwner == "" {
			a.LaunchOwner = owner
		}
		if a.Phase == "created" {
			a.Phase = "provisioning"
		}
	}
	return *cloneAgent(a), nil
}

func (s *RestateStore) EndLaunch(_ context.Context, agentID, launchID, reason string) error {
	return s.EndLaunchWithCode(context.Background(), agentID, launchID, reason, "")
}

func (s *RestateStore) EndLaunchWithCode(_ context.Context, agentID, launchID, reason, code string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[agentID]
	if !ok {
		return store.ErrNotFound
	}
	if a.LaunchID == launchID && a.LaunchState == store.LaunchStateActive {
		a.LaunchState = store.LaunchStateEnded
		a.LaunchEndReason = reason
		a.LaunchError = code
	}
	return nil
}

func (s *RestateStore) ApplyLaunchReport(_ context.Context, agentID, _ string, r store.LaunchReport) (store.LaunchReportAnswer, store.Agent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[agentID]
	if !ok {
		return store.LaunchReportAnswer{}, store.Agent{}, store.ErrNotFound
	}
	a.LaunchSeq = r.Seq
	a.LaunchStep = r.Step
	a.LaunchLastReportAt = time.Now().UTC()
	if r.State == store.LaunchReportStateSucceeded {
		a.LaunchState = store.LaunchStateEnded
		a.LaunchEndReason = store.LaunchEndReasonSucceeded
		a.Phase = "running"
		a.StateVersion++
	} else if r.State == store.LaunchReportStateFailed {
		a.LaunchState = store.LaunchStateEnded
		a.LaunchEndReason = store.LaunchEndReasonFailed
		a.LaunchError = r.ErrorCode
		a.LaunchCause = r.Cause
		a.Phase = "error"
		a.StateVersion++
	}
	return store.LaunchReportAnswer{Result: store.LaunchReportResultApplied, Changed: true}, *cloneAgent(a), nil
}

func (s *RestateStore) RunLaunchReaperTick(_ context.Context, _ store.ReaperParams) (store.ReaperTickResult, error) {
	return store.ReaperTickResult{}, nil
}

func (s *RestateStore) UpdateAgentDeletion(_ context.Context, id string, pred store.DeletionPredicate, set store.DeletionFields) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[id]
	if !ok || !pred.Matches(a) {
		return 0, nil
	}
	if set.Phase != nil {
		a.Phase = *set.Phase
	}
	a.StateVersion++
	a.Updated = time.Now().UTC()
	return 1, nil
}

func (s *RestateStore) SetRunIntent(_ context.Context, agentID string, intent store.RunIntent) (time.Time, error) {
	_, at, err := s.SwapRunIntent(context.Background(), agentID, intent)
	return at, err
}

func (s *RestateStore) SwapRunIntent(_ context.Context, agentID string, intent store.RunIntent) (store.RunIntent, time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[agentID]
	if !ok {
		return "", time.Time{}, store.ErrNotFound
	}
	prior := a.RunIntent
	now := time.Now().UTC()
	a.RunIntent = intent
	a.RunIntentAt = &now
	return prior, now, nil
}

func (s *RestateStore) RevertRunIntent(_ context.Context, agentID string, from store.RunIntent, _ time.Time, to store.RunIntent) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[agentID]
	if !ok || a.RunIntent != from {
		return false, nil
	}
	a.RunIntent = to
	return true, nil
}

func (s *RestateStore) BackfillRunIntent(_ context.Context) (int, error) {
	return 0, nil
}

func (s *RestateStore) ClaimAgentStart(_ context.Context, agentID, owner string, kind store.StartClaimKind, target string, ttl time.Duration) (store.StartClaim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[agentID]
	if !ok {
		return store.StartClaim{}, store.ErrNotFound
	}
	now := time.Now().UTC()
	if a.StartClaimID != "" && a.StartClaimLeaseUntil != nil && a.StartClaimLeaseUntil.After(now) && kind != store.StartClaimStop {
		since := now
		if a.StartClaimAt != nil {
			since = *a.StartClaimAt
		}
		return store.StartClaim{}, &store.ClaimHeldError{
			ClaimID:    a.StartClaimID,
			Kind:       a.StartClaimKind,
			State:      a.StartClaimState,
			Owner:      a.StartClaimOwner,
			Since:      since,
			LeaseUntil: a.StartClaimLeaseUntil,
		}
	}
	until := now.Add(ttl)
	claimID := uuid.NewString()
	a.StartClaimID = claimID
	a.StartClaimKind = kind
	a.StartClaimState = store.StartClaimLive
	a.StartClaimOwner = owner
	a.StartClaimTarget = target
	a.StartClaimAt = &now
	a.StartClaimLeaseUntil = &until
	a.RunIntent = store.RunIntentRunning
	a.RunIntentAt = &now
	return store.StartClaim{
		ID:          claimID,
		Kind:        kind,
		Owner:       owner,
		Target:      target,
		At:          now,
		LeaseUntil:  until,
		RunIntentAt: now,
	}, nil
}

func (s *RestateStore) ClaimAgentStop(_ context.Context, agentID, owner string, intentAt time.Time, ttl time.Duration) (store.StartClaim, error) {
	return s.ClaimAgentStart(context.Background(), agentID, owner, store.StartClaimStop, "", ttl)
}

func (s *RestateStore) RenewAgentStart(_ context.Context, agentID, claimID, owner string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[agentID]
	if !ok || a.StartClaimID != claimID || a.StartClaimOwner != owner {
		return false, nil
	}
	until := time.Now().UTC().Add(ttl)
	a.StartClaimLeaseUntil = &until
	return true, nil
}

func (s *RestateStore) MarkStartUnconfirmed(_ context.Context, agentID, claimID, owner string, _ time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[agentID]
	if !ok || a.StartClaimID != claimID || a.StartClaimOwner != owner {
		return false, nil
	}
	a.StartClaimState = store.StartClaimUnconfirmed
	return true, nil
}

func (s *RestateStore) ReleaseAgentStart(_ context.Context, agentID, claimID, owner string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[agentID]
	if !ok || a.StartClaimID != claimID {
		return false, nil
	}
	a.StartClaimID = ""
	a.StartClaimState = ""
	a.StartClaimOwner = ""
	return true, nil
}

func (s *RestateStore) ReleaseSupersededStart(_ context.Context, agentID, claimID string, _ time.Time) (bool, error) {
	return s.ReleaseAgentStart(context.Background(), agentID, claimID, "")
}

func (s *RestateStore) DemoteExpiredStartClaim(_ context.Context, _, _ string, _ store.StartClaimHolds) (bool, error) {
	return false, nil
}

func (s *RestateStore) DemoteOwnerStartClaims(_ context.Context, _, _ string, _ store.StartClaimHolds) (int, error) {
	return 0, nil
}

func (s *RestateStore) ConvertUnconfirmedToStop(_ context.Context, agentID, claimID, owner string, ttl time.Duration) (store.StartClaim, error) {
	return s.ClaimAgentStart(context.Background(), agentID, owner, store.StartClaimStop, "", ttl)
}

func (s *RestateStore) ReleaseUnconfirmedStart(_ context.Context, agentID, claimID string) (bool, error) {
	return s.ReleaseAgentStart(context.Background(), agentID, claimID, "")
}

func (s *RestateStore) SettleEndedLaunchClaim(_ context.Context, _, _ string) (bool, error) {
	return false, nil
}

func (s *RestateStore) ListAgentsWithStartClaim(_ context.Context) ([]*store.Agent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*store.Agent
	for _, a := range s.agents {
		if a.StartClaimID != "" && a.DeletedAt.IsZero() {
			out = append(out, cloneAgent(a))
		}
	}
	return out, nil
}

func (s *RestateStore) StoreClock(_ context.Context) (time.Time, error) {
	return time.Now().UTC(), nil
}

func (s *RestateStore) ClaimAgentReincarnation(_ context.Context, agentID string, expectedVersion int64, at time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[agentID]
	if !ok {
		return 0, store.ErrNotFound
	}
	if expectedVersion > 0 && a.StateVersion != expectedVersion {
		return 0, store.ErrVersionConflict
	}
	a.ReincarnationState = store.AgentReincarnationStatePending
	a.ReincarnationUpdatedAt = &at
	a.StateVersion++
	return a.StateVersion, nil
}

func (s *RestateStore) RecordRecoveryObservations(_ context.Context, brokerID string, _ []string, obs []store.RecoveryObservation) (time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	for _, o := range obs {
		s.recoveryObservations[o.AgentID] = store.RecoveryObservationRecord{
			AgentID:    o.AgentID,
			BrokerID:   brokerID,
			Target:     o.Target,
			State:      o.State,
			ObservedAt: now,
			InFlight:   o.InFlight,
		}
	}
	return now, nil
}

func (s *RestateStore) GetRecoveryObservations(_ context.Context, agentIDs []string) (map[string]store.RecoveryObservationRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]store.RecoveryObservationRecord)
	for _, id := range agentIDs {
		if rec, ok := s.recoveryObservations[id]; ok {
			out[id] = rec
		}
	}
	return out, nil
}

func (s *RestateStore) ListBrokerTargetInventory(_ context.Context, brokerID string) ([]store.BrokerTargetInventory, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Clone(s.brokerTargetInventory[brokerID]), nil
}
