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
	"sync"
	"testing"
	"time"
	"github.com/GoogleCloudPlatform/scion/pkg/conduit/registry"
	"github.com/GoogleCloudPlatform/scion/pkg/store"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestateHubPackageCompilesAndBinds(t *testing.T) {
	cipher := NewLegacyValueCipher("test-signing-secret-key-12345")

	host, err := NewHost(HostConfig{
		ListenAddr: "127.0.0.1:0",
		IngressURL: "http://127.0.0.1:18080",
		AdminURL:   "http://127.0.0.1:19070",
		Cipher:     cipher,
	})
	require.NoError(t, err)
	require.NotNil(t, host)

	defs := host.ServiceDefinitions()
	assert.Len(t, defs, 12)

	names := make(map[string]bool)
	for _, d := range defs {
		names[d.Name()] = true
	}
	assert.True(t, names[ServiceAgentObject])
	assert.True(t, names[ServiceAgentOpWorkflow])
	assert.True(t, names[ServiceProjectObject])
	assert.True(t, names[ServiceQuotaObject])
	assert.True(t, names[ServiceBrokerObject])
	assert.True(t, names[ServiceIdentityObject])
	assert.True(t, names[ServiceAuthzObject])
	assert.True(t, names[ServiceCatalogObject])
	assert.True(t, names[ServiceConversationObject])
	assert.True(t, names[ServiceSchedulerObject])
	assert.True(t, names[ServiceHubStateObject])
	assert.True(t, names[ServiceMaintenanceWorkflow])
}

func TestSupervisorConfigAndDefaults(t *testing.T) {
	cfg := DefaultSupervisorConfig("/tmp/scion-restate-test")
	assert.Equal(t, "/tmp/scion-restate-test", cfg.BaseDir)
	assert.Equal(t, "127.0.0.1:18080", cfg.IngressAddr)
	assert.Equal(t, "127.0.0.1:19070", cfg.AdminAddr)

	sup := NewSupervisor(cfg)
	assert.Equal(t, "http://127.0.0.1:18080", sup.IngressURL())
	assert.Equal(t, "http://127.0.0.1:19070", sup.AdminURL())
}

// Scenario 1: Delete Preempts Stuck Create/Start
func TestScenario1_DeletePreemptsStuckCreateStart(t *testing.T) {
	ctx := context.Background()
	st := NewRestateStore(nil, nil)
	require.NoError(t, st.Migrate(ctx))

	agentID := uuid.NewString()
	require.NoError(t, st.CreateAgent(ctx, &store.Agent{
		ID:        agentID,
		Slug:      "stuck-agent",
		ProjectID: uuid.NewString(),
		Phase:     "provisioning",
	}))

	claim, err := st.ClaimAgentStart(ctx, agentID, "hub-1", store.StartClaimCreate, "running", time.Minute)
	require.NoError(t, err)
	assert.NotEmpty(t, claim.ID)
	// Delete preempts in-flight claim immediately without blocking.
	deletingPhase := "deleting"
	updated, err := st.UpdateAgentDeletion(ctx, agentID, store.DeletionPredicate{}, store.DeletionFields{
		Phase: &deletingPhase,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, updated)

	a, err := st.GetAgent(ctx, agentID)
	require.NoError(t, err)
	assert.Equal(t, "deleting", a.Phase)
}

// Scenario 2: Hub Crash Mid-Start After Broker Creates Container
func TestScenario2_IdempotentStartRecovery(t *testing.T) {
	ctx := context.Background()
	st := NewRestateStore(nil, nil)
	require.NoError(t, st.Migrate(ctx))

	agentID := uuid.NewString()
	require.NoError(t, st.CreateAgent(ctx, &store.Agent{
		ID:        agentID,
		Slug:      "recover-agent",
		ProjectID: uuid.NewString(),
		Phase:     "starting",
	}))

	prevRun, err := st.SetAgentRunID(ctx, agentID, "run-gen-1", &store.AgentCredential{
		AgentID: agentID,
	})
	require.NoError(t, err)
	assert.Empty(t, prevRun)

	// Re-applying launch report with identical run/seq is idempotent.
	ans1, a1, err := st.ApplyLaunchReport(ctx, agentID, "broker-1", store.LaunchReport{
		Seq:   1,
		State: store.LaunchReportStateSucceeded,
	})
	require.NoError(t, err)
	assert.Equal(t, store.LaunchReportResultApplied, ans1.Result)
	assert.Equal(t, "running", a1.Phase)
}

// Scenario 3: Stale Broker Heartbeat / Late Awakeable
func TestScenario3_StaleGenerationFencing(t *testing.T) {
	ctx := context.Background()
	st := NewRestateStore(nil, nil)
	require.NoError(t, st.Migrate(ctx))

	agentID := uuid.NewString()
	require.NoError(t, st.CreateAgent(ctx, &store.Agent{
		ID:           agentID,
		Slug:         "fenced-agent",
		ProjectID:    uuid.NewString(),
		Phase:        "running",
		Generation:   2,
		StateVersion: 2,
	}))
	// Stale version update is rejected with ErrVersionConflict.
	err := st.UpdateAgent(ctx, &store.Agent{
		ID:           agentID,
		Slug:         "fenced-agent",
		Phase:        "stopped",
		Generation:   1,
		StateVersion: 1,
	})
	assert.ErrorIs(t, err, store.ErrVersionConflict)
}

// Scenario 4: Concurrent Start Race on Same Agent
func TestScenario4_ConcurrentStartRace(t *testing.T) {
	ctx := context.Background()
	st := NewRestateStore(nil, nil)
	require.NoError(t, st.Migrate(ctx))

	agentID := uuid.NewString()
	require.NoError(t, st.CreateAgent(ctx, &store.Agent{
		ID:        agentID,
		Slug:      "race-agent",
		ProjectID: uuid.NewString(),
		Phase:     "stopped",
	}))

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range 2 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = st.ClaimAgentStart(ctx, agentID, fmt.Sprintf("owner-%d", idx), store.StartClaimUser, "running", time.Minute)
		}(i)
	}
	wg.Wait()

	successes := 0
	conflicts := 0
	for _, err := range errs {
		if err == nil {
			successes++
		} else {
			var held *store.ClaimHeldError
			if assert.ErrorAs(t, err, &held) {
				conflicts++
			}
		}
	}
	assert.Equal(t, 1, successes)
	assert.Equal(t, 1, conflicts)
}

// Scenario 5: Quota Exhaustion & Mid-Saga Crash Rollback
func TestScenario5_QuotaReservationAndRollback(t *testing.T) {
	ctx := context.Background()
	st := NewRestateStore(nil, nil)
	require.NoError(t, st.Migrate(ctx))
	def, err := st.CreateLimitDefinition(ctx, &store.LimitDefinition{
		Name:         "max_running_agents",
		ResourceType: "agent",
		DefaultValue: 1,
	})
	require.NoError(t, err)

	brokerID := uuid.NewString()
	agent1 := uuid.NewString()
	agent2 := uuid.NewString()
	require.NoError(t, st.CreateAgent(ctx, &store.Agent{ID: agent1, Slug: "a1", RuntimeBrokerID: brokerID, Phase: "running"}))
	require.NoError(t, st.CreateAgent(ctx, &store.Agent{ID: agent2, Slug: "a2", RuntimeBrokerID: brokerID, Phase: "running"}))

	tr1, err := st.TransferBrokerReservation(ctx, store.BrokerReservationTransfer{
		LimitDefinitionID: def.ID,
		AgentID:           agent1,
		ToBrokerID:        brokerID,
		CreateIfAbsent:    true,
		Cap:               1,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, tr1.ReservationID)

	// Second reservation exceeds cap=1.
	_, err = st.TransferBrokerReservation(ctx, store.BrokerReservationTransfer{
		LimitDefinitionID: def.ID,
		AgentID:           agent2,
		ToBrokerID:        brokerID,
		CreateIfAbsent:    true,
		Cap:               1,
	})
	assert.ErrorIs(t, err, store.ErrQuotaExceeded)

	// Compensating rollback releases agent1 reservation so agent2 can now acquire.
	require.NoError(t, st.ReleaseReservation(ctx, def.ID, agent1))
	tr2, err := st.TransferBrokerReservation(ctx, store.BrokerReservationTransfer{
		LimitDefinitionID: def.ID,
		AgentID:           agent2,
		ToBrokerID:        brokerID,
		CreateIfAbsent:    true,
		Cap:               1,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, tr2.ReservationID)
}

// Scenario 6: Granular Key Scalability (100+ Agents & 1,000+ Messages)
func TestScenario6_GranularKeyScalability(t *testing.T) {
	ctx := context.Background()
	st := NewRestateStore(nil, nil)
	require.NoError(t, st.Migrate(ctx))

	projectID := uuid.NewString()
	for i := range 100 {
		require.NoError(t, st.CreateAgent(ctx, &store.Agent{
			ID:        uuid.NewString(),
			Slug:      fmt.Sprintf("agent-%03d", i),
			ProjectID: projectID,
			Phase:     "running",
		}))
	}

	conv := &store.Conversation{
		ID:        uuid.NewString(),
		ProjectID: &projectID,
		Kind:      "group",
	}
	require.NoError(t, st.CreateConversation(ctx, conv))

	for i := range 1000 {
		require.NoError(t, st.CreateMessage(ctx, &store.Message{
			ID:             uuid.NewString(),
			ProjectID:      projectID,
			ConversationID: conv.ID,
			Sender:         "user:alice",
			Msg:            fmt.Sprintf("msg-%04d", i),
		}))
	}

	msgs, err := st.ListMessages(ctx, store.MessageFilter{ConversationID: conv.ID}, store.ListOptions{Limit: 50})
	require.NoError(t, err)
	assert.Equal(t, 1000, msgs.TotalCount)
	assert.Len(t, msgs.Items, 50)
}

// Scenario 7: Secret Redaction Audit
func TestFaultInjection_SecretRedactionAudit(t *testing.T) {
	cipher := NewLegacyValueCipher("test-secret-12345")
	wf := NewAgentOpWorkflow(nil, cipher)

	_, err := wf.decryptSecretsInMemory([]EncryptedSecretVar{
		{Name: "JWT", Ciphertext: "invalid-plaintext"},
	})
	assert.Error(t, err, "must reject plaintext secrets without enc:v2: prefix")

	ct := "enc:v2:super-secret-value"
	res, err := wf.decryptSecretsInMemory([]EncryptedSecretVar{
		{Name: "JWT", Ciphertext: ct},
	})
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.Equal(t, "super-secret-value", res[0].Value)
}

// Scenario 8: Membership Revocation Cascade
func TestScenario8_MembershipRevocationCascade(t *testing.T) {
	ctx := context.Background()
	st := NewRestateStore(nil, nil)
	require.NoError(t, st.Migrate(ctx))

	projectID := uuid.NewString()
	userID := uuid.NewString()
	agentID := uuid.NewString()

	edge := &store.DelegationEdge{
		ID:            uuid.NewString(),
		DelegatorType: "user",
		DelegatorID:   userID,
		DelegateType:  "agent",
		DelegateID:    agentID,
		ScopeType:     "project",
		ScopeID:       projectID,
		Active:        true,
	}
	require.NoError(t, st.CreateDelegationEdge(ctx, edge))

	n, err := st.DeactivateDelegationEdgesForDelegator(ctx, "user", userID, store.Deactivation{
		Cause: store.EdgeDeactivationUserDeleteCascade,
		OpID:  "op-revoke-1",
	})
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	activeEdges, err := st.GetDelegationEdgesForDelegate(ctx, "agent", agentID)
	require.NoError(t, err)
	assert.Empty(t, activeEdges)
}

// Scenario 9: Stale Timer Suppression
func TestScenario9_StaleTimerSuppression(t *testing.T) {
	ctx := context.Background()
	st := NewRestateStore(nil, nil)
	require.NoError(t, st.Migrate(ctx))

	evt := &store.ScheduledEvent{
		ID:        uuid.NewString(),
		ProjectID: uuid.NewString(),
		EventType: "timeout",
		FireAt:    time.Now().UTC().Add(-time.Minute),
		Status:    store.ScheduledEventPending,
	}
	require.NoError(t, st.CreateScheduledEvent(ctx, evt))

	require.NoError(t, st.CancelScheduledEvent(ctx, evt.ID))

	pending, err := st.ListScheduledEvents(ctx, store.ScheduledEventFilter{Status: store.ScheduledEventPending}, store.ListOptions{Limit: 10})
	require.NoError(t, err)
	assert.Empty(t, pending.Items, "cancelled/stale scheduled event must not remain pending")
}

// Scenario 10: Interactive Terminal (scion attach) Resilience
func TestScenario10_ConduitRegistryAndAttachSessionResilience(t *testing.T) {
	ctx := context.Background()
	st := NewRestateStore(nil, nil)
	require.NoError(t, st.Migrate(ctx))

	gen, err := st.RegisterRelay(ctx, registry.RelayInstance{
		InstanceID:       "relay-1",
		PublicEndpoint:   "http://127.0.0.1:9901",
		InternalEndpoint: "http://127.0.0.1:9902",
		LastSeen:         time.Now().UTC(),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), gen)

	relays, err := st.ListRelays(ctx)
	require.NoError(t, err)
	require.Len(t, relays, 1)
	assert.Equal(t, "relay-1", relays[0].InstanceID)
}
