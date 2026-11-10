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

// Granular state keys inside BrokerObject[broker_id] (Invariant 2).
const (
	brokerKeyRegistration = "reg"
	brokerKeyHMACSecret   = "hmac_secret"
	brokerKeySettings     = "settings"
	brokerKeyTaskPrefix   = "task:"
	brokerKeyWaiterPrefix = "waiter:"
)

// BrokerRegistration holds Runtime Broker registration, capabilities, and presence metadata.
type BrokerRegistration struct {
	BrokerID       string            `json:"brokerId"`
	Name           string            `json:"name"`
	Endpoint       string            `json:"endpoint,omitempty"`
	Status         string            `json:"status"` // "online", "offline", "draining"
	Version        string            `json:"version,omitempty"`
	Capabilities   []string          `json:"capabilities,omitempty"`
	Profiles       []string          `json:"profiles,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
	ActiveAgents   int               `json:"activeAgents"`
	LastHeartbeat  time.Time         `json:"lastHeartbeat"`
	RegisteredAt   time.Time         `json:"registeredAt"`
	HMACSecretEnc  string            `json:"hmacSecretEnc,omitempty"` // Always enc:v2:
}

// BrokerTask represents a durable command queued for a NAT-isolated or remote Runtime Broker.
type BrokerTask struct {
	OpID        string            `json:"opId"`
	BrokerID    string            `json:"brokerId"`
	AgentID     string            `json:"agentId"`
	RunID       string            `json:"runId,omitempty"`
	Generation  uint64            `json:"generation"`
	Kind        string            `json:"kind"` // "start", "stop", "delete", "exec"
	Payload     []byte            `json:"payload,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	AwakeableID string            `json:"awakeableId"`
	CreatedAt   time.Time         `json:"createdAt"`
}

// BrokerTaskResult is the response submitted by a Runtime Broker when completing a BrokerTask.
type BrokerTaskResult struct {
	OpID        string `json:"opId"`
	BrokerID    string `json:"brokerId"`
	StatusCode  int    `json:"statusCode"`
	ContainerID string `json:"containerId,omitempty"`
	RunID       string `json:"runId,omitempty"`
	Body        []byte `json:"body,omitempty"`
	Error       string `json:"error,omitempty"`
}

// BrokerHeartbeatRequest updates broker liveness and active agent count.
type BrokerHeartbeatRequest struct {
	ActiveAgents int               `json:"activeAgents"`
	Status       string            `json:"status,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
	Now          time.Time         `json:"now,omitempty"`
}

// BrokerObject is the Restate Virtual Object managing Runtime Broker registration, heartbeat,
// and Awakeable-backed NAT command dispatch.
type BrokerObject struct {
	cipher *ValueCipher
}

// NewBrokerObject constructs a BrokerObject.
func NewBrokerObject(cipher *ValueCipher) *BrokerObject {
	return &BrokerObject{cipher: cipher}
}

// ServiceName returns the canonical Restate Virtual Object name.
func (b *BrokerObject) ServiceName() string {
	return ServiceBrokerObject
}

// Register registers or updates a Runtime Broker and stores its HMAC secret encrypted under "enc:v2:".
func (b *BrokerObject) Register(ctx restate.ObjectContext, req BrokerRegistration) (BrokerRegistration, error) {
	brokerID := restate.Key(ctx)
	req.BrokerID = brokerID
	now := time.Now().UTC()
	if req.RegisteredAt.IsZero() {
		req.RegisteredAt = now
	}
	req.LastHeartbeat = now
	if req.Status == "" {
		req.Status = "online"
	}
	if req.HMACSecretEnc != "" {
		if !strings.HasPrefix(req.HMACSecretEnc, "enc:v2:") {
			return BrokerRegistration{}, restate.ToTerminalError(
				fmt.Errorf("Invariant 4 violation: broker HMAC secret must be enc:v2: encrypted"),
				restate.WithErrorCode(http.StatusBadRequest),
			)
		}
		restate.Set(ctx, brokerKeyHMACSecret, req.HMACSecretEnc)
	}
	restate.Set(ctx, brokerKeyRegistration, req)
	return req, nil
}

// Heartbeat updates the broker's last heartbeat timestamp and status.
func (b *BrokerObject) Heartbeat(ctx restate.ObjectContext, req BrokerHeartbeatRequest) (BrokerRegistration, error) {
	reg, err := restate.Get[BrokerRegistration](ctx, brokerKeyRegistration)
	if err != nil {
		return BrokerRegistration{}, err
	}
	if reg.BrokerID == "" {
		return BrokerRegistration{}, restate.ToTerminalError(
			fmt.Errorf("broker %s is not registered", restate.Key(ctx)),
			restate.WithErrorCode(http.StatusNotFound),
		)
	}
	now := req.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	reg.ActiveAgents = req.ActiveAgents
	if req.Status != "" {
		reg.Status = req.Status
	}
	if len(req.Labels) > 0 {
		reg.Labels = req.Labels
	}
	reg.LastHeartbeat = now
	restate.Set(ctx, brokerKeyRegistration, reg)
	return reg, nil
}

// EnqueueCommand records a durable command under "task:<op_id>" and immediately wakes any long-polling broker waiter.
func (b *BrokerObject) EnqueueCommand(ctx restate.ObjectContext, task BrokerTask) (BrokerTask, error) {
	if task.OpID == "" || task.AwakeableID == "" {
		return BrokerTask{}, restate.ToTerminalError(
			fmt.Errorf("opId and awakeableId are required for broker command"),
			restate.WithErrorCode(http.StatusBadRequest),
		)
	}
	task.BrokerID = restate.Key(ctx)
	if task.CreatedAt.IsZero() {
		task.CreatedAt = time.Now().UTC()
	}
	restate.Set(ctx, brokerKeyTaskPrefix+task.OpID, task)

	keys, err := restate.Keys(ctx)
	if err == nil {
		for _, k := range keys {
			if strings.HasPrefix(k, brokerKeyWaiterPrefix) {
				awkID := strings.TrimPrefix(k, brokerKeyWaiterPrefix)
				restate.ResolveAwakeable(ctx, awkID, []BrokerTask{task})
				restate.Clear(ctx, k)
			}
		}
	}

	return task, nil
}

// RegisterPollWaiter registers an Awakeable ID for a NAT broker long-polling for new tasks.
func (b *BrokerObject) RegisterPollWaiter(ctx restate.ObjectContext, awakeableID string) ([]BrokerTask, error) {
	tasks, err := b.listTasksInternal(ctx)
	if err != nil {
		return nil, err
	}
	if len(tasks) > 0 {
		if awakeableID != "" {
			restate.ResolveAwakeable(ctx, awakeableID, tasks)
		}
		return tasks, nil
	}
	if awakeableID != "" {
		restate.Set(ctx, brokerKeyWaiterPrefix+awakeableID, true)
	}
	return nil, nil
}

// ResolveCommand completes a queued BrokerTask under "task:<op_id>".
func (b *BrokerObject) ResolveCommand(ctx restate.ObjectContext, res BrokerTaskResult) (restate.Void, error) {
	key := brokerKeyTaskPrefix + res.OpID
	task, err := restate.Get[BrokerTask](ctx, key)
	if err != nil {
		return restate.Void{}, err
	}
	if task.OpID == "" {
		return restate.Void{}, nil
	}
	restate.Clear(ctx, key)

	if res.Error != "" {
		restate.RejectAwakeable(ctx, task.AwakeableID, fmt.Errorf("%s", res.Error))
	} else {
		restate.ResolveAwakeable(ctx, task.AwakeableID, res)
	}
	return restate.Void{}, nil
}

// GetRegistration returns registration metadata.
func (b *BrokerObject) GetRegistration(ctx restate.ObjectSharedContext, _ restate.Void) (BrokerRegistration, error) {
	reg, err := restate.Get[BrokerRegistration](ctx, brokerKeyRegistration)
	if err != nil {
		return BrokerRegistration{}, err
	}
	if reg.BrokerID == "" {
		return BrokerRegistration{}, restate.ToTerminalError(
			fmt.Errorf("broker %s not found", restate.Key(ctx)),
			restate.WithErrorCode(http.StatusNotFound),
		)
	}
	return reg, nil
}

// ListPendingCommands returns all pending "task:<op_id>" entries.
func (b *BrokerObject) ListPendingCommands(ctx restate.ObjectSharedContext, _ restate.Void) ([]BrokerTask, error) {
	return b.listTasksInternal(ctx)
}

func (b *BrokerObject) listTasksInternal(ctx restate.ObjectSharedContext) ([]BrokerTask, error) {
	keys, err := restate.Keys(ctx)
	if err != nil {
		return nil, err
	}
	var out []BrokerTask
	for _, k := range keys {
		if strings.HasPrefix(k, brokerKeyTaskPrefix) {
			t, err := restate.Get[BrokerTask](ctx, k)
			if err != nil {
				return nil, err
			}
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].OpID < out[j].OpID
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}
