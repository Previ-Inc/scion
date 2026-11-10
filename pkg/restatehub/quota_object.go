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

// Granular state keys inside QuotaObject[scope_id] (Invariant 2).
const (
	quotaKeyLimits    = "limits"
	quotaKeyResPrefix = "res:"
)

// QuotaLimits defines the capacity ceiling for a scope ("project:<id>" or "broker:<id>").
type QuotaLimits struct {
	ScopeID   string    `json:"scopeId"`
	MaxAgents int       `json:"maxAgents"` // <= 0 means unlimited unless Enforced is true with 0 cap
	Enforced  bool      `json:"enforced"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// QuotaReservation is stored under granular key "res:<op_id>" in QuotaObject[scope_id].
type QuotaReservation struct {
	OpID      string    `json:"opId"`
	AgentID   string    `json:"agentId"`
	ScopeID   string    `json:"scopeId"`
	Amount    int       `json:"amount"`
	CreatedAt time.Time `json:"createdAt"`
}

// QuotaReserveRequest requests an idempotent reservation for OpID.
type QuotaReserveRequest struct {
	OpID    string    `json:"opId"`
	AgentID string    `json:"agentId"`
	Amount  int       `json:"amount"`
	Now     time.Time `json:"now,omitempty"`
}

// QuotaReleaseRequest releases a reservation by OpID.
type QuotaReleaseRequest struct {
	OpID    string `json:"opId"`
	AgentID string `json:"agentId,omitempty"`
}

// QuotaTransferRequest transfers a reservation from this scope to TargetScopeID.
type QuotaTransferRequest struct {
	OpID          string `json:"opId"`
	NewOpID       string `json:"newOpId"`
	AgentID       string `json:"agentId"`
	TargetScopeID string `json:"targetScopeId"`
	Amount        int    `json:"amount"`
}

// QuotaStatus reports current limits and active usage across all "res:<op_id>" keys.
type QuotaStatus struct {
	ScopeID      string             `json:"scopeId"`
	Limits       QuotaLimits        `json:"limits"`
	ActiveAmount int                `json:"activeAmount"`
	Reservations []QuotaReservation `json:"reservations,omitempty"`
}

// QuotaObject is the Restate Virtual Object managing capacity limits and tokenized
// reservations for a single scope ("project:<id>" or "broker:<id>").
type QuotaObject struct{}

// NewQuotaObject constructs a QuotaObject.
func NewQuotaObject() *QuotaObject {
	return &QuotaObject{}
}

// ServiceName returns the canonical Restate Virtual Object name.
func (q *QuotaObject) ServiceName() string {
	return ServiceQuotaObject
}

// SetLimits sets the capacity ceiling for this quota scope.
func (q *QuotaObject) SetLimits(ctx restate.ObjectContext, limits QuotaLimits) (QuotaLimits, error) {
	limits.ScopeID = restate.Key(ctx)
	if limits.UpdatedAt.IsZero() {
		limits.UpdatedAt = time.Now().UTC()
	}
	restate.Set(ctx, quotaKeyLimits, limits)
	return limits, nil
}

// Reserve acquires an idempotent reservation under key "res:<op_id>" (Invariant 2).
func (q *QuotaObject) Reserve(ctx restate.ObjectContext, req QuotaReserveRequest) (QuotaReservation, error) {
	if req.OpID == "" || req.AgentID == "" {
		return QuotaReservation{}, restate.ToTerminalError(
			fmt.Errorf("opId and agentId are required for quota reservation"),
			restate.WithErrorCode(http.StatusBadRequest),
		)
	}
	if req.Amount <= 0 {
		req.Amount = 1
	}

	resKey := quotaKeyResPrefix + req.OpID
	existing, err := restate.Get[QuotaReservation](ctx, resKey)
	if err != nil {
		return QuotaReservation{}, err
	}
	if existing.OpID != "" {
		if existing.AgentID != req.AgentID || existing.Amount != req.Amount {
			return QuotaReservation{}, restate.ToTerminalError(
				fmt.Errorf("quota reservation token %s reused with mismatched payload", req.OpID),
				restate.WithErrorCode(http.StatusConflict),
			)
		}
		return existing, nil
	}

	limits, err := restate.Get[QuotaLimits](ctx, quotaKeyLimits)
	if err != nil {
		return QuotaReservation{}, err
	}

	keys, err := restate.Keys(ctx)
	if err != nil {
		return QuotaReservation{}, err
	}

	activeTotal := 0
	for _, k := range keys {
		if strings.HasPrefix(k, quotaKeyResPrefix) {
			r, err := restate.Get[QuotaReservation](ctx, k)
			if err != nil {
				return QuotaReservation{}, err
			}
			// If the same agent already holds a reservation under an earlier op, don't double-charge it.
			if r.AgentID == req.AgentID {
				return r, nil
			}
			activeTotal += r.Amount
		}
	}

	if limits.Enforced && activeTotal+req.Amount > limits.MaxAgents {
		return QuotaReservation{}, restate.ToTerminalError(
			fmt.Errorf("quota exceeded on %s: active %d + requested %d > max %d", restate.Key(ctx), activeTotal, req.Amount, limits.MaxAgents),
			restate.WithErrorCode(http.StatusTooManyRequests),
		)
	}

	now := req.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	res := QuotaReservation{
		OpID:      req.OpID,
		AgentID:   req.AgentID,
		ScopeID:   restate.Key(ctx),
		Amount:    req.Amount,
		CreatedAt: now,
	}
	restate.Set(ctx, resKey, res)
	return res, nil
}

// Release removes the reservation under key "res:<op_id>" (idempotent).
func (q *QuotaObject) Release(ctx restate.ObjectContext, req QuotaReleaseRequest) (restate.Void, error) {
	if req.OpID == "" {
		return restate.Void{}, nil
	}
	resKey := quotaKeyResPrefix + req.OpID
	existing, err := restate.Get[QuotaReservation](ctx, resKey)
	if err != nil {
		return restate.Void{}, err
	}
	if existing.OpID != "" && (req.AgentID == "" || existing.AgentID == req.AgentID) {
		restate.Clear(ctx, resKey)
	}
	return restate.Void{}, nil
}

// ReleaseByAgent releases all reservations held by agentID in this scope.
func (q *QuotaObject) ReleaseByAgent(ctx restate.ObjectContext, agentID string) (int, error) {
	keys, err := restate.Keys(ctx)
	if err != nil {
		return 0, err
	}
	released := 0
	for _, k := range keys {
		if strings.HasPrefix(k, quotaKeyResPrefix) {
			r, err := restate.Get[QuotaReservation](ctx, k)
			if err != nil {
				return 0, err
			}
			if r.AgentID == agentID {
				restate.Clear(ctx, k)
				released++
			}
		}
	}
	return released, nil
}

// Transfer reserves capacity on TargetScopeID first, then releases the reservation on this scope.
func (q *QuotaObject) Transfer(ctx restate.ObjectContext, req QuotaTransferRequest) (QuotaReservation, error) {
	newOpID := req.NewOpID
	if newOpID == "" {
		newOpID = req.OpID + "-transfer"
	}
	amount := req.Amount
	if amount <= 0 {
		amount = 1
	}

	targetRes, err := restate.Object[QuotaReservation](ctx, ServiceQuotaObject, req.TargetScopeID, "Reserve").
		Request(QuotaReserveRequest{
			OpID:    newOpID,
			AgentID: req.AgentID,
			Amount:  amount,
		})
	if err != nil {
		return QuotaReservation{}, err
	}

	if req.OpID != "" {
		restate.Clear(ctx, quotaKeyResPrefix+req.OpID)
	} else {
		_, _ = q.ReleaseByAgent(ctx, req.AgentID)
	}
	return targetRes, nil
}

// GetStatus returns the current limits and active reservations in this scope.
func (q *QuotaObject) GetStatus(ctx restate.ObjectSharedContext, _ restate.Void) (QuotaStatus, error) {
	limits, err := restate.Get[QuotaLimits](ctx, quotaKeyLimits)
	if err != nil {
		return QuotaStatus{}, err
	}
	keys, err := restate.Keys(ctx)
	if err != nil {
		return QuotaStatus{}, err
	}
	var reservations []QuotaReservation
	total := 0
	for _, k := range keys {
		if strings.HasPrefix(k, quotaKeyResPrefix) {
			r, err := restate.Get[QuotaReservation](ctx, k)
			if err != nil {
				return QuotaStatus{}, err
			}
			reservations = append(reservations, r)
			total += r.Amount
		}
	}
	return QuotaStatus{
		ScopeID:      restate.Key(ctx),
		Limits:       limits,
		ActiveAmount: total,
		Reservations: reservations,
	}, nil
}
