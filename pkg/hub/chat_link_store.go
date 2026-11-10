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

package hub

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"sync"
	"time"
)

// ChatLinkProvider identifies the chat platform for link codes.
type ChatLinkProvider string

const (
	ChatLinkProviderDiscord  ChatLinkProvider = "discord"
	ChatLinkProviderTeams    ChatLinkProvider = "teams"
	ChatLinkProviderTelegram ChatLinkProvider = "telegram"
)

const (
	chatLinkStatusPending   = "pending"
	chatLinkStatusConfirmed = "confirmed"
)

type chatLinkRecord struct {
	codeHash       string
	codePrefix     string
	provider       ChatLinkProvider
	userIdentifier string
	status         string
	userID         string
	userEmail      string
	expiresAt      time.Time
	createdAt      time.Time
}

// ChatLinkStore is the persistence layer for chat link codes.
type ChatLinkStore struct {
	mu   sync.Mutex
	rows []chatLinkRecord
}

// NewChatLinkStore creates a ChatLinkStore.
func NewChatLinkStore(_ ...any) *ChatLinkStore {
	return &ChatLinkStore{}
}

// hashCode computes the SHA-256 hex digest of a raw link code.
func hashCode(code string) string {
	h := sha256.Sum256([]byte(code))
	return hex.EncodeToString(h[:])
}

// RegisterCode persists a new pending link code.
func (s *ChatLinkStore) RegisterCode(ctx context.Context, code, userIdentifier string, provider ChatLinkProvider, ttl time.Duration) error {
	return s.CreateCode(ctx, code, provider, userIdentifier, time.Now().Add(ttl))
}

// CreateCode persists a new pending link code.
func (s *ChatLinkStore) CreateCode(_ context.Context, code string, provider ChatLinkProvider, userIdentifier string, expiresAt time.Time) error {
	codeHash := hashCode(code)
	prefix := code
	if len(prefix) > 8 {
		prefix = prefix[:8]
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = slices.DeleteFunc(s.rows, func(r chatLinkRecord) bool {
		return r.provider == provider && r.userIdentifier == userIdentifier && r.status == chatLinkStatusPending
	})
	s.rows = append(s.rows, chatLinkRecord{
		codeHash:       codeHash,
		codePrefix:     prefix,
		provider:       provider,
		userIdentifier: userIdentifier,
		status:         chatLinkStatusPending,
		expiresAt:      expiresAt,
		createdAt:      time.Now(),
	})
	return nil
}

// VerifyCode validates a link code and marks it confirmed.
func (s *ChatLinkStore) VerifyCode(_ context.Context, code string, provider ChatLinkProvider, userID, userEmail string) (userIdentifier string, errReason string) {
	codeHash := hashCode(code)
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := -1
	for i := range s.rows {
		if s.rows[i].codeHash == codeHash && s.rows[i].provider == provider {
			idx = i
			break
		}
	}
	if idx < 0 {
		return "", "code_not_found"
	}
	if time.Now().After(s.rows[idx].expiresAt) {
		return "", "code_expired"
	}
	if s.rows[idx].status == chatLinkStatusConfirmed {
		return s.rows[idx].userIdentifier, ""
	}
	s.rows[idx].status = chatLinkStatusConfirmed
	s.rows[idx].userID = userID
	s.rows[idx].userEmail = userEmail
	return s.rows[idx].userIdentifier, ""
}

// GetStatusByUser returns the linking status for a provider-specific user identifier.
func (s *ChatLinkStore) GetStatusByUser(_ context.Context, provider ChatLinkProvider, userIdentifier string) (status, userID, userEmail string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var latest *chatLinkRecord
	for i := range s.rows {
		r := &s.rows[i]
		if r.provider == provider && r.userIdentifier == userIdentifier {
			if latest == nil || r.createdAt.After(latest.createdAt) {
				latest = r
			}
		}
	}
	if latest == nil {
		return "not_found", "", ""
	}
	if time.Now().After(latest.expiresAt) {
		return "expired", "", ""
	}
	return latest.status, latest.userID, latest.userEmail
}

// ConsumePending removes a confirmed entry so it isn't returned again.
func (s *ChatLinkStore) ConsumePending(_ context.Context, provider ChatLinkProvider, userIdentifier string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = slices.DeleteFunc(s.rows, func(r chatLinkRecord) bool {
		return r.provider == provider && r.userIdentifier == userIdentifier
	})
}

// PurgeExpired deletes all link codes where expires_at < now.
func (s *ChatLinkStore) PurgeExpired(_ context.Context) error {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = slices.DeleteFunc(s.rows, func(r chatLinkRecord) bool {
		return r.expiresAt.Before(now)
	})
	return nil
}
