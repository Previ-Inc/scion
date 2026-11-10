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
	"sync"
	"time"
)

// NonceCacheStore is a shared nonce cache for HMAC replay protection.
type NonceCacheStore struct {
	mu      sync.Mutex
	entries map[string]time.Time
}

// NewNonceCacheStore creates a NonceCacheStore.
func NewNonceCacheStore(_ ...any) *NonceCacheStore {
	return &NonceCacheStore{
		entries: make(map[string]time.Time),
	}
}

// CheckAndStore checks if a nonce has been seen within its TTL window.
// Returns (true, nil) if the nonce is fresh and was recorded.
// Returns (false, nil) if the nonce was already seen (replay).
func (s *NonceCacheStore) CheckAndStore(_ context.Context, nonce string, ttl time.Duration) (bool, error) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.entries == nil {
		s.entries = make(map[string]time.Time)
	}
	if exp, ok := s.entries[nonce]; ok && now.Before(exp) {
		return false, nil
	}
	s.entries[nonce] = now.Add(ttl)
	return true, nil
}

// PurgeExpired deletes all nonce entries where expires_at < now and returns the
// number of rows removed for observability.
func (s *NonceCacheStore) PurgeExpired(_ context.Context) (int, error) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for k, exp := range s.entries {
		if exp.Before(now) {
			delete(s.entries, k)
			removed++
		}
	}
	return removed, nil
}
