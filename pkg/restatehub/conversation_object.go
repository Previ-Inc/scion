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
	"sort"
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"
)

// Granular state key prefixes inside ConversationObject[conversation_id] (Invariant 2).
const (
	convKeyMeta         = "meta"
	convKeyMsgPrefix    = "msg:"
	convKeyWaiterPrefix = "waiter:"
)

// ConversationMeta stores conversation metadata under "meta".
type ConversationMeta struct {
	ID        string            `json:"id"`
	ProjectID string            `json:"projectId,omitempty"`
	Title     string            `json:"title,omitempty"`
	Kind      string            `json:"kind"` // "direct", "group", "channel"
	CreatedBy string            `json:"createdBy,omitempty"`
	CreatedAt time.Time         `json:"createdAt"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// ConversationMessage represents a single message stored under granular key "msg:<08d_seq>" (Invariant 2).
type ConversationMessage struct {
	Seq       uint64            `json:"seq"`
	MessageID string            `json:"messageId"`
	SenderID  string            `json:"senderId"`
	Content   string            `json:"content"`
	Kind      string            `json:"kind,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
}

// ConversationObject is the Restate Virtual Object managing durable message logs
// and Awakeable SSE fan-out for real-time chat and log streaming (Invariant 2 & Invariant 6).
type ConversationObject struct{}

// NewConversationObject constructs a ConversationObject.
func NewConversationObject() *ConversationObject {
	return &ConversationObject{}
}

// ServiceName returns the canonical Restate Virtual Object name.
func (c *ConversationObject) ServiceName() string {
	return ServiceConversationObject
}

// InitConversation initializes conversation metadata under "meta".
func (c *ConversationObject) InitConversation(ctx restate.ObjectContext, meta ConversationMeta) (ConversationMeta, error) {
	convID := restate.Key(ctx)
	meta.ID = convID
	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = time.Now().UTC()
	}
	restate.Set(ctx, convKeyMeta, meta)
	return meta, nil
}

// AppendMessage appends a message with an O(1) granular key "msg:<08d_seq>" (Invariant 2)
// and immediately notifies any long-polling SSE readers via Awakeable resolution (Invariant 6).
func (c *ConversationObject) AppendMessage(ctx restate.ObjectContext, msg ConversationMessage) (uint64, error) {
	seq, err := restate.Get[uint64](ctx, "next_seq")
	if err != nil {
		return 0, err
	}
	seq++
	restate.Set(ctx, "next_seq", seq)
	msg.Seq = seq
	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now().UTC()
	}
	if msg.MessageID == "" {
		msg.MessageID = restate.UUID(ctx).String()
	}

	msgKey := fmt.Sprintf("%s%08d", convKeyMsgPrefix, seq)
	restate.Set(ctx, msgKey, msg)

	keys, err := restate.Keys(ctx)
	if err == nil {
		for _, k := range keys {
			if strings.HasPrefix(k, convKeyWaiterPrefix) {
				awkID := strings.TrimPrefix(k, convKeyWaiterPrefix)
				restate.ResolveAwakeable(ctx, awkID, msg)
				restate.Clear(ctx, k)
			}
		}
	}

	return seq, nil
}

// RegisterWaiter registers an Awakeable ID for an SSE client waiting for messages after afterSeq.
// If new messages already exist beyond afterSeq, it resolves the Awakeable immediately.
func (c *ConversationObject) RegisterWaiter(ctx restate.ObjectContext, req struct {
	AfterSeq    uint64 `json:"afterSeq"`
	AwakeableID string `json:"awakeableId"`
}) ([]ConversationMessage, error) {
	msgs, err := c.ListMessages(ctx, req.AfterSeq)
	if err != nil {
		return nil, err
	}
	if len(msgs) > 0 {
		if req.AwakeableID != "" {
			restate.ResolveAwakeable(ctx, req.AwakeableID, msgs[0])
		}
		return msgs, nil
	}
	if req.AwakeableID != "" {
		restate.Set(ctx, convKeyWaiterPrefix+req.AwakeableID, req.AfterSeq)
	}
	return nil, nil
}

// ListMessages returns all messages with Seq > afterSeq in ascending order.
func (c *ConversationObject) ListMessages(ctx restate.ObjectSharedContext, afterSeq uint64) ([]ConversationMessage, error) {
	keys, err := restate.Keys(ctx)
	if err != nil {
		return nil, err
	}
	var out []ConversationMessage
	for _, k := range keys {
		if strings.HasPrefix(k, convKeyMsgPrefix) {
			m, err := restate.Get[ConversationMessage](ctx, k)
			if err != nil {
				return nil, err
			}
			if m.Seq > afterSeq {
				out = append(out, m)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}
