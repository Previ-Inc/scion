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

// Granular state keys inside IdentityObject[user_id] or IdentityObject["directory"].
const (
	identityKeyProfile      = "profile"
	identityKeyUatPrefix    = "uat:"
	identityKeyTermWS       = "term_ws"
	identityKeyExtPrefix    = "ext:"
	identityKeyEmailPrefix  = "email:"
	identityKeyAllowPrefix  = "allow:"
	identityKeyInvitePrefix = "invite:"
)

// UserProfileRecord stores user identity and profile metadata.
type UserProfileRecord struct {
	UserID    string    `json:"userId"`
	Email     string    `json:"email"`
	Name      string    `json:"name"`
	Kind      string    `json:"kind"` // "standard", "federated", "test", "super_admin"
	Status    string    `json:"status"` // "active", "pending", "disabled"
	Role      string    `json:"role"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// IdentityLinkRequest carries external identity binding parameters.
type IdentityLinkRequest struct {
	Issuer  string `json:"issuer"`
	Subject string `json:"subject"`
	Email   string `json:"email"`
}

// IdentityResolveRequest carries external identity lookup parameters.
type IdentityResolveRequest struct {
	Issuer  string `json:"issuer"`
	Subject string `json:"subject"`
}

// IdentityObject is the Restate Virtual Object managing user profiles, external identity bindings,
// allow lists, and invite codes.
type IdentityObject struct{}

// NewIdentityObject constructs an IdentityObject.
func NewIdentityObject() *IdentityObject {
	return &IdentityObject{}
}

// ServiceName returns the canonical Restate Virtual Object name.
func (i *IdentityObject) ServiceName() string {
	return ServiceIdentityObject
}

// SaveProfile creates or updates a user profile under "profile".
func (i *IdentityObject) SaveProfile(ctx restate.ObjectContext, profile UserProfileRecord) (UserProfileRecord, error) {
	userID := restate.Key(ctx)
	profile.UserID = userID
	now := time.Now().UTC()
	if profile.CreatedAt.IsZero() {
		profile.CreatedAt = now
	}
	profile.UpdatedAt = now
	restate.Set(ctx, identityKeyProfile, profile)
	if profile.Email != "" {
		restate.Set(ctx, identityKeyEmailPrefix+strings.ToLower(profile.Email), userID)
	}
	return profile, nil
}

// GetProfile reads the user profile.
func (i *IdentityObject) GetProfile(ctx restate.ObjectSharedContext, _ restate.Void) (UserProfileRecord, error) {
	prof, err := restate.Get[UserProfileRecord](ctx, identityKeyProfile)
	if err != nil {
		return UserProfileRecord{}, err
	}
	if prof.UserID == "" {
		return UserProfileRecord{}, restate.ToTerminalError(
			fmt.Errorf("user identity %s not found", restate.Key(ctx)),
			restate.WithErrorCode(http.StatusNotFound),
		)
	}
	return prof, nil
}

// LinkExternalIdentity binds an external issuer identity (e.g. OIDC sub) to a user ID.
func (i *IdentityObject) LinkExternalIdentity(ctx restate.ObjectContext, req IdentityLinkRequest) (restate.Void, error) {
	userID := restate.Key(ctx)
	extKey := fmt.Sprintf("%s%s:%s", identityKeyExtPrefix, strings.ToLower(req.Issuer), req.Subject)
	restate.Set(ctx, extKey, userID)
	return restate.Void{}, nil
}

// ResolveExternalIdentity resolves an (issuer, subject) pair to a user ID.
func (i *IdentityObject) ResolveExternalIdentity(ctx restate.ObjectSharedContext, req IdentityResolveRequest) (string, error) {
	extKey := fmt.Sprintf("%s%s:%s", identityKeyExtPrefix, strings.ToLower(req.Issuer), req.Subject)
	userID, err := restate.Get[string](ctx, extKey)
	if err != nil {
		return "", err
	}
	if userID == "" {
		return "", restate.ToTerminalError(
			fmt.Errorf("external identity not linked"),
			restate.WithErrorCode(http.StatusNotFound),
		)
	}
	return userID, nil
}
