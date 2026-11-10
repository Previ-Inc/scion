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
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/ingress"
	"github.com/restatedev/sdk-go/server"
)

// All 12 canonical Restate service, Virtual Object, and Workflow names in pkg/restatehub.
const (
	ServiceAgentObject         = "AgentObject"
	ServiceAgentOpWorkflow     = "AgentOpWorkflow"
	ServiceProjectObject       = "ProjectObject"
	ServiceQuotaObject         = "QuotaObject"
	ServiceBrokerObject        = "BrokerObject"
	ServiceIdentityObject      = "IdentityObject"
	ServiceAuthzObject         = "AuthzObject"
	ServiceCatalogObject       = "CatalogObject"
	ServiceConversationObject  = "ConversationObject"
	ServiceSchedulerObject     = "SchedulerObject"
	ServiceHubStateObject      = "HubStateObject"
	ServiceMaintenanceWorkflow = "MaintenanceWorkflow"
)

// HostConfig configures the Restate SDK worker host and its ingress client.
type HostConfig struct {
	ListenAddr          string
	IngressURL          string
	AdminURL            string
	IdentityKeys        []string
	Cipher              *ValueCipher
	BrokerExecutor      BrokerRuntimeExecutor
	MaintenanceExecutor MaintenanceTaskExecutor
	Logger              *slog.Logger
	HTTPClient          *http.Client
}

// Host binds all 12 Restate Virtual Objects and Workflows.
type Host struct {
	cfg           HostConfig
	restateSrv    *server.Restate
	ingressClient *ingress.Client
	httpClient    *http.Client
	cipher        *ValueCipher
	log           *slog.Logger

	agentObj        *AgentObject
	agentOpWf       *AgentOpWorkflow
	projectObj      *ProjectObject
	quotaObj        *QuotaObject
	brokerObj       *BrokerObject
	identityObj     *IdentityObject
	authzObj        *AuthzObject
	catalogObj      *CatalogObject
	conversationObj *ConversationObject
	schedulerObj    *SchedulerObject
	hubStateObj     *HubStateObject
	maintenanceWf   *MaintenanceWorkflow

	mu         sync.RWMutex
	listenAddr string
	httpSrv    *http.Server
}

// NewHost constructs a new Restate service host.
func NewHost(cfg HostConfig) (*Host, error) {
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = "127.0.0.1:0"
	}
	if cfg.IngressURL == "" {
		cfg.IngressURL = "http://127.0.0.1:18080"
	}
	if cfg.AdminURL == "" {
		cfg.AdminURL = "http://127.0.0.1:19070"
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	cipher := cfg.Cipher
	if cipher == nil {
		cipher = NewLegacyValueCipher("restatehub-default-ephemeral-key")
	}

	h := &Host{
		cfg:        cfg,
		httpClient: hc,
		cipher:     cipher,
		log:        log,
	}

	h.agentObj = NewAgentObject()
	h.agentOpWf = NewAgentOpWorkflow(cfg.BrokerExecutor, cipher)
	h.projectObj = NewProjectObject(cipher)
	h.quotaObj = NewQuotaObject()
	h.brokerObj = NewBrokerObject(cipher)
	h.identityObj = NewIdentityObject()
	h.authzObj = NewAuthzObject()
	h.catalogObj = NewCatalogObject()
	h.conversationObj = NewConversationObject()
	h.schedulerObj = NewSchedulerObject()
	h.hubStateObj = NewHubStateObject(cipher)
	h.maintenanceWf = NewMaintenanceWorkflow(cfg.MaintenanceExecutor)

	rs := server.NewRestate().WithLogger(log.Handler(), true)
	if len(cfg.IdentityKeys) > 0 {
		rs.WithIdentityV1(cfg.IdentityKeys...)
	}

	for _, def := range h.ServiceDefinitions() {
		rs.Bind(def)
	}
	h.restateSrv = rs
	h.ingressClient = ingress.NewClient(cfg.IngressURL, ingress.WithHttpClient(hc))

	return h, nil
}

// ServiceDefinitions returns the 12 Restate service definitions.
func (h *Host) ServiceDefinitions() []restate.ServiceDefinition {
	return []restate.ServiceDefinition{
		restate.Reflect(h.agentObj),
		restate.Reflect(h.agentOpWf),
		restate.Reflect(h.projectObj),
		restate.Reflect(h.quotaObj),
		restate.Reflect(h.brokerObj),
		restate.Reflect(h.identityObj),
		restate.Reflect(h.authzObj),
		restate.Reflect(h.catalogObj),
		restate.Reflect(h.conversationObj),
		restate.Reflect(h.schedulerObj),
		restate.Reflect(h.hubStateObj),
		restate.Reflect(h.maintenanceWf),
	}
}

func (h *Host) Handler() (http.HandlerFunc, error) { return h.restateSrv.Handler() }
func (h *Host) Ingress() *ingress.Client           { return h.ingressClient }
func (h *Host) Cipher() *ValueCipher               { return h.cipher }
func (h *Host) ListenAddr() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.listenAddr
}

func (h *Host) Start(ctx context.Context) (string, error) {
	handler, err := h.restateSrv.Handler()
	if err != nil {
		return "", err
	}
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	ln, err := net.Listen("tcp", h.cfg.ListenAddr)
	if err != nil {
		return "", fmt.Errorf("listen on %s: %w", h.cfg.ListenAddr, err)
	}
	srv := &http.Server{
		Handler:           handler,
		Protocols:         &protocols,
		ReadHeaderTimeout: 10 * time.Second,
	}

	h.mu.Lock()
	h.listenAddr = ln.Addr().String()
	h.httpSrv = srv
	addr := h.listenAddr
	h.mu.Unlock()

	go func() {
		<-ctx.Done()
		scx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(scx)
	}()
	go func() {
		_ = srv.Serve(ln)
	}()
	return addr, nil
}

func (h *Host) Stop(ctx context.Context) error {
	h.mu.Lock()
	srv := h.httpSrv
	h.httpSrv = nil
	h.mu.Unlock()
	if srv == nil {
		return nil
	}
	return srv.Shutdown(ctx)
}

// InvokeObject calls a Restate Virtual Object handler synchronously via ingress.
func InvokeObject[I any, O any](ctx context.Context, h *Host, serviceName, key, method string, input I, opts ...ingress.RequestOption) (O, error) {
	return ingress.Object[I, O](h.ingressClient, serviceName, key, method).Request(ctx, input, opts...)
}

// SendObject dispatches an asynchronous message to a Restate Virtual Object handler via ingress.
func SendObject[I any](ctx context.Context, h *Host, serviceName, key, method string, input I, opts ...ingress.SendOption) (ingress.SimpleSendResponse, error) {
	return ingress.ObjectSend[I](h.ingressClient, serviceName, key, method).Send(ctx, input, opts...)
}

// InvokeWorkflow calls a Restate Workflow handler synchronously via ingress.
func InvokeWorkflow[I any, O any](ctx context.Context, h *Host, serviceName, workflowID, method string, input I, opts ...ingress.RequestOption) (O, error) {
	return ingress.Workflow[I, O](h.ingressClient, serviceName, workflowID, method).Request(ctx, input, opts...)
}
