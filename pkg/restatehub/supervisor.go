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

// Package restatehub provides the Restate-native persistence, state coordination,
// and durable workflow execution engine for the Scion Hub.
package restatehub

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// SupervisorConfig configures the local or managed restate-server process.
type SupervisorConfig struct {
	// BinaryPath is the path to the restate-server executable.
	// If empty, LookPath("restate-server") and standard fallback paths are searched.
	BinaryPath string
	// BaseDir is the directory where restate-server stores Bifrost WAL and RocksDB state.
	// Defaults to ~/.scion/restate in workstation mode.
	BaseDir string
	// IngressAddr is the HTTP ingress listen address (default "127.0.0.1:18080").
	IngressAddr string
	// AdminAddr is the HTTP admin listen address (default "127.0.0.1:19070").
	AdminAddr string
	// NodeName is the Restate node name (default "scion-workstation").
	NodeName string
	// AutoProvision controls whether the cluster/node is auto-provisioned on start.
	AutoProvision bool
	// External indicates that restate-server is managed externally (e.g., Hosted StatefulSet)
	// and Supervisor should only verify readiness and register deployments without spawning a child process.
	External bool
	// Logger is the structured logger for supervisor lifecycle events.
	Logger *slog.Logger
	// StartupTimeout bounds how long Start waits for restate-server /health readiness.
	StartupTimeout time.Duration
	// HTTPClient is used for health checks and deployment registration.
	HTTPClient *http.Client
}

// DefaultSupervisorConfig returns workstation defaults for supervising restate-server on loopback.
func DefaultSupervisorConfig(baseDir string) SupervisorConfig {
	if baseDir == "" {
		if home, err := os.UserHomeDir(); err == nil {
			baseDir = filepath.Join(home, ".scion", "restate")
		} else {
			baseDir = filepath.Join(os.TempDir(), "scion-restate")
		}
	}
	return SupervisorConfig{
		BaseDir:        baseDir,
		IngressAddr:    "127.0.0.1:18080",
		AdminAddr:      "127.0.0.1:19070",
		NodeName:       "scion-workstation",
		AutoProvision:  true,
		StartupTimeout: 30 * time.Second,
		Logger:         slog.Default(),
	}
}

// Supervisor manages the lifecycle of a restate-server child process and registers
// the Scion Hub SDK service endpoint with Restate's Admin API.
type Supervisor struct {
	cfg        SupervisorConfig
	httpClient *http.Client
	log        *slog.Logger

	mu      sync.Mutex
	cmd     *exec.Cmd
	doneCh  chan struct{}
	waitErr error
	started bool
}

// NewSupervisor creates a new Restate supervisor with the given configuration.
func NewSupervisor(cfg SupervisorConfig) *Supervisor {
	if cfg.IngressAddr == "" {
		cfg.IngressAddr = "127.0.0.1:18080"
	}
	if cfg.AdminAddr == "" {
		cfg.AdminAddr = "127.0.0.1:19070"
	}
	if cfg.NodeName == "" {
		cfg.NodeName = "scion-workstation"
	}
	if cfg.StartupTimeout <= 0 {
		cfg.StartupTimeout = 30 * time.Second
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second}
	}
	return &Supervisor{
		cfg:        cfg,
		httpClient: hc,
		log:        log,
	}
}

// IngressURL returns the full HTTP base URL for the Restate ingress endpoint.
func (s *Supervisor) IngressURL() string {
	if strings.HasPrefix(s.cfg.IngressAddr, "http://") || strings.HasPrefix(s.cfg.IngressAddr, "https://") {
		return strings.TrimRight(s.cfg.IngressAddr, "/")
	}
	return "http://" + s.cfg.IngressAddr
}

// AdminURL returns the full HTTP base URL for the Restate admin endpoint.
func (s *Supervisor) AdminURL() string {
	if strings.HasPrefix(s.cfg.AdminAddr, "http://") || strings.HasPrefix(s.cfg.AdminAddr, "https://") {
		return strings.TrimRight(s.cfg.AdminAddr, "/")
	}
	return "http://" + s.cfg.AdminAddr
}

// ResolveBinary locates the restate-server binary on the host.
func (s *Supervisor) ResolveBinary() (string, error) {
	if s.cfg.BinaryPath != "" {
		if _, err := os.Stat(s.cfg.BinaryPath); err != nil {
			return "", fmt.Errorf("configured restate-server binary %q not accessible: %w", s.cfg.BinaryPath, err)
		}
		return s.cfg.BinaryPath, nil
	}
	if envBin := os.Getenv("SCION_RESTATE_SERVER_BIN"); envBin != "" {
		if _, err := os.Stat(envBin); err == nil {
			return envBin, nil
		}
	}
	if p, err := exec.LookPath("restate-server"); err == nil {
		return p, nil
	}
	if home, err := os.UserHomeDir(); err == nil {
		candidates := []string{
			filepath.Join(home, ".local", "bin", "restate-server"),
			filepath.Join(home, ".cargo", "bin", "restate-server"),
			filepath.Join(home, "dev", "restate", "target", "release", "restate-server"),
			filepath.Join(home, "dev", "restate", "target", "debug", "restate-server"),
		}
		for _, c := range candidates {
			if st, err := os.Stat(c); err == nil && !st.IsDir() {
				return c, nil
			}
		}
		// Check local container overlay cache if present in sandbox workstation environments
		overlayPattern := filepath.Join(home, ".local", "share", "containers", "storage", "overlay", "*", "diff", "usr", "local", "bin", "restate-server")
		if matches, err := filepath.Glob(overlayPattern); err == nil && len(matches) > 0 {
			return matches[len(matches)-1], nil
		}
	}
	return "", errors.New("restate-server binary not found in PATH or standard locations")
}

// Start launches restate-server (unless External is true) and waits until both
// the Admin and Ingress health endpoints report ready.
func (s *Supervisor) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return nil
	}

	if !s.cfg.External {
		bin, err := s.ResolveBinary()
		if err != nil {
			s.mu.Unlock()
			return err
		}
		if s.cfg.BaseDir == "" {
			s.mu.Unlock()
			return errors.New("restate supervisor BaseDir is required when managing local restate-server")
		}
		if err := os.MkdirAll(s.cfg.BaseDir, 0o700); err != nil {
			s.mu.Unlock()
			return fmt.Errorf("create restate base directory %s: %w", s.cfg.BaseDir, err)
		}

		args := []string{
			"--base-dir", s.cfg.BaseDir,
			"--node-name", s.cfg.NodeName,
			"--no-logo",
		}
		if !s.cfg.AutoProvision {
			args = append(args, "--auto-provision=false")
		}

		cmd := exec.CommandContext(context.Background(), bin, args...)
		cmd.Env = append(os.Environ(),
			"RESTATE_LOG_FORMAT=json",
			"RUST_LOG=warn,restate=info",
			"RESTATE_INGRESS__BIND_ADDRESS="+stripScheme(s.cfg.IngressAddr),
			"RESTATE_ADMIN__BIND_ADDRESS="+stripScheme(s.cfg.AdminAddr),
		)
		doneCh := make(chan struct{})
		if err := cmd.Start(); err != nil {
			s.mu.Unlock()
			return fmt.Errorf("start restate-server: %w", err)
		}
		s.cmd = cmd
		s.doneCh = doneCh
		s.started = true
		go func() {
			err := cmd.Wait()
			s.mu.Lock()
			s.waitErr = err
			s.mu.Unlock()
			close(doneCh)
		}()
	} else {
		s.started = true
	}
	s.mu.Unlock()

	waitCtx, cancel := context.WithTimeout(ctx, s.cfg.StartupTimeout)
	defer cancel()
	if err := s.WaitReady(waitCtx); err != nil {
		_ = s.Stop(context.Background())
		return err
	}
	return nil
}

// WaitReady polls the restate-server admin /health endpoint until it responds 200 OK.
func (s *Supervisor) WaitReady(ctx context.Context) error {
	healthURL := s.AdminURL() + "/health"
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if err := s.checkHealth(ctx, healthURL); err == nil {
			return nil
		}
		s.mu.Lock()
		doneCh := s.doneCh
		waitErr := s.waitErr
		s.mu.Unlock()
		if doneCh != nil {
			select {
			case <-doneCh:
				return fmt.Errorf("restate-server exited prematurely before becoming ready: %w", waitErr)
			default:
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for restate-server readiness at %s: %w", healthURL, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (s *Supervisor) checkHealth(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d from %s", resp.StatusCode, url)
	}
	return nil
}
// Started reports whether the Supervisor has started or connected to restate-server.
func (s *Supervisor) Started() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.started
}

// RegisterDeployment registers the Scion Hub Restate SDK HTTP/2 endpoint with
// restate-server's /deployments admin endpoint. It is idempotent (force=true updates
// existing local workstation deployments in place).
func (s *Supervisor) RegisterDeployment(ctx context.Context, sdkEndpointURI string) error {
	if !strings.HasPrefix(sdkEndpointURI, "http://") && !strings.HasPrefix(sdkEndpointURI, "https://") {
		sdkEndpointURI = "http://" + sdkEndpointURI
	}
	payload := map[string]any{
		"uri":   sdkEndpointURI,
		"force": true,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.AdminURL()+"/deployments", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("register restate deployment %s: %w", sdkEndpointURI, err)
	}
	defer func() { _ = resp.Body.Close() }()
	respBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("register restate deployment %s failed (status %d): %s", sdkEndpointURI, resp.StatusCode, string(respBytes))
	}
	s.log.Info("Registered Scion Hub Restate service deployment", "uri", sdkEndpointURI)
	return nil
}

// Stop gracefully terminates the supervised restate-server child process.
func (s *Supervisor) Stop(ctx context.Context) error {
	s.mu.Lock()
	cmd := s.cmd
	doneCh := s.doneCh
	s.started = false
	s.cmd = nil
	s.doneCh = nil
	s.mu.Unlock()

	if cmd == nil || cmd.Process == nil {
		return nil
	}
	_ = cmd.Process.Signal(os.Interrupt)
	select {
	case <-doneCh:
		return nil
	case <-ctx.Done():
		_ = cmd.Process.Kill()
		<-doneCh
		return ctx.Err()
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		<-doneCh
		return nil
	}
}

func stripScheme(addr string) string {
	addr = strings.TrimPrefix(addr, "http://")
	addr = strings.TrimPrefix(addr, "https://")
	if _, _, err := net.SplitHostPort(addr); err != nil && !strings.Contains(addr, ":") {
		return "127.0.0.1:" + addr
	}
	return addr
}
