// SPDX-License-Identifier: AGPL-3.0-only
//
// host_test.go — integration tests driving the real WASM artifact
// (wasm/build/gateway.wasm) through the wazero host runtime.
//
// Every test asserts actual results: outbound messages, error codes,
// lifecycle sentinel errors.  No success-by-logging, no optional pong
// checks.  HTTP tests run against a local TLS server; no real network.

package host_test

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dividebyzero/neondoll-gateway-telegram/contract"
	"github.com/dividebyzero/neondoll-gateway-telegram/host"
)

// loadWASM reads the committed build artifact produced by:
//
//	cd wasm/gateway && tinygo build -target=wasi -o ../build/gateway.wasm .
func loadWASM(t *testing.T) []byte {
	t.Helper()
	paths := []string{
		"wasm/build/gateway.wasm",
		"../../neondoll-gateway-telegram/wasm/build/gateway.wasm",
	}
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err == nil {
			return b
		}
	}
	t.Fatalf("could not locate wasm/build/gateway.wasm")
	return nil
}

// startRuntime boots the guest and asserts the required exports resolve.
func startRuntime(t *testing.T, allowed []string, opts ...host.Option) *host.GatewayRuntime {
	t.Helper()
	r := host.NewGatewayRuntime(allowed, opts...)
	if err := r.Start(loadWASM(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if v, err := r.ABIVersion(); err != nil || v != 2 {
		t.Fatalf("ABIVersion: got %d, %v; want 2", v, err)
	}
	t.Cleanup(func() { _ = r.Shutdown() })
	return r
}

// outboundPayloads returns the "payload" of every recorded outbound message.
func outboundPayloads(r *host.GatewayRuntime) []string {
	msgs := r.Outbound
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		if m.Type == "outgoing" {
			out = append(out, string(m.Payload))
		}
	}
	return out
}

func containsMsg(msgs []string, want string) bool {
	for _, m := range msgs {
		if m == want {
			return true
		}
	}
	return false
}

// ═══════════════════════════════════════════════════════════════════════
// Polling architecture — the guest must sustain poll cycles and respond
// to inbound events deterministically.
// ═══════════════════════════════════════════════════════════════════════

func TestTelegramPollingViability(t *testing.T) {
	// Mock server returns empty result (simulating Telegram with no messages).
	empty := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true,"result":[]}`))
	}))
	t.Cleanup(empty.Close)

	r := startRuntime(t, []string{"127.0.0.1"},
		host.WithConfig(contract.GatewayConfig{
			Version: contract.ConfigVersion,
			Telegram: contract.TelegramConfig{
				BotToken: "test:inert",
				Host:     empty.URL,
			},
			Polling: contract.PollingConfig{TimeoutSeconds: 3},
		}),
		host.WithHTTPTransport(empty.Client().Transport.(*http.Transport)),
	)

	// Init ran: deterministic result of gateway_init via guest log line.
	if len(r.Log) == 0 {
		t.Fatal("no guest log lines recorded; gateway_init did not run")
	}
	if !strings.Contains(r.Log[0], "gateway_init") {
		t.Fatalf("first log line %q does not confirm gateway_init", r.Log[0])
	}

	// Inbound ping must produce exactly one deterministic pong.
	if err := r.HandleEvent(contract.Version0, "ping", ""); err != nil {
		t.Fatalf("HandleEvent(ping): %v", err)
	}
	got := outboundPayloads(r)
	if len(got) != 1 {
		t.Fatalf("outbound after ping: got %d messages %v; want 1", len(got), got)
	}
	if got[0] != `{"type":"pong","echo":"ping"}` {
		t.Fatalf("outbound after ping: got %q; want deterministic pong", got[0])
	}

	// Three poll cycles execute without error (HTTP to mock server succeeds,
	// returns empty result — no events produced because no Telegram messages).
	for i := 0; i < 3; i++ {
		if err := r.Poll(); err != nil {
			t.Fatalf("Poll #%d: %v", i+1, err)
		}
	}
	// Only the pong from the ping event should be present.
	got = outboundPayloads(r)
	if len(got) != 1 {
		t.Fatalf("outbound after 3 polls: got %d messages %v; want 1 (only pong, HTTP returned empty updates)",
			len(got), got)
	}
	// Verify polls executed via guest logs.
	var pollCount int
	for _, l := range r.Log {
		if strings.Contains(l, "gateway_poll:") {
			pollCount++
		}
	}
	if pollCount < 3 {
		t.Fatalf("gateway_poll log lines: got %d; want at least 3", pollCount)
	}
}

// ═══════════════════════════════════════════════════════════════════════
// Destination policy — parsed URL validation, HTTPS only, exact hostnames.
// ═══════════════════════════════════════════════════════════════════════

func TestDestinationPolicy(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "hello, gateway!")
	}))
	defer srv.Close()

	// Trust the test server's self-signed cert.
	client := srv.Client()
	transport := client.Transport

	tests := []struct {
		name    string
		allowed []string
		url     string
		wantErr string
	}{
		{
			name:    "exact_host_allowed",
			allowed: []string{"127.0.0.1"},
			url:     srv.URL + "/ok",
			wantErr: "0",
		},
		{
			name:    "plain_http_blocked",
			allowed: []string{"127.0.0.1"},
			url:     strings.Replace(srv.URL, "https://", "http://", 1) + "/ok",
			wantErr: "2",
		},
		{
			name:    "foreign_host_blocked",
			allowed: []string{"api.telegram.org"},
			url:     "https://example.com/getMe",
			wantErr: "2",
		},
		{
			name:    "lookalike_suffix_not_allowed",
			allowed: []string{"api.telegram.org"},
			url:     "https://api.telegram.org.evil.com/getMe",
			wantErr: "2",
		},
		{
			name:    "subdomain_not_allowed_exact_match",
			allowed: []string{"api.telegram.org"},
			url:     "https://www.api.telegram.org/getMe",
			wantErr: "2",
		},
		{
			name:    "missing_host_blocked",
			allowed: []string{"api.telegram.org"},
			url:     "https://",
			wantErr: "2",
		},
		{
			name:    "ftp_scheme_blocked",
			allowed: []string{"api.telegram.org"},
			url:     "ftp://api.telegram.org/getMe",
			wantErr: "2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := startRuntime(t, tt.allowed, host.WithHTTPTransport(transport))
			payload := fmt.Sprintf(`{"url":%q}`, tt.url)
			if err := r.HandleEvent(contract.Version0, "http_request", payload); err != nil {
				t.Fatalf("HandleEvent(http_request): %v", err)
			}
			got := outboundPayloads(r)
			if len(got) != 1 {
				t.Fatalf("outbound: got %d messages %v; want 1 http_result", len(got), got)
			}
			if !strings.Contains(got[0], `"err":`+tt.wantErr) {
				t.Fatalf("result %q: want err=%s (policy violation maps to 2, success to 0)", got[0], tt.wantErr)
			}
		})
	}
}

func TestHTTPExactHostAndLocalBody(t *testing.T) {
	const body = "hello, gateway!"
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, body)
	}))
	defer srv.Close()

	r := startRuntime(t, []string{"127.0.0.1"}, host.WithHTTPTransport(srv.Client().Transport))
	payload := fmt.Sprintf(`{"url":%q}`, srv.URL+"/greet")
	if err := r.HandleEvent(contract.Version0, "http_request", payload); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}
	got := outboundPayloads(r)
	if len(got) != 1 {
		t.Fatalf("outbound: got %v; want 1 http_result", got)
	}
	want := fmt.Sprintf(`{"type":"http_result","err":0,"status":200,"len":%d,"head":%q}`, len(body), body)
	if got[0] != want {
		t.Fatalf("http_result: got %q; want %q", got[0], want)
	}
}

// ═══════════════════════════════════════════════════════════════════════
// Non-2xx and oversized body handling
// ═══════════════════════════════════════════════════════════════════════

func TestHTTPNon2xxIsExplicit(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer srv.Close()

	r := startRuntime(t, []string{"127.0.0.1"}, host.WithHTTPTransport(srv.Client().Transport))
	payload := fmt.Sprintf(`{"url":%q}`, srv.URL+"/secret")
	if err := r.HandleEvent(contract.Version0, "http_request", payload); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}
	got := outboundPayloads(r)
	if len(got) != 1 {
		t.Fatalf("outbound: got %v; want 1 http_result", got)
	}
	// err=6 is httpErrStatus: the status code is surfaced, no body data.
	want := `{"type":"http_result","err":6,"status":401,"len":0}`
	if got[0] != want {
		t.Fatalf("http_result: got %q; want %q", got[0], want)
	}
}

func TestHTTPOversizedBodyIsExplicit(t *testing.T) {
	// Server returns 8000 bytes; the guest's respBuf is 4096.
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(make([]byte, 8000))
	}))
	defer srv.Close()

	r := startRuntime(t, []string{"127.0.0.1"}, host.WithHTTPTransport(srv.Client().Transport))
	payload := fmt.Sprintf(`{"url":%q}`, srv.URL+"/big")
	if err := r.HandleEvent(contract.Version0, "http_request", payload); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}
	got := outboundPayloads(r)
	if len(got) != 1 {
		t.Fatalf("outbound: got %v; want 1 http_result", got)
	}
	// err=5 is httpErrTooLarge: explicitly rejected, never truncated silently.
	want := `{"type":"http_result","err":5,"status":200,"len":0}`
	if got[0] != want {
		t.Fatalf("http_result: got %q; want %q", got[0], want)
	}
}

// ═══════════════════════════════════════════════════════════════════════
// Redirect validation on every hop
// ═══════════════════════════════════════════════════════════════════════

func TestRedirectToAllowedTargetIsFollowed(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/final", http.StatusFound)
	})
	mux.HandleFunc("/final", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "landed")
	})
	srv := httptest.NewTLSServer(mux)
	defer srv.Close()

	r := startRuntime(t, []string{"127.0.0.1"}, host.WithHTTPTransport(srv.Client().Transport))
	payload := fmt.Sprintf(`{"url":%q}`, srv.URL+"/start")
	if err := r.HandleEvent(contract.Version0, "http_request", payload); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}
	got := outboundPayloads(r)
	if len(got) != 1 {
		t.Fatalf("outbound: got %v; want 1 http_result", got)
	}
	want := `{"type":"http_result","err":0,"status":200,"len":6,"head":"landed"}`
	if got[0] != want {
		t.Fatalf("http_result: got %q; want %q", got[0], want)
	}
}

func TestRedirectAwayFromAllowlistIsBlocked(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://api.telegram.org/bot123/getMe", http.StatusFound)
	})
	srv := httptest.NewTLSServer(mux)
	defer srv.Close()

	r := startRuntime(t, []string{"127.0.0.1"}, host.WithHTTPTransport(srv.Client().Transport))
	payload := fmt.Sprintf(`{"url":%q}`, srv.URL+"/start")
	if err := r.HandleEvent(contract.Version0, "http_request", payload); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}
	got := outboundPayloads(r)
	if len(got) != 1 {
		t.Fatalf("outbound: got %v; want 1 http_result", got)
	}
	// err=3 is httpErrRedirect.  The cross-host redirect must not be followed.
	want := `{"type":"http_result","err":3,"status":0,"len":0}`
	if got[0] != want {
		t.Fatalf("http_result: got %q; want %q", got[0], want)
	}
}

// ═══════════════════════════════════════════════════════════════════════
// Lifecycle safety — uninitialized, closed, concurrent shutdown
// ═══════════════════════════════════════════════════════════════════════

func TestUnstartedOperationsFail(t *testing.T) {
	r := host.NewGatewayRuntime([]string{"api.telegram.org"})

	if err := r.HandleEvent(contract.Version0, "ping", ""); !errors.Is(err, host.ErrNotStarted) {
		t.Fatalf("HandleEvent before start: got %v; want ErrNotStarted", err)
	}
	if err := r.Poll(); !errors.Is(err, host.ErrNotStarted) {
		t.Fatalf("Poll before start: got %v; want ErrNotStarted", err)
	}
	if _, err := r.ABIVersion(); !errors.Is(err, host.ErrNotStarted) {
		t.Fatalf("ABIVersion before start: got %v; want ErrNotStarted", err)
	}
	// Shutdown on an uninitialized runtime is a safe no-op.
	if err := r.Shutdown(); err != nil {
		t.Fatalf("Shutdown before start: got %v; want nil", err)
	}
}

func TestDoubleStartFails(t *testing.T) {
	r := host.NewGatewayRuntime([]string{"api.telegram.org"})
	if err := r.Start(loadWASM(t)); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if err := r.Start(loadWASM(t)); !errors.Is(err, host.ErrAlreadyStarted) {
		t.Fatalf("second Start: got %v; want ErrAlreadyStarted", err)
	}
	_ = r.Shutdown()
}

func TestShutdownIsIdempotentAndLocksEverythingOut(t *testing.T) {
	r := startRuntime(t, []string{"api.telegram.org"})
	for i := 0; i < 3; i++ {
		if err := r.Shutdown(); err != nil {
			t.Fatalf("Shutdown #%d: got %v; want nil", i+1, err)
		}
	}
	if err := r.HandleEvent(contract.Version0, "ping", ""); !errors.Is(err, host.ErrClosed) {
		t.Fatalf("HandleEvent after shutdown: got %v; want ErrClosed", err)
	}
	if err := r.Poll(); !errors.Is(err, host.ErrClosed) {
		t.Fatalf("Poll after shutdown: got %v; want ErrClosed", err)
	}
	if _, err := r.ABIVersion(); !errors.Is(err, host.ErrClosed) {
		t.Fatalf("ABIVersion after shutdown: got %v; want ErrClosed", err)
	}
}

func TestStartAfterShutdownFails(t *testing.T) {
	r := startRuntime(t, []string{"api.telegram.org"})
	if err := r.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := r.Start(loadWASM(t)); !errors.Is(err, host.ErrClosed) {
		t.Fatalf("Start after shutdown: got %v; want ErrClosed", err)
	}
}

func TestConcurrentShutdownIsSafe(t *testing.T) {
	r := startRuntime(t, []string{"api.telegram.org"})

	const workers = 16
	var wg sync.WaitGroup
	errs := make([]error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = r.Shutdown()
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent Shutdown #%d: got %v; want nil", i, err)
		}
	}
	if err := r.HandleEvent(contract.Version0, "ping", ""); !errors.Is(err, host.ErrClosed) {
		t.Fatalf("HandleEvent after concurrent shutdown: got %v; want ErrClosed", err)
	}
}

func TestConcurrentHandleEventAndShutdownNoPanic(t *testing.T) {
	r := startRuntime(t, []string{"127.0.0.1"})

	errs := make(chan error, 64)
	var wg sync.WaitGroup

	// Fire events while shutdown happens concurrently.
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			err := r.HandleEvent(contract.Version0, "ping", "")
			if err != nil {
				errs <- err
			}
			time.Sleep(100 * time.Microsecond)
		}
	}()

	time.Sleep(10 * time.Millisecond)
	if err := r.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	close(stop)
	wg.Wait()
	close(errs)

	// Every error produced during the race must be a lifecycle error —
	// never a panic, never a use-after-close.
	for err := range errs {
		if !errors.Is(err, host.ErrClosed) {
			t.Fatalf("HandleEvent during shutdown: got %v; want ErrClosed (or nil)", err)
		}
	}
}

func TestPollAfterShutdownNoPanic(t *testing.T) {
	r := startRuntime(t, []string{"api.telegram.org"})
	_ = r.Shutdown()
	if err := r.Poll(); !errors.Is(err, host.ErrClosed) {
		t.Fatalf("Poll after shutdown: got %v; want ErrClosed", err)
	}
}

func TestShutdownAndRestart(t *testing.T) {
	wasm := loadWASM(t)

	r := host.NewGatewayRuntime([]string{"api.telegram.org"})
	if err := r.Start(wasm); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if err := r.HandleEvent(contract.Version0, "ping", ""); err != nil {
		t.Fatalf("HandleEvent on first lifecycle: %v", err)
	}
	got := outboundPayloads(r)
	if len(got) != 1 || got[0] != `{"type":"pong","echo":"ping"}` {
		t.Fatalf("first lifecycle pong: got %v", got)
	}
	if err := r.Shutdown(); err != nil {
		t.Fatalf("first Shutdown: %v", err)
	}

	// Fresh runtime instance: the WASM bytes must instantiate again.
	r2 := host.NewGatewayRuntime([]string{"api.telegram.org"})
	if err := r2.Start(wasm); err != nil {
		t.Fatalf("second Start: %v", err)
	}
	defer r2.Shutdown()
	if err := r2.HandleEvent(contract.Version0, "ping", ""); err != nil {
		t.Fatalf("HandleEvent on second lifecycle: %v", err)
	}
	got2 := outboundPayloads(r2)
	if len(got2) != 1 || got2[0] != `{"type":"pong","echo":"ping"}` {
		t.Fatalf("second lifecycle pong: got %v", got2)
	}
}

// ═══════════════════════════════════════════════════════════════════════
// Event encoding sanity
// ═══════════════════════════════════════════════════════════════════════

func TestInvalidEventRejected(t *testing.T) {
	r := startRuntime(t, []string{"api.telegram.org"})
	if err := r.HandleEvent(contract.Version0, "", ""); !errors.Is(err, host.ErrInvalidEvent) {
		t.Fatalf("HandleEvent(empty type): got %v; want ErrInvalidEvent", err)
	}
}

// ═══════════════════════════════════════════════════════════════════════
// Lifecycle state-machine races
// ═══════════════════════════════════════════════════════════════════════

// TestConcurrentStartShutdown verifies that Shutdown called immediately
// after Start (or during its init phase) does not panic and leaves the
// runtime in a closed state.  Start may succeed, be interrupted, or
// observe an already-closed runtime — any of those is valid.
func TestConcurrentStartShutdown(t *testing.T) {
	wasm := loadWASM(t)

	r := host.NewGatewayRuntime([]string{"api.telegram.org"})

	var startErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		startErr = r.Start(wasm)
	}()

	// Let Start begin executing, then interrupt.
	time.Sleep(time.Millisecond)
	shutdownErr := r.Shutdown()
	wg.Wait()

	if shutdownErr != nil {
		t.Errorf("Shutdown during Start: %v", shutdownErr)
	}

	// Start could have completed before Shutdown ran, or been interrupted.
	if startErr != nil &&
		!errors.Is(startErr, host.ErrClosed) &&
		!errors.Is(startErr, host.ErrAlreadyStarted) {
		t.Errorf("Start during Shutdown race: got %v; want nil, ErrClosed, or ErrAlreadyStarted", startErr)
	}

	// The runtime must be in closed state.  A subsequent Shutdown must
	// return nil (safe to call on closed runtime).
	if err := r.Shutdown(); err != nil {
		t.Errorf("second Shutdown after race: got %v; want nil", err)
	}
	// Operations on a closed runtime must fail with ErrClosed.
	if err := r.Poll(); !errors.Is(err, host.ErrClosed) {
		t.Errorf("Poll after race-shutdown: got %v; want ErrClosed", err)
	}
}

// TestConcurrentShutdownStart verifies that calling Start after a
// concurrent Start+Shutdown race completes does not leave the runtime
// in a permanently closed state (new Start on a fresh runtime succeeds).
func TestConcurrentShutdownStart(t *testing.T) {
	wasm := loadWASM(t)

	r := host.NewGatewayRuntime([]string{"api.telegram.org"})

	// Provoke the race.
	go func() { _ = r.Start(wasm) }()
	time.Sleep(time.Millisecond)

	// Shutdown while Start is in progress.
	_ = r.Shutdown()

	// A fresh runtime (new instance) must Start cleanly.
	r2 := host.NewGatewayRuntime([]string{"api.telegram.org"})
	if err := r2.Start(wasm); err != nil {
		t.Fatalf("fresh Start after race: %v", err)
	}
	defer r2.Shutdown()

	if err := r2.HandleEvent(contract.Version0, "ping", ""); err != nil {
		t.Fatalf("HandleEvent on fresh runtime: %v", err)
	}
}

// TestStartShutdownStartDuringInit synchronises so that Shutdown runs
// during Start's init phase, exercising the stateStarting → stateClosed
// transition explicitly.
func TestStartShutdownStartDuringInit(t *testing.T) {
	wasm := loadWASM(t)

	r := host.NewGatewayRuntime([]string{"api.telegram.org"})
	var startErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		startErr = r.Start(wasm)
	}()
	// Ensure Shutdown happens.  The tiny sleep puts the call inside the
	// stateStarting window on almost every run.
	time.Sleep(2 * time.Millisecond)
	_ = r.Shutdown() // signals cancellation
	wg.Wait()

	if startErr != nil &&
		!errors.Is(startErr, host.ErrClosed) &&
		!errors.Is(startErr, host.ErrAlreadyStarted) {
		t.Errorf("Start during Shutdown: got %v; want nil, ErrClosed, or ErrAlreadyStarted", startErr)
	}

	// Closed runtime stays closed.
	if err := r.Poll(); !errors.Is(err, host.ErrClosed) {
		t.Errorf("Poll after Shutdown-during-Start: got %v; want ErrClosed", err)
	}
}

// TestRaceDetectorStartShutdownStress runs concurrent Start and Shutdown
// in tight goroutines for the race detector to catch unprotected state
// transitions.
func TestRaceDetectorStartShutdownStress(t *testing.T) {
	wasm := loadWASM(t)
	r := host.NewGatewayRuntime([]string{"api.telegram.org"})

	done := make(chan struct{})
	var startWg, shutdownWg sync.WaitGroup

	startWg.Add(1)
	go func() {
		defer startWg.Done()
		for {
			select {
			case <-done:
				return
			default:
				_ = r.Start(wasm)
			}
		}
	}()

	shutdownWg.Add(1)
	go func() {
		defer shutdownWg.Done()
		for {
			select {
			case <-done:
				return
			default:
				_ = r.Shutdown()
			}
		}
	}()

	// Let the race detector observe many concurrent transitions.
	time.Sleep(50 * time.Millisecond)
	close(done)
	startWg.Wait()
	shutdownWg.Wait()

	// Final Shutdown must always succeed (no panic, no deadlock).
	if err := r.Shutdown(); err != nil {
		t.Errorf("final Shutdown after stress: %v", err)
	}
}
