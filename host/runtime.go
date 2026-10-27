// SPDX-License-Identifier: AGPL-3.0-only
//
// host — wazero host runtime for the external WASM Gateway module.
//
// ABI v2: the guest owns its event buffer and exports accessor
// functions (gateway_event_buf / gateway_event_buf_size).  The host
// queries those after instantiation instead of assuming a fixed offset.
//
// Destination policy: outbound HTTPS is restricted to an explicit
// allowlist of hostnames.  URLs are parsed and validated (scheme https,
// exact hostname match).  Redirects are validated on every hop via the
// http.Client CheckRedirect policy.
//
// Lifecycle: uninitialized → started → closed.  All public methods are
// safe against uninitialized, closed, and concurrently shutting-down
// runtimes.

package host

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"

	"github.com/dividebyzero/neondoll-gateway-telegram/contract"
)

const (
	// moduleName is the WASI module name with which the guest registers imports.
	moduleName = "gateway_host"

	// abiVersion is the guest ABI this host expects.
	abiVersion = 2

	// httpResultLen is the size of the result struct hostHTTPGet writes:
	// [written:4][err:4][status:4], all little-endian.
	httpResultLen = 12

	// maxEventLen bounds inbound event payloads (guest eventBufSize).
	maxEventLen = 4096

	// httpTimeout bounds a single host-mediated HTTP request.
	httpTimeout = 15 * time.Second

	// redirectLimit bounds redirect hops (http.Client default is 10).
	redirectLimit = 5
)

// HTTP result error codes written to the guest result struct.
const (
	httpErrOK         = 0
	httpErrInvalidURL = 1 // URL could not be parsed
	httpErrBlocked    = 2 // scheme or hostname violates destination policy
	httpErrRedirect   = 3 // redirect destination violates destination policy
	httpErrNetwork    = 4 // network/request error
	httpErrTooLarge   = 5 // response body exceeds the guest's respMax
	httpErrStatus     = 6 // non-2xx HTTP status; status field carries it
	httpErrMemWrite   = 7 // could not write result/body into guest memory
	httpErrReadBody   = 8 // failed while reading response body
)

// Lifecycle sentinel errors returned by public methods.
var (
	ErrNotStarted      = errors.New("gateway runtime: not started")
	ErrAlreadyStarted  = errors.New("gateway runtime: already started")
	ErrClosed          = errors.New("gateway runtime: closed")
	ErrShuttingDown    = errors.New("gateway runtime: shutting down")
	ErrInvalidEvent    = errors.New("gateway runtime: invalid event")
	ErrMissingExport   = errors.New("gateway runtime: guest missing required export")
	errRedirectBlocked = errors.New("gateway runtime: redirect destination blocked")
)

// GatewayRuntime mediates the external WASM Gateway module.
type GatewayRuntime struct {
	allowedHosts  []string
	httpTransport http.RoundTripper // injectable transport (tests use TLS test server)
	httpClient    *http.Client      // built lazily from httpTransport + policy

	mu      sync.Mutex
	state   runtimeState
	runtime wazero.Runtime
	module  api.Module
	ctx     context.Context
	cancel  context.CancelFunc

	// initDone is closed exactly once when Start() completes (success or
	// failure).  Shutdown waits on it when it observes stateStarting so no
	// initialization work remains active after Shutdown returns.
	initDone chan struct{}

	// initBarrier, if non-nil, is invoked by Start after transitioning to
	// stateStarting and before any init work.  Tests use it to make
	// lifecycle races deterministic.
	initBarrier func()

	// Guest-owned event buffer address/size, resolved after Start.
	eventBufPtr uint32

	// cfg is the optional gateway configuration (M2.2 boundary).
	// nil means the runtime is unconfigured: projection/credential
	// imports report nothing and appended records are not redacted.
	cfg *contract.GatewayConfig

	// Recorded host-observable results (for feasibility tests).
	Log      []string
	Outbound []contract.GatewayMessage
}

// Option configures a GatewayRuntime.
type Option func(*GatewayRuntime)

// WithHTTPTransport overrides the RoundTripper used for host-mediated HTTP.
// Tests inject the transport of an httptest TLS server here.
func WithHTTPTransport(rt http.RoundTripper) Option {
	return func(r *GatewayRuntime) { r.httpTransport = rt }
}

// WithConfig supplies the gateway configuration (M2.2 config boundary).
// The config is validated during Start with explicit errors; the guest
// receives a redacted projection via the config_projection import and
// host-owned credentials (Telegram.BotToken) exclusively through the
// mediated telegram_token import.  Optional: without it the runtime is
// unconfigured and both imports report nothing.
func WithConfig(cfg contract.GatewayConfig) Option {
	return func(r *GatewayRuntime) { r.cfg = &cfg }
}

type runtimeState int

const (
	stateUninitialized runtimeState = iota
	stateStarting
	stateStarted
	stateClosed
)

// NewGatewayRuntime builds a runtime restricted to the given hostnames.
func NewGatewayRuntime(allowedHosts []string, opts ...Option) *GatewayRuntime {
	norm := make([]string, 0, len(allowedHosts))
	for _, h := range allowedHosts {
		norm = append(norm, strings.ToLower(strings.TrimSpace(h)))
	}
	r := &GatewayRuntime{allowedHosts: norm, initDone: make(chan struct{})}
	for _, o := range opts {
		o(r)
	}
	return r
}

func (r *GatewayRuntime) stateName(s runtimeState) string {
	switch s {
	case stateUninitialized:
		return "uninitialized"
	case stateStarting:
		return "starting"
	case stateStarted:
		return "started"
	case stateClosed:
		return "closed"
	}
	return "unknown"
}

// Start instantiates the WASM module.  Only valid from uninitialized.
// A failed Start closes all resources and leaves the runtime in closed
// state — callers build a fresh runtime to retry.
func (r *GatewayRuntime) Start(wasmBytes []byte) error {
	r.mu.Lock()
	if r.state != stateUninitialized {
		r.mu.Unlock()
		if r.state == stateClosed {
			return ErrClosed
		}
		return ErrAlreadyStarted
	}
	r.state = stateStarting
	r.mu.Unlock()
	// Signal Start completion on every path from here; Shutdown waits on
	// this when it observes stateStarting.
	defer close(r.initDone)

	if r.initBarrier != nil {
		r.initBarrier()
	}

	// Validate the optional gateway configuration before any init work.
	// Failures are explicit and never carry secret values.  A failed
	// Start leaves the runtime closed (initDone is closed by the defer
	// above, so Shutdown unblocks).
	if r.cfg != nil {
		if err := r.cfg.Validate(); err != nil {
			r.mu.Lock()
			r.state = stateClosed
			r.mu.Unlock()
			return err
		}
	}

	// Create init-time resources as local variables.
	ctx, cancel := context.WithCancel(context.Background())
	rt := wazero.NewRuntime(ctx)

	// fail marks the runtime closed and cleans up init-level resources
	// (cancel, rt).  Callers close module-level resources (mod) first.
	fail := func(cause error) error {
		r.mu.Lock()
		r.state = stateClosed
		r.mu.Unlock()
		cancel()
		_ = rt.Close(ctx)
		return cause
	}

	// Instantiate WASI so the TinyGo-compiled module's runtime calls work.
	if _, err := wasi_snapshot_preview1.Instantiate(ctx, rt); err != nil {
		return fail(fmt.Errorf("instantiate wasi: %w", err))
	}

	// Host capability functions.
	builder := rt.NewHostModuleBuilder(moduleName)
	builder.NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module, ptr, size uint32) {
			msg := readMem(m, ptr, size)
			r.appendLog(msg)
		}).
		Export("log")
	builder.NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module, ptr, size uint32) {
			payload := readMem(m, ptr, size)
			r.appendOutbound(contract.GatewayMessage{
				Version: contract.Version0,
				Type:    "outgoing",
				Payload: []byte(payload),
			})
		}).
		Export("outgoing")
	builder.NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module, urlPtr, urlLen, respBuf, respMax, resultPtr uint32) {
			r.hostHTTPGet(ctx, m, urlPtr, urlLen, respBuf, respMax, resultPtr)
		}).
		Export("http_get")
	builder.NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module, ptr, maxLen uint32) int32 {
			return r.hostConfigProjection(m, ptr, maxLen)
		}).
		Export("config_projection")
	builder.NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module, ptr, maxLen uint32) int32 {
			return r.hostTelegramToken(m, ptr, maxLen)
		}).
		Export("telegram_token")
	if _, err := builder.Instantiate(ctx); err != nil {
		return fail(fmt.Errorf("instantiate host module: %w", err))
	}

	// Compile and instantiate the guest.
	compiled, err := rt.CompileModule(ctx, wasmBytes)
	if err != nil {
		return fail(fmt.Errorf("compile guest: %w", err))
	}
	mod, err := rt.InstantiateModule(ctx, compiled, wazero.NewModuleConfig().WithName("gateway").WithStartFunctions())
	if err != nil {
		return fail(fmt.Errorf("instantiate guest: %w", err))
	}

	// Resolve guest-owned event buffer.
	bufFn := mod.ExportedFunction("gateway_event_buf")
	sizeFn := mod.ExportedFunction("gateway_event_buf_size")
	if bufFn == nil || sizeFn == nil {
		_ = mod.Close(ctx)
		return fail(fmt.Errorf("%w: gateway_event_buf/gateway_event_buf_size", ErrMissingExport))
	}
	ptrRes, err := bufFn.Call(ctx)
	if err != nil {
		_ = mod.Close(ctx)
		return fail(fmt.Errorf("resolve event buffer: %w", err))
	}
	sizeRes, err := sizeFn.Call(ctx)
	if err != nil {
		_ = mod.Close(ctx)
		return fail(fmt.Errorf("resolve event buffer size: %w", err))
	}
	eventBufPtr := uint32(ptrRes[0])
	if eventBufPtr == 0 || uint32(sizeRes[0]) == 0 {
		_ = mod.Close(ctx)
		return fail(fmt.Errorf("%w: gateway_event_buf returned zero address/size", ErrMissingExport))
	}

	// Run guest init.
	initFn := mod.ExportedFunction("gateway_init")
	if initFn == nil {
		_ = mod.Close(ctx)
		return fail(fmt.Errorf("%w: gateway_init", ErrMissingExport))
	}
	if _, err := initFn.Call(ctx); err != nil {
		_ = mod.Close(ctx)
		return fail(fmt.Errorf("gateway_init: %w", err))
	}

	// Re-acquire lock.  If Shutdown raced during our init it set stateClosed
	// as a signal — detect that and roll back.
	r.mu.Lock()
	if r.state == stateClosed {
		_ = mod.Close(ctx)
		cancel()
		_ = rt.Close(ctx)
		r.mu.Unlock()
		return ErrClosed
	}
	r.runtime = rt
	r.module = mod
	r.ctx = ctx
	r.cancel = cancel
	r.eventBufPtr = eventBufPtr
	r.state = stateStarted
	r.mu.Unlock()
	return nil
}

// Shutdown stops the guest, closes the module and runtime, and marks the
// runtime closed.  Safe to call multiple times and on uninitialized/closed
// runtimes (no-op).  Concurrent Shutdown calls are serialized; exactly one
// performs the close.  If called while Start is in progress (stateStarting),
// it signals cancellation and blocks until Start has finished cleaning up,
// so no initialization work remains active after Shutdown returns.
func (r *GatewayRuntime) Shutdown() error {
	r.mu.Lock()
	switch r.state {
	case stateUninitialized:
		r.mu.Unlock()
		return nil
	case stateStarting:
		// Start is in its init phase: signal it to abort, then wait
		// for Start to observe the closed state and clean up.  The
		// mutex is released before waiting so Start can make
		// progress; initDone closes on every Start return path.
		r.state = stateClosed
		initDone := r.initDone
		r.mu.Unlock()
		<-initDone
		return nil
	case stateClosed:
		r.mu.Unlock()
		return nil
	case stateStarted:
		r.state = stateClosed
		mod := r.module
		runtime := r.runtime
		ctx := r.ctx
		cancel := r.cancel
		r.mu.Unlock()

		if mod != nil {
			if fn := mod.ExportedFunction("gateway_shutdown"); fn != nil {
				if _, err := fn.Call(ctx); err != nil {
					// Log the shutdown error but continue closing.
					errcode := 0
					if err != nil {
						errcode = 1
					}
					_ = errcode
				}
			}
			_ = mod.Close(ctx)
		}
		if cancel != nil {
			cancel()
		}
		if runtime != nil {
			_ = runtime.Close(ctx)
		}
		r.mu.Lock()
		r.module = nil
		r.runtime = nil
		r.ctx = nil
		r.cancel = nil
		r.mu.Unlock()
		return nil
	}
	return nil
}

// ABIVersion returns the guest's declared ABI version.  Requires started.
func (r *GatewayRuntime) ABIVersion() (int32, error) {
	r.mu.Lock()
	if r.state != stateStarted || r.module == nil {
		state := r.state
		r.mu.Unlock()
		return 0, stateErr(state, ErrNotStarted)
	}
	mod := r.module
	ctx := r.ctx
	r.mu.Unlock()
	fn := mod.ExportedFunction("gateway_abi_version")
	if fn == nil {
		return 0, fmt.Errorf("%w: gateway_abi_version", ErrMissingExport)
	}
	res, err := r.callGuest(ctx, fn)
	if err != nil {
		return 0, err
	}
	return int32(res[0]), nil
}

// callGuest invokes a guest function and normalizes lifecycle races.
// If Shutdown closes the module while the call is in flight, the engine
// error (e.g. "module closed with exit_code(0)") must be reported as
// ErrClosed so callers can distinguish intentional shutdown from a real
// guest failure; the test contract forbids leaking raw close errors.
func (r *GatewayRuntime) callGuest(ctx context.Context, fn api.Function, args ...uint64) ([]uint64, error) {
	res, err := fn.Call(ctx, args...)
	if err != nil {
		r.mu.Lock()
		closed := r.state == stateClosed
		r.mu.Unlock()
		if closed {
			return nil, fmt.Errorf("%w: %v", ErrClosed, err)
		}
		return nil, err
	}
	return res, nil
}

// HandleEvent writes an inbound event envelope into the guest-owned event
// buffer and invokes gateway_handle_event.  Requires started.
func (r *GatewayRuntime) HandleEvent(version contract.EnvelopeVersion, eventType, payload string) error {
	if eventType == "" {
		return fmt.Errorf("%w: empty event type", ErrInvalidEvent)
	}
	enc, err := contract.EncodeEvent(version, eventType, payload)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidEvent, err)
	}
	if uint32(len(enc)) > maxEventLen {
		return fmt.Errorf("%w: event %d bytes exceeds %d", ErrInvalidEvent, len(enc), maxEventLen)
	}

	r.mu.Lock()
	if r.state != stateStarted || r.module == nil {
		state := r.state
		r.mu.Unlock()
		return stateErr(state, ErrNotStarted)
	}
	mod := r.module
	ctx := r.ctx
	eventBufPtr := r.eventBufPtr
	r.mu.Unlock()

	if !mod.Memory().Write(eventBufPtr, []byte(enc)) {
		return fmt.Errorf("%w: write into guest event buffer at %d", ErrClosed, eventBufPtr)
	}
	fn := mod.ExportedFunction("gateway_handle_event")
	if fn == nil {
		return fmt.Errorf("%w: gateway_handle_event", ErrMissingExport)
	}
	if _, err := r.callGuest(ctx, fn, uint64(eventBufPtr), uint64(len(enc))); err != nil {
		return err
	}
	return nil
}

// Poll invokes one guest poll cycle.  Requires started.
func (r *GatewayRuntime) Poll() error {
	r.mu.Lock()
	if r.state != stateStarted || r.module == nil {
		state := r.state
		r.mu.Unlock()
		return stateErr(state, ErrNotStarted)
	}
	mod := r.module
	ctx := r.ctx
	r.mu.Unlock()
	fn := mod.ExportedFunction("gateway_poll")
	if fn == nil {
		return fmt.Errorf("%w: gateway_poll", ErrMissingExport)
	}
	if _, err := r.callGuest(ctx, fn); err != nil {
		return err
	}
	return nil
}

// ═══════════════════════════════════════════════════════════════════════
// Host capability: HTTP GET with destination policy
// ═══════════════════════════════════════════════════════════════════════

// hostHTTPGet performs a GET for the guest, enforcing the destination
// policy on the URL and on every redirect hop.  Results are written into
// the guest's memory as [written:4][err:4][status:4].
func (r *GatewayRuntime) hostHTTPGet(ctx context.Context, m api.Module, urlPtr, urlLen, respBuf, respMax, resultPtr uint32) {
	writeResult := func(written, errcode, status uint32) {
		r.writeHTTPResult(m, resultPtr, written, errcode, status)
	}

	raw, ok := m.Memory().Read(urlPtr, urlLen)
	if !ok {
		writeResult(0, httpErrInvalidURL, 0)
		return
	}
	u, err := url.Parse(string(raw))
	if err != nil {
		writeResult(0, httpErrInvalidURL, 0)
		return
	}
	if err := r.validateDestination(u); err != nil {
		if errors.Is(err, errRedirectBlocked) {
			writeResult(0, httpErrRedirect, 0)
		} else {
			writeResult(0, httpErrBlocked, 0)
		}
		return
	}

	reqCtx, cancel := context.WithTimeout(ctx, httpTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, u.String(), nil)
	if err != nil {
		writeResult(0, httpErrNetwork, 0)
		return
	}
	resp, err := r.http().Do(req)
	if err != nil {
		if errors.Is(err, errRedirectBlocked) {
			writeResult(0, httpErrRedirect, 0)
			return
		}
		writeResult(0, httpErrNetwork, 0)
		return
	}
	defer resp.Body.Close()

	// Non-2xx responses are reported explicitly, never returned as data.
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		// Drain a bounded amount so the connection can be reused.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		writeResult(0, httpErrStatus, uint32(resp.StatusCode))
		return
	}

	// Read at most respMax+1 bytes to detect oversized bodies.
	body, err := io.ReadAll(io.LimitReader(resp.Body, int64(respMax)+1))
	if err != nil {
		writeResult(0, httpErrReadBody, uint32(resp.StatusCode))
		return
	}
	if uint32(len(body)) > respMax {
		writeResult(0, httpErrTooLarge, uint32(resp.StatusCode))
		return
	}
	if !m.Memory().Write(respBuf, body) {
		writeResult(0, httpErrMemWrite, uint32(resp.StatusCode))
		return
	}
	writeResult(uint32(len(body)), httpErrOK, uint32(resp.StatusCode))
}

// validateDestination enforces the destination policy for the initial URL.
func (r *GatewayRuntime) validateDestination(u *url.URL) error {
	if u.Scheme != "https" {
		return fmt.Errorf("scheme %q not allowed (https only)", u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("missing hostname")
	}
	for _, allowed := range r.allowedHosts {
		if strings.EqualFold(host, allowed) {
			return nil
		}
	}
	return fmt.Errorf("host %q not allowed", host)
}

// http returns a client with the injectable transport and a redirect
// policy that revalidates every hop.
func (r *GatewayRuntime) http() *http.Client {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.httpClient == nil {
		r.httpClient = &http.Client{
			Transport: r.httpTransport,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= redirectLimit {
					return fmt.Errorf("stopped after %d redirects", redirectLimit)
				}
				if err := r.validateDestination(req.URL); err != nil {
					return errRedirectBlocked
				}
				return nil
			},
		}
	}
	return r.httpClient
}

// writeHTTPResult writes the 12-byte result struct into guest memory.
// Status is included even for success so guest code can build
// deterministic messages.
func (r *GatewayRuntime) writeHTTPResult(m api.Module, resultPtr uint32, written, errcode, status uint32) {
	var buf [httpResultLen]byte
	binary.LittleEndian.PutUint32(buf[0:4], written)
	binary.LittleEndian.PutUint32(buf[4:8], errcode)
	binary.LittleEndian.PutUint32(buf[8:12], status)
	_ = m.Memory().Write(resultPtr, buf[:])
}

// hostConfigProjection writes the redacted JSON projection of the
// gateway configuration into guest memory.  The projection never
// contains the BotToken (json:"-") — the token is only reachable
// through the mediated telegram_token import.  Returns the number of
// bytes written (0 when unconfigured, the projection does not fit, or
// the write fails).  The write is all-or-nothing: an oversized
// projection is rejected in full, never truncated to a partial JSON
// fragment.
func (r *GatewayRuntime) hostConfigProjection(m api.Module, ptr, maxLen uint32) int32 {
	cfg := r.config()
	if cfg == nil || maxLen == 0 {
		return 0
	}
	proj := cfg.Projection()
	if proj == "" {
		return 0
	}
	if uint32(len(proj)) > maxLen {
		return 0
	}
	if !m.Memory().Write(ptr, []byte(proj)) {
		return 0
	}
	return int32(len(proj))
}

// hostTelegramToken is the mediated credential capability: it copies the
// host-owned BotToken into guest memory on demand.  Returns the number
// of bytes written (0 when unconfigured, no token, buffer too small, or
// write fails).  A too-small buffer is rejected rather than truncated so
// a guest can never receive a partial secret silently.  The token is
// never part of any projection, log, or outbound record recorded by the
// host.
func (r *GatewayRuntime) hostTelegramToken(m api.Module, ptr, maxLen uint32) int32 {
	cfg := r.config()
	if cfg == nil || cfg.Telegram.BotToken == "" || maxLen == 0 {
		return 0
	}
	tok := cfg.Telegram.BotToken
	if uint32(len(tok)) > maxLen {
		return 0
	}
	if !m.Memory().Write(ptr, []byte(tok)) {
		return 0
	}
	return int32(len(tok))
}

func (r *GatewayRuntime) appendLog(msg string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cfg != nil {
		msg = r.cfg.Redact(msg)
	}
	r.Log = append(r.Log, msg)
}

func (r *GatewayRuntime) appendOutbound(msg contract.GatewayMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cfg != nil {
		msg.Payload = []byte(r.cfg.Redact(string(msg.Payload)))
	}
	r.Outbound = append(r.Outbound, msg)
}

// TakeOutbound atomically returns and clears guest-emitted messages. Runtime
// adapters use this after a guest call returns so Core callbacks never run
// reentrantly inside Wazero.
func (r *GatewayRuntime) TakeOutbound() []contract.GatewayMessage {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := append([]contract.GatewayMessage(nil), r.Outbound...)
	r.Outbound = nil
	return result
}

// config returns the configured GatewayConfig, or nil when unconfigured.
func (r *GatewayRuntime) config() *contract.GatewayConfig {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cfg
}

func readMem(m api.Module, ptr, size uint32) string {
	if size == 0 {
		return ""
	}
	b, ok := m.Memory().Read(ptr, size)
	if !ok {
		return fmt.Sprintf("<unreadable memory at %d len %d>", ptr, size)
	}
	return string(b)
}

// stateErr maps the current state to a lifecycle error.
func stateErr(state runtimeState, notStarted error) error {
	switch state {
	case stateClosed:
		return ErrClosed
	case stateUninitialized:
		return notStarted
	default:
		return notStarted
	}
}

var _ = net.IP{} // keep net import for future use if needed
