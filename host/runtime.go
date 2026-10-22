// SPDX-License-Identifier: AGPL-3.0-only
//
// host/runtime.go — wazero-based Gateway host runtime.
// Core 5 M2.1 feasibility proof: loads and drives the WASM Gateway module.

package host

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	wasi "github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/tetratelabs/wazero/sys"

	"github.com/dividebyzero/neondoll-gateway-telegram/contract"
)

// ───── ABI constants (must match wasm/gateway/main.go) ──────────────────
const eventBufOffset uint32 = 65536
const eventBufSize uint32 = 4096

// GatewayRuntime manages the lifecycle of a WASM Gateway module.[truncated]
type GatewayRuntime struct {
	mu      sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	runtime wazero.Runtime
	module  api.Module

	// allowedHosts controls destination restrictions for host-mediated HTTPS.
	allowedHosts []string

	// Log captures guest-side log output.
	Log []string

	// Outbound collects messages sent from guest to host.
	Outbound []contract.GatewayMessage
}

// NewGatewayRuntime creates a runtime preconfigured with destination restrictions.
func NewGatewayRuntime(allowedHosts ...string) *GatewayRuntime {
	ctx, cancel := context.WithCancel(context.Background())
	r := &GatewayRuntime{
		ctx:          ctx,
		cancel:       cancel,
		allowedHosts: allowedHosts,
	}
	if len(allowedHosts) == 0 {
		r.allowedHosts = []string{"api.telegram.org"}
	}
	return r
}

// Start compiles, instantiates the WASM module and calls gateway_init.
func (r *GatewayRuntime) Start(wasmBytes []byte) error {
	r.mu.Lock()

	rt := wazero.NewRuntime(r.ctx)
	r.runtime = rt

	// WASI (required by TinyGo runtime).
	wasi.MustInstantiate(r.ctx, rt)

	// Build the host capability module.
	_, err := rt.NewHostModuleBuilder("gateway_host").
		NewFunctionBuilder().WithFunc(r.fnLog).Export("log").
		NewFunctionBuilder().WithFunc(r.fnHTTPGet).Export("http_get").
		NewFunctionBuilder().WithFunc(r.fnOutgoing).Export("outgoing").
		Instantiate(r.ctx)
	if err != nil {
		r.runtime.Close(r.ctx)
		r.mu.Unlock()
		return fmt.Errorf("host module: %w", err)
	}

	// Compile the WASM artifact (independently built, external binary).
	compiled, err := rt.CompileModule(r.ctx, wasmBytes)
	if err != nil {
		r.runtime.Close(r.ctx)
		r.mu.Unlock()
		return fmt.Errorf("compile: %w", err)
	}

	mod, err := rt.InstantiateModule(r.ctx, compiled,
		wazero.NewModuleConfig().WithName("gateway"))
	if err != nil {
		r.runtime.Close(r.ctx)
		r.mu.Unlock()
		return fmt.Errorf("instantiate: %w", err)
	}
	r.module = mod
	r.mu.Unlock()

	// Call gateway_init WITHOUT holding the lock (guest may call back into host).
	init := mod.ExportedFunction("gateway_init")
	if init == nil {
		r.module.Close(r.ctx)
		r.runtime.Close(r.ctx)
		return fmt.Errorf("gateway_init not exported")
	}
	_, err = init.Call(r.ctx)
	if err != nil && !isExitOK(err) {
		r.module.Close(r.ctx)
		r.runtime.Close(r.ctx)
		return fmt.Errorf("gateway_init call: %w", err)
	}
	return nil
}

// isExitOK returns true when err is a WASI ExitError with code 0,
// which TinyGo emits after main() completes. This is a normal condition
// for reactor-style modules.
func isExitOK(err error) bool {
	var exitErr *sys.ExitError
	return errors.As(err, &exitErr) && exitErr.ExitCode() == 0
}

// HandleEvent writes an inbound JSON event to the guest's fixed buffer and
// calls gateway_handle_event. Uses a static ABI buffer offset rather than
// dynamic allocation to avoid TinyGo's proc_exit swallowing the return value.
func (r *GatewayRuntime) HandleEvent(version contract.EnvelopeVersion, eventType string) error {
	payload := fmt.Sprintf(`{"v":%d,"type":"%s"}`, version, eventType)
	if len(payload) > int(eventBufSize) {
		return fmt.Errorf("event payload %d bytes exceeds buffer %d", len(payload), eventBufSize)
	}

	r.mu.Lock()
	if !r.module.Memory().Write(eventBufOffset, []byte(payload)) {
		r.mu.Unlock()
		return fmt.Errorf("memory write at offset %d failed", eventBufOffset)
	}
	fn := r.module.ExportedFunction("gateway_handle_event")
	r.mu.Unlock()
	if fn == nil {
		return fmt.Errorf("gateway_handle_event not exported")
	}
	_, err := fn.Call(r.ctx, uint64(eventBufOffset), uint64(len(payload)))
	if isExitOK(err) {
		return nil
	}
	return err
}

// Poll triggers a poll cycle on the module.
func (r *GatewayRuntime) Poll() error {
	r.mu.Lock()
	fn := r.module.ExportedFunction("gateway_poll")
	r.mu.Unlock()
	if fn == nil {
		return fmt.Errorf("gateway_poll not exported")
	}
	_, err := fn.Call(r.ctx)
	if isExitOK(err) {
		return nil
	}
	return err
}

// Shutdown calls gateway_shutdown and closes the runtime.
func (r *GatewayRuntime) Shutdown() error {
	r.mu.Lock()
	fn := r.module.ExportedFunction("gateway_shutdown")
	r.mu.Unlock()
	if fn != nil {
		_, err := fn.Call(r.ctx)
		if err != nil && !isExitOK(err) {
			return fmt.Errorf("gateway_shutdown: %w", err)
		}
	}

	// Close module and runtime.
	if err := r.module.Close(r.ctx); err != nil {
		return fmt.Errorf("module close: %w", err)
	}
	r.cancel()
	return r.runtime.Close(r.ctx)
}

// ───── Host function implementations ────────────────────────────────────

// fnLog writes a log line from the guest.
func (r *GatewayRuntime) fnLog(ctx context.Context, m api.Module, ptr, len uint32) {
	raw, ok := m.Memory().Read(ptr, len)
	if !ok {
		r.appendLog("[host-log: invalid memory read]")
		return
	}
	r.appendLog(string(raw))
}

// fnHTTPGet performs a destination-restricted HTTPS GET.
func (r *GatewayRuntime) fnHTTPGet(ctx context.Context, m api.Module, urlPtr, urlLen, respBuf, respMax, resultPtr uint32) {
	raw, ok := m.Memory().Read(urlPtr, urlLen)
	if !ok {
		r.writeResult(m, resultPtr, 0, 1)
		return
	}
	urlStr := string(raw)

	// Destination allowlist.
	allowed := false
	for _, host := range r.allowedHosts {
		if strings.Contains(urlStr, host) {
			allowed = true
			break
		}
	}
	if !allowed {
		r.appendLog(fmt.Sprintf("[host] http_get BLOCKED: %s", urlStr))
		r.writeResult(m, resultPtr, 0, 2)
		return
	}

	ctx2, cancel := context.WithTimeout(r.ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx2, http.MethodGet, urlStr, nil)
	if err != nil {
		r.writeResult(m, resultPtr, 0, 3)
		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		r.writeResult(m, resultPtr, 0, 4)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, int64(respMax)))
	if err != nil {
		r.writeResult(m, resultPtr, 0, 5)
		return
	}

	if !m.Memory().Write(respBuf, body) {
		r.writeResult(m, resultPtr, 0, 6)
		return
	}
	r.writeResult(m, resultPtr, uint32(len(body)), 0)
	r.appendLog(fmt.Sprintf("[host] http_get OK: %s → %d bytes", urlStr, len(body)))
}

// fnOutgoing captures messages sent from the gateway to the host.
func (r *GatewayRuntime) fnOutgoing(ctx context.Context, m api.Module, ptr, len uint32) {
	raw, ok := m.Memory().Read(ptr, len)
	if !ok {
		r.appendLog("[host-outgoing: invalid memory read]")
		return
	}
	r.appendLog(fmt.Sprintf("[host] outgoing: %s", string(raw)))
	r.mu.Lock()
	r.Outbound = append(r.Outbound, contract.GatewayMessage{
		Version: contract.Version0,
		Type:    "outgoing",
		Payload: raw,
	})
	r.mu.Unlock()
}

// ───── Memory helpers ───────────────────────────────────────────────────

// writeResult writes an 8-byte {written, err} struct to guest memory.
func (r *GatewayRuntime) writeResult(m api.Module, resultPtr uint32, written, errCode uint32) {
	buf := []byte{
		byte(written), byte(written >> 8), byte(written >> 16), byte(written >> 24),
		byte(errCode), byte(errCode >> 8), byte(errCode >> 16), byte(errCode >> 24),
	}
	m.Memory().Write(resultPtr, buf)
}

func (r *GatewayRuntime) appendLog(s string) {
	r.mu.Lock()
	r.Log = append(r.Log, s)
	r.mu.Unlock()
}
