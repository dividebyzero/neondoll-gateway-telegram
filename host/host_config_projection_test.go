// SPDX-License-Identifier: AGPL-3.0-only
//
// host_config_projection_test.go — white-box (package host) tests for
// the hostConfigProjection wasm import, proving the all-or-nothing
// write contract: an undersized guest buffer must receive no partial
// JSON at all.

package host

import (
	"bytes"
	"context"
	"testing"

	"github.com/dividebyzero/neondoll-gateway-telegram/contract"
	"github.com/tetratelabs/wazero"
)

func TestHostConfigProjectionAllOrNothing(t *testing.T) {
	ctx := context.Background()
	rt := wazero.NewRuntime(ctx)
	defer rt.Close(ctx)

	// Minimal module exporting one page of memory: (module (memory 1)
	// (export "memory" (memory 0))).  Hand-crafted so the test needs no
	// wat compiler or guest build step.
	wasmBytes := []byte{
		0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, // magic + version
		0x05, 0x03, 0x01, 0x00, 0x01, // memory section: 1 x (min 1)
		0x07, 0x0a, 0x01, 0x06, 0x6d, 0x65, 0x6d, 0x6f, 0x72, 0x79, 0x02, 0x00, // export "memory"
	}
	compiled, err := rt.CompileModule(ctx, wasmBytes)
	if err != nil {
		t.Fatalf("compile probe module: %v", err)
	}
	mod, err := rt.InstantiateModule(ctx, compiled, wazero.NewModuleConfig())
	if err != nil {
		t.Fatalf("instantiate probe module: %v", err)
	}

	r := NewGatewayRuntime(nil, WithConfig(contract.GatewayConfig{
		Version:  contract.ConfigVersion,
		Telegram: contract.TelegramConfig{BotToken: "123456:TESTTOKEN"},
		Polling:  contract.PollingConfig{TimeoutSeconds: 25},
	}))

	proj := r.config().Projection()
	if proj == "" {
		t.Fatal("expected a non-empty projection")
	}

	// Fill the buffer with a sentinel so any partial write is detectable.
	const sentinel = 0xAA
	if !mod.Memory().Write(0, bytes.Repeat([]byte{sentinel}, len(proj))) {
		t.Fatal("failed to seed sentinel memory")
	}

	// Undersized buffer: must return 0 and leave every byte untouched.
	if got := r.hostConfigProjection(mod, 0, uint32(len(proj)-1)); got != 0 {
		t.Fatalf("undersized buffer: got %d bytes, want 0 (all-or-nothing)", got)
	}
	gotBytes, ok := mod.Memory().Read(0, uint32(len(proj)))
	if !ok {
		t.Fatal("failed to read back guest memory")
	}
	for i, b := range gotBytes {
		if b != sentinel {
			t.Fatalf("partial write detected: byte %d = 0x%02x, want sentinel 0x%02x", i, b, sentinel)
		}
	}

	// Deadly-small buffer (1 byte) must also receive no partial data.
	if got := r.hostConfigProjection(mod, 0, 1); got != 0 {
		t.Fatalf("1-byte buffer: got %d, want 0", got)
	}
	if b, ok := mod.Memory().ReadByte(0); !ok || b != sentinel {
		t.Fatalf("1-byte buffer: first byte = 0x%02x, want sentinel 0x%02x", b, sentinel)
	}

	// Zero-length buffer is rejected too.
	if got := r.hostConfigProjection(mod, 0, 0); got != 0 {
		t.Fatalf("zero-length buffer: got %d, want 0", got)
	}

	// Valid buffer: full projection must be written intact.
	if got := r.hostConfigProjection(mod, 0, uint32(len(proj))); got != int32(len(proj)) {
		t.Fatalf("valid buffer: got %d bytes, want %d", got, len(proj))
	}
	written, ok := mod.Memory().Read(0, uint32(len(proj)))
	if !ok {
		t.Fatal("failed to read back projection")
	}
	if !bytes.Equal(written, []byte(proj)) {
		t.Fatalf("projection mismatch:\n got %q\nwant %q", written, proj)
	}
}
