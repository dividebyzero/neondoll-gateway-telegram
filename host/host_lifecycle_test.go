// SPDX-License-Identifier: AGPL-3.0-only
//
// host_lifecycle_test.go — white-box (package host) tests for the
// Start/Shutdown lifecycle.  Uses the unexported initBarrier seam to make
// the Start/Shutdown race deterministic: the barrier blocks Start mid-init,
// and the test proves Shutdown does not return until initialization has
// fully cleaned up.

package host

import (
	"errors"
	"os"
	"testing"
)

// loadWASMWhiteBox loads the committed WASM build artifact.  Duplicated
// from host_test.go because the black-box tests live in package host_test.
func loadWASMWhiteBox(t *testing.T) []byte {
	t.Helper()
	paths := []string{
		"../wasm/build/gateway.wasm",
		"wasm/build/gateway.wasm",
	}
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err == nil {
			return b
		}
	}
	t.Fatal("WASM artifact not found; run tinygo build first (see wasm/gateway)")
	return nil
}

// TestLifecycleShutdownWaitsForInit proves deterministically that when
// Shutdown is called during Start (stateStarting), it does not return until
// Start's initialization has completely finished cleaning up.
//
// Determinism: the initBarrier blocks Start inside its init phase before
// any WASM work.  Start cannot close initDone until the test releases the
// barrier, so Shutdown returning before the release would be a definitive
// violation — no sleeps, no timing assumptions.
func TestLifecycleShutdownWaitsForInit(t *testing.T) {
	r := NewGatewayRuntime(nil)

	entered := make(chan struct{})
	release := make(chan struct{})
	r.initBarrier = func() {
		close(entered) // Start has passed stateStarting and is mid-init.
		<-release      // Block until the test says proceed.
	}

	wasm := loadWASMWhiteBox(t)

	var startErr error
	startDone := make(chan struct{})
	go func() {
		defer close(startDone)
		startErr = r.Start(wasm)
	}()

	// Deterministically wait until Start is blocked at the barrier.
	<-entered

	shutdownReturned := make(chan error, 1)
	go func() {
		shutdownReturned <- r.Shutdown()
	}()

	// Shutdown MUST NOT return while Start is still blocked: Start cannot
	// have finished cleaning up, and Shutdown is required to wait for that.
	select {
	case err := <-shutdownReturned:
		t.Fatalf("Shutdown returned before initialization completed (err=%v)", err)
	default:
	}

	// Same for Start: it is still pinned at the barrier.
	select {
	case <-startDone:
		t.Fatal("Start returned while blocked at the init barrier")
	default:
	}

	// Release the barrier. Start observes stateClosed, cleans up, closes
	// initDone; only then may Shutdown unblock.
	close(release)

	if err := <-shutdownReturned; err != nil {
		t.Errorf("Shutdown: got %v; want nil", err)
	}
	<-startDone
	if !errors.Is(startErr, ErrClosed) {
		t.Errorf("Start: got %v; want ErrClosed", startErr)
	}

	// After Shutdown returns, no initialization remains active: the
	// completion signal is closed and the runtime is closed.
	select {
	case <-r.initDone:
	default:
		t.Fatal("initDone not closed after Shutdown returned: init still active")
	}
	if r.state != stateClosed {
		t.Errorf("state: got %s; want closed", r.stateName(r.state))
	}

	// A later Start attempt must be rejected on the closed runtime.
	if err := r.Start(wasm); !errors.Is(err, ErrClosed) {
		t.Errorf("Start after Shutdown: got %v; want ErrClosed", err)
	}
}

// TestLifecycleStartCompletesThenShutdown verifies the normal
// (non-overlapping) path still works after the wait-on-init change: a
// completed Start leaves initDone closed and Shutdown takes the started
// path.
func TestLifecycleStartCompletesThenShutdown(t *testing.T) {
	r := NewGatewayRuntime(nil)
	wasm := loadWASMWhiteBox(t)

	if err := r.Start(wasm); err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-r.initDone:
	default:
		t.Fatal("initDone not closed after successful Start")
	}
	if err := r.Shutdown(); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
	if r.state != stateClosed {
		t.Errorf("state: got %s; want closed", r.stateName(r.state))
	}
}
