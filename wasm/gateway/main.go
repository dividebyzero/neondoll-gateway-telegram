// SPDX-License-Identifier: AGPL-3.0-only
//
// wasm/gateway — Minimal Gateway module compiled via TinyGo to wasm.
// Imports host functions from "gateway_host" and exports lifecycle.
// Independently buildable:
//   tinygo build -target=wasi -o ../build/gateway.wasm .

package main

import "unsafe"

// ───── ABI constants (must match host/contract) ──────────────────────────
// Event buffer: fixed offset in guest linear memory agreed between host and guest.
// The host writes the event payload here before calling gateway_handle_event.
const eventBufOffset uint32 = 65536
const eventBufSize uint32 = 4096

// ───── Host capability imports ──────────────────────────────────────────

//go:wasmimport gateway_host log
func hostLog(ptr uint32, len uint32)

//go:wasmimport gateway_host http_get
func hostHTTPGet(urlPtr uint32, urlLen uint32, respBuf uint32, respMax uint32, resultPtr uint32)

//go:wasmimport gateway_host outgoing
func hostOutgoing(ptr uint32, len uint32)

// ───── Guest exports ────────────────────────────────────────────────────

//export gateway_init
func gateway_init() int32 {
	emitLog("gateway_init: module ready (TinyGo M2.1)")
	return 0
}

//export gateway_handle_event
func gateway_handle_event(ptr uint32, len uint32) int32 {
	emitLog("handle_event: received " + itoa(len) + " bytes")
	return 0
}

//export gateway_poll
func gateway_poll() int32 {
	emitLog("poll: cycle start")

	url := "https://api.telegram.org/bot123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11/getMe"
	var respBuf [4096]byte
	var result [8]byte // [4:written][4:error]

	hostHTTPGet(
		stringPtr(url), uint32(len(url)),
		uint32(uintptr(unsafe.Pointer(&respBuf[0]))), 4096,
		uint32(uintptr(unsafe.Pointer(&result[0]))),
	)

	written := uint32(result[0]) | uint32(result[1])<<8 |
		uint32(result[2])<<16 | uint32(result[3])<<24
	err := uint32(result[4]) | uint32(result[5])<<8 |
		uint32(result[6])<<16 | uint32(result[7])<<24

	if err != 0 {
		emitLog("poll: http_get error " + itoa(err) + " (expected — destination-restricted)")
		return 0
	}
	emitLog("poll: http_get returned " + itoa(written) + " bytes")

	msg := `{"type":"pong","from":"gateway","seq":1}`
	hostOutgoing(stringPtr(msg), uint32(len(msg)))
	return 0
}

//export gateway_shutdown
func gateway_shutdown() int32 {
	emitLog("gateway_shutdown: clean exit")
	return 0
}

// ───── Helpers ──────────────────────────────────────────────────────────

func stringPtr(s string) uint32 {
	if len(s) == 0 {
		return 0
	}
	return uint32(uintptr(unsafe.Pointer(unsafe.StringData(s))))
}

func itoa(n uint32) string {
	if n == 0 {
		return "0"
	}
	var buf [12]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

func emitLog(msg string) { hostLog(stringPtr(msg), uint32(len(msg))) }

func main() {}
