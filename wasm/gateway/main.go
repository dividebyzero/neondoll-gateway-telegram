// SPDX-License-Identifier: AGPL-3.0-only
//
// wasm/gateway — Minimal Gateway module compiled via TinyGo to wasm.
// Imports host functions from "gateway_host" and exports lifecycle.
// Independently buildable:
//   tinygo build -target=wasi -o ../build/gateway.wasm .
//
// ABI v2: guest-owned buffers exported via accessor functions.
// The host queries gateway_event_buf / gateway_event_buf_size after
// instantiation — no hardcoded offsets.

package main

import (
	"strings"
	"unsafe"
)

// ═══════════════════════════════════════════════════════════════════════
// ABI — guest-owned buffer exports
// ═══════════════════════════════════════════════════════════════════════
// The host NEVER hardcodes memory offsets.  After instantiation it calls
// gateway_event_buf to learn the address of the guest's static buffer.

const eventBufSize = 4096
const respBufSize = 4096

// eventBuf is a static, zero-initialised array in the guest's data section.
// The host writes inbound event payloads here before calling
// gateway_handle_event.
var eventBuf [eventBufSize]byte

// respBuf is used for HTTP response bodies.  The guest passes its address
// directly to hostHTTPGet so it is implicitly guest-owned.
var respBuf [respBufSize]byte

//export gateway_event_buf
func gateway_event_buf() uint32 { return uint32(uintptr(unsafe.Pointer(&eventBuf[0]))) }

//export gateway_event_buf_size
func gateway_event_buf_size() uint32 { return eventBufSize }

//export gateway_abi_version
func gateway_abi_version() int32 { return 2 }

// ═══════════════════════════════════════════════════════════════════════
// Host capability imports
// ═══════════════════════════════════════════════════════════════════════

//go:wasmimport gateway_host log
func hostLog(ptr uint32, len uint32)

//go:wasmimport gateway_host http_get
func hostHTTPGet(urlPtr uint32, urlLen uint32, respBuf uint32, respMax uint32, resultPtr uint32)

//go:wasmimport gateway_host outgoing
func hostOutgoing(ptr uint32, len uint32)

// ═══════════════════════════════════════════════════════════════════════
// Guest exports  —  lifecycle
// ═══════════════════════════════════════════════════════════════════════

//export gateway_init
func gateway_init() int32 {
	emitLog("gateway_init: module ready (abi 2, guest-owned buffers)")
	return 0
}

//export gateway_handle_event
//
// gateway_handle_event receives an inbound event written by the host into
// the guest-owned event buffer.  The guest reads the payload and may
// call host functions or emit outgoing messages in response.
//
// Expected payload types (M2.1 feasibility):
//   ping         –  emits a deterministic pong outbound
//   http_request –  calls hostHTTPGet, emits an http_result outbound
func gateway_handle_event(ptr uint32, size uint32) int32 {
	payload := readString(ptr, size)

	switch {
	case strings.Contains(payload, `"type":"ping"`):
		msg := `{"type":"pong","echo":"ping"}`
		hostOutgoing(stringPtr(msg), uint32(len(msg)))
		return 0

	case strings.Contains(payload, `"type":"http_request"`):
		url := extractURL(payload)
		if url == "" {
			emitLog("handle_event: http_request with no url")
			return 0
		}
		var result [12]byte // [written:4][err:4][status:4]
		hostHTTPGet(
			stringPtr(url), uint32(len(url)),
			uint32(uintptr(unsafe.Pointer(&respBuf[0]))), respBufSize,
			uint32(uintptr(unsafe.Pointer(&result[0]))),
		)
		written := le32(result[0:4])
		err := le32(result[4:8])
		status := le32(result[8:12])

		// Build a deterministic result message.
		msg := `{"type":"http_result","err":` + itoa(err) +
			`,"status":` + itoa(status) +
			`,"len":` + itoa(written)
		if err == 0 && written > 0 {
			headN := written
			if headN > 64 {
				headN = 64
			}
			raw := string(respBuf[:headN])
			msg += `,"head":"` + escStr(raw) + `"`
		}
		msg += `}`
		hostOutgoing(stringPtr(msg), uint32(len(msg)))
		return 0

	default:
		emitLog("handle_event: unknown type (len=" + itoa(size) + ")")
		return 0
	}
}

//export gateway_poll
func gateway_poll() int32 {
	msg := `{"type":"poll_tick"}`
	hostOutgoing(stringPtr(msg), uint32(len(msg)))
	return 0
}

//export gateway_shutdown
func gateway_shutdown() int32 {
	emitLog("gateway_shutdown: clean exit")
	return 0
}

// ═══════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════

// readString zero-copy reads len bytes starting at ptr in linear memory
// into a Go string.  The host has already written the event there.
func readString(ptr uint32, size uint32) string {
	if size == 0 {
		return ""
	}
	return unsafe.String((*byte)(unsafe.Pointer(uintptr(ptr))), size)
}

// extractURL finds the first "url":"<value>" in a JSON payload and
// returns <value>.  Returns "" when not found.
func extractURL(payload string) string {
	marker := `"url":"`
	idx := strings.Index(payload, marker)
	if idx < 0 {
		return ""
	}
	rest := payload[idx+len(marker):]
	end := strings.IndexByte(rest, '"')
	if end < 0 {
		return rest
	}
	return rest[:end]
}

// le32 reads a little-endian uint32 from the first 4 bytes of b.
func le32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

// stringPtr returns the linear-memory address of the backing data of s.
func stringPtr(s string) uint32 {
	if len(s) == 0 {
		return 0
	}
	return uint32(uintptr(unsafe.Pointer(unsafe.StringData(s))))
}

// escStr escapes \ and " so the string can be embedded inside a JSON value.
func escStr(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return s
}

// itoa formats n as a decimal string (no leading zeros; 0 → "0").
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