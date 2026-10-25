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
//
// M2.3: gateway_poll implements Telegram Bot API polling with
// getUpdates.  On each poll tick the guest constructs the API URL
// from the host-owned credential, calls http_get, parses the JSON
// response, and emits a gateway event envelope for each new
// inbound text message.  update_id offset tracking prevents
// replay.  All token secrets remain host-owned — only presence
// and length cross the boundary.

package main

import (
	"strings"
	"unsafe"
)

const eventBufSize = 4096
const respBufSize = 4096

var eventBuf [eventBufSize]byte
var respBuf [respBufSize]byte

// cfgBuf scratch space layout:
//   [0:1024]    — config projection (init) / token read (echo_token)
//   [256:1024]  — token read during poll (768 bytes, enough for any bot token)
//   [0:1024]    — URL construction during poll (overlaps, never used concurrently)
var cfgBuf [1024]byte

// Persistent poll state.
var lastUpdateID int32        // highest seen update_id + 1; 0 = fetch all
var pollTimeoutSec int32 = 25 // default, overridden by config projection
var apiBaseURL string         // Telegram API host, set from config projection

//export gateway_event_buf
func gateway_event_buf() uint32 { return uint32(uintptr(unsafe.Pointer(&eventBuf[0]))) }

//export gateway_event_buf_size
func gateway_event_buf_size() uint32 { return eventBufSize }

//export gateway_abi_version
func gateway_abi_version() int32 { return 2 }

//go:wasmimport gateway_host log
func hostLog(ptr uint32, len uint32)

//go:wasmimport gateway_host http_get
func hostHTTPGet(urlPtr uint32, urlLen uint32, respBuf uint32, respMax uint32, resultPtr uint32)

//go:wasmimport gateway_host outgoing
func hostOutgoing(ptr uint32, len uint32)

//go:wasmimport gateway_host config_projection
func hostConfigProjection(ptr uint32, maxLen uint32) int32

//go:wasmimport gateway_host telegram_token
func hostTelegramToken(ptr uint32, maxLen uint32) int32

//export gateway_init
func gateway_init() int32 {
	emitLog("gateway_init: module ready (abi 2, guest-owned buffers)")

	n := hostConfigProjection(uint32(uintptr(unsafe.Pointer(&cfgBuf[0]))), uint32(len(cfgBuf)))
	if n > 0 {
		cfgStr := string(cfgBuf[:n])
		emitLog("gateway_init: config projection ok (len=" + itoa(uint32(n)) + ")")

		if v, ok := extractJSONInt(cfgStr, "timeout_seconds"); ok && v > 0 {
			pollTimeoutSec = v
			emitLog("gateway_init: poll_timeout=" + itoa(uint32(v)))
		}
		// Extract telegram.host from the config projection using extractJSONObject.
		if telObj := extractJSONObject(cfgStr, "telegram"); telObj != "" {
			if h := extractJSONString(telObj, "host"); h != "" {
				apiBaseURL = h
				emitLog("gateway_init: api_host=" + apiBaseURL)
			}
		}
	} else {
		emitLog("gateway_init: no config projection")
	}

	n = hostTelegramToken(uint32(uintptr(unsafe.Pointer(&cfgBuf[0]))), uint32(len(cfgBuf)))
	if n > 0 {
		emitLog("gateway_init: telegram token capability ready (len=" + itoa(uint32(n)) + ")")
	} else {
		emitLog("gateway_init: no telegram token")
	}
	return 0
}

//export gateway_handle_event
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
		var result [12]byte
		hostHTTPGet(
			stringPtr(url), uint32(len(url)),
			uint32(uintptr(unsafe.Pointer(&respBuf[0]))), respBufSize,
			uint32(uintptr(unsafe.Pointer(&result[0]))),
		)
		written := le32(result[0:4])
		err := le32(result[4:8])
		status := le32(result[8:12])

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

	case strings.Contains(payload, `"type":"echo_token"`):
		n := hostTelegramToken(uint32(uintptr(unsafe.Pointer(&cfgBuf[0]))), uint32(len(cfgBuf)))
		if n <= 0 {
			emitLog("handle_event: echo_token with no token available")
			return 0
		}
		tok := string(cfgBuf[0:n])
		msg := `{"type":"token_echo","token":"` + escStr(tok) + `"}`
		hostOutgoing(stringPtr(msg), uint32(len(msg)))
		return 0

	default:
		emitLog("handle_event: unknown type (len=" + itoa(size) + ")")
		return 0
	}
}

//export gateway_poll
func gateway_poll() int32 {
	// Check for credential capability before attempting a poll.
	tokLen := hostTelegramToken(uint32(uintptr(unsafe.Pointer(&cfgBuf[256]))), 768)
	if tokLen <= 0 {
		return 0 // no token available — skip poll silently
	}

	// Read token from cfgBuf[256:256+tokLen].
	token := readString(uint32(uintptr(unsafe.Pointer(&cfgBuf[256]))), uint32(tokLen))

	// Build the getUpdates URL from apiBaseURL (defaults to api.telegram.org).
	offset := itoa(uint32(lastUpdateID))
	timout := itoa(uint32(pollTimeoutSec))

		base := apiBaseURL
	if base == "" {
		base = "https://api.telegram.org"
	}
	url := base + "/bot" + token + "/getUpdates?offset=" + offset + "&timeout=" + timout

	emitLog("gateway_poll: fetching " + url)

	// Execute the GET request. Response lands in respBuf.
	var result [12]byte
	hostHTTPGet(
		stringPtr(url), uint32(len(url)),
		uint32(uintptr(unsafe.Pointer(&respBuf[0]))), respBufSize,
		uint32(uintptr(unsafe.Pointer(&result[0]))),
	)
	written := le32(result[0:4])
	err := le32(result[4:8])
	status := le32(result[8:12])

	if err != 0 {
		emitLog("gateway_poll: http error " + itoa(err) + " (status=" + itoa(status) + ")")
		return 0
	}
	if status != 200 {
		emitLog("gateway_poll: non-200 status " + itoa(status))
		return 0
	}
	if written == 0 {
		return 0 // empty response (e.g. long poll timed out)
	}

	// Parse the JSON response body for updates containing text messages.
	body := string(respBuf[:written])
	updates := parseTelegramUpdates(body)
	if len(updates) == 0 {
		emitLog("gateway_poll: ok, 0 new updates")
		return 0
	}

	maxID := lastUpdateID
	haveNew := false
	for _, u := range updates {
		if u.text == "" || u.chatID == "" {
			if u.id > maxID {
				maxID = u.id
			}
			continue
		}
		// Emit a versioned gateway event envelope for each incoming text message.
		env := `{"event_type":"message.incoming","event_version":1,` +
			`"update_id":` + itoa(uint32(u.id)) + `,` +
			`"message_id":` + itoa(uint32(u.messageID)) + `,` +
			`"chat_id":` + u.chatID + `,` +
			`"chat_type":"` + escStr(u.chatType) + `",` +
			`"sender_id":` + u.fromID + `,` +
			`"sender_name":"` + escStr(u.fromName) + `",` +
			`"text":"` + escStr(u.text) + `",` +
			`"received_at":` + itoa(uint32(u.date)) + `}`
		hostOutgoing(stringPtr(env), uint32(len(env)))
		haveNew = true
		if u.id > maxID {
			maxID = u.id
		}
	}

	if haveNew {
		lastUpdateID = maxID + 1
		emitLog("gateway_poll: advanced offset to " + itoa(uint32(lastUpdateID)))
	} else {
		// Advance past non-message updates so we don't re-fetch them.
		if maxID > lastUpdateID {
			lastUpdateID = maxID + 1
		}
	}
	return 0
}

// ─── helpers ────────────────────────────────────────────────────────────

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

// extractJSONInt finds a numeric value for the given key in a JSON
// object (key:"value" or "key":<number>) and returns it.
// Only supports decimal integers (no negative, no scientific notation).
func extractJSONInt(json string, key string) (int32, bool) {
	marker := `"` + key + `":`
	idx := strings.Index(json, marker)
	if idx < 0 {
		return 0, false
	}
	rest := json[idx+len(marker):]
	// Skip whitespace
	i := 0
	for i < len(rest) && (rest[i] == ' ' || rest[i] == '\t' || rest[i] == '\n' || rest[i] == '\r') {
		i++
	}
	if i >= len(rest) || rest[i] < '0' || rest[i] > '9' {
		return 0, false
	}
	var v int32
	for i < len(rest) && rest[i] >= '0' && rest[i] <= '9' {
		v = v*10 + int32(rest[i]-'0')
		i++
	}
	return v, true
}

// TelegramUpdate holds extracted fields from a Telegram API update object.
type TelegramUpdate struct {
	id        int32
	messageID int32
	text      string
	chatID    string
	chatType  string
	fromID    string
	fromName  string
	date      int32
}

// parseTelegramUpdates scans a JSON response body from getUpdates and
// extracts all updates that contain a text message.
func parseTelegramUpdates(body string) []TelegramUpdate {
	var updates []TelegramUpdate

	// Find the start of the "result" array.
	arrMarker := `"result":[`
	idx := strings.Index(body, arrMarker)
	if idx < 0 {
		return updates
	}
	// Scan inside the array for update objects.
	rest := body[idx+len(arrMarker):]
	pos := 0

	for pos < len(rest) {
		// Find the next opening brace for an update object.
		braceIdx := strings.IndexByte(rest[pos:], '{')
		if braceIdx < 0 {
			break
			}
		pos += braceIdx

		// Extract the update object between { and matching closing }.
		obj, end := extractObject(rest[pos:])
		if obj == "" {
			pos++
			continue
		}
		pos += end

		u := parseTelegramUpdateObj(obj)
		if u.id > 0 {
			updates = append(updates, u)
		}
	}
	return updates
}

// extractObject reads a single JSON object from a string starting
// with { and returns the object content (including braces) and the
// end position offset.
func extractObject(s string) (string, int) {
	if len(s) == 0 || s[0] != '{' {
		return "", 0
	}
	depth := 0
	inStr := false
	esc := false
	for i, c := range s {
		if esc {
			esc = false
			continue
		}
		if c == '\\' && inStr {
			esc = true
			continue
		}
		if c == '"' {
			inStr = !inStr
			continue
		}
		if inStr {
			continue
		}
		switch c {
		case '{':
			depth++
		case '}':
			depth--
		}
		if depth == 0 {
			return s[:i+1], i + 1
		}
	}
	return "", 0
}

// parseTelegramUpdateObj extracts fields from a single Telegram update JSON object.
func parseTelegramUpdateObj(obj string) TelegramUpdate {
	var u TelegramUpdate

	// Extract update_id.
	u.id, _ = extractJSONInt(obj, "update_id")

	// Check for message sub-object.
	msgMarker := `"message":{`
	msgIdx := strings.Index(obj, msgMarker)
	if msgIdx < 0 {
		return u
	}
	msgStr := obj[msgIdx+len(msgMarker)-1:] // include the opening brace

	// Extract text from message.
	u.text = extractJSONString(msgStr, "text")

	// Extract message_id and date from the message object.
	u.messageID, _ = extractJSONInt(msgStr, "message_id")
	u.date, _ = extractJSONInt(msgStr, "date")

	// Extract chat.id.
	chatMarker := `"chat":{`
	chatIdx := strings.Index(msgStr, chatMarker)
	var chatStr string
	if chatIdx >= 0 {
		chatObj, _ := extractObject(msgStr[chatIdx+len(chatMarker)-1:])
		if chatObj != "" {
			chatStr = chatObj
			// chat.id is numeric in Telegram API, store as string for generality
			id, _ := extractJSONInt(chatStr, "id")
			if id > 0 {
				u.chatID = itoa(uint32(id))
			}
			// Extract chat.type.
			u.chatType = extractJSONString(chatStr, "type")
		}
	}

	// Extract form.id and from.first_name.
	fromMarker := `"from":{`
	fromIdx := strings.Index(msgStr, fromMarker)
	if fromIdx >= 0 {
		fromObj, _ := extractObject(msgStr[fromIdx+len(fromMarker)-1:])
		if fromObj != "" {
			u.fromName = extractJSONString(fromObj, "first_name")
			id, _ := extractJSONInt(fromObj, "id")
			if id > 0 {
				u.fromID = itoa(uint32(id))
			}
		}
	}

	return u
}

// extractJSONString finds the string value for key in a JSON object.
// Returns "" when the key is missing or the value is not a string.
func extractJSONString(json string, key string) string {
	marker := `"` + key + `":"`
	idx := strings.Index(json, marker)
	if idx < 0 {
		return ""
	}
	rest := json[idx+len(marker):]
	end := strings.IndexByte(rest, '"')
	if end < 0 {
		return ""
	}
	// Handle escaped quotes in the value by scanning forward.
	val := ""
	i := 0
	for i < end {
		if rest[i] == '\\' && i+1 < end {
			val += string(rest[i+1])
			i += 2
			continue
		}
		val += string(rest[i])
		i++
	}
	return val
}

// extractJSONObject finds the value for key in a JSON object as a raw
// JSON subobject.  The value must be a brace-delimited object ("key":{...}).
// Returns "" when the key is missing or the value is not an object.
func extractJSONObject(json string, key string) string {
	marker := `"` + key + `":{`
	idx := strings.Index(json, marker)
	if idx < 0 {
		return ""
	}
	rest := json[idx+len(marker)-1:] // rest starts at '{'
	depth := 0
	for i, c := range rest {
		switch c {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return rest[:i+1]
			}
		}
	}
	return ""
}

// le32 reads a little-endian uint32 from the first 4 bytes of b.
func le32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

// readString reconstructs a Go string from a byte slice in linear memory.
func readString(ptr uint32, size uint32) string {
	if size == 0 {
		return ""
	}
	return unsafe.String((*byte)(unsafe.Pointer(uintptr(ptr))), size)
}

// stringPtr returns the linear-memory address of the backing data of s.
func stringPtr(s string) uint32 {
	if len(s) == 0 {
		return 0
	}
	return uint32(uintptr(unsafe.Pointer(unsafe.StringData(s))))
}

// escStr escapes \\ and \" so the string can be embedded inside a JSON value.
func escStr(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return s
}

// itoa formats n as a decimal string (no leading zeros; 0 -> "0").
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
