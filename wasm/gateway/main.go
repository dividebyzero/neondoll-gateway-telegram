// SPDX-License-Identifier: AGPL-3.0-only
//
// WASM gateway for neondoll Telegram polling (ABI v2).
//
// Implements the host-mediated Telegram getUpdates polling loop, converting
// incoming text messages into gateway events. Operates within the host's
// lifecycle: gateway_init → (getUpdates loop via gateway_poll) → shutdown.
package main

import (
	"encoding/json"
	"strconv"
	"strings"
	"unsafe"
)

// TinyGo WASI needs a main() to compile the module.

func main() {}

// ── Host imports ──────────────────────────────────────────────────────────

//go:wasmimport gateway_host outgoing
func hostOutgoing(ptr, size uint32)

//go:wasmimport gateway_host http_get
func hostHTTPGet(urlPtr, urlLen, respBuf, respMax, resultPtr uint32)

//go:wasmimport gateway_host config_projection
func hostConfigProjection(ptr, maxLen uint32) int32

//go:wasmimport gateway_host telegram_token
func hostTelegramToken(ptr, maxLen uint32) int32

//go:wasmimport gateway_host log
func hostEmitLog(ptr, size uint32)

// ── Helpers ───────────────────────────────────────────────────────────────

var logBuf [256]byte

var configBuf [1024]byte

func emitLog(msg string) {
	src := unsafe.StringData(msg)
	hostEmitLog(uint32(uintptr(unsafe.Pointer(src))), uint32(len(msg)))
}

func emitOutgoing(data []byte) {
	hostOutgoing(uint32(uintptr(unsafe.Pointer(unsafe.SliceData(data)))), uint32(len(data)))
}

func readConfig() (cfg string, ok bool) {
	n := hostConfigProjection(uint32(uintptr(unsafe.Pointer(&configBuf[0]))), 1024)
	if n <= 0 {
		return "", false
	}
	cfg = string(append([]byte(nil), configBuf[:n]...))
	return cfg, true
}

func readToken() (tok string, ok bool) {
	n := hostTelegramToken(uint32(uintptr(unsafe.Pointer(&logBuf[0]))), 256)
	if n <= 0 {
		return "", false
	}
	tok = string(append([]byte(nil), logBuf[:n]...))
	return tok, true
}

// ── Event buffer ───────────────────────────────────────────────────────────

var eventBuffer [4096]byte

//export gateway_event_buf
func gateway_event_buf() uint32 {
	return uint32(uintptr(unsafe.Pointer(&eventBuffer)))
}

//export gateway_event_buf_size
func gateway_event_buf_size() uint32 {
	return 4096
}

//export gateway_abi_version
func gateway_abi_version() int32 {
	return 2
}

// ── Exports ───────────────────────────────────────────────────────────────

//export gateway_init
func gatewayInit() int32 {
	cfg, haveCfg := readConfig()

	if !haveCfg {
		emitLog("no config projection")
	}
	tok, haveTok := readToken()
	if !haveTok {
		emitLog("no telegram token")
	}
	if haveCfg {
		emitLog("gateway_init: config projection ok")
	}
	if haveTok {
		emitLog("telegram token capability ready")
	}

	// Parse the config for allowed_updates.
	if haveCfg {
		var cfgObj struct {
			Telegram struct {
				Host string `json:"host"`
			} `json:"telegram"`
			Polling struct {
				TimeoutSeconds int      `json:"timeout_seconds"`
				AllowedUpdates []string `json:"allowed_updates"`
			} `json:"polling"`
		}
		if err := json.Unmarshal([]byte(cfg), &cfgObj); err == nil {
			if cfgObj.Telegram.Host != "" {
				telegramHost = cfgObj.Telegram.Host
			}
			pollTimeout = cfgObj.Polling.TimeoutSeconds
			if pollTimeout <= 0 {
				pollTimeout = 25
			}
			allowedUpdates = cfgObj.Polling.AllowedUpdates
		}
	}

	// Read token into global
	if haveTok {
		token = tok
	}
	return 0
}

// ── State ─────────────────────────────────────────────────────────────────

var (
	token          string
	offset         int64
	pollTimeout    int
	allowedUpdates []string
	telegramHost   string
	outboundChatID int64
	outboundID     string
)

// ── JSON array URL encoding ───────────────────────────────────────────────

// encodeJSONArray encodes a Go string slice as a URL-escaped JSON array,
// for use in query parameters. Produces e.g. %5B%22message%22%5D for
// ["message"].
func encodeJSONArray(elems []string) string {
	if len(elems) == 0 {
		return ""
	}
	var b strings.Builder
	b.Grow(128)
	b.WriteString("%5B") // [
	for i, e := range elems {
		if i > 0 {
			b.WriteString("%2C") // ,
		}
		b.WriteString("%22") // "
		// Basic char-by-char encoding; Telegram update types are ASCII.
		for _, r := range e {
			switch {
			case r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_':
				b.WriteByte(byte(r))
			default:
				// URL-encode any non-ASCII-alphanumeric char.
				b.WriteString("%" + strconv.FormatUint(uint64(r), 16))
			}
		}
		b.WriteString("%22") // "
	}
	b.WriteString("%5D") // ]
	return b.String()
}

// ── Polling ───────────────────────────────────────────────────────────────

//export gateway_poll
func gatewayPoll() int32 {
	if token == "" {
		emitLog("gateway_poll: no token configured")
		return 0
	}

	// Build the URL.
	base := telegramHost
	if base == "" {
		base = "https://api.telegram.org"
	}
	url := base + "/bot" + token + "/getUpdates?timeout=" + strconv.Itoa(pollTimeout)
	if offset > 0 {
		url += "&offset=" + strconv.FormatInt(offset, 10)
	}
	if encoded := encodeJSONArray(allowedUpdates); encoded != "" {
		url += "&allowed_updates=" + encoded
	}

	emitLog("gateway_poll: getUpdates")

	// HTTP GET via host import. 4 KB response buffer.
	var result [12]byte // [written:4][err:4][status:4]
	hostHTTPGet(
		uint32(uintptr(unsafe.Pointer(unsafe.StringData(url)))),
		uint32(len(url)),
		uint32(uintptr(unsafe.Pointer(&responseBuf[0]))),
		uint32(len(responseBuf)),
		uint32(uintptr(unsafe.Pointer(&result[0]))),
	)

	// Parse the result header.
	written := uint32(result[0]) | uint32(result[1])<<8 | uint32(result[2])<<16 | uint32(result[3])<<24
	code := uint32(result[4]) | uint32(result[5])<<8 | uint32(result[6])<<16 | uint32(result[7])<<24
	status := uint32(result[8]) | uint32(result[9])<<8 | uint32(result[10])<<16 | uint32(result[11])<<24

	if code != 0 {
		emitLog("gateway_poll: HTTP error code=" + itoa(int(code)) + " status=" + itoa(int(status)))
		return 0
	}

	body := responseBuf[:written]
	emitLog("gateway_poll: got " + itoa(int(written)) + " bytes")

	// Parse Telegram response.
	var tgResp struct {
		Ok     bool `json:"ok"`
		Result []struct {
			UpdateID int64 `json:"update_id"`
			Message  *struct {
				MessageID int64 `json:"message_id"`
				From      *struct {
					ID        int64  `json:"id"`
					FirstName string `json:"first_name"`
				} `json:"from"`
				Chat struct {
					ID   int64  `json:"id"`
					Type string `json:"type"`
				} `json:"chat"`
				Date int64  `json:"date"`
				Text string `json:"text"`
			} `json:"message"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &tgResp); err != nil {
		emitLog("gateway_poll: json error: " + err.Error())
		return 0
	}
	if !tgResp.Ok {
		emitLog("gateway_poll: telegram returned not ok")
		return 0
	}
	for _, u := range tgResp.Result {
		if u.UpdateID < offset {
			continue
		}
		offset = u.UpdateID + 1
		if u.Message == nil || u.Message.Text == "" {
			continue
		}
		// Emit a gateway event in GatewayEventEnvelope format.
		evt := struct {
			EventVersion int    `json:"event_version"`
			EventType    string `json:"event_type"`
			Source       string `json:"source"`
			UpdateID     int64  `json:"update_id"`
			MessageID    int64  `json:"message_id"`
			ChatID       int64  `json:"chat_id"`
			ChatType     string `json:"chat_type"`
			SenderID     int64  `json:"sender_id,omitempty"`
			SenderName   string `json:"sender_name,omitempty"`
			Text         string `json:"text"`
			ReceivedAt   int64  `json:"received_at"`
		}{
			EventVersion: 1,
			EventType:    "message.incoming",
			Source:       "telegram",
			UpdateID:     u.UpdateID,
			MessageID:    u.Message.MessageID,
			ChatID:       u.Message.Chat.ID,
			ChatType:     u.Message.Chat.Type,
			Text:         u.Message.Text,
			ReceivedAt:   u.Message.Date,
		}
		if u.Message.From != nil {
			evt.SenderID = u.Message.From.ID
			evt.SenderName = u.Message.From.FirstName
		}
		data, err := json.Marshal(evt)
		if err != nil {
			emitLog("gateway_poll: marshal event: " + err.Error())
			continue
		}
		emitOutgoing(data)
	}

	return 0
}

var responseBuf [4096]byte

// ── Event handler ─────────────────────────────────────────────────────────

//export gateway_handle_event
func gatewayHandleEvent(ptr uint32, size uint32) int32 {
	if size == 0 || size > 4096 {
		emitLog("gateway_handle_event: bad size")
		return 0
	}

	eventData := string(append([]byte(nil), rawMem(ptr, size)...))

	var ev struct {
		V       int             `json:"v"`
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal([]byte(eventData), &ev); err != nil {
		emitLog("gateway_handle_event: json error: " + err.Error())
		return 0
	}

	switch ev.Type {
	case "ping":
		emitOutgoing([]byte(`{"type":"pong","echo":"ping"}`))
	case "echo_token":
		tok, ok := readToken()
		if !ok {
			emitLog("echo_token with no token available")
			return 0
		}
		emitOutgoing([]byte("token:" + tok))
	case "http_request":
		handleHTTPRequest(string(ev.Payload))
	case "session.route":
		var route struct {
			ChatID     int64  `json:"chat_id"`
			DeliveryID string `json:"delivery_id"`
		}
		if json.Unmarshal(ev.Payload, &route) != nil || route.ChatID == 0 {
			emitLog("session.route: invalid payload")
			return 0
		}
		outboundChatID, outboundID = route.ChatID, route.DeliveryID
	case "session.event":
		handleSessionEvent(ev.Payload)
	default:
		emitLog("gateway_event: unknown event type " + ev.Type)
	}

	return 0
}

func handleSessionEvent(payload json.RawMessage) {
	text := ""
	deliveryID := outboundID
	var delivery struct {
		DeliveryID string `json:"delivery_id"`
		Event      struct {
			Type    string          `json:"type"`
			Payload json.RawMessage `json:"payload"`
		} `json:"event"`
	}
	if json.Unmarshal(payload, &delivery) == nil && delivery.Event.Type != "" {
		if delivery.DeliveryID != "" {
			deliveryID = delivery.DeliveryID
		}
		switch delivery.Event.Type {
		case "message":
			var message struct {
				Parts []struct{ Type, Text string } `json:"parts"`
			}
			if json.Unmarshal(delivery.Event.Payload, &message) == nil {
				var parts []string
				for _, part := range message.Parts {
					if part.Type == "text" && part.Text != "" {
						parts = append(parts, part.Text)
					}
				}
				text = strings.Join(parts, "\n")
			}
		case "permission.requested":
			var request struct {
				RequestID string `json:"request_id"`
				Question  string `json:"question"`
			}
			if json.Unmarshal(delivery.Event.Payload, &request) == nil {
				text = request.Question + "\n\nReply /approve " + request.RequestID + " or /deny " + request.RequestID
			}
		case "status", "error":
			var status struct {
				Status  string `json:"status"`
				Code    string `json:"code"`
				Message string `json:"message"`
			}
			if json.Unmarshal(delivery.Event.Payload, &status) == nil {
				text = status.Message
				if text == "" {
					text = status.Status
				}
			}
		}
	} else {
		var legacy struct {
			Text string `json:"text"`
		}
		_ = json.Unmarshal(payload, &legacy)
		text = legacy.Text
	}
	if outboundChatID == 0 || text == "" {
		emitDeliveryResult(deliveryID, false, "unsupported session event")
		return
	}
	ok := sendTelegramText(outboundChatID, text)
	emitDeliveryResult(deliveryID, ok, "telegram sendMessage failed")
}

func sendTelegramText(chatID int64, text string) bool {
	if token == "" {
		return false
	}
	base := telegramHost
	if base == "" {
		base = "https://api.telegram.org"
	}
	requestURL := base + "/bot" + token + "/sendMessage?chat_id=" + strconv.FormatInt(chatID, 10) + "&text=" + queryEscape(text)
	var result [12]byte
	hostHTTPGet(uint32(uintptr(unsafe.Pointer(unsafe.StringData(requestURL)))), uint32(len(requestURL)),
		uint32(uintptr(unsafe.Pointer(&responseBuf[0]))), uint32(len(responseBuf)), uint32(uintptr(unsafe.Pointer(&result[0]))))
	code := uint32(result[4]) | uint32(result[5])<<8 | uint32(result[6])<<16 | uint32(result[7])<<24
	return code == 0
}

func emitDeliveryResult(deliveryID string, ok bool, message string) {
	result := struct {
		EventType  string `json:"event_type"`
		DeliveryID string `json:"delivery_id,omitempty"`
		OK         bool   `json:"ok"`
		Error      string `json:"error,omitempty"`
	}{
		EventType: "delivery.result", DeliveryID: deliveryID, OK: ok,
	}
	if !ok {
		result.Error = message
	}
	data, _ := json.Marshal(result)
	emitOutgoing(data)
}

func queryEscape(value string) string {
	const hex = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-_.~", rune(c)) {
			b.WriteByte(c)
		} else {
			b.WriteByte('%')
			b.WriteByte(hex[c>>4])
			b.WriteByte(hex[c&15])
		}
	}
	return b.String()
}

func handleHTTPRequest(payload string) {
	var req struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal([]byte(payload), &req); err != nil {
		emitLog("http_request: invalid payload: " + err.Error())
		return
	}

	var result [12]byte
	hostHTTPGet(
		uint32(uintptr(unsafe.Pointer(unsafe.StringData(req.URL)))),
		uint32(len(req.URL)),
		uint32(uintptr(unsafe.Pointer(&responseBuf[0]))),
		uint32(len(responseBuf)),
		uint32(uintptr(unsafe.Pointer(&result[0]))),
	)

	written := uint32(result[0]) | uint32(result[1])<<8 | uint32(result[2])<<16 | uint32(result[3])<<24
	code := uint32(result[4]) | uint32(result[5])<<8 | uint32(result[6])<<16 | uint32(result[7])<<24
	httpStatus := uint32(result[8]) | uint32(result[9])<<8 | uint32(result[10])<<16 | uint32(result[11])<<24

	body := ""
	if written > 0 {
		n := int(written)
		if n > 128 {
			n = 128
		}
		body = string(responseBuf[:n])
	}

	httpResult := struct {
		Type   string `json:"type"`
		Err    int    `json:"err"`
		Status int    `json:"status"`
		Len    int    `json:"len"`
		Head   string `json:"head,omitempty"`
	}{
		Type:   "http_result",
		Err:    int(code),
		Status: int(httpStatus),
		Len:    int(written),
		Head:   body,
	}

	data, err := json.Marshal(httpResult)
	if err != nil {
		emitLog("http_request: marshal error: " + err.Error())
		return
	}
	emitOutgoing(data)
}

// ── Memory helpers ────────────────────────────────────────────────────────

// rawMem returns a read-only slice of the guest's linear memory at ptr for
// size bytes. This is safe because we never write beyond the bounds and
// TinyGo's linear memory is contiguous.
func rawMem(ptr, size uint32) []byte {
	if size == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(uintptr(ptr))), size)
}

// itoa converts an int to ASCII without importing fmt (smaller WASM).
func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	u := v
	if u < 0 {
		u = -u
	}
	var buf [20]byte
	i := len(buf)
	for u > 0 {
		i--
		buf[i] = byte('0' + u%10)
		u /= 10
	}
	if v < 0 {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
