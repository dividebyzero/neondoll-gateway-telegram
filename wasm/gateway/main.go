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

//go:wasmimport gateway_host state_get
func hostStateGet(keyPtr, keyLen, valuePtr, valueMax uint32) int32

//go:wasmimport gateway_host state_put
func hostStatePut(keyPtr, keyLen, valuePtr, valueLen uint32) int32

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
	if !loadDurableState() {
		emitLog("gateway_init: durable state capability unavailable")
		panic("durable state capability unavailable")
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
	durable        durableState
	stateReady     bool
)

const (
	durableStateKey      = "telegram.gateway.v1"
	maxPendingUpdates    = 16
	maxDurableStateBytes = 48 * 1024
	maxPendingEventBytes = 3 * 1024
	maxDeliveryIDBytes   = 256
	durableStateHeadroom = maxPendingEventBytes + 512
)

type durableState struct {
	Offset     int64           `json:"offset,omitempty"`
	Pending    []pendingUpdate `json:"pending,omitempty"`
	Deliveries []string        `json:"deliveries,omitempty"`
}

type pendingUpdate struct {
	UpdateID int64           `json:"update_id"`
	Event    json.RawMessage `json:"event,omitempty"`
	Acked    bool            `json:"acked,omitempty"`
	Blocked  string          `json:"blocked,omitempty"`
	Emitted  bool            `json:"-"`
}

var durableStateBuf [65536]byte

func loadDurableState() bool {
	key := []byte(durableStateKey)
	n := hostStateGet(uint32(uintptr(unsafe.Pointer(&key[0]))), uint32(len(key)), uint32(uintptr(unsafe.Pointer(&durableStateBuf[0]))), uint32(len(durableStateBuf)))
	if n >= 0 {
		if n > maxDurableStateBytes || n > 0 && json.Unmarshal(durableStateBuf[:n], &durable) != nil || !validDurableState() {
			emitLog("durable state is invalid")
			return false
		}
		offset = durable.Offset
		stateReady = true
		return true
	}
	// A missing key and a denied capability both return -1 in ABI v2.
	// Writing the initial state distinguishes a usable empty store from a
	// missing capability without weakening fail-closed behavior.
	durable = durableState{}
	stateReady = persistDurableState()
	return stateReady
}

func persistDurableState() bool {
	durable.Offset = offset
	data, ok := marshalDurableState()
	if !ok {
		return false
	}
	key := []byte(durableStateKey)
	if len(data) == 0 {
		return false
	}
	return hostStatePut(uint32(uintptr(unsafe.Pointer(&key[0]))), uint32(len(key)), uint32(uintptr(unsafe.Pointer(&data[0]))), uint32(len(data))) == 0
}

func marshalDurableState() ([]byte, bool) {
	data, err := json.Marshal(durable)
	return data, err == nil && len(data) <= maxDurableStateBytes
}

func validDurableState() bool {
	if len(durable.Pending) > maxPendingUpdates {
		return false
	}
	previous := int64(-1)
	for _, pending := range durable.Pending {
		if pending.UpdateID < 0 || pending.UpdateID <= previous || len(pending.Event) > maxPendingEventBytes {
			return false
		}
		previous = pending.UpdateID
	}
	for _, deliveryID := range durable.Deliveries {
		if deliveryID == "" || len(deliveryID) > maxDeliveryIDBytes {
			return false
		}
	}
	_, ok := marshalDurableState()
	return ok
}

func pendingIndex(updateID int64) int {
	for i := range durable.Pending {
		if durable.Pending[i].UpdateID == updateID {
			return i
		}
	}
	return -1
}

func advanceAcceptedPrefix() {
	for len(durable.Pending) > 0 && durable.Pending[0].Acked {
		if next := durable.Pending[0].UpdateID + 1; next > offset {
			offset = next
		}
		durable.Pending = durable.Pending[1:]
	}
}

func emitPending() {
	for i := range durable.Pending {
		pending := &durable.Pending[i]
		if pending.Acked || pending.Emitted || len(pending.Event) == 0 {
			continue
		}
		emitOutgoing(pending.Event)
		pending.Emitted = true
	}
}

func pendingBackpressure() bool {
	if len(durable.Pending) >= maxPendingUpdates {
		return true
	}
	for _, pending := range durable.Pending {
		if pending.Blocked != "" {
			return true
		}
	}
	data, ok := marshalDurableState()
	return !ok || len(data) > maxDurableStateBytes-durableStateHeadroom
}

func acceptInbound(eventID string) {
	updateID, err := strconv.ParseInt(eventID, 10, 64)
	if err != nil {
		emitLog("inbound.accepted: invalid event_id")
		return
	}
	index := pendingIndex(updateID)
	if index < 0 {
		if updateID >= offset {
			emitLog("inbound.accepted: unknown event_id")
		}
		return
	}
	durable.Pending[index].Acked = true
	advanceAcceptedPrefix()
	if !persistDurableState() {
		stateReady = false
		emitLog("inbound.accepted: durable state unavailable")
	}
}

func appendPending(pending pendingUpdate) bool {
	if len(durable.Pending) >= maxPendingUpdates {
		return false
	}
	index := len(durable.Pending)
	for i := range durable.Pending {
		if pending.UpdateID < durable.Pending[i].UpdateID {
			index = i
			break
		}
	}
	durable.Pending = append(durable.Pending, pendingUpdate{})
	copy(durable.Pending[index+1:], durable.Pending[index:])
	durable.Pending[index] = pending
	if _, ok := marshalDurableState(); !ok {
		durable.Pending = append(durable.Pending[:index], durable.Pending[index+1:]...)
		return false
	}
	return true
}

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
	if !stateReady {
		emitLog("gateway_poll: durable state unavailable")
		panic("durable state unavailable")
	}
	if token == "" {
		emitLog("gateway_poll: no token configured")
		return 0
	}
	// Restored, unacknowledged updates are retried before asking Telegram for
	// newer work. Emitted is intentionally process-local and resets on restart.
	for i := range durable.Pending {
		if durable.Pending[i].Blocked != "" {
			emitLog("gateway_poll: pending update requires backpressure: " + durable.Pending[i].Blocked)
			return 0
		}
		if !durable.Pending[i].Acked && !durable.Pending[i].Emitted {
			emitPending()
			return 0
		}
	}
	if pendingBackpressure() {
		emitLog("gateway_poll: pending queue backpressure")
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
					IsBot     bool   `json:"is_bot"`
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
	changed := false
	for _, u := range tgResp.Result {
		if u.UpdateID < offset || u.UpdateID < 0 {
			if u.UpdateID < 0 {
				emitLog("gateway_poll: unsupported invalid update_id")
			}
			continue
		}
		if pendingIndex(u.UpdateID) >= 0 {
			continue
		}
		pending := pendingUpdate{UpdateID: u.UpdateID}
		if u.Message == nil || u.Message.Text == "" {
			emitLog("gateway_poll: unsupported update type")
			pending.Acked = true
			if !appendPending(pending) {
				break
			}
			changed = true
			continue
		}
		if u.Message.MessageID <= 0 || u.Message.Chat.ID == 0 || u.Message.From == nil || u.Message.From.ID <= 0 || u.Message.From.IsBot {
			emitLog("gateway_poll: unsupported message sender")
			pending.Acked = true
			if !appendPending(pending) {
				break
			}
			changed = true
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
		if len(data) > maxPendingEventBytes {
			emitLog("gateway_poll: update exceeds durable event limit")
			pending.Blocked = "oversized update"
			if !appendPending(pending) {
				break
			}
			changed = true
			break
		}
		pending.Event = data
		if !appendPending(pending) {
			break
		}
		changed = true
	}
	advanceAcceptedPrefix()
	if changed && !persistDurableState() {
		stateReady = false
		emitLog("gateway_poll: durable state unavailable")
		return 1
	}
	emitPending()

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
	case "inbound.accepted":
		var accepted struct {
			EventID string `json:"event_id"`
		}
		if json.Unmarshal(ev.Payload, &accepted) != nil || accepted.EventID == "" {
			emitLog("inbound.accepted: invalid payload")
			return 0
		}
		acceptInbound(accepted.EventID)
	default:
		emitLog("gateway_event: unknown event type " + ev.Type)
	}

	return 0
}

func handleSessionEvent(payload json.RawMessage) {
	text := ""
	deliveryID := outboundID
	chatID := outboundChatID
	// A route applies to exactly one following Session event. Core sends a
	// fresh route for retries, so stale routing state cannot cross-correlate.
	outboundID, outboundChatID = "", 0
	var delivery struct {
		DeliveryID string `json:"delivery_id"`
		Event      struct {
			Type    string          `json:"type"`
			Payload json.RawMessage `json:"payload"`
		} `json:"event"`
	}
	if json.Unmarshal(payload, &delivery) == nil && delivery.Event.Type != "" {
		if delivery.DeliveryID != "" && deliveryID != "" && delivery.DeliveryID != deliveryID {
			emitDeliveryResult(deliveryID, false, "delivery correlation mismatch")
			return
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
	if deliveryID == "" || chatID == 0 || text == "" {
		emitDeliveryResult(deliveryID, false, "unsupported session event")
		return
	}
	if len(deliveryID) > maxDeliveryIDBytes {
		emitDeliveryResult(deliveryID, false, "delivery ID exceeds limit")
		return
	}
	if deliveryRecorded(deliveryID) {
		emitDeliveryResult(deliveryID, true, "")
		return
	}
	if !stateReady {
		emitDeliveryResult(deliveryID, false, "durable state unavailable")
		return
	}
	ok, deliveryError := sendTelegramText(chatID, text)
	if ok && !recordDelivery(deliveryID) {
		emitDeliveryResult(deliveryID, false, "durable state unavailable")
		return
	}
	emitDeliveryResult(deliveryID, ok, deliveryError)
}

func deliveryRecorded(deliveryID string) bool {
	for _, recorded := range durable.Deliveries {
		if recorded == deliveryID {
			return true
		}
	}
	return false
}

func recordDelivery(deliveryID string) bool {
	if deliveryID == "" || len(deliveryID) > maxDeliveryIDBytes {
		return false
	}
	if deliveryRecorded(deliveryID) {
		return true
	}
	original := append([]string(nil), durable.Deliveries...)
	durable.Deliveries = append(durable.Deliveries, deliveryID)
	if len(durable.Deliveries) > 256 {
		durable.Deliveries = durable.Deliveries[len(durable.Deliveries)-256:]
	}
	for {
		if _, ok := marshalDurableState(); ok {
			break
		}
		if len(durable.Deliveries) <= 1 {
			durable.Deliveries = original
			return false
		}
		durable.Deliveries = durable.Deliveries[1:]
	}
	if !persistDurableState() {
		durable.Deliveries = original
		stateReady = false
		return false
	}
	return true
}

func sendTelegramText(chatID int64, text string) (bool, string) {
	if token == "" {
		return false, "telegram token unavailable"
	}
	base := telegramHost
	if base == "" {
		base = "https://api.telegram.org"
	}
	requestURL := base + "/bot" + token + "/sendMessage?chat_id=" + strconv.FormatInt(chatID, 10) + "&text=" + queryEscape(text)
	var result [12]byte
	hostHTTPGet(uint32(uintptr(unsafe.Pointer(unsafe.StringData(requestURL)))), uint32(len(requestURL)),
		uint32(uintptr(unsafe.Pointer(&responseBuf[0]))), uint32(len(responseBuf)), uint32(uintptr(unsafe.Pointer(&result[0]))))
	written := uint32(result[0]) | uint32(result[1])<<8 | uint32(result[2])<<16 | uint32(result[3])<<24
	code := uint32(result[4]) | uint32(result[5])<<8 | uint32(result[6])<<16 | uint32(result[7])<<24
	if code != 0 {
		return false, "telegram sendMessage HTTP failure"
	}
	if written == 0 || written > uint32(len(responseBuf)) {
		return false, "telegram sendMessage empty response"
	}
	var response struct {
		OK     bool `json:"ok"`
		Result *struct {
			MessageID int64 `json:"message_id"`
		} `json:"result"`
	}
	if json.Unmarshal(responseBuf[:written], &response) != nil {
		return false, "telegram sendMessage malformed response"
	}
	if !response.OK {
		return false, "telegram sendMessage rejected"
	}
	if response.Result == nil || response.Result.MessageID <= 0 {
		return false, "telegram sendMessage invalid result"
	}
	return true, ""
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
