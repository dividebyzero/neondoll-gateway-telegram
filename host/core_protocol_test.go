package host

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/dividebyzero/neondoll-gateway-telegram/contract"
)

type protocolFixture struct {
	t            *testing.T
	server       *httptest.Server
	state        *MemoryStateStore
	mu           sync.Mutex
	updates      string
	paths        []string
	sendFailures int
	sendAttempts int
	sendBodies   []string
}

func newProtocolFixture(t *testing.T, updates string) *protocolFixture {
	f := &protocolFixture{t: t, state: NewMemoryStateStore(), updates: updates}
	f.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.paths = append(f.paths, r.URL.RequestURI())
		updates := f.updates
		if strings.Contains(r.URL.Path, "/sendMessage") {
			f.sendAttempts++
			fail := f.sendFailures > 0
			if fail {
				f.sendFailures--
			}
			body := `{"ok":true,"result":{"message_id":1}}`
			if !fail && len(f.sendBodies) > 0 {
				body = f.sendBodies[0]
				f.sendBodies = f.sendBodies[1:]
			}
			f.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			if fail {
				http.Error(w, "try again", http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte(body))
			return
		}
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(updates))
	}))
	t.Cleanup(f.server.Close)
	return f
}

func (f *protocolFixture) runtime(t *testing.T, state *MemoryStateStore) *GatewayRuntime {
	transport := f.server.Client().Transport.(*http.Transport)
	cfg := contract.GatewayConfig{
		Version: contract.ConfigVersion,
		Telegram: contract.TelegramConfig{
			BotToken: "123:test",
			Host:     f.server.URL,
		},
		Polling: contract.PollingConfig{TimeoutSeconds: 1},
	}
	runtime := NewGatewayRuntime([]string{"127.0.0.1"}, WithConfig(cfg), WithHTTPTransport(transport), WithStateStore(state))
	if err := runtime.Start(loadWASMWhiteBox(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown() })
	return runtime
}

func incomingIDs(t *testing.T, messages []contract.GatewayMessage) []int64 {
	t.Helper()
	var ids []int64
	for _, message := range messages {
		var event contract.GatewayEventEnvelope
		if message.Type == "outgoing" && json.Unmarshal(message.Payload, &event) == nil && event.EventType == "message.incoming" {
			ids = append(ids, event.UpdateID)
		}
	}
	return ids
}

func incomingEvents(t *testing.T, messages []contract.GatewayMessage) []contract.GatewayEventEnvelope {
	t.Helper()
	var events []contract.GatewayEventEnvelope
	for _, message := range messages {
		var event contract.GatewayEventEnvelope
		if message.Type == "outgoing" && json.Unmarshal(message.Payload, &event) == nil && event.EventType == "message.incoming" {
			events = append(events, event)
		}
	}
	return events
}

func TestInboundAcceptancePersistsAcrossRestart(t *testing.T) {
	fixture := newProtocolFixture(t, `{"ok":true,"result":[{"update_id":42,"message":{"message_id":1,"from":{"id":9},"chat":{"id":7,"type":"private"},"date":1,"text":"hello"}}]}`)
	first := fixture.runtime(t, fixture.state)
	if err := first.Poll(); err != nil {
		t.Fatal(err)
	}
	if got := incomingIDs(t, first.TakeOutbound()); len(got) != 1 || got[0] != 42 {
		t.Fatalf("first emission=%v", got)
	}
	if err := first.Shutdown(); err != nil {
		t.Fatal(err)
	}

	second := fixture.runtime(t, fixture.state)
	if err := second.Poll(); err != nil {
		t.Fatal(err)
	}
	if got := incomingIDs(t, second.TakeOutbound()); len(got) != 1 || got[0] != 42 {
		t.Fatalf("restart retry=%v", got)
	}
	fixture.mu.Lock()
	requestsBeforeAck := len(fixture.paths)
	fixture.mu.Unlock()
	if requestsBeforeAck != 1 {
		t.Fatalf("Telegram repolled before acceptance: requests=%d", requestsBeforeAck)
	}
	if err := second.HandleEvent(contract.Version0, "inbound.accepted", `{"event_id":"42"}`); err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	fixture.updates = `{"ok":true,"result":[]}`
	fixture.mu.Unlock()
	if err := second.Poll(); err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	lastPath := fixture.paths[len(fixture.paths)-1]
	fixture.mu.Unlock()
	if !strings.Contains(lastPath, "offset=43") {
		t.Fatalf("accepted cursor path=%q", lastPath)
	}
}

func TestPendingPersistenceFailureRecoversAfterRestart(t *testing.T) {
	fixture := newProtocolFixture(t, `{"ok":true,"result":[{"update_id":42,"message":{"message_id":1,"from":{"id":9},"chat":{"id":7,"type":"private"},"date":1,"text":"hello"}}]}`)
	first := fixture.runtime(t, fixture.state)
	fixture.state.FailNextPuts(1)
	if err := first.Poll(); err != nil {
		t.Fatal(err)
	}
	if got := incomingIDs(t, first.TakeOutbound()); len(got) != 0 {
		t.Fatalf("event escaped before durable write: %v", got)
	}
	if err := first.Shutdown(); err != nil {
		t.Fatal(err)
	}
	second := fixture.runtime(t, fixture.state)
	if err := second.Poll(); err != nil {
		t.Fatal(err)
	}
	if got := incomingIDs(t, second.TakeOutbound()); len(got) != 1 || got[0] != 42 {
		t.Fatalf("recovered emission=%v", got)
	}
}

func TestOutOfOrderAcknowledgementsDoNotSkipEarlierUpdates(t *testing.T) {
	fixture := newProtocolFixture(t, `{"ok":true,"result":[
		{"update_id":10,"message":{"message_id":1,"from":{"id":110},"chat":{"id":7,"type":"private"},"date":1,"text":"ten"}},
		{"update_id":11,"message":{"message_id":2,"from":{"id":111},"chat":{"id":7,"type":"private"},"date":2,"text":"eleven"}},
		{"update_id":12,"message":{"message_id":3,"from":{"id":112},"chat":{"id":7,"type":"private"},"date":3,"text":"twelve"}}
	]}`)
	runtime := fixture.runtime(t, fixture.state)
	if err := runtime.Poll(); err != nil {
		t.Fatal(err)
	}
	if got := incomingIDs(t, runtime.TakeOutbound()); len(got) != 3 || got[0] != 10 || got[1] != 11 || got[2] != 12 {
		t.Fatalf("emissions=%v", got)
	}
	if err := runtime.HandleEvent(contract.Version0, "inbound.accepted", `{"event_id":"12"}`); err != nil {
		t.Fatal(err)
	}
	if state := fixture.stateJSON(t); state.Offset != 0 {
		t.Fatalf("later acknowledgement advanced cursor=%d", state.Offset)
	}
	if err := runtime.Shutdown(); err != nil {
		t.Fatal(err)
	}
	runtime = fixture.runtime(t, fixture.state)
	if err := runtime.Poll(); err != nil {
		t.Fatal(err)
	}
	if got := incomingIDs(t, runtime.TakeOutbound()); len(got) != 2 || got[0] != 10 || got[1] != 11 {
		t.Fatalf("restart retry after out-of-order acknowledgement=%v", got)
	}
	if err := runtime.HandleEvent(contract.Version0, "inbound.accepted", `{"event_id":"10"}`); err != nil {
		t.Fatal(err)
	}
	if state := fixture.stateJSON(t); state.Offset != 11 {
		t.Fatalf("cursor after first acknowledgement=%d", state.Offset)
	}
	if err := runtime.HandleEvent(contract.Version0, "inbound.accepted", `{"event_id":"11"}`); err != nil {
		t.Fatal(err)
	}
	if state := fixture.stateJSON(t); state.Offset != 13 || len(state.Pending) != 0 {
		t.Fatalf("final state=%+v", state)
	}
}

func (f *protocolFixture) stateJSON(t *testing.T) durableStateFixture {
	t.Helper()
	data, ok := f.state.get("telegram.gateway.v1")
	if !ok {
		t.Fatal("durable guest state missing")
	}
	var state durableStateFixture
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	return state
}

type durableStateFixture struct {
	Offset     int64             `json:"offset"`
	Pending    []json.RawMessage `json:"pending"`
	Deliveries []string          `json:"deliveries"`
	Skipped    []struct {
		UpdateID int64  `json:"update_id"`
		Reason   string `json:"reason"`
	} `json:"skipped"`
}

func TestDurableStateCapabilityIsRequired(t *testing.T) {
	fixture := newProtocolFixture(t, `{"ok":true,"result":[]}`)
	transport := fixture.server.Client().Transport.(*http.Transport)
	cfg := contract.GatewayConfig{Version: contract.ConfigVersion, Telegram: contract.TelegramConfig{BotToken: "123:test", Host: fixture.server.URL}}
	runtime := NewGatewayRuntime([]string{"127.0.0.1"}, WithConfig(cfg), WithHTTPTransport(transport), WithStateStore(nil))
	if err := runtime.Start(loadWASMWhiteBox(t)); err == nil {
		t.Fatal("guest started without durable state capability")
	}
}

func TestOutboundDeliveryCorrelationFailureRetryAndDeduplication(t *testing.T) {
	fixture := newProtocolFixture(t, `{"ok":true,"result":[]}`)
	fixture.sendFailures = 1
	runtime := fixture.runtime(t, fixture.state)
	deliver := func(deliveryID string) deliveryResultFixture {
		t.Helper()
		route := `{"chat_id":7,"delivery_id":"` + deliveryID + `"}`
		if err := runtime.HandleEvent(contract.Version0, "session.route", route); err != nil {
			t.Fatal(err)
		}
		payload := `{"delivery_id":"` + deliveryID + `","event":{"type":"message","payload":{"parts":[{"type":"text","text":"hello"}]}}}`
		if err := runtime.HandleEvent(contract.Version0, "session.event", payload); err != nil {
			t.Fatal(err)
		}
		for _, message := range runtime.TakeOutbound() {
			var result deliveryResultFixture
			if json.Unmarshal(message.Payload, &result) == nil && result.EventType == "delivery.result" {
				return result
			}
		}
		t.Fatal("delivery.result missing")
		return deliveryResultFixture{}
	}
	first := deliver("delivery-1")
	if first.OK || first.DeliveryID != "delivery-1" || !strings.Contains(first.Error, "HTTP") {
		t.Fatalf("first result=%+v", first)
	}
	second := deliver("delivery-1")
	if !second.OK || second.DeliveryID != "delivery-1" {
		t.Fatalf("retry result=%+v", second)
	}
	third := deliver("delivery-1")
	fixture.mu.Lock()
	attempts := fixture.sendAttempts
	fixture.mu.Unlock()
	if !third.OK || attempts != 2 {
		t.Fatalf("deduplicated result=%+v attempts=%d", third, attempts)
	}
}

func TestSendMessageRequiresTelegramSuccessResponse(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "telegram rejection", body: `{"ok":false,"description":"denied"}`, want: "rejected"},
		{name: "malformed JSON", body: `{`, want: "malformed"},
		{name: "missing result", body: `{"ok":true}`, want: "invalid result"},
		{name: "invalid message", body: `{"ok":true,"result":{"message_id":0}}`, want: "invalid result"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newProtocolFixture(t, `{"ok":true,"result":[]}`)
			fixture.sendBodies = []string{test.body}
			runtime := fixture.runtime(t, fixture.state)
			result := deliverSessionMessage(t, runtime, "delivery-1")
			if result.OK || result.DeliveryID != "delivery-1" || !strings.Contains(result.Error, test.want) {
				t.Fatalf("result=%+v", result)
			}
			if state := fixture.stateJSON(t); len(state.Deliveries) != 0 {
				t.Fatalf("failed delivery recorded=%v", state.Deliveries)
			}
		})
	}
}

func deliverSessionMessage(t *testing.T, runtime *GatewayRuntime, deliveryID string) deliveryResultFixture {
	t.Helper()
	route := `{"chat_id":7,"delivery_id":"` + deliveryID + `"}`
	if err := runtime.HandleEvent(contract.Version0, "session.route", route); err != nil {
		t.Fatal(err)
	}
	payload := `{"delivery_id":"` + deliveryID + `","event":{"type":"message","payload":{"parts":[{"type":"text","text":"hello"}]}}}`
	if err := runtime.HandleEvent(contract.Version0, "session.event", payload); err != nil {
		t.Fatal(err)
	}
	for _, message := range runtime.TakeOutbound() {
		var result deliveryResultFixture
		if json.Unmarshal(message.Payload, &result) == nil && result.EventType == "delivery.result" {
			return result
		}
	}
	t.Fatal("delivery.result missing")
	return deliveryResultFixture{}
}

type deliveryResultFixture struct {
	EventType  string `json:"event_type"`
	DeliveryID string `json:"delivery_id"`
	OK         bool   `json:"ok"`
	Error      string `json:"error"`
}

func TestDeliveryCorrelationMismatchFailsClosed(t *testing.T) {
	fixture := newProtocolFixture(t, `{"ok":true,"result":[]}`)
	runtime := fixture.runtime(t, fixture.state)
	if err := runtime.HandleEvent(contract.Version0, "session.route", `{"chat_id":7,"delivery_id":"route-id"}`); err != nil {
		t.Fatal(err)
	}
	payload := `{"delivery_id":"forged-id","event":{"type":"message","payload":{"parts":[{"type":"text","text":"hello"}]}}}`
	if err := runtime.HandleEvent(contract.Version0, "session.event", payload); err != nil {
		t.Fatal(err)
	}
	var result deliveryResultFixture
	for _, message := range runtime.TakeOutbound() {
		_ = json.Unmarshal(message.Payload, &result)
	}
	if result.OK || result.DeliveryID != "route-id" || !strings.Contains(result.Error, "correlation") {
		t.Fatalf("mismatch result=%+v", result)
	}
}

func TestIndependentInstallationsDoNotShareCursorState(t *testing.T) {
	fixture := newProtocolFixture(t, `{"ok":true,"result":[{"update_id":42,"message":{"message_id":1,"from":{"id":9},"chat":{"id":7,"type":"private"},"date":1,"text":"hello"}}]}`)
	firstState, secondState := NewMemoryStateStore(), NewMemoryStateStore()
	first, second := fixture.runtime(t, firstState), fixture.runtime(t, secondState)
	if err := first.Poll(); err != nil {
		t.Fatal(err)
	}
	if err := second.Poll(); err != nil {
		t.Fatal(err)
	}
	_ = first.TakeOutbound()
	_ = second.TakeOutbound()
	if err := first.HandleEvent(contract.Version0, "inbound.accepted", `{"event_id":"42"}`); err != nil {
		t.Fatal(err)
	}
	decode := func(state *MemoryStateStore) durableStateFixture {
		t.Helper()
		data, ok := state.get("telegram.gateway.v1")
		if !ok {
			t.Fatal("state missing")
		}
		var decoded durableStateFixture
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatal(err)
		}
		return decoded
	}
	if got := decode(firstState); got.Offset != 43 || len(got.Pending) != 0 {
		t.Fatalf("first state=%+v", got)
	}
	if got := decode(secondState); got.Offset != 0 || len(got.Pending) != 1 {
		t.Fatalf("second state=%+v", got)
	}
}

func TestPendingQueueBackpressureRestartAndRecovery(t *testing.T) {
	var updates strings.Builder
	updates.WriteString(`{"ok":true,"result":[`)
	for id := 1; id <= 17; id++ {
		if id > 1 {
			updates.WriteByte(',')
		}
		updates.WriteString(fmt.Sprintf(`{"update_id":%d,"message":{"message_id":%d,"from":{"id":%d},"chat":{"id":7,"type":"private"},"date":1,"text":"m%d"}}`, id, id, id, id))
	}
	updates.WriteString(`]}`)
	fixture := newProtocolFixture(t, updates.String())
	runtime := fixture.runtime(t, fixture.state)
	if err := runtime.Poll(); err != nil {
		t.Fatal(err)
	}
	if got := incomingIDs(t, runtime.TakeOutbound()); len(got) != 16 || got[0] != 1 || got[15] != 16 {
		t.Fatalf("initial bounded emissions=%v", got)
	}
	if err := runtime.Poll(); err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	requests := len(fixture.paths)
	firstPath := fixture.paths[0]
	fixture.mu.Unlock()
	if requests != 1 || !strings.Contains(firstPath, "limit=16") {
		t.Fatalf("full queue requests=%d path=%q", requests, firstPath)
	}
	if err := runtime.Shutdown(); err != nil {
		t.Fatal(err)
	}
	runtime = fixture.runtime(t, fixture.state)
	if err := runtime.Poll(); err != nil {
		t.Fatal(err)
	}
	if got := incomingIDs(t, runtime.TakeOutbound()); len(got) != 16 {
		t.Fatalf("restart pending emissions=%v", got)
	}
	fixture.mu.Lock()
	requests = len(fixture.paths)
	fixture.mu.Unlock()
	if requests != 1 {
		t.Fatalf("restart bypassed backpressure: requests=%d", requests)
	}
	if err := runtime.HandleEvent(contract.Version0, "inbound.accepted", `{"event_id":"1"}`); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Poll(); err != nil {
		t.Fatal(err)
	}
	if got := incomingIDs(t, runtime.TakeOutbound()); len(got) != 1 || got[0] != 17 {
		t.Fatalf("recovered queue emissions=%v", got)
	}
	fixture.mu.Lock()
	resumePath := fixture.paths[len(fixture.paths)-1]
	fixture.mu.Unlock()
	if !strings.Contains(resumePath, "limit=1") {
		t.Fatalf("resume path=%q", resumePath)
	}
	state := fixture.stateJSON(t)
	if state.Offset != 2 || len(state.Pending) != 16 {
		t.Fatalf("recovered state=%+v", state)
	}
	data, _ := fixture.state.get("telegram.gateway.v1")
	if len(data) > 2<<20 {
		t.Fatalf("serialized state bytes=%d", len(data))
	}
}

func TestFullLengthTelegramTextPreserved(t *testing.T) {
	for name, text := range map[string]string{
		"maximum ASCII":     strings.Repeat("x", 4096),
		"maximum multibyte": strings.Repeat("界", 4096),
		"escaped JSON":      strings.Repeat("\"\\\n<&>", 600),
	} {
		t.Run(name, func(t *testing.T) {
			updates, err := json.Marshal(map[string]any{"ok": true, "result": []any{map[string]any{"update_id": 1, "message": map[string]any{"message_id": 1, "from": map[string]any{"id": 9}, "chat": map[string]any{"id": 7, "type": "private"}, "date": 1, "text": text}}}})
			if err != nil {
				t.Fatal(err)
			}
			fixture := newProtocolFixture(t, string(updates))
			runtime := fixture.runtime(t, fixture.state)
			if err = runtime.Poll(); err != nil {
				t.Fatal(err)
			}
			events := incomingEvents(t, runtime.TakeOutbound())
			if len(events) != 1 {
				t.Fatalf("events=%d, want 1", len(events))
			}
			if events[0].Text != text {
				t.Fatalf("text bytes=%d want=%d", len(events[0].Text), len(text))
			}
		})
	}
}

func TestProviderInvalidTextIsDurablySkippedWithoutPoisoningQueue(t *testing.T) {
	oversized := strings.Repeat("x", 16*1024+1)
	updates, err := json.Marshal(map[string]any{"ok": true, "result": []any{
		map[string]any{"update_id": 1, "message": map[string]any{"message_id": 1, "from": map[string]any{"id": 9}, "chat": map[string]any{"id": 7, "type": "private"}, "date": 1, "text": oversized}},
		map[string]any{"update_id": 2, "message": map[string]any{"message_id": 2, "from": map[string]any{"id": 9}, "chat": map[string]any{"id": 7, "type": "private"}, "date": 1, "text": "valid"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	fixture := newProtocolFixture(t, string(updates))
	runtime := fixture.runtime(t, fixture.state)
	if err = runtime.Poll(); err != nil {
		t.Fatal(err)
	}
	if got := incomingIDs(t, runtime.TakeOutbound()); len(got) != 1 || got[0] != 2 {
		t.Fatalf("emitted IDs=%v", got)
	}
	state := fixture.stateJSON(t)
	if state.Offset != 2 || len(state.Pending) != 1 || len(state.Skipped) != 1 || state.Skipped[0].Reason != "provider_text_exceeds_telegram_limit" {
		t.Fatalf("durable skip state=%+v", state)
	}
	if err = runtime.HandleEvent(contract.Version0, "inbound.accepted", `{"event_id":"2"}`); err != nil {
		t.Fatal(err)
	}
	if state = fixture.stateJSON(t); state.Offset != 3 || len(state.Pending) != 0 {
		t.Fatalf("recovered state=%+v", state)
	}
}

func TestOversizedValidEnvelopeRetriesAndRecoversAfterRestart(t *testing.T) {
	hugeName := strings.Repeat("n", 70<<10)
	update := map[string]any{
		"update_id": 1,
		"message": map[string]any{
			"message_id": 1, "from": map[string]any{"id": 9, "first_name": hugeName},
			"chat": map[string]any{"id": 7, "type": "private"}, "date": 1, "text": "valid text",
		},
	}
	firstResponse, err := json.Marshal(map[string]any{"ok": true, "result": []any{update}})
	if err != nil {
		t.Fatal(err)
	}
	fixture := newProtocolFixture(t, string(firstResponse))
	first := fixture.runtime(t, fixture.state)
	if err = first.Poll(); err != nil {
		t.Fatal(err)
	}
	if got := incomingIDs(t, first.TakeOutbound()); len(got) != 0 {
		t.Fatalf("oversized envelope emitted=%v", got)
	}
	state := fixture.stateJSON(t)
	if state.Offset != 0 || len(state.Pending) != 0 || len(state.Skipped) != 0 {
		t.Fatalf("capacity failure advanced or skipped=%+v", state)
	}
	if err = first.Shutdown(); err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	fixture.updates = `{"ok":true,"result":[{"update_id":1,"message":{"message_id":1,"from":{"id":9,"first_name":"ok"},"chat":{"id":7,"type":"private"},"date":1,"text":"valid text"}}]}`
	fixture.mu.Unlock()
	second := fixture.runtime(t, fixture.state)
	if err = second.Poll(); err != nil {
		t.Fatal(err)
	}
	events := incomingEvents(t, second.TakeOutbound())
	if len(events) != 1 || events[0].UpdateID != 1 || events[0].Text != "valid text" {
		t.Fatalf("recovered events=%+v", events)
	}
}

func TestMediaUpdateEmitsReferenceWithoutBinary(t *testing.T) {
	updates := `{"ok":true,"result":[{"update_id":7,"message":{"message_id":70,"from":{"id":9},"chat":{"id":77,"type":"private"},"date":1,"caption":"look","photo":[{"file_id":"small","file_unique_id":"u1","file_size":10},{"file_id":"large","file_unique_id":"u2","file_size":99}]}}]}`
	fixture := newProtocolFixture(t, updates)
	runtime := fixture.runtime(t, fixture.state)
	if err := runtime.Poll(); err != nil {
		t.Fatal(err)
	}
	events := incomingEvents(t, runtime.TakeOutbound())
	if len(events) != 1 || events[0].Text != "look" || len(events[0].Media) != 1 {
		t.Fatalf("events=%+v", events)
	}
	media := events[0].Media[0]
	if media.Type != "image" || media.MediaID != "large" || media.UniqueID != "u2" || media.SizeBytes != 99 || media.Caption != "look" || media.MessageID != "70" || media.ConversationID != "77" {
		t.Fatalf("media=%+v", media)
	}
}

func TestInvalidSendersAndUnsupportedUpdatesAreNotEmitted(t *testing.T) {
	updates := `{"ok":true,"result":[
		{"update_id":1,"message":{"message_id":1,"chat":{"id":7,"type":"private"},"date":1,"text":"missing sender"}},
		{"update_id":2,"message":{"message_id":2,"from":{"id":0},"chat":{"id":7,"type":"private"},"date":1,"text":"invalid sender"}},
		{"update_id":3,"message":{"message_id":3,"from":{"id":8,"is_bot":true},"chat":{"id":7,"type":"private"},"date":1,"text":"bot sender"}},
		{"update_id":4},
		{"update_id":5,"message":{"message_id":5,"from":{"id":9},"chat":{"id":7,"type":"private"},"date":1,"text":"valid"}}
	]}`
	fixture := newProtocolFixture(t, updates)
	runtime := fixture.runtime(t, fixture.state)
	if err := runtime.Poll(); err != nil {
		t.Fatal(err)
	}
	if got := incomingIDs(t, runtime.TakeOutbound()); len(got) != 1 || got[0] != 5 {
		t.Fatalf("emitted IDs=%v", got)
	}
	state := fixture.stateJSON(t)
	if state.Offset != 5 || len(state.Pending) != 1 || len(state.Skipped) != 4 {
		t.Fatalf("unsupported update state=%+v", state)
	}
}
