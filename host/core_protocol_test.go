package host

import (
	"encoding/json"
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
			f.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			if fail {
				http.Error(w, "try again", http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":1}}`))
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

func TestOutOfOrderAcknowledgementsDoNotSkipEarlierUpdates(t *testing.T) {
	fixture := newProtocolFixture(t, `{"ok":true,"result":[
		{"update_id":10,"message":{"message_id":1,"chat":{"id":7,"type":"private"},"date":1,"text":"ten"}},
		{"update_id":11,"message":{"message_id":2,"chat":{"id":7,"type":"private"},"date":2,"text":"eleven"}},
		{"update_id":12,"message":{"message_id":3,"chat":{"id":7,"type":"private"},"date":3,"text":"twelve"}}
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
	Offset  int64             `json:"offset"`
	Pending []json.RawMessage `json:"pending"`
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
	if first.OK || first.DeliveryID != "delivery-1" {
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
	fixture := newProtocolFixture(t, `{"ok":true,"result":[{"update_id":42,"message":{"message_id":1,"chat":{"id":7,"type":"private"},"date":1,"text":"hello"}}]}`)
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
