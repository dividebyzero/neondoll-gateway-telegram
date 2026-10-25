// SPDX-License-Identifier: AGPL-3.0-only
//
// host_poll_mock_test — deterministic Telegram Bot API poll test with a mock
// HTTPS server validating the full path: config -> projection -> getUpdates -> event.
package host

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dividebyzero/neondoll-gateway-telegram/contract"
)

func TestTelegramPollMockServer(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("mock received: %s %s", r.Method, r.URL.String())
		if !strings.HasSuffix(r.URL.Path, "/getUpdates") {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(
			`{"ok":true,"result":[{"update_id":42,"message":{` +
				`"message_id":1,"from":{"id":123,"is_bot":false,"first_name":"Test"},` +
				`"chat":{"id":456,"type":"private"},"date":1700000000,"text":"/start"}}]}`,
		))
	})

	srv := httptest.NewTLSServer(handler)
	defer srv.Close()

	httpTransport, ok := srv.Client().Transport.(*http.Transport)
	if !ok {
		t.Fatal("mock server client transport is not *http.Transport")
	}

	cfg := contract.GatewayConfig{
		Version: contract.ConfigVersion,
		Telegram: contract.TelegramConfig{
			BotToken: "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
			Host:     srv.URL,
		},
		Polling: contract.PollingConfig{
			TimeoutSeconds: 5,
		},
	}

	r := NewGatewayRuntime(
		[]string{"127.0.0.1"},
		WithConfig(cfg),
		WithHTTPTransport(httpTransport),
	)
	defer r.Shutdown()

	if err := r.Start(loadWASMWhiteBox(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := r.Poll(); err != nil {
		t.Fatalf("Poll: %v", err)
	}

	t.Logf("Host Logs:")
	for _, l := range r.Log {
		t.Logf("  %s", l)
	}
	t.Logf("Outbound events: %d", len(r.Outbound))
	for i, m := range r.Outbound {
		t.Logf("  outbound[%d]: type=%s payload=%q", i, m.Type, string(m.Payload))
	}

	if len(r.Outbound) == 0 {
		t.Fatal("no outbound events after poll")
	}

	var envelope contract.GatewayEventEnvelope
	if err := json.Unmarshal(r.Outbound[0].Payload, &envelope); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}

	if envelope.EventType != "message.incoming" {
			t.Fatalf("event type = %q, want %q", envelope.EventType, "message.incoming")
		}
	if envelope.UpdateID != 42 {
		t.Fatalf("update_id = %d, want 42", envelope.UpdateID)
	}
	if envelope.Text != "/start" {
		t.Fatalf("text = %q, want %q", envelope.Text, "/start")
	}
	if envelope.ChatType != "private" {
		t.Fatalf("chat_type = %q, want %q", envelope.ChatType, "private")
	}
}
