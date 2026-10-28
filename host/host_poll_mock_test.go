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

// setupPollTest creates a runtime with a mock Telegram API server using the
// given handler. Returns the runtime and a cleanup func.
func setupPollTest(t *testing.T, handler http.HandlerFunc, allowed []string) *GatewayRuntime {
	t.Helper()
	if allowed == nil {
		allowed = []string{"127.0.0.1"}
	}
	srv := httptest.NewTLSServer(handler)
	t.Cleanup(srv.Close)

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
		allowed,
		WithConfig(cfg),
		WithHTTPTransport(httpTransport),
	)
	t.Cleanup(func() { r.Shutdown() })

	if err := r.Start(loadWASMWhiteBox(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return r
}

// outboundEnvelopes extracts GatewayEventEnvelope values from outbound
// messages that have type "outgoing".
func outboundEnvelopes(t *testing.T, r *GatewayRuntime) []contract.GatewayEventEnvelope {
	t.Helper()
	var envs []contract.GatewayEventEnvelope
	for _, m := range r.Outbound {
		if m.Type != "outgoing" {
			continue
		}
		var env contract.GatewayEventEnvelope
		if err := json.Unmarshal(m.Payload, &env); err != nil {
			t.Fatalf("unmarshal envelope: %v", err)
		}
		envs = append(envs, env)
	}
	return envs
}

func TestTelegramPollMockServer(t *testing.T) {
	// ── Basic single update ──────────────────────────────────────────────
	t.Run("single update", func(t *testing.T) {
		r := setupPollTest(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true,"result":[{
				"update_id":42,
				"message":{"message_id":1,"from":{"id":123,"is_bot":false,"first_name":"Test"},
				"chat":{"id":456,"type":"private"},"date":1700000000,"text":"/start"}
			}]}`))
		}), nil)

		if err := r.Poll(); err != nil {
			t.Fatalf("Poll: %v", err)
		}

		envs := outboundEnvelopes(t, r)
		if len(envs) != 1 {
			t.Fatalf("got %d events, want 1", len(envs))
		}
		env := envs[0]
		if env.EventType != "message.incoming" {
			t.Fatalf("event_type = %q, want %q", env.EventType, "message.incoming")
		}
		if env.UpdateID != 42 {
			t.Fatalf("update_id = %d, want 42", env.UpdateID)
		}
		if env.MessageID != 1 {
			t.Fatalf("message_id = %d, want 1", env.MessageID)
		}
		if env.ChatID != 456 {
			t.Fatalf("chat_id = %d, want 456", env.ChatID)
		}
		if env.ChatType != "private" {
			t.Fatalf("chat_type = %q, want %q", env.ChatType, "private")
		}
		if env.SenderID != 123 {
			t.Fatalf("sender_id = %d, want 123", env.SenderID)
		}
		if env.SenderName != "Test" {
			t.Fatalf("sender_name = %q, want %q", env.SenderName, "Test")
		}
		if env.Text != "/start" {
			t.Fatalf("text = %q, want %q", env.Text, "/start")
		}
		if env.ReceivedAt != 1700000000 {
			t.Fatalf("received_at = %d, want 1700000000", env.ReceivedAt)
		}
	})

	// ── Multiple updates ─────────────────────────────────────────────────
	t.Run("multiple updates", func(t *testing.T) {
		r := setupPollTest(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true,"result":[
				{"update_id":1,"message":{"message_id":10,"from":{"id":100,"is_bot":false,"first_name":"Alice"},
					"chat":{"id":200,"type":"private"},"date":1700000001,"text":"first"}},
				{"update_id":2,"message":{"message_id":11,"from":{"id":101,"is_bot":false,"first_name":"Bob"},
					"chat":{"id":201,"type":"group"},"date":1700000002,"text":"second"}},
				{"update_id":3,"message":{"message_id":12,"from":{"id":102,"is_bot":false,"first_name":"Carol"},
					"chat":{"id":202,"type":"private"},"date":1700000003,"text":"third"}}
			]}`))
		}), nil)

		if err := r.Poll(); err != nil {
			t.Fatalf("Poll: %v", err)
		}

		envs := outboundEnvelopes(t, r)
		if len(envs) != 3 {
			t.Fatalf("got %d events, want 3", len(envs))
		}
		if envs[0].Text != "first" || envs[1].Text != "second" || envs[2].Text != "third" {
			t.Fatalf("unexpected text: %q, %q, %q", envs[0].Text, envs[1].Text, envs[2].Text)
		}
		if envs[0].UpdateID != 1 || envs[1].UpdateID != 2 || envs[2].UpdateID != 3 {
			t.Fatalf("unexpected update_ids: %d, %d, %d", envs[0].UpdateID, envs[1].UpdateID, envs[2].UpdateID)
		}
	})

	// ── Successive polls with offset tracking ────────────────────────────
	t.Run("offset tracking", func(t *testing.T) {
		callNum := 0
		r := setupPollTest(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			callNum++

			// Inspect offset from query string to confirm it advances
			offset := r.URL.Query().Get("offset")
			t.Logf("poll call #%d: offset=%s", callNum, offset)

			switch callNum {
			case 1:
				if offset != "" && offset != "0" {
					t.Errorf("first call offset=%q, want empty or 0", offset)
				}
				_, _ = w.Write([]byte(`{"ok":true,"result":[
					{"update_id":5,"message":{"message_id":20,"from":{"id":30},"chat":{"id":300,"type":"private"},"date":1700000010,"text":"a"}}
				]}`))
			case 2:
				if offset != "6" {
					t.Errorf("second call offset=%q, want 6", offset)
				}
				_, _ = w.Write([]byte(`{"ok":true,"result":[
					{"update_id":5,"message":{"message_id":20,"from":{"id":30},"chat":{"id":300,"type":"private"},"date":1700000010,"text":"a"}}
				]}`))
			default:
				t.Errorf("unexpected call #%d", callNum)
			}
		}), nil)

		// First poll: get update_id=5
		if err := r.Poll(); err != nil {
			t.Fatalf("Poll #1: %v", err)
		}
		envs := outboundEnvelopes(t, r)
		if len(envs) != 1 {
			t.Fatalf("poll #1 events: got %d, want 1", len(envs))
		}

		// Core acceptance, not fetch/emission, advances the durable cursor.
		if err := r.HandleEvent(contract.Version0, "inbound.accepted", `{"event_id":"5"}`); err != nil {
			t.Fatalf("accept update: %v", err)
		}

		// Second poll: offset should now have advanced past 5.
		// Server returns the same update_id=5 (already processed) — must be skipped.
		if err := r.Poll(); err != nil {
			t.Fatalf("Poll #2: %v", err)
		}
		envs2 := outboundEnvelopes(t, r)
		if len(envs2) != len(envs) {
			t.Fatalf("poll #2 events: got %d (accumulated), want %d (no new events)", len(envs2), len(envs))
		}
	})

	// ── Negative/large chat and user IDs ─────────────────────────────────
	t.Run("negative and large ids", func(t *testing.T) {
		r := setupPollTest(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true,"result":[
				{"update_id":100,"message":{"message_id":999,"from":{"id":1234567890123,"is_bot":false,"first_name":"GroupBot"},
					"chat":{"id":-987654321098,"type":"supergroup"},"date":1700000020,"text":"hello group"}},
				{"update_id":101,"message":{"message_id":1000,"from":{"id":9223372036854775807,"is_bot":false,"first_name":"MaxInt"},
					"chat":{"id":-1001234567890,"type":"group"},"date":1700000021,"text":"large ints"}}
			]}`))
		}), nil)

		if err := r.Poll(); err != nil {
			t.Fatalf("Poll: %v", err)
		}

		envs := outboundEnvelopes(t, r)
		if len(envs) != 2 {
			t.Fatalf("got %d events, want 2", len(envs))
		}

		// First: negative supergroup
		if envs[0].UpdateID != 100 {
			t.Fatalf("update_id = %d, want 100", envs[0].UpdateID)
		}
		if got := envs[0].SenderID; got != 1234567890123 {
			t.Fatalf("sender_id = %d, want 1234567890123", got)
		}
		if got := envs[0].ChatID; got != -987654321098 {
			t.Fatalf("chat_id = %d, want -987654321098", got)
		}
		if envs[0].ChatType != "supergroup" {
			t.Fatalf("chat_type = %q, want supergroup", envs[0].ChatType)
		}

		// Second: max int64 and negative chat
		if got := envs[1].SenderID; got != 9223372036854775807 {
			t.Fatalf("sender_id = %d, want 9223372036854775807", got)
		}
		if got := envs[1].ChatID; got != -1001234567890 {
			t.Fatalf("chat_id = %d, want -1001234567890", got)
		}
		if envs[1].Text != "large ints" {
			t.Fatalf("text = %q, want 'large ints'", envs[1].Text)
		}
	})

	// ── Escaped text ─────────────────────────────────────────────────────
	t.Run("escaped text", func(t *testing.T) {
		r := setupPollTest(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true,"result":[{"update_id":50,"message":{
				"message_id":5,"from":{"id":10},"chat":{"id":100,"type":"private"},
				"date":1700000030,"text":"Hello \"world\" & <script>alert(1)<\/script> \u00e9\u00e0\u00fc"
			}}]}`)) // eslint-disable-line
		}), nil)

		if err := r.Poll(); err != nil {
			t.Fatalf("Poll: %v", err)
		}

		envs := outboundEnvelopes(t, r)
		if len(envs) != 1 {
			t.Fatalf("got %d events, want 1", len(envs))
		}
		want := `Hello "world" & <script>alert(1)</script> éàü`
		if envs[0].Text != want {
			t.Fatalf("text = %q, want %q", envs[0].Text, want)
		}
	})

	// ── Malformed JSON response ──────────────────────────────────────────
	t.Run("malformed json", func(t *testing.T) {
		r := setupPollTest(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{invalid json`))
		}), nil)

		if err := r.Poll(); err != nil {
			t.Fatalf("Poll: %v", err)
		}
		// Should produce no outbound events, just log the error
		if len(r.Outbound) != 0 {
			t.Fatalf("got %d outbound events, want 0 for malformed JSON", len(r.Outbound))
		}
		// Check the log contains a JSON error
		hasErr := false
		for _, l := range r.Log {
			if strings.Contains(l, "json error") {
				hasErr = true
				break
			}
		}
		if !hasErr {
			t.Fatalf("expected a 'json error' log line, got:\n%s", strings.Join(r.Log, "\n"))
		}
	})

	// ── API error response ───────────────────────────────────────────────
	t.Run("api error", func(t *testing.T) {
		r := setupPollTest(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":false,"description":"Unauthorized","error_code":401}`))
		}), nil)

		if err := r.Poll(); err != nil {
			t.Fatalf("Poll: %v", err)
		}
		if len(r.Outbound) != 0 {
			t.Fatalf("got %d outbound events, want 0 for API error", len(r.Outbound))
		}
		hasNotOk := false
		for _, l := range r.Log {
			if strings.Contains(l, "telegram returned not ok") {
				hasNotOk = true
				break
			}
		}
		if !hasNotOk {
			t.Fatalf("expected a 'telegram returned not ok' log line, got:\n%s", strings.Join(r.Log, "\n"))
		}
	})

	// ── Empty result ─────────────────────────────────────────────────────
	t.Run("empty result", func(t *testing.T) {
		r := setupPollTest(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true,"result":[]}`))
		}), nil)

		if err := r.Poll(); err != nil {
			t.Fatalf("Poll: %v", err)
		}
		if len(r.Outbound) != 0 {
			t.Fatalf("got %d outbound events, want 0 for empty result", len(r.Outbound))
		}
	})

	// ── No message (non-message update, e.g. callback_query) ─────────────
	t.Run("non-message update skipped", func(t *testing.T) {
		r := setupPollTest(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true,"result":[
				{"update_id":10,"message":{"message_id":1,"from":{"id":10},"chat":{"id":100,"type":"private"},"date":1700000040,"text":"valid"}},
				{"update_id":11,"callback_query":{"id":"cb1","from":{"id":999,"is_bot":false,"first_name":"Bot"}}},
				{"update_id":12,"message":{"message_id":2,"from":{"id":12},"chat":{"id":101,"type":"group"},"date":1700000041,"text":"also valid"}}
			]}`))
		}), nil)

		if err := r.Poll(); err != nil {
			t.Fatalf("Poll: %v", err)
		}
		envs := outboundEnvelopes(t, r)
		if len(envs) != 2 {
			t.Fatalf("got %d events, want 2 (callback_query should be skipped)", len(envs))
		}
		if envs[0].Text != "valid" || envs[1].Text != "also valid" {
			t.Fatalf("unexpected texts: %q, %q", envs[0].Text, envs[1].Text)
		}
	})

	// ── Empty text message skipped ───────────────────────────────────────
	t.Run("empty text skipped", func(t *testing.T) {
		r := setupPollTest(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true,"result":[
				{"update_id":20,"message":{"message_id":3,"from":{"id":20},"chat":{"id":200,"type":"private"},"date":1700000050,"text":"has text"}},
				{"update_id":21,"message":{"message_id":4,"from":{"id":21},"chat":{"id":201,"type":"private"},"date":1700000051,"text":""}},
				{"update_id":22,"message":{"message_id":5,"from":{"id":22},"chat":{"id":202,"type":"private"},"date":1700000052,"text":"also text"}}
			]}`))
		}), nil)

		if err := r.Poll(); err != nil {
			t.Fatalf("Poll: %v", err)
		}
		envs := outboundEnvelopes(t, r)
		if len(envs) != 2 {
			t.Fatalf("got %d events, want 2 (empty text should be skipped)", len(envs))
		}
		if envs[0].Text != "has text" || envs[1].Text != "also text" {
			t.Fatalf("unexpected texts: %q, %q", envs[0].Text, envs[1].Text)
		}
	})

	// ── allowed_updates in request ──────────────────────────────────────
	t.Run("allowed_updates query parameter", func(t *testing.T) {
		var seenUpdates string
		r := setupPollTest(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			seenUpdates = r.URL.Query().Get("allowed_updates")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true,"result":[]}`))
		}), nil)

		// Set allowed_updates via config (the config is already set at runtime
		// creation; but the guest reads it from config projection. We need a
		// config with AllowedUpdates populated.)
		_ = r // The config was set in setupPollTest without AllowedUpdates
		// We handle this via a sub-test with a custom setup
		_ = seenUpdates
	})

	// ── HTTP error from server (non-JSON) ────────────────────────────────
	t.Run("server error", func(t *testing.T) {
		r := setupPollTest(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		}), nil)

		if err := r.Poll(); err != nil {
			t.Fatalf("Poll: %v", err)
		}
		if len(r.Outbound) != 0 {
			t.Fatalf("got %d outbound events, want 0 for server error", len(r.Outbound))
		}
		// Should have logged the error
		hasErr := false
		for _, l := range r.Log {
			if strings.Contains(l, "HTTP error") {
				hasErr = true
				break
			}
		}
		if !hasErr {
			t.Fatalf("expected 'HTTP error' in logs, got:\n%s", strings.Join(r.Log, "\n"))
		}
	})
}

// TestTelegramPollAllowedUpdates specifically tests that the allowed_updates
// query parameter is passed through to the Telegram API.
func TestTelegramPollAllowedUpdates(t *testing.T) {
	var seenUpdates string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenUpdates = r.URL.Query().Get("allowed_updates")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":[]}`))
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
			BotToken: "abc:def",
			Host:     srv.URL,
		},
		Polling: contract.PollingConfig{
			TimeoutSeconds: 5,
			AllowedUpdates: []string{"message", "channel_post"},
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

	if seenUpdates == "" {
		t.Fatal("allowed_updates query parameter not sent")
	}
	t.Logf("allowed_updates = %s", seenUpdates)
	// The parameter should be a URL-encoded JSON array like %5B%22message%22%2C%22channel_post%22%5D
	// We just verify it's non-empty and looks like an encoded array
	if !strings.Contains(seenUpdates, "message") {
		t.Fatalf("allowed_updates=%q does not contain 'message'", seenUpdates)
	}
}
