package adapter

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Neon-Dolls/neondoll/Core/Gateway"
	"github.com/Neon-Dolls/neondoll/Core/PermissionRequest"
	"github.com/Neon-Dolls/neondoll/Core/Session"
	"github.com/Neon-Dolls/neondoll/Core/SessionEvent"
	"github.com/Neon-Dolls/neondoll/DollLink/Events"
	"github.com/dividebyzero/neondoll-gateway-telegram/contract"
	"github.com/dividebyzero/neondoll-gateway-telegram/host"
)

type integrationIngress struct {
	mu        sync.Mutex
	messages  []gateway.VerifiedSender
	decisions []sessionevent.Event
}

func (*integrationIngress) ResolvePlatformSession(context.Context, string) (*gateway.SessionRoute, error) {
	return &gateway.SessionRoute{SessionID: "session-telegram", ExternalConversationRef: "telegram/chat/9001"}, nil
}
func (*integrationIngress) SessionRoute(context.Context, session.ID) (*gateway.SessionRoute, error) {
	return &gateway.SessionRoute{SessionID: "session-telegram", ExternalConversationRef: "telegram/chat/9001"}, nil
}
func (i *integrationIngress) HandleLegacy(_ context.Context, _ session.ID, sender gateway.VerifiedSender, event *events.Event) (*events.Event, error) {
	i.mu.Lock()
	i.messages = append(i.messages, sender)
	i.mu.Unlock()
	response := events.NewResponse(event.ID, event.DollID, "core reply")
	return &response, nil
}
func (i *integrationIngress) HandlePlatformMessage(ctx context.Context, _ string, sender gateway.VerifiedSender, event *events.Event) (*gateway.PlatformResponse, error) {
	response, err := i.HandleLegacy(ctx, "session-telegram", sender, event)
	return &gateway.PlatformResponse{SessionID: "session-telegram", Event: response}, err
}
func (i *integrationIngress) HandleSessionEvent(_ context.Context, _ gateway.VerifiedSender, event sessionevent.Event) (*permissionrequest.Request, error) {
	i.mu.Lock()
	i.decisions = append(i.decisions, event)
	i.mu.Unlock()
	return &permissionrequest.Request{ID: "request-1", Status: permissionrequest.StatusApproved}, nil
}

func loadWASM(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "wasm", "build", "gateway.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestActualWazeroCoreAdapterRoundTripAndDeduplication(t *testing.T) {
	var mu sync.Mutex
	var sends []string
	polls := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case strings.HasSuffix(r.URL.Path, "/getUpdates"):
			polls++
			w.Header().Set("Content-Type", "application/json")
			if polls == 1 {
				fmt.Fprint(w, `{"ok":true,"result":[{"update_id":1,"message":{"message_id":2,"from":{"id":42,"first_name":"Owner"},"chat":{"id":9001,"type":"private"},"date":1700000000,"text":"hello"}}]}`)
			} else if polls == 2 {
				fmt.Fprint(w, `{"ok":true,"result":[{"update_id":2,"message":{"message_id":3,"from":{"id":42,"first_name":"Owner"},"chat":{"id":9001,"type":"private"},"date":1700000001,"text":"/approve request-1"}}]}`)
			} else {
				fmt.Fprint(w, `{"ok":true,"result":[]}`)
			}
		case strings.HasSuffix(r.URL.Path, "/sendMessage"):
			sends = append(sends, r.URL.Query().Get("text"))
			fmt.Fprint(w, `{"ok":true,"result":{"message_id":99}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	runtime := host.NewGatewayRuntime([]string{"127.0.0.1"}, host.WithHTTPTransport(server.Client().Transport), host.WithConfig(contract.GatewayConfig{
		Version: contract.ConfigVersion, Telegram: contract.TelegramConfig{BotToken: "123:test", Host: server.URL},
		Polling: contract.PollingConfig{TimeoutSeconds: 1, AllowedUpdates: []string{"message"}},
	}))
	adapter, err := New(Config{InstanceID: "telegram-main", DollID: "doll-1", WASM: loadWASM(t), PollDelay: time.Millisecond, Runtime: runtime})
	if err != nil {
		t.Fatal(err)
	}
	ingress := &integrationIngress{}
	if err := adapter.Start(context.Background(), ingress); err != nil {
		t.Fatal(err)
	}
	defer adapter.Stop(context.Background())

	deadline := time.Now().Add(3 * time.Second)
	for {
		ingress.mu.Lock()
		messages, decisions := len(ingress.messages), len(ingress.decisions)
		ingress.mu.Unlock()
		if messages == 1 && decisions == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("incoming messages=%d decisions=%d", messages, decisions)
		}
		time.Sleep(time.Millisecond)
	}
	ingress.mu.Lock()
	if ingress.messages[0].Platform != Platform || ingress.messages[0].SubjectID != "42" {
		t.Fatalf("sender = %#v", ingress.messages[0])
	}
	ingress.mu.Unlock()

	event, err := sessionevent.NewEvent("session-telegram", sessionevent.TypePermissionRequested,
		sessionevent.PermissionRequestedPayload{RequestID: "request-1", Question: "Proceed?", ExpiresAt: time.Now().Add(time.Hour)},
		sessionevent.Correlation{RequestID: "request-1"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	delivery := sessionevent.Delivery{ID: "delivery-1", Event: event, Owner: session.Owner{Kind: session.OwnerGateway, InstanceID: "telegram-main"}}
	envelope, err := gateway.NewOutboundEnvelope(delivery)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Deliver(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Deliver(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	gotSends := append([]string(nil), sends...)
	mu.Unlock()
	permissionCount := 0
	for _, text := range gotSends {
		if strings.Contains(text, "Proceed?") && strings.Contains(text, "/approve request-1") {
			permissionCount++
		}
	}
	if permissionCount != 1 {
		t.Fatalf("Telegram sends = %#v", gotSends)
	}
}

var _ gateway.Ingress = (*integrationIngress)(nil)
