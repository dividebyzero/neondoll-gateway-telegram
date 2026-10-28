package corecompat

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Neon-Dolls/neondoll/Core/Extension"
	"github.com/Neon-Dolls/neondoll/Core/ExternalIdentity"
	"github.com/Neon-Dolls/neondoll/Core/Gateway"
	"github.com/Neon-Dolls/neondoll/Core/PermissionRequest"
	"github.com/Neon-Dolls/neondoll/Core/Session"
	"github.com/Neon-Dolls/neondoll/Core/SessionEvent"
	"github.com/Neon-Dolls/neondoll/DollLink/Events"
)

type ingress struct {
	mu       sync.Mutex
	failures int
	attempts int
	accepted int
}

func (*ingress) ResolvePlatformSession(context.Context, string) (session.ID, error) {
	return "session-1", nil
}

func (*ingress) ExternalConversationRef(context.Context, session.ID) (string, error) {
	return "telegram/chat/7", nil
}

func (i *ingress) HandleLegacy(_ context.Context, _ session.ID, _ gateway.VerifiedSender, _ *events.Event) (*events.Event, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.attempts++
	if i.failures > 0 {
		i.failures--
		return nil, errors.New("transient ingress rejection")
	}
	i.accepted++
	return nil, nil
}

func (i *ingress) HandlePlatformMessage(ctx context.Context, _ string, sender gateway.VerifiedSender, event *events.Event) (*gateway.PlatformResponse, error) {
	_, err := i.HandleLegacy(ctx, "session-1", sender, event)
	return &gateway.PlatformResponse{SessionID: "session-1"}, err
}

func (*ingress) HandleSessionEvent(context.Context, gateway.VerifiedSender, sessionevent.Event) (*permissionrequest.Request, error) {
	return nil, nil
}

type gatewayHost struct {
	mu      sync.Mutex
	ingress *ingress
	adapter gateway.Adapter
}

func (h *gatewayHost) Register(ctx context.Context, adapter gateway.Adapter) error {
	if err := adapter.Start(ctx, h.ingress); err != nil {
		return err
	}
	h.mu.Lock()
	h.adapter = adapter
	h.mu.Unlock()
	return nil
}

func (h *gatewayHost) Unregister(ctx context.Context, _ externalidentity.GatewayInstanceID) error {
	h.mu.Lock()
	adapter := h.adapter
	h.adapter = nil
	h.mu.Unlock()
	if adapter == nil {
		return nil
	}
	return adapter.Stop(ctx)
}

func TestMergedCoreExtensionManagerWithMockTelegram(t *testing.T) {
	artifactPath := os.Getenv("TELEGRAM_WASM")
	if artifactPath == "" {
		t.Fatal("TELEGRAM_WASM is required")
	}
	artifact, err := os.ReadFile(artifactPath)
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var paths []string
	sendCount := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		mu.Lock()
		paths = append(paths, request.URL.RequestURI())
		if strings.Contains(request.URL.Path, "/sendMessage") {
			sendCount++
			mu.Unlock()
			_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":1}}`))
			return
		}
		offset := request.URL.Query().Get("offset")
		mu.Unlock()
		if offset == "" || offset == "0" {
			_, _ = w.Write([]byte(`{"ok":true,"result":[{"update_id":42,"message":{"message_id":1,"from":{"id":9},"chat":{"id":7,"type":"private"},"date":1,"text":"hello"}}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"ok":true,"result":[]}`))
	}))
	defer server.Close()
	originalTransport := http.DefaultTransport
	http.DefaultTransport = server.Client().Transport
	defer func() { http.DefaultTransport = originalTransport }()

	parsed, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	dataRoot, secretsRoot := t.TempDir(), t.TempDir()
	if err = os.WriteFile(filepath.Join(dataRoot, "telegram.wasm"), artifact, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(secretsRoot, "token"), []byte("123:test"), 0600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(artifact)
	configuration, _ := json.Marshal(map[string]any{
		"version":  1,
		"telegram": map[string]any{"host": server.URL},
		"polling":  map[string]any{"timeout_seconds": 1, "allowed_updates": []string{"message"}},
	})
	manifest := extension.Manifest{
		Version: 1, ExtensionID: "telegram", InstanceID: "telegram-integration",
		Contract: extension.ContractGateway, ABI: 2, Enabled: true,
		Artifact:      extension.Artifact{Path: "telegram.wasm", SHA256: fmt.Sprintf("%x", sum)},
		Configuration: configuration,
		Requested:     []extension.Capability{extension.CapabilityHTTP, extension.CapabilitySecrets, extension.CapabilityState, extension.CapabilityLog},
		Granted:       []extension.Capability{extension.CapabilityHTTP, extension.CapabilitySecrets, extension.CapabilityState, extension.CapabilityLog},
		HTTPAllowlist: []string{parsed.Hostname()},
		Secrets:       map[string]string{"telegram_token": "token"},
		Gateway:       &extension.GatewayConfiguration{Platform: "telegram", DollID: "doll", ConversationPrefix: "telegram/chat/"},
	}
	ctx := context.Background()
	runtime, err := extension.NewWASMRuntime(ctx, extension.NewFileStateStore(filepath.Join(dataRoot, "state")), secretsRoot)
	if err != nil {
		t.Fatal(err)
	}
	coreIngress := &ingress{failures: 1}
	host := &gatewayHost{ingress: coreIngress}
	manager, err := extension.NewManager(runtime, host, dataRoot, []extension.Manifest{manifest})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Shutdown(ctx)
	if err = manager.LoadEnabled(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		coreIngress.mu.Lock()
		defer coreIngress.mu.Unlock()
		return coreIngress.attempts >= 2 && coreIngress.accepted == 1
	})
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, path := range paths {
			if strings.Contains(path, "offset=43") {
				return true
			}
		}
		return false
	})

	event, err := sessionevent.NewEvent("session-1", sessionevent.TypeMessage, sessionevent.MessagePayload{Parts: []sessionevent.MessagePart{{Type: sessionevent.PartText, Text: "reply"}}}, sessionevent.Correlation{}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	delivery := sessionevent.Delivery{ID: "delivery-1", Event: event, Owner: session.Owner{Kind: session.OwnerGateway, InstanceID: "telegram-integration"}}
	envelope, err := gateway.NewOutboundEnvelope(delivery)
	if err != nil {
		t.Fatal(err)
	}
	host.mu.Lock()
	adapter := host.adapter
	host.mu.Unlock()
	if err = adapter.Deliver(ctx, envelope); err != nil {
		t.Fatal(err)
	}
	if err = adapter.Deliver(ctx, envelope); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	sends := sendCount
	mu.Unlock()
	if sends != 1 {
		t.Fatalf("Telegram sendMessage count=%d", sends)
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
