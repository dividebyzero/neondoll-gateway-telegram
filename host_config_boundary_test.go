package host_test

import (
	"strings"
	"testing"

	"github.com/dividebyzero/neondoll-gateway-telegram/contract"
	"github.com/dividebyzero/neondoll-gateway-telegram/host"
)

const boundaryToken = "1234567:AA-boundary-test-token-SECRET"

func boundaryConfig() contract.GatewayConfig {
	return contract.GatewayConfig{
		Version: contract.ConfigVersion,
		Telegram: contract.TelegramConfig{
			BotToken: boundaryToken,
		},
		Polling: contract.PollingConfig{
			TimeoutSeconds: 25,
			AllowedUpdates: []string{"message"},
		},
	}
}

// TestConfigBoundaryProjection verifies that with a config the guest
// receives a redacted projection and the mediated credential capability
// is ready, and that the raw token never appears in host logs.
func TestConfigBoundaryProjection(t *testing.T) {
	rt := startRuntime(t, nil, host.WithConfig(boundaryConfig()))
	defer rt.Shutdown()

	if err := rt.HandleEvent(contract.Version0, "echo_token", ""); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}

	allLog := strings.Join(rt.Log, "\n")
	if !strings.Contains(allLog, "gateway_init: config projection ok") {
		t.Fatalf("want config projection fetched, logs:\n%s", allLog)
	}
	if !strings.Contains(allLog, "telegram token capability ready") {
		t.Fatalf("want telegram token capability ready, logs:\n%s", allLog)
	}
	if strings.Contains(allLog, boundaryToken) {
		t.Fatalf("token leaked into host logs:\n%s", allLog)
	}
}

// TestConfigBoundaryRedactedOutbound verifies that a guest that echoes
// the host-owned credential verbatim is recorded with the secret
// replaced by the redaction marker, never raw.
func TestConfigBoundaryRedactedOutbound(t *testing.T) {
	rt := startRuntime(t, nil, host.WithConfig(boundaryConfig()))
	defer rt.Shutdown()

	if err := rt.HandleEvent(contract.Version0, "echo_token", ""); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}

	out := outboundPayloads(rt)
	if len(out) != 1 {
		t.Fatalf("want 1 outbound message, got %d: %v", len(out), out)
	}
	payload := out[0]
	if strings.Contains(payload, boundaryToken) {
		t.Fatalf("outbound leak: token present verbatim: %s", payload)
	}
	if !strings.Contains(payload, "[REDACTED]") {
		t.Fatalf("outbound not redacted: %s", payload)
	}
}

// TestConfigBoundaryUnconfigured verifies the runtime works without a
// config: no projection, no credential capability, and echo_token is
// declined by the guest.
func TestConfigBoundaryUnconfigured(t *testing.T) {
	rt := startRuntime(t, nil) // no WithConfig
	defer rt.Shutdown()

	allLog := strings.Join(rt.Log, "\n")
	if !strings.Contains(allLog, "no config projection") {
		t.Fatalf("want unconfigured projection notice, logs:\n%s", allLog)
	}
	if !strings.Contains(allLog, "no telegram token") {
		t.Fatalf("want unconfigured token notice, logs:\n%s", allLog)
	}

	if err := rt.HandleEvent(contract.Version0, "echo_token", ""); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}
	if n := len(outboundPayloads(rt)); n != 0 {
		t.Fatalf("want no outbound when unconfigured, got %d", n)
	}
	if !strings.Contains(strings.Join(rt.Log, "\n"), "echo_token with no token available") {
		t.Fatalf("want declined echo notice, logs:\n%s", allLog)
	}
}

// TestConfigBoundaryStartValidation verifies config validation happens
// at Start with explicit errors, and that failed Start leaves the
// runtime closed.
func TestConfigBoundaryStartValidation(t *testing.T) {
	cases := []struct {
		name string
		cfg  contract.GatewayConfig
		want string
	}{
		{"unsupported version", contract.GatewayConfig{Version: 999, Telegram: contract.TelegramConfig{BotToken: boundaryToken}}, "version"},
		{"missing token", contract.GatewayConfig{Version: contract.ConfigVersion}, "token"},
		{"invalid token", contract.GatewayConfig{Version: contract.ConfigVersion, Telegram: contract.TelegramConfig{BotToken: "not-a-real-token"}}, "colon"},
		{"invalid polling", contract.GatewayConfig{Version: contract.ConfigVersion, Telegram: contract.TelegramConfig{BotToken: boundaryToken}, Polling: contract.PollingConfig{TimeoutSeconds: contract.MaxAllowedTimeout + 1}}, "exceeds"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt := host.NewGatewayRuntime(nil, host.WithConfig(tc.cfg))
			err := rt.Start(loadWASM(t))
			if err == nil {
				t.Fatal("Start: want error, got nil")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Start error %q does not mention %q", err, tc.want)
			}
			// Failed Start must leave the runtime closed: a second
			// Start attempt must fail, and Shutdown stays idempotent.
			if err := rt.Start(loadWASM(t)); err == nil {
				t.Fatal("second Start: want error after failed Start, got nil")
			}
			if err := rt.Shutdown(); err != nil {
				t.Fatalf("Shutdown after failed Start: %v", err)
			}
		})
	}
}