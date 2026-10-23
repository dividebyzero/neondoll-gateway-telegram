// SPDX-License-Identifier: AGPL-3.0-only
//
// contract/config_test.go — tests for the versioned gateway config
// contract: validation rules, guest projection redaction, and the
// Redact safety-net helper.

package contract

import (
	"strings"
	"testing"
)

const testToken = "1234567:AA-test-token-SECRET"

func validConfig() *GatewayConfig {
	return &GatewayConfig{
		Version: ConfigVersion,
		Telegram: TelegramConfig{
			BotToken: testToken,
		},
		Polling: PollingConfig{
			TimeoutSeconds: 25,
		},
	}
}

func TestGatewayConfigValidateValid(t *testing.T) {
	if err := validConfig().Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}

func TestGatewayConfigValidateVersionMismatch(t *testing.T) {
	cfg := validConfig()
	cfg.Version = ConfigVersion + 1

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected version mismatch error")
	}
	if !strings.Contains(err.Error(), "version") {
		t.Fatalf("error should mention version, got: %v", err)
	}
}

func TestGatewayConfigValidateMissingToken(t *testing.T) {
	cfg := validConfig()
	cfg.Telegram.BotToken = "   "

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected missing-token error")
	}
	if !strings.Contains(err.Error(), "bot token missing") {
		t.Fatalf("error should mention missing token, got: %v", err)
	}
}

func TestGatewayConfigValidateMalformedToken(t *testing.T) {
	cfg := validConfig()
	cfg.Telegram.BotToken = "not-a-real-token"

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected malformed-token error")
	}
	if !strings.Contains(err.Error(), "malformed") {
		t.Fatalf("error should mention malformed token, got: %v", err)
	}
}

func TestGatewayConfigValidatePollingTimeout(t *testing.T) {
	cfg := validConfig()
	cfg.Polling.TimeoutSeconds = MaxAllowedTimeout + 1

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("error should mention timeout, got: %v", err)
	}
}

// Secrets must never leak into validation error strings.
func TestGatewayConfigValidateErrorNoTokenLeak(t *testing.T) {
	cfg := validConfig()
	cfg.Version = ConfigVersion + 1

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), testToken) {
		t.Fatalf("validation error leaked token: %v", err)
	}
}

func TestGatewayConfigProjectionRedactsToken(t *testing.T) {
	proj := validConfig().Projection()

	if strings.Contains(proj, testToken) {
		t.Fatalf("projection contains token: %s", proj)
	}
	if !strings.Contains(proj, `"version":1`) {
		t.Fatalf("projection should include version, got: %s", proj)
	}
	if !strings.Contains(proj, `"timeout_seconds":25`) {
		t.Fatalf("projection should include polling settings, got: %s", proj)
	}
}

func TestGatewayConfigProjectionNil(t *testing.T) {
	var cfg *GatewayConfig
	if got := cfg.Projection(); got != "{}" {
		t.Fatalf("nil projection = %q, want {}", got)
	}
}

func TestGatewayConfigRedact(t *testing.T) {
	cfg := validConfig()
	input := "log line with " + testToken + " embedded twice: " + testToken

	got := cfg.Redact(input)
	if strings.Contains(got, testToken) {
		t.Fatalf("Redact left token in: %q", got)
	}
	if strings.Count(got, "[REDACTED]") != 2 {
		t.Fatalf("expected 2 redactions, got: %q", got)
	}
}

func TestGatewayConfigRedactUnconfigured(t *testing.T) {
	cfg := validConfig()
	cfg.Telegram.BotToken = ""

	input := "text without any secret"
	if got := cfg.Redact(input); got != input {
		t.Fatalf("unconfigured Redact changed input: %q", got)
	}
}

func TestGatewayConfigRedactNil(t *testing.T) {
	var cfg *GatewayConfig
	input := "no config means no redaction"
	if got := cfg.Redact(input); got != input {
		t.Fatalf("nil Redact changed input: %q", got)
	}
}
