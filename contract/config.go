// SPDX-License-Identifier: AGPL-3.0-only
//
// contract/config.go — Versioned gateway configuration contract.
//
// ConfigVersion is bumped when fields or validation rules break backward
// compatibility.  The host owns credentials (BotToken); guest sees only a
// redacted projection serialised via json:"-".  The guest accesses the
// token through the mediated telegram_token host capability.
//
// GatewayConfig.Validate returns nil on success or an explicit error
// message.  Error strings NEVER contain secret values — only field names
// or expected ranges.  Use Redact(s) for defence-in-depth sanitisation
// when crossing the host→guest boundary (logs, outbound, errors).

package contract

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ConfigVersion identifies this contract revision.
// Bump when non-backward-compatible changes are introduced
// (version mismatch in Validate → explicit error).
const ConfigVersion = 1

// MaxAllowedTimeout is a practical upper bound for Telegram Bot API
// getUpdates long-polling timeout (max 50 s per Telegram docs).
const MaxAllowedTimeout uint32 = 50

// ═══════════════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════════════

type GatewayConfig struct {
	Version  int            `json:"version"`
	Telegram TelegramConfig `json:"telegram,omitempty"`
	Polling  PollingConfig  `json:"polling,omitempty"`
}

type TelegramConfig struct {
	// BotToken authenticates with the Telegram Bot API.
	// Host-owned secret (json:"-" → never serialised in projections).
	BotToken string `json:"-"`
	// Host is the Telegram Bot API base URL (scheme + host, no trailing slash).
	// Empty → defaults to "https://api.telegram.org".
	Host string `json:"host,omitempty"`
}

type PollingConfig struct {
	TimeoutSeconds uint32   `json:"timeout_seconds,omitempty"`
	AllowedUpdates []string `json:"allowed_updates,omitempty"`
}

// ═══════════════════════════════════════════════════════════════════════
// Validation
// ═══════════════════════════════════════════════════════════════════════

// Validate checks config version, token presence and basic shape, and
// polling limits.  Returns nil on success.  Error text never includes
// the BotToken value.
func (c *GatewayConfig) Validate() error {
	if c.Version != ConfigVersion {
		return fmt.Errorf("config: version %d unsupported (want %d)",
			c.Version, ConfigVersion)
	}

	tok := strings.TrimSpace(c.Telegram.BotToken)
	if tok == "" {
		return fmt.Errorf("config: telegram bot token missing")
	}
	if !strings.Contains(tok, ":") {
		return fmt.Errorf("config: telegram bot token malformed (missing colon)")
	}

	if c.Polling.TimeoutSeconds > MaxAllowedTimeout {
		return fmt.Errorf("config: polling timeout %ds exceeds max %ds",
			c.Polling.TimeoutSeconds, MaxAllowedTimeout)
	}

	return nil
}

// ═══════════════════════════════════════════════════════════════════════
// Redaction
// ═══════════════════════════════════════════════════════════════════════

// Redact replaces every occurrence of the configured BotToken in s with
// "[REDACTED]".  When called on nil or an unconfigured config (empty
// token) the original string is returned unchanged.  Use this for
// defence-in-depth sanitisation of logs, outbound messages, and error
// strings before they leave the host boundary.
func (c *GatewayConfig) Redact(s string) string {
	if c == nil || c.Telegram.BotToken == "" {
		return s
	}
	return strings.ReplaceAll(s, c.Telegram.BotToken, "[REDACTED]")
}

// ═══════════════════════════════════════════════════════════════════════
// Guest projection
// ═══════════════════════════════════════════════════════════════════════

// Projection returns a JSON-encoded config form that is safe to send to
// the WASM guest.  The BotToken is excluded from serialisation (json:"-")
// so the projection contains only version and polling settings.
//
// Call on nil → returns "{}".
func (c *GatewayConfig) Projection() string {
	if c == nil {
		return "{}"
	}
	b, err := json.Marshal(c)
	if err != nil {
		// Struct contains only marshalable, omitempty-tagged fields;
		// treat an encode failure as a programming error.
		return "{}"
	}
	return string(b)
}
