// Package contract defines shared types for the Gateway update flow.
package contract

// Telegram API response structures (used by mock server and guests).
// Only the fields the gateway consumes are included.

// UpdatesResponse is the top-level Telegram Bot API getUpdates response.
type UpdatesResponse struct {
	OK     bool            `json:"ok"`
	Result []TelegramUpdate `json:"result"`
}

// TelegramUpdate is a single update from the Telegram Bot API.
type TelegramUpdate struct {
	UpdateID int64        `json:"update_id"`
	Message  *TelegramMessage `json:"message,omitempty"`
}

// TelegramMessage is a message within a Telegram update.
type TelegramMessage struct {
	MessageID int64        `json:"message_id"`
	From      *TelegramUser   `json:"from,omitempty"`
	Chat      TelegramChat    `json:"chat"`
	Date      int64         `json:"date"`
	Text      string        `json:"text,omitempty"`
}

// TelegramUser represents a Telegram user.
type TelegramUser struct {
	ID        int64  `json:"id"`
	IsBot      bool   `json:"is_bot"`
	FirstName  string `json:"first_name"`
	LastName   string `json:"last_name,omitempty"`
	LanguageCode string `json:"language_code,omitempty"`
}

// TelegramChat represents a Telegram chat.
type TelegramChat struct {
	ID        int64  `json:"id"`
	FirstName string `json:"first_name,omitempty"`
	Type      string `json:"type"`
}

// GatewayEventEnvelope is the versioned event emitted by the gateway
// for each inbound Telegram message.  This is the contract between the
// gateway guest and whatever Core ingresses later.
//
// Fields should remain stable across gateway versions.
type GatewayEventEnvelope struct {
	// EventVersion is the event schema version.
	EventVersion int `json:"event_version"`

	// EventType identifies the kind of event.
	EventType string `json:"event_type"`

	// Source identifies the gateway that produced this event.
	Source string `json:"source"`

	// UpdateID from Telegram — used for idempotent replay.
	UpdateID int64 `json:"update_id"`

	// MessageID from Telegram.
	MessageID int64 `json:"message_id"`

	// ChatID is the Telegram chat identifier.
	ChatID int64 `json:"chat_id"`

	// ChatType is "private", "group", "supergroup", or "channel".
	ChatType string `json:"chat_type"`

	// SenderID is the Telegram user id of the message sender.
	SenderID int64 `json:"sender_id,omitempty"`

	// SenderName is the first name of the message sender.
	SenderName string `json:"sender_name,omitempty"`

	// Text is the message text content.
	Text string `json:"text"`

	// ReceivedAt is the Unix timestamp from the Telegram message.
	ReceivedAt int64 `json:"received_at"`
}