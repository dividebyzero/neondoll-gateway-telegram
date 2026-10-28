// Package contract defines shared types for the Gateway update flow.
package contract

// Telegram API response structures (used by mock server and guests).
// Only the fields the gateway consumes are included.

// UpdatesResponse is the top-level Telegram Bot API getUpdates response.
type UpdatesResponse struct {
	OK     bool             `json:"ok"`
	Result []TelegramUpdate `json:"result"`
}

// TelegramUpdate is a single update from the Telegram Bot API.
type TelegramUpdate struct {
	UpdateID int64            `json:"update_id"`
	Message  *TelegramMessage `json:"message,omitempty"`
}

// TelegramMessage is a message within a Telegram update.
type TelegramMessage struct {
	MessageID int64           `json:"message_id"`
	From      *TelegramUser   `json:"from,omitempty"`
	Chat      TelegramChat    `json:"chat"`
	Date      int64           `json:"date"`
	Text      string          `json:"text,omitempty"`
	Caption   string          `json:"caption,omitempty"`
	Photo     []TelegramMedia `json:"photo,omitempty"`
	Document  *TelegramMedia  `json:"document,omitempty"`
	Audio     *TelegramMedia  `json:"audio,omitempty"`
	Voice     *TelegramMedia  `json:"voice,omitempty"`
	Video     *TelegramMedia  `json:"video,omitempty"`
}

// TelegramMedia contains references only. Binary content is retrieved later
// through a separately authorized capability, never embedded in the event.
type TelegramMedia struct {
	FileID       string `json:"file_id"`
	FileUniqueID string `json:"file_unique_id,omitempty"`
	FileName     string `json:"file_name,omitempty"`
	MIMEType     string `json:"mime_type,omitempty"`
	FileSize     int64  `json:"file_size,omitempty"`
}

// TelegramUser represents a Telegram user.
type TelegramUser struct {
	ID           int64  `json:"id"`
	IsBot        bool   `json:"is_bot"`
	FirstName    string `json:"first_name"`
	LastName     string `json:"last_name,omitempty"`
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

	// Media contains provider-scoped metadata references, never file bytes.
	Media []MediaReference `json:"media,omitempty"`

	// ReceivedAt is the Unix timestamp from the Telegram message.
	ReceivedAt int64 `json:"received_at"`
}

// MediaReference is the transport-neutral metadata boundary for future media
// retrieval. MediaID is Telegram's file_id for this Gateway.
type MediaReference struct {
	Type           string `json:"type"`
	MediaID        string `json:"media_id"`
	UniqueID       string `json:"unique_id,omitempty"`
	Filename       string `json:"filename,omitempty"`
	MIMEType       string `json:"mime_type,omitempty"`
	SizeBytes      int64  `json:"size_bytes,omitempty"`
	Caption        string `json:"caption,omitempty"`
	MessageID      string `json:"message_id,omitempty"`
	ConversationID string `json:"conversation_id,omitempty"`
}
