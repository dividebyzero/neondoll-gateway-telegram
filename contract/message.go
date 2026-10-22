// SPDX-License-Identifier: AGPL-3.0-only
//
// gateway/message.go — Versioned Gateway Envelope
// Core 5 M2.1: minimal proof-of-concept message type.
// Will be replaced by a binary protocol in production.

package contract

// EnvelopeVersion identifies the Gateway message protocol revision.
type EnvelopeVersion uint32

const (
	// Version0 is the initial M2.1 proof-of-concept version.
	// Payload is a raw JSON byte slice with an implied schema:
	//   {"type":"<message_type>","payload":{...}}
	Version0 EnvelopeVersion = iota
)

// GatewayMessage is the versioned envelope exchanged between
// the host runtime and a Gateway module.
type GatewayMessage struct {
	Version EnvelopeVersion `json:"v"`
	Seq     uint64          `json:"seq,omitempty"`
	Source  string          `json:"src,omitempty"`
	Dest    string          `json:"dst,omitempty"`
	Type    string          `json:"type"`
	Payload []byte          `json:"payload,omitempty"`
}

// Message types used by the M2.1 feasibility proof.
const (
	MsgTypePing        = "ping"
	MsgTypePong        = "pong"
	MsgTypeLog         = "log"
	MsgTypeEvent       = "event"
	MsgTypeHTTPRequest = "http_request"
	MsgTypeError       = "error"
)
