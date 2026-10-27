// Package adapter connects the Telegram Wazero runtime to Core's managed
// Gateway Host. Core owns identity, Sessions, authority, and delivery state;
// the WASM guest owns Telegram polling and rendering.
package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Neon-Dolls/neondoll/Core/ExternalIdentity"
	"github.com/Neon-Dolls/neondoll/Core/Gateway"
	"github.com/Neon-Dolls/neondoll/Core/PermissionRequest"
	"github.com/Neon-Dolls/neondoll/Core/SessionEvent"
	"github.com/Neon-Dolls/neondoll/DollLink/Events"
	"github.com/dividebyzero/neondoll-gateway-telegram/contract"
	"github.com/dividebyzero/neondoll-gateway-telegram/host"
)

const (
	Platform           = "telegram"
	conversationPrefix = "telegram/chat/"
	defaultPollDelay   = 100 * time.Millisecond
)

type Runtime interface {
	Start([]byte) error
	ABIVersion() (int32, error)
	Poll() error
	HandleEvent(contract.EnvelopeVersion, string, string) error
	TakeOutbound() []contract.GatewayMessage
	Shutdown() error
}

type Config struct {
	InstanceID externalidentity.GatewayInstanceID
	DollID     string
	WASM       []byte
	PollDelay  time.Duration
	Runtime    Runtime
}

type Adapter struct {
	config Config

	mu         sync.Mutex
	ingress    gateway.Ingress
	cancel     context.CancelFunc
	done       chan struct{}
	started    bool
	stopped    bool
	opMu       sync.Mutex
	deliveryMu sync.Mutex
	deliveries map[string]*deliveryAttempt
}

type deliveryAttempt struct {
	done chan struct{}
	err  error
}

func New(config Config) (*Adapter, error) {
	if strings.TrimSpace(string(config.InstanceID)) == "" || strings.TrimSpace(config.DollID) == "" || len(config.WASM) == 0 || config.Runtime == nil {
		return nil, gateway.ErrInvalidAdapter
	}
	if config.PollDelay <= 0 {
		config.PollDelay = defaultPollDelay
	}
	return &Adapter{config: config, deliveries: make(map[string]*deliveryAttempt)}, nil
}

func (a *Adapter) InstanceID() externalidentity.GatewayInstanceID { return a.config.InstanceID }
func (*Adapter) SessionEventVersion() int                         { return gateway.StructuredSessionEventVersion }

func (a *Adapter) Start(ctx context.Context, ingress gateway.Ingress) error {
	if ingress == nil {
		return gateway.ErrInvalidAdapter
	}
	a.mu.Lock()
	if a.started || a.stopped {
		a.mu.Unlock()
		return host.ErrAlreadyStarted
	}
	a.started = true
	a.ingress = ingress
	a.mu.Unlock()
	if err := a.config.Runtime.Start(a.config.WASM); err != nil {
		return err
	}
	version, err := a.config.Runtime.ABIVersion()
	if err != nil || version != 2 {
		_ = a.config.Runtime.Shutdown()
		if err != nil {
			return err
		}
		return fmt.Errorf("telegram adapter: unsupported guest ABI %d", version)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	a.mu.Lock()
	a.cancel, a.done = cancel, make(chan struct{})
	a.mu.Unlock()
	go a.poll(runCtx)
	return nil
}

func (a *Adapter) poll(ctx context.Context) {
	defer close(a.done)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		a.opMu.Lock()
		err := a.config.Runtime.Poll()
		messages := a.config.Runtime.TakeOutbound()
		a.opMu.Unlock()
		if err != nil && !errors.Is(err, host.ErrClosed) {
			if !wait(ctx, a.config.PollDelay) {
				return
			}
			continue
		}
		for _, message := range messages {
			_ = a.handleOutgoing(ctx, message)
		}
		if !wait(ctx, a.config.PollDelay) {
			return
		}
	}
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (a *Adapter) handleOutgoing(ctx context.Context, message contract.GatewayMessage) error {
	var incoming contract.GatewayEventEnvelope
	if message.Type != "outgoing" || json.Unmarshal(message.Payload, &incoming) != nil || incoming.EventType != "message.incoming" || incoming.Source != Platform {
		return nil
	}
	conversation := conversationPrefix + strconv.FormatInt(incoming.ChatID, 10)
	sender := gateway.VerifiedSender{Platform: Platform, SubjectID: strconv.FormatInt(incoming.SenderID, 10)}
	if decision, requestID, ok := parseDecision(incoming.Text); ok {
		route, err := a.ingress.ResolvePlatformSession(ctx, conversation)
		if err != nil {
			return err
		}
		event, err := sessionevent.NewEvent(route.SessionID, sessionevent.TypePermissionDecided,
			sessionevent.PermissionDecidedPayload{RequestID: requestID, Decision: decision},
			sessionevent.Correlation{RequestID: requestID}, time.Now())
		if err != nil {
			return err
		}
		_, err = a.ingress.HandleSessionEvent(ctx, sender, event)
		return err
	}
	event := events.NewDollMessage("telegram:"+strconv.FormatInt(incoming.UpdateID, 10), a.config.DollID, incoming.Text)
	response, err := a.ingress.HandlePlatformMessage(ctx, conversation, sender, &event)
	if err != nil {
		return err
	}
	if response != nil && response.Event != nil {
		if payload, ok := response.Event.Payload.(events.MessagePayload); ok && strings.TrimSpace(payload.Text) != "" {
			return a.deliverText(ctx, incoming.ChatID, "", payload.Text)
		}
	}
	return nil
}

func parseDecision(text string) (permissionrequest.Decision, string, bool) {
	fields := strings.Fields(strings.TrimSpace(text))
	if len(fields) != 2 {
		return "", "", false
	}
	switch strings.ToLower(fields[0]) {
	case "/approve":
		return permissionrequest.DecisionApprove, fields[1], true
	case "/deny":
		return permissionrequest.DecisionDeny, fields[1], true
	default:
		return "", "", false
	}
}

func (a *Adapter) Deliver(ctx context.Context, envelope gateway.OutboundEnvelope) error {
	delivery, err := envelope.DecodeDelivery()
	if err != nil {
		return err
	}
	a.deliveryMu.Lock()
	if prior := a.deliveries[delivery.ID]; prior != nil {
		a.deliveryMu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-prior.done:
			return prior.err
		}
	}
	attempt := &deliveryAttempt{done: make(chan struct{})}
	a.deliveries[delivery.ID] = attempt
	a.deliveryMu.Unlock()
	err = a.deliver(ctx, envelope, delivery)
	a.deliveryMu.Lock()
	attempt.err = err
	close(attempt.done)
	if err != nil {
		delete(a.deliveries, delivery.ID)
	}
	a.deliveryMu.Unlock()
	return err
}

func (a *Adapter) deliver(ctx context.Context, envelope gateway.OutboundEnvelope, delivery sessionevent.Delivery) error {
	route, err := a.ingress.SessionRoute(ctx, delivery.Event.SessionID)
	if err != nil {
		return err
	}
	chatID, err := parseConversation(route.ExternalConversationRef)
	if err != nil {
		return sessionevent.TerminalDeliveryError(err)
	}
	return a.deliverEvent(ctx, chatID, delivery.ID, string(envelope.Payload))
}

func (a *Adapter) deliverText(ctx context.Context, chatID int64, deliveryID, text string) error {
	payload, err := json.Marshal(map[string]string{"text": text})
	if err != nil {
		return err
	}
	return a.deliverEvent(ctx, chatID, deliveryID, string(payload))
}

func (a *Adapter) deliverEvent(ctx context.Context, chatID int64, deliveryID, payload string) error {
	route, _ := json.Marshal(map[string]any{"chat_id": chatID, "delivery_id": deliveryID})
	a.opMu.Lock()
	defer a.opMu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if err := a.config.Runtime.HandleEvent(contract.Version0, "session.route", string(route)); err != nil {
		return err
	}
	if err := a.config.Runtime.HandleEvent(contract.Version0, "session.event", payload); err != nil {
		return err
	}
	// The guest performs sendMessage synchronously and reports the outcome so a
	// transport failure leaves Core's durable delivery pending for replay.
	for _, message := range a.config.Runtime.TakeOutbound() {
		var result struct {
			EventType string `json:"event_type"`
			OK        bool   `json:"ok"`
			Error     string `json:"error"`
		}
		if message.Type == "outgoing" && json.Unmarshal(message.Payload, &result) == nil && result.EventType == "delivery.result" {
			if result.OK {
				return nil
			}
			return errors.New("telegram adapter: " + result.Error)
		}
	}
	return errors.New("telegram adapter: guest did not acknowledge delivery")
}

func parseConversation(reference string) (int64, error) {
	if !strings.HasPrefix(reference, conversationPrefix) {
		return 0, errors.New("telegram adapter: invalid conversation reference")
	}
	chatID, err := strconv.ParseInt(strings.TrimPrefix(reference, conversationPrefix), 10, 64)
	if err != nil || chatID == 0 {
		return 0, errors.New("telegram adapter: invalid conversation reference")
	}
	return chatID, nil
}

func (a *Adapter) Stop(context.Context) error {
	a.mu.Lock()
	if a.stopped {
		a.mu.Unlock()
		return nil
	}
	a.stopped = true
	cancel, done := a.cancel, a.done
	a.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	err := a.config.Runtime.Shutdown()
	if done != nil {
		<-done
	}
	return err
}

var _ gateway.Adapter = (*Adapter)(nil)
