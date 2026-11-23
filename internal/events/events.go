// Package events publishes notification status changes over Redis Pub/Sub so
// that every API instance can push them to its WebSocket subscribers, no matter
// which process (API or worker) made the change.
package events

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/keremsirtikizil/insider-notification-system/internal/domain"
)

// TypeStatusChanged is the only event type for now.
const TypeStatusChanged = "notification.status_changed"

// Event is a status change of one notification.
type Event struct {
	Type              string     `json:"type"`
	NotificationID    uuid.UUID  `json:"notification_id"`
	BatchID           *uuid.UUID `json:"batch_id,omitempty"`
	Channel           string     `json:"channel"`
	Status            string     `json:"status"`
	Attempts          int        `json:"attempts"`
	ProviderMessageID *string    `json:"provider_message_id,omitempty"`
	Error             *string    `json:"error,omitempty"`
	CorrelationID     string     `json:"correlation_id,omitempty"`
	OccurredAt        time.Time  `json:"occurred_at"`
}

// FromNotification builds the event for the current state of n.
func FromNotification(n *domain.Notification) Event {
	return Event{
		Type:              TypeStatusChanged,
		NotificationID:    n.ID,
		BatchID:           n.BatchID,
		Channel:           string(n.Channel),
		Status:            string(n.Status),
		Attempts:          n.Attempts,
		ProviderMessageID: n.ProviderMessageID,
		Error:             n.LastError,
		CorrelationID:     n.CorrelationID,
		OccurredAt:        n.UpdatedAt,
	}
}

// Publisher emits events. Publishing is best effort: the database stays the
// source of truth, so a lost event only delays what a WebSocket client sees.
type Publisher interface {
	Publish(ctx context.Context, events ...Event)
}

// Nop discards events.
type Nop struct{}

// Publish implements Publisher.
func (Nop) Publish(context.Context, ...Event) {}

// RedisBus publishes and subscribes on one Redis Pub/Sub channel.
type RedisBus struct {
	rdb     *redis.Client
	channel string
	logger  *slog.Logger
}

// NewRedisBus creates a bus on the given Pub/Sub channel name.
func NewRedisBus(rdb *redis.Client, channel string, logger *slog.Logger) *RedisBus {
	return &RedisBus{rdb: rdb, channel: channel, logger: logger}
}

// Publish sends all events in one pipelined round trip.
func (b *RedisBus) Publish(ctx context.Context, events ...Event) {
	if len(events) == 0 {
		return
	}
	pipe := b.rdb.Pipeline()
	for _, e := range events {
		payload, err := json.Marshal(e)
		if err != nil {
			continue
		}
		pipe.Publish(ctx, b.channel, payload)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		b.logger.WarnContext(ctx, "publish status events failed", "error", err, "count", len(events))
	}
}

// Subscribe streams events until ctx is cancelled. It returns once Redis has
// confirmed the subscription; go-redis re-subscribes after network errors.
func (b *RedisBus) Subscribe(ctx context.Context) (<-chan Event, error) {
	sub := b.rdb.Subscribe(ctx, b.channel)
	if _, err := sub.Receive(ctx); err != nil {
		_ = sub.Close()
		return nil, err
	}
	out := make(chan Event, 1024)
	go func() {
		defer close(out)
		defer sub.Close()
		msgs := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				var e Event
				if err := json.Unmarshal([]byte(msg.Payload), &e); err != nil {
					b.logger.WarnContext(ctx, "drop malformed event", "error", err)
					continue
				}
				select {
				case out <- e:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}
