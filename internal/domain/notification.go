// Package domain holds the core types of the notification system and the
// business rules that do not depend on any infrastructure.
package domain

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Channel is a delivery medium.
type Channel string

const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

// Channels lists every supported channel. Workers run one dispatcher per entry.
var Channels = []Channel{ChannelSMS, ChannelEmail, ChannelPush}

// ParseChannel converts user input into a Channel.
func ParseChannel(s string) (Channel, error) {
	switch c := Channel(strings.ToLower(strings.TrimSpace(s))); c {
	case ChannelSMS, ChannelEmail, ChannelPush:
		return c, nil
	default:
		return "", fmt.Errorf("unknown channel %q (expected sms, email or push)", s)
	}
}

// Priority orders work inside a channel queue. Lower numeric value = served first,
// so the dispatch index can be scanned in natural order.
type Priority int16

const (
	PriorityHigh   Priority = 1
	PriorityNormal Priority = 2
	PriorityLow    Priority = 3
)

// ParsePriority converts user input into a Priority. Empty input means normal.
func ParsePriority(s string) (Priority, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "high":
		return PriorityHigh, nil
	case "", "normal":
		return PriorityNormal, nil
	case "low":
		return PriorityLow, nil
	default:
		return 0, fmt.Errorf("unknown priority %q (expected high, normal or low)", s)
	}
}

func (p Priority) String() string {
	switch p {
	case PriorityHigh:
		return "high"
	case PriorityNormal:
		return "normal"
	case PriorityLow:
		return "low"
	default:
		return fmt.Sprintf("priority(%d)", int16(p))
	}
}

// Status is the lifecycle state of a notification.
//
//	pending ──claim──▶ processing ──2xx──▶ sent
//	scheduled ─(due)─▶ processing ──retryable error──▶ retrying ──(due)──▶ processing
//	                   processing ──permanent error / attempts exhausted──▶ failed
//	pending | scheduled | retrying ──cancel──▶ cancelled
//	failed ──manual retry──▶ pending
type Status string

const (
	StatusPending    Status = "pending"
	StatusScheduled  Status = "scheduled"
	StatusProcessing Status = "processing"
	StatusRetrying   Status = "retrying"
	StatusSent       Status = "sent"
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
)

// Statuses lists every status, in lifecycle order.
var Statuses = []Status{
	StatusPending, StatusScheduled, StatusProcessing, StatusRetrying,
	StatusSent, StatusFailed, StatusCancelled,
}

// QueuedStatuses are the states a dispatcher may pick work from.
var QueuedStatuses = []Status{StatusPending, StatusScheduled, StatusRetrying}

// ParseStatus converts user input into a Status.
func ParseStatus(s string) (Status, error) {
	st := Status(strings.ToLower(strings.TrimSpace(s)))
	for _, known := range Statuses {
		if st == known {
			return st, nil
		}
	}
	return "", fmt.Errorf("unknown status %q", s)
}

// Cancellable reports whether a notification in this state can still be cancelled.
// Once a worker has claimed it (processing) the send may already be on the wire.
func (s Status) Cancellable() bool {
	return s == StatusPending || s == StatusScheduled || s == StatusRetrying
}

// Notification is a single message to a single recipient on a single channel.
type Notification struct {
	ID                uuid.UUID
	BatchID           *uuid.UUID
	IdempotencyKey    *string
	CorrelationID     string
	TraceParent       string
	Channel           Channel
	Recipient         string
	Content           string
	Priority          Priority
	Status            Status
	Attempts          int
	TemplateID        *uuid.UUID
	ScheduledAt       *time.Time
	NextAttemptAt     time.Time
	ProviderMessageID *string
	LastError         *string
	CreatedAt         time.Time
	UpdatedAt         time.Time
	SentAt            *time.Time
	FailedAt          *time.Time
	CancelledAt       *time.Time
}

// SamePayload reports whether two notifications describe the same message.
// It is used to detect an idempotency key being reused for a different request.
func (n *Notification) SamePayload(o *Notification) bool {
	return n.Channel == o.Channel && n.Recipient == o.Recipient &&
		n.Content == o.Content && n.Priority == o.Priority
}

// Batch groups notifications created by one batch request.
type Batch struct {
	ID           uuid.UUID
	Total        int
	CreatedAt    time.Time
	StatusCounts map[Status]int
}

// AttemptOutcome classifies one call to the provider.
type AttemptOutcome string

const (
	OutcomeSuccess        AttemptOutcome = "success"
	OutcomeRetryableError AttemptOutcome = "retryable_error"
	OutcomePermanentError AttemptOutcome = "permanent_error"
)

// DeliveryAttempt is the audit record of one provider call.
type DeliveryAttempt struct {
	ID             int64
	NotificationID uuid.UUID
	Attempt        int
	Outcome        AttemptOutcome
	StatusCode     *int
	Error          *string
	Duration       time.Duration
	CreatedAt      time.Time
}

// AttemptResult is what a worker records after calling the provider: the new
// state of the notification plus the audit data of the attempt.
type AttemptResult struct {
	Status            Status // sent, retrying or failed
	ProviderMessageID *string
	Error             *string
	NextAttemptAt     *time.Time // only for retrying
	Outcome           AttemptOutcome
	StatusCode        *int
	Duration          time.Duration
}
