package domain

import (
	"fmt"
	"net/mail"
	"regexp"
	"strings"
	"unicode/utf8"
)

// Content limits per channel, counted in characters (runes), not bytes.
const (
	// MaxSMSLength: 10 concatenated segments, the cap most SMS providers enforce.
	MaxSMSLength = 1600
	// MaxEmailLength keeps a 1000-item batch request within a sane body size.
	MaxEmailLength = 20000
	// MaxPushLength keeps the provider payload well under the 4 KB APNs/FCM limit.
	MaxPushLength = 1000

	MaxEmailAddressLength   = 254
	MaxIdempotencyKeyLength = 255
)

var (
	// E.164: '+', country code (no leading zero), up to 15 digits in total.
	e164Re = regexp.MustCompile(`^\+[1-9][0-9]{7,14}$`)
	// APNs tokens are hex, FCM tokens use [A-Za-z0-9_-:]. Both fit this shape.
	pushTokenRe = regexp.MustCompile(`^[A-Za-z0-9_\-:.]{16,512}$`)
)

// MaxContentLength returns the character limit for a channel.
func MaxContentLength(c Channel) int {
	switch c {
	case ChannelSMS:
		return MaxSMSLength
	case ChannelEmail:
		return MaxEmailLength
	case ChannelPush:
		return MaxPushLength
	default:
		return 0
	}
}

// FieldError describes one invalid input field.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ValidationError is returned when user input breaks business rules.
type ValidationError struct {
	Fields []FieldError
}

func (e *ValidationError) Error() string {
	parts := make([]string, 0, len(e.Fields))
	for _, f := range e.Fields {
		parts = append(parts, f.Field+": "+f.Message)
	}
	return "validation failed: " + strings.Join(parts, "; ")
}

// Add records a field error.
func (e *ValidationError) Add(field, format string, args ...any) {
	e.Fields = append(e.Fields, FieldError{Field: field, Message: fmt.Sprintf(format, args...)})
}

// Err returns nil when no field error was recorded, so callers can write `return v.Err()`.
func (e *ValidationError) Err() error {
	if len(e.Fields) == 0 {
		return nil
	}
	return e
}

// ValidateRecipient checks the recipient format for the channel.
func ValidateRecipient(c Channel, recipient string) error {
	if recipient == "" {
		return fmt.Errorf("is required")
	}
	switch c {
	case ChannelSMS:
		if !e164Re.MatchString(recipient) {
			return fmt.Errorf("must be an E.164 phone number such as +905551234567")
		}
	case ChannelEmail:
		if len(recipient) > MaxEmailAddressLength {
			return fmt.Errorf("must be at most %d characters", MaxEmailAddressLength)
		}
		addr, err := mail.ParseAddress(recipient)
		if err != nil || addr.Address != recipient {
			return fmt.Errorf("must be a plain email address such as jane@example.com")
		}
	case ChannelPush:
		if !pushTokenRe.MatchString(recipient) {
			return fmt.Errorf("must be a device token of 16-512 characters [A-Za-z0-9_-:.]")
		}
	}
	return nil
}

// ValidateContent checks the message body for the channel.
func ValidateContent(c Channel, content string) error {
	if strings.TrimSpace(content) == "" {
		return fmt.Errorf("is required")
	}
	if !utf8.ValidString(content) {
		return fmt.Errorf("must be valid UTF-8")
	}
	if limit := MaxContentLength(c); utf8.RuneCountInString(content) > limit {
		return fmt.Errorf("must be at most %d characters for %s", limit, c)
	}
	return nil
}
