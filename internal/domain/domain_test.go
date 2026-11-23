package domain

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseChannel(t *testing.T) {
	for _, in := range []string{"sms", "SMS", " email ", "push"} {
		_, err := ParseChannel(in)
		assert.NoError(t, err, in)
	}
	_, err := ParseChannel("fax")
	assert.Error(t, err)
}

func TestParsePriority(t *testing.T) {
	cases := map[string]Priority{"": PriorityNormal, "high": PriorityHigh, "NORMAL": PriorityNormal, "low": PriorityLow}
	for in, want := range cases {
		got, err := ParsePriority(in)
		require.NoError(t, err)
		assert.Equal(t, want, got, in)
	}
	_, err := ParsePriority("urgent")
	assert.Error(t, err)
	assert.True(t, PriorityHigh < PriorityNormal && PriorityNormal < PriorityLow, "lower value must dispatch first")
}

func TestStatusCancellable(t *testing.T) {
	cancellable := map[Status]bool{
		StatusPending: true, StatusScheduled: true, StatusRetrying: true,
		StatusProcessing: false, StatusSent: false, StatusFailed: false, StatusCancelled: false,
	}
	for st, want := range cancellable {
		assert.Equal(t, want, st.Cancellable(), st)
	}
}

func TestValidateRecipient(t *testing.T) {
	cases := []struct {
		channel   Channel
		recipient string
		ok        bool
	}{
		{ChannelSMS, "+905551234567", true},
		{ChannelSMS, "+14155552671", true},
		{ChannelSMS, "05551234567", false},       // no country code
		{ChannelSMS, "+0123456789", false},       // country code cannot start with 0
		{ChannelSMS, "+90 555 123", false},       // spaces
		{ChannelSMS, "+1234567890123456", false}, // > 15 digits
		{ChannelEmail, "jane@example.com", true},
		{ChannelEmail, "Jane <jane@example.com>", false}, // display names are not accepted
		{ChannelEmail, "not-an-email", false},
		{ChannelEmail, strings.Repeat("a", 250) + "@x.io", false},
		{ChannelPush, "fcm:APA91bHun4MxP5egoKMwt2KZFBaFUH-1RYqx", true},
		{ChannelPush, "short", false},
		{ChannelPush, "token with spaces in it!!", false},
		{ChannelSMS, "", false},
	}
	for _, c := range cases {
		err := ValidateRecipient(c.channel, c.recipient)
		if c.ok {
			assert.NoError(t, err, "%s %q", c.channel, c.recipient)
		} else {
			assert.Error(t, err, "%s %q", c.channel, c.recipient)
		}
	}
}

func TestValidateContent(t *testing.T) {
	assert.NoError(t, ValidateContent(ChannelSMS, "Your code is 1234"))
	assert.Error(t, ValidateContent(ChannelSMS, "   "))
	assert.Error(t, ValidateContent(ChannelSMS, "bad \xff utf8"))

	for _, ch := range Channels {
		limit := MaxContentLength(ch)
		assert.NoError(t, ValidateContent(ch, strings.Repeat("a", limit)), ch)
		assert.Error(t, ValidateContent(ch, strings.Repeat("a", limit+1)), ch)
	}
	// Limits count characters, not bytes: 1600 Turkish letters (2 bytes each) fit an SMS.
	assert.NoError(t, ValidateContent(ChannelSMS, strings.Repeat("ş", MaxSMSLength)))
}

func TestValidationErrorErr(t *testing.T) {
	ve := &ValidationError{}
	assert.NoError(t, ve.Err())
	ve.Add("recipient", "is %s", "required")
	require.Error(t, ve.Err())
	assert.Equal(t, "validation failed: recipient: is required", ve.Error())
}

func TestSamePayload(t *testing.T) {
	a := &Notification{Channel: ChannelSMS, Recipient: "+905551234567", Content: "hi", Priority: PriorityHigh}
	b := *a
	assert.True(t, a.SamePayload(&b))
	b.Content = "hello"
	assert.False(t, a.SamePayload(&b))
}
