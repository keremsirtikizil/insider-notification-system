CREATE TABLE batches (
    id         UUID PRIMARY KEY,
    total      INTEGER     NOT NULL CHECK (total >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE notifications (
    id                  UUID PRIMARY KEY,
    batch_id            UUID REFERENCES batches (id) ON DELETE SET NULL,
    idempotency_key     TEXT,
    correlation_id      TEXT        NOT NULL DEFAULT '',
    trace_parent        TEXT        NOT NULL DEFAULT '',
    channel             TEXT        NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    recipient           TEXT        NOT NULL,
    content             TEXT        NOT NULL,
    -- 1 = high, 2 = normal, 3 = low: ascending order is dispatch order.
    priority            SMALLINT    NOT NULL CHECK (priority BETWEEN 1 AND 3),
    status              TEXT        NOT NULL CHECK (status IN
        ('pending', 'scheduled', 'processing', 'retrying', 'sent', 'failed', 'cancelled')),
    attempts            INTEGER     NOT NULL DEFAULT 0,
    scheduled_at        TIMESTAMPTZ,
    -- When the row becomes eligible for dispatch (creation, schedule time or retry backoff).
    next_attempt_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Lease of the worker that claimed the row; expired leases are recovered.
    locked_until        TIMESTAMPTZ,
    provider_message_id TEXT,
    last_error          TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at             TIMESTAMPTZ,
    failed_at           TIMESTAMPTZ,
    cancelled_at        TIMESTAMPTZ
);

-- Idempotency: one notification per client supplied key.
CREATE UNIQUE INDEX ux_notifications_idempotency_key
    ON notifications (idempotency_key) WHERE idempotency_key IS NOT NULL;

-- Dispatch queue: per channel, highest priority first, then oldest due time.
-- Partial so it only contains rows that still wait to be sent.
CREATE INDEX ix_notifications_dispatch
    ON notifications (channel, priority, next_attempt_at)
    WHERE status IN ('pending', 'scheduled', 'retrying');

-- Lease recovery scans only in-flight rows.
CREATE INDEX ix_notifications_processing_lease
    ON notifications (locked_until) WHERE status = 'processing';

-- Listing / keyset pagination.
CREATE INDEX ix_notifications_created ON notifications (created_at DESC, id DESC);
CREATE INDEX ix_notifications_status_created ON notifications (status, created_at DESC, id DESC);
CREATE INDEX ix_notifications_channel_created ON notifications (channel, created_at DESC, id DESC);
CREATE INDEX ix_notifications_batch ON notifications (batch_id) WHERE batch_id IS NOT NULL;

-- Windowed success / failure metrics.
CREATE INDEX ix_notifications_sent_at ON notifications (sent_at) WHERE sent_at IS NOT NULL;
CREATE INDEX ix_notifications_failed_at ON notifications (failed_at) WHERE failed_at IS NOT NULL;
