-- Audit trail of every provider call. Feeds the attempts endpoint and the
-- provider latency / error-rate metrics.
CREATE TABLE delivery_attempts (
    id              BIGSERIAL PRIMARY KEY,
    notification_id UUID        NOT NULL REFERENCES notifications (id) ON DELETE CASCADE,
    channel         TEXT        NOT NULL,
    attempt         INTEGER     NOT NULL,
    outcome         TEXT        NOT NULL CHECK (outcome IN ('success', 'retryable_error', 'permanent_error')),
    status_code     INTEGER,
    error           TEXT,
    duration_ms     INTEGER     NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ix_delivery_attempts_notification ON delivery_attempts (notification_id, id);
CREATE INDEX ix_delivery_attempts_created ON delivery_attempts (created_at);
