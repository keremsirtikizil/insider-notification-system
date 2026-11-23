CREATE TABLE templates (
    id         UUID PRIMARY KEY,
    name       TEXT        NOT NULL UNIQUE,
    channel    TEXT        NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    content    TEXT        NOT NULL,
    variables  TEXT[]      NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE notifications
    ADD COLUMN template_id UUID REFERENCES templates (id) ON DELETE SET NULL;
