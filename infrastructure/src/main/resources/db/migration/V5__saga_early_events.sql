CREATE TABLE IF NOT EXISTS saga_early_events (
    event_id UUID PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    order_id BIGINT NOT NULL,
    payload TEXT NOT NULL,
    payload_fingerprint VARCHAR(64) NOT NULL,
    received_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_saga_early_events_order_received
    ON saga_early_events (order_id, received_at);
