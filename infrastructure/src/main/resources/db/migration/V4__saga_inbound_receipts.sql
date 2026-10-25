CREATE TABLE IF NOT EXISTS saga_inbound_receipts (
    event_id UUID PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    order_id BIGINT NOT NULL,
    payload_fingerprint VARCHAR(64) NOT NULL,
    received_at TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_saga_inbound_receipts_order_id
    ON saga_inbound_receipts (order_id);
