CREATE TABLE IF NOT EXISTS dispatch_rounds (
    dispatch_round_id UUID PRIMARY KEY,
    h3_zone VARCHAR(32) NOT NULL,
    state VARCHAR(24) NOT NULL,
    opened_at TIMESTAMP NOT NULL,
    cutoff_at TIMESTAMP NOT NULL,
    closed_at TIMESTAMP NULL,
    order_count INTEGER NOT NULL DEFAULT 0,
    shipper_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    CONSTRAINT ck_dispatch_round_state
        CHECK (state IN ('OPEN', 'RUNNING', 'COMMITTED', 'REQUEUED', 'EXPIRED', 'CANCELLED'))
);

CREATE INDEX IF NOT EXISTS idx_dispatch_rounds_zone_cutoff
    ON dispatch_rounds (h3_zone, state, cutoff_at);

CREATE TABLE IF NOT EXISTS dispatch_pool_items (
    pool_item_id UUID PRIMARY KEY,
    order_id BIGINT NOT NULL,
    delivery_id BIGINT NOT NULL,
    matching_session_id UUID NOT NULL,
    pickup_h3_cell VARCHAR(32) NULL,
    pickup_lat DOUBLE PRECISION NULL,
    pickup_lng DOUBLE PRECISION NULL,
    delivery_lat DOUBLE PRECISION NULL,
    delivery_lng DOUBLE PRECISION NULL,
    total_price DECIMAL(12,2) NULL,
    payment_method VARCHAR(32) NULL,
    eligible_at TIMESTAMP NOT NULL,
    matching_deadline_at TIMESTAMP NOT NULL,
    state VARCHAR(24) NOT NULL,
    claimed_round_id UUID NULL,
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    CONSTRAINT uk_dispatch_pool_delivery_session
        UNIQUE (delivery_id, matching_session_id),
    CONSTRAINT fk_dispatch_pool_round
        FOREIGN KEY (claimed_round_id) REFERENCES dispatch_rounds(dispatch_round_id),
    CONSTRAINT ck_dispatch_pool_state
        CHECK (state IN ('WAITING', 'CLAIMED', 'ASSIGNED', 'REQUEUED', 'EXPIRED', 'CANCELLED'))
);

CREATE INDEX IF NOT EXISTS idx_dispatch_pool_ready
    ON dispatch_pool_items (state, eligible_at, matching_deadline_at);
CREATE INDEX IF NOT EXISTS idx_dispatch_pool_zone
    ON dispatch_pool_items (pickup_h3_cell, state, eligible_at);
CREATE INDEX IF NOT EXISTS idx_dispatch_pool_round
    ON dispatch_pool_items (claimed_round_id, state);

ALTER TABLE match_commands
    ADD COLUMN IF NOT EXISTS dispatch_round_id UUID NULL;
ALTER TABLE match_commands
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMP NULL;
ALTER TABLE match_commands
    ADD COLUMN IF NOT EXISTS matching_deadline_at TIMESTAMP NULL;

CREATE INDEX IF NOT EXISTS idx_match_commands_due
    ON match_commands (status, next_attempt_at, matching_deadline_at);
