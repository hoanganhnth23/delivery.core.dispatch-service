ALTER TABLE match_commands ADD COLUMN matching_session_id UUID;

-- V1 commands used their Kafka command identity as the only generation. Keep
-- those in-flight rows replayable through the V2 fence during rollout.
UPDATE match_commands
SET matching_session_id = event_id
WHERE matching_session_id IS NULL;

ALTER TABLE match_commands ALTER COLUMN matching_session_id SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uk_match_commands_delivery_session
    ON match_commands (delivery_id, matching_session_id);
CREATE INDEX IF NOT EXISTS idx_match_commands_session
    ON match_commands (matching_session_id);

CREATE TABLE IF NOT EXISTS match_cancellation_tombstones (
    event_id UUID PRIMARY KEY,
    order_id BIGINT NOT NULL,
    delivery_id BIGINT NOT NULL,
    matching_session_id UUID NOT NULL,
    payload_fingerprint VARCHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    CONSTRAINT uk_match_cancellation_delivery_session
        UNIQUE (delivery_id, matching_session_id)
);

CREATE INDEX IF NOT EXISTS idx_match_cancellation_delivery
    ON match_cancellation_tombstones (delivery_id, created_at);
CREATE INDEX IF NOT EXISTS idx_match_cancellation_session
    ON match_cancellation_tombstones (matching_session_id);
