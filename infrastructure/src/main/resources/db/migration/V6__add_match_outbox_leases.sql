ALTER TABLE match_outbox_events
    DROP CONSTRAINT IF EXISTS ck_match_outbox_status;

ALTER TABLE match_outbox_events
    ADD COLUMN IF NOT EXISTS lease_token UUID NULL;
ALTER TABLE match_outbox_events
    ADD COLUMN IF NOT EXISTS lease_until TIMESTAMP NULL;

ALTER TABLE match_outbox_events
    ADD CONSTRAINT ck_match_outbox_status
    CHECK (status IN ('PENDING', 'IN_FLIGHT', 'SENT', 'DEAD', 'CANCELLED'));

CREATE INDEX IF NOT EXISTS idx_match_outbox_claimable
    ON match_outbox_events (status, next_attempt_at, lease_until, created_at, id);
