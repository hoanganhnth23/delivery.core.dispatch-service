ALTER TABLE match_cancellation_tombstones
    ADD COLUMN IF NOT EXISTS projection_status VARCHAR(16) NOT NULL DEFAULT 'PENDING';

ALTER TABLE match_cancellation_tombstones
    ADD COLUMN IF NOT EXISTS projection_attempts INTEGER NOT NULL DEFAULT 0;

ALTER TABLE match_cancellation_tombstones
    ADD COLUMN IF NOT EXISTS next_projection_attempt_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE match_cancellation_tombstones
    ADD COLUMN IF NOT EXISTS redis_projected_at TIMESTAMP NULL;

ALTER TABLE match_cancellation_tombstones
    ADD COLUMN IF NOT EXISTS last_projection_error VARCHAR(2000) NULL;

ALTER TABLE match_cancellation_tombstones
    DROP CONSTRAINT IF EXISTS ck_match_cancellation_projection_status;

ALTER TABLE match_cancellation_tombstones
    ADD CONSTRAINT ck_match_cancellation_projection_status
        CHECK (projection_status IN ('PENDING', 'PROJECTED'));

CREATE INDEX IF NOT EXISTS idx_match_cancellation_projection_pending
    ON match_cancellation_tombstones
        (projection_status, next_projection_attempt_at, created_at, event_id);
