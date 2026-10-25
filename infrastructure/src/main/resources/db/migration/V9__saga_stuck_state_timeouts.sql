ALTER TABLE saga_instances ADD COLUMN state_entered_at TIMESTAMP;
ALTER TABLE saga_instances ADD COLUMN stuck_last_resent_at TIMESTAMP;
ALTER TABLE saga_instances ADD COLUMN stuck_resend_attempts INTEGER NOT NULL DEFAULT 0;
UPDATE saga_instances SET state_entered_at = COALESCE(updated_at, created_at);
