ALTER TABLE saga_instances
    ADD COLUMN IF NOT EXISTS order_status_sequence BIGINT NOT NULL DEFAULT 0;
