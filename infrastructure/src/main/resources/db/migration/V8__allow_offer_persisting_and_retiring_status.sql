ALTER TABLE saga_instances DROP CONSTRAINT IF EXISTS saga_instances_status_check;
ALTER TABLE saga_instances ADD CONSTRAINT saga_instances_status_check CHECK (status IN (
    'STARTED',
    'DELIVERY_CREATED',
    'FINDING_SHIPPER',
    'OFFER_PERSISTING',
    'SHIPPER_FOUND',
    'OFFER_RETIRING',
    'SHIPPER_ASSIGNED',
    'PICKING_UP',
    'DELIVERING',
    'COMPLETED',
    'COMPENSATING',
    'FAILED',
    'CANCELLED'
));
