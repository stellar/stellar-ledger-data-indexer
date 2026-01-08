-- +migrate Up
-- SQL in section 'Up' is executed when this migration is applied

ALTER TABLE ttl
ADD COLUMN pk_id BIGSERIAL PRIMARY KEY;

-- +migrate Down
-- SQL section 'Down' is executed when this migration is rolled back
ALTER TABLE ttl
DROP COLUMN pk_id;
