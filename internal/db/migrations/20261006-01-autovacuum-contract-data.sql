-- +migrate Up
-- Every contract_data update is non-HOT (it changes indexed columns), so each
-- one leaves a dead row and a dead entry in every index. With the default
-- scale factor autovacuum waits for ~20% of the table (~92M rows on pubnet)
-- before it runs; the table produces ~4M dead rows a day. Trigger on a fixed
-- row count instead and raise the cost budget so a pass over this table
-- finishes in hours rather than days.
ALTER TABLE contract_data SET (
  autovacuum_vacuum_scale_factor  = 0,
  autovacuum_vacuum_threshold     = 2000000,  -- ~2 runs/day at ~4M dead rows/day
  autovacuum_vacuum_cost_limit    = 2000,     -- 10x the default
  autovacuum_analyze_scale_factor = 0.02
);

-- +migrate Down
ALTER TABLE contract_data RESET (
  autovacuum_vacuum_scale_factor,
  autovacuum_vacuum_threshold,
  autovacuum_vacuum_cost_limit,
  autovacuum_analyze_scale_factor
);
