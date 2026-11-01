\set seq :seq + 1
SELECT allowed::int AS admitted, remaining, retry_after_us, reset_after_us
FROM tower_rate_limiter_gcra_after.charge('bench', 'new-' || :client_id::text || '-' || :seq::text, 1, 10000000)
\gset
\if :admitted <> 1 OR :remaining < 0 OR :remaining >= 10000000 OR :reset_after_us <= 0 OR :retry_after_us <> 0
SELECT 1 / 0;
\endif
