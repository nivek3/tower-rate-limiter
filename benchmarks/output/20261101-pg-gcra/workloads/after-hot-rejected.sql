SELECT allowed::int AS admitted, remaining, retry_after_us, reset_after_us
FROM tower_rate_limiter_gcra_after.charge('bench', 'hot', 60000000, 1)
\gset
\if :admitted <> 0 OR :remaining < 0 OR :remaining >= 1 OR :reset_after_us <= 0 OR :retry_after_us <= 0
SELECT 1 / 0;
\endif
