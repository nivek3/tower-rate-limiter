-- PostgreSQL GCRA state transition, version 1.
--
-- Applications apply this migration through their own migration runner. Runtime rate-limit
-- principals need only USAGE on this schema plus EXECUTE on the function and the table privileges
-- required by the function's invoker security mode.
--
-- This project-owned transition follows the GCRA contract documented by governor 0.10.4. Its
-- single-statement PostgreSQL shape was informed by Betterment's MIT-licensed `delayed` limiter at
-- commit 5c0f5583533a3301ec76b628b0c3c4753ff969df; it is not a copied upstream artifact.

CREATE SCHEMA IF NOT EXISTS gcra_hot_baseline;

CREATE TABLE IF NOT EXISTS gcra_hot_baseline.state (
    namespace TEXT NOT NULL,
    client_key TEXT NOT NULL,
    tat_us BIGINT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (namespace, client_key)
) WITH (fillfactor = 100, autovacuum_enabled = false);

CREATE INDEX IF NOT EXISTS state_expiry_idx
    ON gcra_hot_baseline.state (expires_at);

CREATE OR REPLACE FUNCTION gcra_hot_baseline.charge(
    p_namespace TEXT,
    p_client_key TEXT,
    p_emission_interval_us BIGINT,
    p_burst BIGINT
)
RETURNS TABLE (
    allowed BOOLEAN,
    remaining BIGINT,
    retry_after_us BIGINT,
    reset_after_us BIGINT
)
LANGUAGE plpgsql
SET search_path = pg_catalog, gcra_hot_baseline
AS $$
DECLARE
    v_tat_us BIGINT;
    v_now_us BIGINT;
    v_candidate_tat_us BIGINT;
    v_burst_window_us BIGINT;
    v_elapsed_us BIGINT;
BEGIN
    IF p_namespace IS NULL OR p_client_key IS NULL THEN
        RAISE EXCEPTION 'GCRA namespace and client key must not be null'
            USING ERRCODE = '22004';
    END IF;
    IF p_emission_interval_us IS NULL OR p_emission_interval_us <= 0 OR p_burst IS NULL OR p_burst <= 0 THEN
        RAISE EXCEPTION 'GCRA emission interval and burst must be positive'
            USING ERRCODE = '22023';
    END IF;
    IF p_burst > 9223372036854775807 / p_emission_interval_us THEN
        RAISE EXCEPTION 'GCRA burst window overflows BIGINT microseconds'
            USING ERRCODE = '22003';
    END IF;
    v_burst_window_us := p_burst * p_emission_interval_us;

    -- Read existing state first; retry creation if another charge or cleanup wins the race.
    LOOP
        SELECT tat_us
          INTO v_tat_us
          FROM gcra_hot_baseline.state
         WHERE namespace = p_namespace
           AND client_key = p_client_key
         FOR UPDATE;

        EXIT WHEN FOUND;

        -- Infinity keeps cleanup from removing a placeholder before its first accepted charge.
        INSERT INTO gcra_hot_baseline.state (namespace, client_key, tat_us, expires_at)
        VALUES (p_namespace, p_client_key, 0, TIMESTAMPTZ 'infinity')
        ON CONFLICT (namespace, client_key) DO NOTHING
        RETURNING tat_us INTO v_tat_us;

        EXIT WHEN FOUND;
    END LOOP;

    IF v_tat_us IS NULL OR v_tat_us < 0 THEN
        RAISE EXCEPTION 'GCRA state contains an invalid theoretical arrival time'
            USING ERRCODE = '22000';
    END IF;

    -- `clock_timestamp()` is deliberately called only after this key's row lock is acquired.
    v_now_us := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::BIGINT;
    IF v_now_us < 0 THEN
        RAISE EXCEPTION 'GCRA database clock precedes the Unix epoch'
            USING ERRCODE = '22000';
    END IF;

    v_candidate_tat_us := GREATEST(v_tat_us, v_now_us);
    IF v_candidate_tat_us > 9223372036854775807 - p_emission_interval_us THEN
        RAISE EXCEPTION 'GCRA theoretical arrival time overflows BIGINT microseconds'
            USING ERRCODE = '22003';
    END IF;
    v_candidate_tat_us := v_candidate_tat_us + p_emission_interval_us;

    -- Avoid a potentially underflowing subtraction: a burst window at least as large as the
    -- candidate always admits while database time is non-negative.
    IF v_burst_window_us >= v_candidate_tat_us
       OR v_candidate_tat_us - v_burst_window_us <= v_now_us THEN
        v_elapsed_us := v_candidate_tat_us - v_now_us;
        IF v_elapsed_us > v_burst_window_us THEN
            RAISE EXCEPTION 'GCRA allowed transition exceeds its burst window'
                USING ERRCODE = '22000';
        END IF;

        UPDATE gcra_hot_baseline.state
           SET tat_us = v_candidate_tat_us,
               expires_at = TIMESTAMPTZ 'epoch' + (v_candidate_tat_us * INTERVAL '1 microsecond')
         WHERE namespace = p_namespace
           AND client_key = p_client_key;

        RETURN QUERY SELECT
            TRUE,
            (v_burst_window_us - v_elapsed_us) / p_emission_interval_us,
            0::BIGINT,
            v_elapsed_us;
        RETURN;
    END IF;

    -- A rejected transition intentionally writes nothing: TAT and expiry remain unchanged.
    RETURN QUERY SELECT
        FALSE,
        0::BIGINT,
        (v_candidate_tat_us - v_burst_window_us) - v_now_us,
        v_tat_us - v_now_us;
END;
$$;

REVOKE ALL ON FUNCTION gcra_hot_baseline.charge(TEXT, TEXT, BIGINT, BIGINT) FROM PUBLIC;
