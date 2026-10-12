# PostgreSQL GCRA

`PostgresGcra` stores GCRA state in PostgreSQL and makes each charge through a database function.
It is a shared policy: replicas that use the same database, namespace, policy name, and quota
enforce one client-key state. The adapter accepts a caller-owned `sqlx::PgPool` and is Tokio-only
through SQLx.

## Setup and migration ownership

Enable the policy and provide SQLx in the application:

```toml
[dependencies]
sqlx = { version = "0.9", default-features = false, features = ["postgres", "runtime-tokio"] }
tower-rate-limiter = { version = "0.2", default-features = false, features = ["postgres-gcra"] }
```

Before serving traffic, apply the packaged, versioned migration with **your application's migration
runner**:

```text
migrations/postgres/0001_gcra.sql
```

The same artifact is exported as `POSTGRES_GCRA_MIGRATION_V1` for migration tooling that accepts
SQL text. File-oriented migration frameworks should copy the packaged SQL into the application's
own migration directory so their normal checksum and ordering rules apply.

The crate deliberately never applies DDL or creates schema objects at runtime. Treat the migration
as part of service provisioning and roll it out before deploying code that enables `PostgresGcra`.

```rust,no_run
use std::time::Duration;
use tower_rate_limiter::{GcraQuota, PostgresGcra};

# async fn configure(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
let quota = GcraQuota::new(100, Duration::from_secs(60), 20)?;
let policy = PostgresGcra::new(pool, quota).with_namespace("my-service-v1");
# let _ = policy;
# Ok(())
# }
```

See [GCRA backends](gcra.md) for the shared quota, Decision, and rollout contract.

## Atomicity and time source

The migration installs `tower_rate_limiter_gcra_v1.charge(...)`. It creates the key row if needed,
locks the row, then captures `clock_timestamp()` **after** that lock. It performs the GCRA decision
and accepted-state update in the same database function. A rejected charge intentionally does not
advance state.

Using database time avoids application-host clock skew for a decision. Requests for the same key
serialize on that key's row; different keys can proceed independently. This makes PostgreSQL a
good fit only when its per-request database work and hot-key contention are acceptable for the
protected endpoint.

The adapter executes one statement through the pool and is tested with PostgreSQL's default
`READ COMMITTED` isolation. If the deployment changes the database or role default isolation,
validate concurrent fresh-key behavior and decide how Policy errors such as serialization failures
fit the application's failure mode.

## Permissions

Run the migration as a schema owner or migration principal. The runtime limiter principal needs
access to the installed schema and function plus the table permissions used by this migration's
security-invoker function:

```sql
GRANT USAGE ON SCHEMA tower_rate_limiter_gcra_v1 TO app_limiter;
GRANT EXECUTE ON FUNCTION tower_rate_limiter_gcra_v1.charge(TEXT, TEXT, BIGINT, BIGINT) TO app_limiter;
GRANT SELECT, INSERT, UPDATE ON TABLE tower_rate_limiter_gcra_v1.state TO app_limiter;
```

Use a separate maintenance principal for deletion if the runtime role should not remove state.
Do not change the function to `SECURITY DEFINER` without a deliberate ownership and `search_path`
review.

## Expired-state cleanup

The state table records `expires_at` and indexes it. Expired rows no longer affect a fresh GCRA
decision, but PostgreSQL does not remove them automatically. Schedule a bounded cleanup job under
your operational ownership, for example repeatedly deleting expired rows in small batches during
off-peak periods. Grant that job `DELETE` separately when the runtime principal does not have it.

One concurrency-safe batch shape is:

```sql
WITH expired AS (
    SELECT namespace, client_key
    FROM tower_rate_limiter_gcra_v1.state
    WHERE expires_at <= clock_timestamp()
    ORDER BY expires_at
    LIMIT 1000
    FOR UPDATE SKIP LOCKED
)
DELETE FROM tower_rate_limiter_gcra_v1.state AS state
USING expired
WHERE state.namespace = expired.namespace
  AND state.client_key = expired.client_key;
```

Choose the interval, batch size, vacuum/monitoring policy, and retention budget for your workload;
the library does not run a cleanup worker. Monitor table growth, index size, lock waits, database
latency, and policy failures.

## Namespace and quota rollouts

Use `.with_namespace(...)` to separate deployments sharing one database. The middleware-provided
`policy_name` is also part of the scoped client key. Every replica charging the same resulting
state identity must use the same quota. When changing a quota, version the namespace or policy
name—such as `my-service-v2`—rather than allowing old and new emission intervals to reinterpret
the same stored theoretical-arrival time. Retire the old identity only after its state has expired.

PostgreSQL's primary-key index also imposes a practical bound on the combined namespace and client
key size. Built-in IP keys are small. If a custom extractor can produce long or attacker-controlled
values, use `with_key_encoder` to map the complete scoped key to a bounded digest before it reaches
the Policy.
