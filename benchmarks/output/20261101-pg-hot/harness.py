import hashlib
import json
import math
import platform
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path.cwd()
WORK = Path(__file__).parent
OUT = ROOT / "benchmarks/output/20261101-pg-hot"
SERVER = "tower-rate-limiter-pg35-hot"
IMAGE = "sha256:b51aeb2517df75e92137460fbd98f61c84617650df0f9f1fff98c666e82aff68"
DB = "rate_limiter_hot_bench"
VARIANTS = ["baseline", "rounded", "fill80", "rounded_fill80"]
CASES = [("keys1000", 1000, 100000), ("keys10000", 10000, 100000), ("hot1", 1, 4096)]
CLIENTS = 16
WARMUP_CALLS = 1024
PRECISE = "TIMESTAMPTZ 'epoch' + (v_candidate_tat_us * INTERVAL '1 microsecond')"
ROUNDED = """TIMESTAMPTZ 'epoch' + (
                   (v_candidate_tat_us / 1000000
                    + CASE WHEN v_candidate_tat_us % 1000000 = 0 THEN 0 ELSE 1 END)
                   * INTERVAL '1 second')"""


def command(args, data=None, timeout=120):
    result = subprocess.run(args, input=data, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"{args[:4]} failed:\n{result.stdout}\n{result.stderr}")
    return result.stdout


def sql(query):
    return command(["docker", "exec", "-i", SERVER, "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1",
                    "-U", "postgres", "-d", DB], query).strip()


def schema(variant):
    assert variant in VARIANTS
    return f"gcra_hot_{variant}"


def snapshot(variant):
    name = schema(variant)
    return json.loads(sql(f"""SELECT json_build_object(
 'stats', (SELECT json_build_object(
   'n_tup_ins', n_tup_ins, 'n_tup_upd', n_tup_upd, 'n_tup_hot_upd', n_tup_hot_upd,
   'n_tup_newpage_upd', n_tup_newpage_upd, 'n_dead_tup', n_dead_tup,
   'autovacuum_count', autovacuum_count, 'vacuum_count', vacuum_count,
   'last_autovacuum', last_autovacuum)
   FROM pg_stat_user_tables WHERE schemaname = '{name}' AND relname = 'state'),
 'heap_bytes', pg_relation_size('{name}.state'),
 'table_bytes', pg_table_size('{name}.state'),
 'index_bytes', pg_indexes_size('{name}.state'),
 'total_bytes', pg_total_relation_size('{name}.state'),
 'tuples', (SELECT row_to_json(t) FROM pgstattuple('{name}.state'::regclass) t),
 'primary_index', (SELECT row_to_json(i) FROM pgstatindex('{name}.state_pkey'::regclass) i),
 'expiry_index', (SELECT row_to_json(i) FROM pgstatindex('{name}.state_expiry_idx'::regclass) i)
);"""))


def updates(variant):
    return int(sql(f"SELECT n_tup_upd FROM pg_stat_user_tables WHERE schemaname = '{schema(variant)}' AND relname = 'state';"))


def wait_updates(variant, expected):
    deadline = time.monotonic() + 5
    while True:
        actual = updates(variant)
        if actual == expected:
            return
        if actual > expected or time.monotonic() >= deadline:
            raise AssertionError((variant, "update counters", expected, actual))
        time.sleep(.1)


def pgbench(script, calls, name=None):
    assert calls % CLIENTS == 0
    args = ["docker", "run", "--rm", "--network", f"container:{SERVER}",
            "--cpus", "1", "--memory", "256m", "--pids-limit", "128",
            "-v", f"{WORK}:/work", "-w", "/work", IMAGE, "pgbench", "-n",
            "-h", "127.0.0.1", "-U", "postgres", "-d", DB,
            "-M", "prepared", "-c", str(CLIENTS), "-j", "2", "-t", str(calls // CLIENTS),
            "--random-seed=1337", "--exit-on-abort", "-f", f"/work/workloads/{script}"]
    if name:
        args += ["-l", f"--log-prefix=/work/logs/{name}"]
    output = command(args)
    assert int(re.search(r"number of transactions actually processed:\s+(\d+)", output)[1]) == calls
    assert int(re.search(r"number of failed transactions:\s+(\d+)", output)[1]) == 0
    return output


def setup():
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "raw").mkdir(exist_ok=True)
    (OUT / "sql").mkdir(exist_ok=True)
    (OUT / "workloads").mkdir(exist_ok=True)
    source = (ROOT / "migrations/postgres/0001_gcra.sql").read_text()
    assert source.count(PRECISE) == 1
    (OUT / "sql" / "original.sql").write_text(source)
    for variant in VARIANTS:
        rounded = variant in ["rounded", "rounded_fill80"]
        fill = 80 if variant in ["fill80", "rounded_fill80"] else 100
        artifact = source.replace("tower_rate_limiter_gcra_v1", schema(variant))
        artifact = artifact.replace(
            ");\n\nCREATE INDEX", f") WITH (fillfactor = {fill}, autovacuum_enabled = false);\n\nCREATE INDEX", 1,
        )
        if rounded:
            artifact = artifact.replace(PRECISE, ROUNDED)
        (OUT / "sql" / f"{variant}.sql").write_text(artifact)
        sql(artifact)
        for case, keys, _ in CASES:
            script = f"""\\set key random(1, {keys})
SELECT allowed::int AS admitted, remaining, retry_after_us, reset_after_us
FROM {schema(variant)}.charge('bench', 'key-' || :key::text, 1, 10000000)
\\gset
\\if :admitted <> 1 OR :remaining < 0 OR :remaining >= 10000000 OR :retry_after_us <> 0 OR :reset_after_us <= 0
SELECT 1 / 0;
\\endif
"""
            file = f"{variant}-{case}.sql"
            (WORK / "workloads" / file).write_text(script)
            (OUT / "workloads" / file).write_text(script)
    metadata = {
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "source_commit": command(["git", "rev-parse", "HEAD"]).strip(), "source_dirty": True,
        "baseline": "Current working migration, including previous lookup/RETURNING optimization",
        "baseline_sha256": hashlib.sha256(source.encode()).hexdigest(),
        "postgres": sql("SELECT version();"), "pgstattuple": sql("SELECT extversion FROM pg_extension WHERE extname = 'pgstattuple';"),
        "fsync": sql("SHOW fsync;"), "synchronous_commit": sql("SHOW synchronous_commit;"),
        "host": platform.platform(), "image_id": IMAGE,
        "server_limits": "1 CPU / 512 MiB / 128 PIDs", "client_limits": "1 CPU / 256 MiB / 128 PIDs / 2 threads",
        "protocol": "prepared; TCP loopback between separate containers sharing a network namespace",
        "clients": CLIENTS, "repeats": 3, "warmup_calls": WARMUP_CALLS,
        "cases": [{"case": c, "keys": k, "measured_calls": n} for c, k, n in CASES],
        "quota": "emission interval 1 us / burst 10000000; all requests should be admitted",
        "autovacuum": "Disabled equally on the four experimental tables only, to compare pre-vacuum growth at equal work; manual VACUUM afterwards",
        "variants": {v: {"round_expiry_up_to_seconds": v in ["rounded", "rounded_fill80"], "fillfactor": 80 if v in ["fill80", "rounded_fill80"] else 100} for v in VARIANTS},
        "rounding": "Integer quotient and remainder; tat_us, admission and retry/reset calculations remain precise",
        "ordering": "Variant order rotates and alternates across repeated runs",
        "raw_latency_logs": str(WORK / "logs"),
        "growth_metrics": "Measured after identical warmup and call counts; pgstattuple dead/free fractions and pgstatindex density are indicators, not an exact bloat estimate",
    }
    (OUT / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")


def preflight():
    rounding = json.loads(sql("""SELECT json_agg(json_build_object('tat_us', n, 'seconds',
 n / 1000000 + CASE WHEN n % 1000000 = 0 THEN 0 ELSE 1 END))
 FROM unnest(ARRAY[0,1,999999,1000000,1000001,1793512345678901,9223372036854775807]::bigint[]) n;"""))
    assert all(r["seconds"] == (r["tat_us"] + 999999) // 1000000 for r in rounding)
    for variant in VARIANTS:
        name = schema(variant)
        sql(f"""TRUNCATE {name}.state;
DO $check$
DECLARE r RECORD; old_tat BIGINT; old_expiry TIMESTAMPTZ; expected INT;
BEGIN
 FOR expected IN REVERSE 7..0 LOOP
  SELECT * INTO r FROM {name}.charge('check', 'burst', 60000000, 8);
  ASSERT r.allowed AND r.remaining = expected AND r.retry_after_us = 0;
 END LOOP;
 SELECT tat_us, expires_at INTO old_tat, old_expiry FROM {name}.state WHERE client_key = 'burst';
 SELECT * INTO r FROM {name}.charge('check', 'burst', 60000000, 8);
 ASSERT NOT r.allowed AND r.remaining = 0 AND r.retry_after_us > 0;
 ASSERT EXISTS(SELECT 1 FROM {name}.state WHERE client_key = 'burst' AND tat_us = old_tat AND expires_at = old_expiry);
 SELECT * INTO r FROM {name}.charge('check', 'isolated', 60000000, 8);
 ASSERT r.allowed AND r.remaining = 7;
END $check$;
CREATE TABLE {name}.preflight_decisions (allowed BOOLEAN NOT NULL);""")
        script = f"INSERT INTO {name}.preflight_decisions SELECT allowed FROM {name}.charge('check', 'concurrent', 60000000, 8);\n"
        file = f"{variant}-concurrent.sql"
        (WORK / "workloads" / file).write_text(script)
        (OUT / "workloads" / file).write_text(script)
        output = pgbench(file, 64)
        (OUT / "raw" / f"preflight-{variant}.txt").write_text(output)
        assert sql(f"SELECT count(*) || ':' || sum(allowed::int) FROM {name}.preflight_decisions;") == "64:8"
        bound = ">= 0 AND (EXTRACT(EPOCH FROM expires_at) * 1000000)::BIGINT - tat_us < 1000000" if variant in ["rounded", "rounded_fill80"] else "= 0"
        assert sql(f"SELECT bool_and((EXTRACT(EPOCH FROM expires_at) * 1000000)::BIGINT - tat_us {bound}) FROM {name}.state;") == "t"
        sql(f"DROP TABLE {name}.preflight_decisions;")
    (OUT / "verification.json").write_text(json.dumps({
        "rounding_vectors": rounding, "variants_passed": VARIANTS,
        "checks": ["rounding boundaries and BIGINT-safe quotient", "exact burst / positive retry", "rejected charge preserves precise TAT and expiry", "key isolation", "64 concurrent requests admit exactly 8", "expiry is at or above TAT with less than 1 second slack"],
    }, indent=2) + "\n")


def run_case(variant, case, keys, calls, repeat):
    name = schema(variant)
    sql(f"TRUNCATE {name}.state; INSERT INTO {name}.state SELECT 'bench', 'key-' || n, 0, TIMESTAMPTZ 'epoch' FROM generate_series(1, {keys}) n;")
    sql(f"VACUUM ANALYZE {name}.state;")
    seeded = snapshot(variant)
    initial_updates = updates(variant)
    pgbench(f"{variant}-{case}.sql", WARMUP_CALLS)
    wait_updates(variant, initial_updates + WARMUP_CALLS)
    before = snapshot(variant)
    start_lsn = sql("SELECT pg_current_wal_insert_lsn();")
    label = f"{case}-r{repeat}-{variant}"
    output = pgbench(f"{variant}-{case}.sql", calls, label)
    end_lsn = sql("SELECT pg_current_wal_insert_lsn();")
    wal_bytes = int(sql(f"SELECT pg_wal_lsn_diff('{end_lsn}', '{start_lsn}')::BIGINT;"))
    wait_updates(variant, before["stats"]["n_tup_upd"] + calls)
    after = snapshot(variant)
    assert after["stats"]["autovacuum_count"] == before["stats"]["autovacuum_count"]
    assert after["tuples"]["tuple_count"] == keys
    assert sql(f"SELECT bool_and((EXTRACT(EPOCH FROM expires_at) * 1000000)::BIGINT >= tat_us) FROM {name}.state;") == "t"
    (OUT / "raw" / f"{label}.txt").write_text(output)
    latency = []
    for file in (WORK / "logs").glob(label + ".*"):
        latency.extend(int(line.split()[2]) for line in file.read_text().splitlines())
    latency.sort()
    assert len(latency) == calls
    sql(f"VACUUM ANALYZE {name}.state;")
    vacuumed = snapshot(variant)
    delta_updates = after["stats"]["n_tup_upd"] - before["stats"]["n_tup_upd"]
    delta_hot = after["stats"]["n_tup_hot_upd"] - before["stats"]["n_tup_hot_upd"]
    record = {
        "variant": variant, "case": case, "keys": keys, "clients": CLIENTS, "repeat": repeat,
        "calls": calls, "failed": 0,
        "tps": float(re.search(r"tps = ([0-9.]+)", output)[1]),
        "mean_ms": float(re.search(r"latency average = ([0-9.]+)", output)[1]),
        "p95_ms": latency[math.ceil(len(latency) * .95) - 1] / 1000,
        "p99_ms": latency[math.ceil(len(latency) * .99) - 1] / 1000,
        "updates": delta_updates, "hot_updates": delta_hot, "hot_pct": 100 * delta_hot / delta_updates,
        "wal_bytes": wal_bytes, "wal_bytes_per_call": wal_bytes / calls,
        "heap_growth_bytes": after["heap_bytes"] - seeded["heap_bytes"],
        "index_growth_bytes": after["index_bytes"] - seeded["index_bytes"],
        "total_growth_bytes": after["total_bytes"] - seeded["total_bytes"],
        "seeded": seeded, "before": before, "after": after, "vacuumed": vacuumed,
    }
    (OUT / "raw" / f"{label}.json").write_text(json.dumps(record, indent=2) + "\n")
    return record


setup()
preflight()
print("Preflight passed for all four variants.", flush=True)
if "--smoke" in sys.argv:
    result = run_case("rounded_fill80", "keys1000", 1000, 4096, 0)
    (OUT / "smoke.json").write_text(json.dumps(result, indent=2) + "\n")
    print(f"Smoke passed: {result['hot_pct']:.1f}% HOT; {result['wal_bytes_per_call']:.1f} WAL bytes/call", flush=True)
else:
    results = []
    for case, keys, calls in CASES:
        for repeat in range(1, 4):
            shift = repeat - 1
            order = VARIANTS[shift:] + VARIANTS[:shift]
            if repeat % 2 == 0:
                order = list(reversed(order))
            for variant in order:
                row = run_case(variant, case, keys, calls, repeat)
                results.append(row)
                (OUT / "summary.json").write_text(json.dumps(results, indent=2) + "\n")
                print(f"{len(results)}/36 {case} r={repeat} {variant}: {row['tps']:.0f}/s p95={row['p95_ms']:.2f}ms HOT={row['hot_pct']:.1f}% WAL={row['wal_bytes_per_call']:.1f}B/call total={row['after']['total_bytes']/1048576:.2f}MiB", flush=True)
    print(f"Completed: {OUT}", flush=True)
