# Benchmark containers

This directory contains the benchmark server, the `wrk` runner, and a Docker Compose setup with
isolated Redis, benchmark-server, and optional in-network load-generator containers.

## Start the transaction variant

Run these commands from the repository root:

```sh
docker compose -f benchmarks/docker-compose.yml up -d --build
curl -fsS http://127.0.0.1:3000/info
```

Run a bounded smoke test from the host:

```sh
BENCH_TOPOLOGY=docker \
BENCH_REDIS_IMAGE=redis:7.4-bookworm \
BENCH_THREADS=1 \
BENCH_CONCURRENCIES="2 4 8" \
BENCH_KEY_SPACE=1000 \
BENCH_DURATION=2s \
BENCH_WARMUP_DURATION=1s \
BENCH_RUNS=1 \
bash benchmarks/run.sh
```

The benchmark server is published only on `127.0.0.1:3000`. Redis is reachable from the `bench`
container as `redis:6379` and is not published to the host.

Results are written by default to `benchmarks/output/<timestamp>/`; set `BENCH_OUTPUT_DIR` to use a
different directory. Each run includes raw `wrk` output, `summary.csv`, and a page-style
`report.txt` with throughput, latency percentiles, and error counts.

The benchmark defaults to a one-hour fixed-window policy so the full workload measures the
steady-state `FixedWindowStore` path without crossing a Redis expiration boundary. Window rollover
behavior is outside this throughput benchmark and should be tested separately.

For the canonical workload, run `wrk` inside the Compose network so requests go directly to the
`bench` container instead of crossing the host-to-Docker Desktop port boundary. Commit the source
first: the Docker runner rejects missing metadata and source marked as dirty.

```sh
test -z "$(git status --porcelain)" || { echo "commit or stash changes first" >&2; exit 1; }

docker compose -f benchmarks/docker-compose.yml up -d --build
docker compose --profile loadgen -f benchmarks/docker-compose.yml build loadgen

BENCH_SOURCE_COMMIT="$(git rev-parse HEAD)" \
BENCH_SOURCE_DIRTY=false \
BENCH_MACHINE_DETAILS="$(uname -a)" \
docker compose --profile loadgen -f benchmarks/docker-compose.yml run --rm \
  loadgen

docker compose -f benchmarks/docker-compose.yml down
```

The `loadgen` service calls `http://bench:3000` over the internal network and writes its results to
the host's `benchmarks/output/` directory. It is opt-in and is not started by the default Compose
command. The command above uses the specified 16/64/256 concurrency, 30-second, three-run workload.
For a short smoke test, add `-e BENCH_THREADS=1 -e BENCH_CONCURRENCIES="2 4 8" \
-e BENCH_DURATION=2s -e BENCH_WARMUP_DURATION=1s -e BENCH_RUNS=1` before `loadgen`.

## Start the Lua variant

Rebuild the `bench` image with the opt-in Lua feature. The server derives a separate Redis namespace
from the compiled implementation:

```sh
docker compose -f benchmarks/docker-compose.yml down

BENCH_FEATURES=axum,memory,redis-lua,runtime-tokio \
docker compose -f benchmarks/docker-compose.yml build --no-cache bench

BENCH_FEATURES=axum,memory,redis-lua,runtime-tokio \
docker compose -f benchmarks/docker-compose.yml up -d
```

After the Lua server reports that it is listening, run the same metadata-prefixed `loadgen` command
without `--build`; rebuilding `bench` without `BENCH_FEATURES` would select the transaction variant.

## Resource limits

The Compose file limits the benchmark server to 1 CPU and 2 GiB, Redis to 1 CPU and 512 MiB, and
the optional load generator to 1 CPU and 512 MiB. These are local test limits, not production
capacity claims. Check the effective limits with `docker stats` before interpreting results.

Stop the environment with:

```sh
docker compose -f benchmarks/docker-compose.yml down
```
