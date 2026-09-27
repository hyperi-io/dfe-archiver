#!/usr/bin/env bash
# Project:   dfe-archiver
# File:      scripts/pgo-workload.sh
# Purpose:   PGO workload orchestrator -- Redpanda + archiver + producer
# Language:  Bash
#
# License:   BUSL-1.1
# Copyright: (c) 2026 HYPERI PTY LIMITED
#
# Reference: dfe-loader Canary 2 (v1.17.5) PGO workload pattern.
#
# Usage:
#   scripts/pgo-workload.sh <path-to-dfe-archiver-binary>
#
# Drives the archiver's hot path (Redpanda consume -> SIMD JSON route -> buffer
# accumulate -> zstd compress -> file write + roll) under representative
# load so a PGO-instrumented binary accumulates useful profile data.
#
# Environment variables (all optional):
#   PGO_WORKLOAD_DURATION_SECS   Duration of load (default 300, floor 60)
#   PGO_WORKLOAD_KAFKA_IMAGE     Override Kafka image
#   PGO_WORKLOAD_KEEP            Set to 1 to skip cleanup (debug)
#   PGO_DRIVER_PATH              Override pgo-driver binary path
#   PGO_DRIVER_RPS               Override producer rate (default 5000)
#
# Preconditions:
#   - Docker daemon running, user has access
#   - $1 is the archiver binary built with --features jemalloc
#   - pgo-driver binary built with --features pgo-driver (auto-built if missing)
#
# Behaviour:
#   - Starts single-node Kafka (KRaft) -- no MinIO, file:// destination is
#     adequate to drive compress + write hot paths
#   - Writes ephemeral archiver config pointing at Kafka + a temp dir
#   - Starts the passed-in archiver binary in background
#   - Waits for archiver readiness probe
#   - Runs pgo-driver to produce messages for the configured duration
#   - Cleans up (traps EXIT): kills archiver, removes container, removes
#     the temp archive directory

set -euo pipefail

# ----------------------------------------------------------------------------
# Args + env
# ----------------------------------------------------------------------------

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <path-to-dfe-archiver-binary>" >&2
    exit 1
fi

ARCHIVER_BIN="$1"
if [[ ! -x "$ARCHIVER_BIN" ]]; then
    echo "error: $ARCHIVER_BIN is not executable" >&2
    exit 1
fi

DURATION="${PGO_WORKLOAD_DURATION_SECS:-300}"
# Tag on its own line, separate from the image name, so one Renovate regex
# covers every language in the fleet. Embedded in the ref it would need a
# pattern that picks the right colon out of "${VAR:-name:tag}", and RE2 has no
# lookahead to do that cleanly.
# renovate: datasource=docker depName=redpandadata/redpanda
KAFKA_TAG="v26.2.1"
KAFKA_IMAGE="${PGO_WORKLOAD_KAFKA_IMAGE:-docker.redpanda.com/redpandadata/redpanda:${KAFKA_TAG}}"
KEEP="${PGO_WORKLOAD_KEEP:-0}"

# Floor of 60s -- shorter workloads produce bad PGO profiles
if [[ "$DURATION" -lt 60 ]]; then
    echo "error: PGO_WORKLOAD_DURATION_SECS must be >= 60 (got $DURATION)" >&2
    echo "  short workloads produce NEGATIVE PGO gains by biasing the" >&2
    echo "  compiler toward startup paths instead of hot paths" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Locate pgo-driver binary; build on demand if missing.
PGO_DRIVER_PATH="${PGO_DRIVER_PATH:-}"
if [[ -z "$PGO_DRIVER_PATH" ]]; then
    for candidate in \
        "$PROJECT_ROOT/target/release/pgo-driver" \
        "$PROJECT_ROOT/target/debug/pgo-driver"; do
        if [[ -x "$candidate" ]]; then
            PGO_DRIVER_PATH="$candidate"
            break
        fi
    done
fi
if [[ -z "$PGO_DRIVER_PATH" || ! -x "$PGO_DRIVER_PATH" ]]; then
    echo "pgo-workload: pgo-driver not found, building..." >&2
    (cd "$PROJECT_ROOT" && cargo build --release --features pgo-driver --bin pgo-driver) \
        || { echo "error: failed to build pgo-driver" >&2; exit 1; }
    PGO_DRIVER_PATH="$PROJECT_ROOT/target/release/pgo-driver"
    if [[ ! -x "$PGO_DRIVER_PATH" ]]; then
        echo "error: pgo-driver still missing after build at $PGO_DRIVER_PATH" >&2
        exit 1
    fi
fi

# ----------------------------------------------------------------------------
# Cleanup
# ----------------------------------------------------------------------------

ARCHIVER_PID=""
KAFKA_CID=""
WORK_DIR=""

cleanup() {
    local rc=$?
    if [[ "$KEEP" == "1" ]]; then
        echo "PGO_WORKLOAD_KEEP=1 -- skipping cleanup" >&2
        echo "  archiver PID: $ARCHIVER_PID" >&2
        echo "  kafka CID:    $KAFKA_CID" >&2
        echo "  work dir:     $WORK_DIR" >&2
        return $rc
    fi
    echo "pgo-workload: cleanup" >&2
    if [[ -n "$ARCHIVER_PID" ]] && kill -0 "$ARCHIVER_PID" 2>/dev/null; then
        kill -TERM "$ARCHIVER_PID" 2>/dev/null || true
        for _ in 1 2 3 4 5 6 7 8 9 10; do
            if ! kill -0 "$ARCHIVER_PID" 2>/dev/null; then
                break
            fi
            sleep 1
        done
        kill -KILL "$ARCHIVER_PID" 2>/dev/null || true
    fi
    if [[ -n "$KAFKA_CID" ]]; then
        docker rm -f "$KAFKA_CID" >/dev/null 2>&1 || true
    fi
    if [[ -n "$WORK_DIR" && -d "$WORK_DIR" ]]; then
        rm -rf "$WORK_DIR"
    fi
    exit $rc
}
trap cleanup EXIT INT TERM

# ----------------------------------------------------------------------------
# Start Kafka (KRaft mode, single-node, auto-create topics)
# ----------------------------------------------------------------------------

echo "pgo-workload: starting Redpanda ($KAFKA_IMAGE)"
# Redpanda (Kafka-API, C++/Seastar) replaces the Kafka JVM: the JVM's 1.5-2GB
# heap starves the PGO-instrumented binary on 4GB CI runners (arm64 ARC + the
# free OSS runners we target post-OSS). dev-container mode bundles
# --overprovisioned, --reserve-memory 0M, --check=false, --unsafe-bypass-fsync
# and auto-creates topics; the explicit --memory cap leaves headroom for the
# instrumented binary + load driver. Same Kafka wire protocol -- app config and
# the localhost:19092 endpoint are unchanged.
KAFKA_CID=$(docker run -d --rm \
    -p 19092:9092 \
    "$KAFKA_IMAGE" \
    redpanda start \
        --mode dev-container \
        --smp 1 \
        --memory 512M \
        --kafka-addr PLAINTEXT://0.0.0.0:9092 \
        --advertise-kafka-addr PLAINTEXT://localhost:19092)
echo "pgo-workload: Redpanda CID: $KAFKA_CID"

# Real protocol readiness via the admin API (rpk), not a bare TCP-open probe:
# only reports healthy once the broker is actually serving.
for attempt in $(seq 1 60); do
    if docker exec "$KAFKA_CID" rpk cluster health 2>/dev/null | grep -q "Healthy:.*true"; then
        echo "pgo-workload: Redpanda ready (attempt $attempt)"
        break
    fi
    if [[ $attempt -eq 60 ]]; then
        echo "error: Redpanda did not become ready in 120s" >&2
        docker logs --tail 50 "$KAFKA_CID" >&2
        exit 1
    fi
    sleep 2
done

# Pre-create the topic. Redpanda auto-creates on PRODUCE but not on a consumer
# SUBSCRIBE, so the archiver (a consumer) would never find "events" and never
# reach ready -- and the load driver only starts after the archiver is ready,
# so nothing ever produces it. Apache Kafka's auto-create-on-subscribe masked
# this ordering. Create from a --network host client so it reaches the
# advertised localhost:19092 listener (in-container rpk follows the advertised
# address, which only resolves on the host).
docker run --rm --network host "$KAFKA_IMAGE" \
    topic create events -p 3 -X brokers=localhost:19092 >/dev/null 2>&1 || true
echo "pgo-workload: created topic 'events'"

# ----------------------------------------------------------------------------
# Write ephemeral archiver config
# ----------------------------------------------------------------------------

WORK_DIR=$(mktemp -d -t pgo-workload-XXXXXX)
CONFIG_FILE="$WORK_DIR/config.yaml"
ARCHIVE_DIR="$WORK_DIR/archive"
mkdir -p "$ARCHIVE_DIR"

# Expression-based routing on org_id exercises the writer-LRU eviction path
# (the pgo-driver produces 32 distinct org_ids by default; max_writers caps
# the active set so eviction-close fires under load). zstd level 3 is the
# default production codec; rolling on small size + short interval keeps the
# close-and-roll path warm during the workload.
cat > "$CONFIG_FILE" <<YAML
kafka:
  brokers:
    - "localhost:19092"
  group_id: "pgo-workload"
  topics:
    - "events"
  batch_size: 10000
  session_timeout_ms: 10000
  max_poll_interval_ms: 300000

archive:
  destination: "file://$ARCHIVE_DIR"
  path_template: "{year}/{month}/{day}/{hour}"
  file_extension: "jsonl"
  roll_size_bytes: 67108864       # 64MB -- keeps roll path hot during 5-min runs
  roll_interval_secs: 60
  multipart_chunk_size: 8388608
  max_writers: 64                 # pgo-driver uses 32 orgs -> exercises but doesn't saturate

routing:
  mode: "expression"
  expression_fields:
    - "org_id"
  default_segment: "unknown"

compression:
  codec: "zstd"
  level: 3

buffer:
  flush_bytes: 4194304            # 4MB -- tighter than default to keep flushes frequent
  flush_age_secs: 5
  writer_parallelism: 4

memory:
  max_bytes: 536870912            # 512MB

metrics:
  enabled: true
  address: "127.0.0.1:9091"
  path: "/metrics"

# DLQ: scalo eagerly creates the file backend regardless of dlq.enabled.
# Default path /var/spool/dfe/dlq is unwriteable in CI runners. Disabling
# the file backend (file.enabled: false) skips writer init entirely.
dlq:
  enabled: false
  file:
    enabled: false
YAML

# ----------------------------------------------------------------------------
# Start archiver
# ----------------------------------------------------------------------------

echo "pgo-workload: starting archiver: $ARCHIVER_BIN"
echo "pgo-workload: config: $CONFIG_FILE"
echo "pgo-workload: archive dir: $ARCHIVE_DIR"

# PGO profiles go here by default with cargo-pgo
export LLVM_PROFILE_FILE="${LLVM_PROFILE_FILE:-$PROJECT_ROOT/target/pgo-profiles/pgo-%p_%m.profraw}"
mkdir -p "$(dirname "$LLVM_PROFILE_FILE")"

# The readiness grep below parses json-shaped log output, and scalo >= 2.10.13
# defaults to lines format when no OTEL_EXPORTER_OTLP_ENDPOINT is set. Pin the
# format the script parses.
export LOG_FORMAT=json

"$ARCHIVER_BIN" --config "$CONFIG_FILE" \
    >"$WORK_DIR/archiver.log" 2>&1 &
ARCHIVER_PID=$!
echo "pgo-workload: archiver PID: $ARCHIVER_PID"

for attempt in $(seq 1 60); do
    if ! kill -0 "$ARCHIVER_PID" 2>/dev/null; then
        echo "error: archiver died during startup" >&2
        tail -100 "$WORK_DIR/archiver.log" >&2
        exit 1
    fi
    # Primary signal: the archiver logs "<service> ready" once the pipeline
    # is up (main.rs). Rust line-buffers stdout, so the line hits archiver.log
    # immediately. The HTTP /readyz probe is kept as a fallback but proved
    # unreliable in this metrics-only config -- the readiness route is not
    # served on the metrics port (9091) here -- so the log line is the
    # authoritative readiness check.
    if grep -q ' ready"' "$WORK_DIR/archiver.log" 2>/dev/null \
        || curl -sf -o /dev/null --max-time 1 "http://127.0.0.1:9091/readyz" \
        || curl -sf -o /dev/null --max-time 1 "http://127.0.0.1:9091/healthz"; then
        echo "pgo-workload: archiver ready (attempt $attempt)"
        break
    fi
    if [[ $attempt -eq 60 ]]; then
        echo "error: archiver did not become ready in 60s" >&2
        tail -100 "$WORK_DIR/archiver.log" >&2
        exit 1
    fi
    sleep 1
done

# Extra settle so the consumer group is fully joined before we start producing
sleep 2

# ----------------------------------------------------------------------------
# Run load driver
# ----------------------------------------------------------------------------

echo "pgo-workload: driving load for ${DURATION}s via $PGO_DRIVER_PATH"

PGO_DRIVER_DURATION_SECS="$DURATION" \
PGO_DRIVER_BROKERS="127.0.0.1:19092" \
PGO_DRIVER_TOPIC="events" \
PGO_DRIVER_RPS="${PGO_DRIVER_RPS:-5000}" \
PGO_DRIVER_ORG_CARDINALITY="${PGO_DRIVER_ORG_CARDINALITY:-32}" \
    "$PGO_DRIVER_PATH"

echo "pgo-workload: driver complete"

# Give the archiver a moment to drain buffers + flush profile data
sleep 5

echo "pgo-workload: done (archiver logs: $WORK_DIR/archiver.log, archive: $ARCHIVE_DIR)"
