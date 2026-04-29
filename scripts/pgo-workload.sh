#!/usr/bin/env bash
# Project:   dfe-archiver
# File:      scripts/pgo-workload.sh
# Purpose:   PGO workload orchestrator — Kafka + archiver + producer
# Language:  Bash
#
# License:   FSL-1.1-ALv2
# Copyright: (c) 2026 HYPERI PTY LIMITED
#
# Usage:
#   scripts/pgo-workload.sh <path-to-dfe-archiver-binary>
#
# Drives the archiver's hot path (Kafka consume → SIMD JSON route → buffer
# accumulate → zstd compress → file write + roll) under representative
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
#   - Starts single-node Kafka (KRaft) — no MinIO, file:// destination is
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
KAFKA_IMAGE="${PGO_WORKLOAD_KAFKA_IMAGE:-apache/kafka:3.8.0}"
KEEP="${PGO_WORKLOAD_KEEP:-0}"

# Floor of 60s — shorter workloads produce bad PGO profiles
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
        echo "PGO_WORKLOAD_KEEP=1 — skipping cleanup" >&2
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

echo "pgo-workload: starting Kafka ($KAFKA_IMAGE)"
KAFKA_CID=$(docker run -d --rm \
    -p 19092:9092 \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS='PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093' \
    -e KAFKA_ADVERTISED_LISTENERS='PLAINTEXT://localhost:19092' \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP='CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT' \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS='1@localhost:9093' \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
    -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
    -e KAFKA_NUM_PARTITIONS=3 \
    -e KAFKA_DEFAULT_REPLICATION_FACTOR=1 \
    -e CLUSTER_ID="$(printf '%s' "pgo$(date +%s)$$" | base64 | head -c 22)" \
    "$KAFKA_IMAGE")
echo "pgo-workload: Kafka CID: $KAFKA_CID"

for attempt in $(seq 1 30); do
    if (echo > /dev/tcp/127.0.0.1/19092) 2>/dev/null; then
        sleep 2  # let RAFT bootstrap finish
        echo "pgo-workload: Kafka ready (attempt $attempt)"
        break
    fi
    if [[ $attempt -eq 30 ]]; then
        echo "error: Kafka did not become ready in 60s" >&2
        docker logs --tail 50 "$KAFKA_CID" >&2
        exit 1
    fi
    sleep 2
done

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
  roll_size_bytes: 67108864       # 64MB — keeps roll path hot during 5-min runs
  roll_interval_secs: 60
  multipart_chunk_size: 8388608
  max_writers: 64                 # pgo-driver uses 32 orgs → exercises but doesn't saturate

routing:
  mode: "expression"
  expression_fields:
    - "org_id"
  default_segment: "unknown"

compression:
  codec: "zstd"
  level: 3

buffer:
  flush_bytes: 4194304            # 4MB — tighter than default to keep flushes frequent
  flush_age_secs: 5
  writer_parallelism: 4

memory:
  max_bytes: 536870912            # 512MB

metrics:
  enabled: true
  address: "127.0.0.1:9091"
  path: "/metrics"

dlq:
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
    if curl -sf -o /dev/null --max-time 1 "http://127.0.0.1:9091/readyz" \
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
