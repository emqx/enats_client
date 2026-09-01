#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
RUNS=3
CORE_COUNT=5000
JETSTREAM_COUNT=2000
PAYLOAD_SIZES="128,4096"
BATCH_SIZE=256
CORE_PORT=""
JS_PORT=""
TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/enats-benchmark.XXXXXX")
RAW_FILE="$TMP_DIR/raw.txt"
RESULT_FILE="${ENATS_BENCHMARK_OUTPUT:-$ROOT_DIR/_build/benchmark/raw-$(date +%Y%m%d-%H%M%S).txt}"
GO_BIN="$TMP_DIR/benchmark-go"
CORE_PID=""
JS_PID=""

cleanup() {
    [ -z "$CORE_PID" ] || kill "$CORE_PID" 2>/dev/null || true
    [ -z "$JS_PID" ] || kill "$JS_PID" 2>/dev/null || true
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT INT TERM

usage() {
    echo "usage: $0 [--runs N] [--core-count N] [--jetstream-count N] [--payload-sizes SIZES] [--batch-size N]"
    echo "       payload sizes are comma-separated, for example 128,4096"
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --runs) RUNS=$2; shift 2 ;;
        --core-count) CORE_COUNT=$2; shift 2 ;;
        --jetstream-count) JETSTREAM_COUNT=$2; shift 2 ;;
        --payload-sizes) PAYLOAD_SIZES=$2; shift 2 ;;
        --batch-size) BATCH_SIZE=$2; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) usage >&2; exit 2 ;;
    esac
done

wait_for_port() {
    local port=$1
    for _ in $(seq 1 100); do
        if (echo >/dev/tcp/127.0.0.1/$port) 2>/dev/null; then return 0; fi
        sleep 0.1
    done
    echo "server did not listen on $port" >&2
    return 1
}

echo "building official nats.go benchmark"
(
    cd "$ROOT_DIR/scripts/benchmark_go"
    go build -o "$GO_BIN" .
)
(cd "$ROOT_DIR" && rebar3 compile >/dev/null)

read -r CORE_PORT JS_PORT < <(python3 -c 'import socket; a=socket.socket(); a.bind(("127.0.0.1", 0)); p=a.getsockname()[1]; a.close(); b=socket.socket(); b.bind(("127.0.0.1", 0)); q=b.getsockname()[1]; b.close(); print(p, q)')

nats-server -a 127.0.0.1 -p "$CORE_PORT" >"$TMP_DIR/core.log" 2>&1 &
CORE_PID=$!
nats-server -a 127.0.0.1 -p "$JS_PORT" -js -sd "$TMP_DIR/jetstream" >"$TMP_DIR/js.log" 2>&1 &
JS_PID=$!
wait_for_port "$CORE_PORT"
wait_for_port "$JS_PORT"

run_case() {
    local mode=$1
    local count=$2
    local size=$3
    local port=$4
    local go_mode=${5:-$mode}
    local run output
    for run in $(seq 1 "$RUNS"); do
        output=$(ENATS_BENCHMARK_BATCH_SIZE="$BATCH_SIZE" escript "$ROOT_DIR/scripts/benchmark.escript" "$mode" "$port" "$count" "$size")
        printf 'run=%s client=erl %s\n' "$run" "$output" | tee -a "$RAW_FILE"
        if [ "$go_mode" != "-" ]; then
            output=$("$GO_BIN" "$go_mode" "nats://127.0.0.1:$port" "$count" "$size")
            printf 'run=%s %s\n' "$run" "$output" | tee -a "$RAW_FILE"
        fi
    done
}

IFS=',' read -r -a SIZES <<< "$PAYLOAD_SIZES"
for size in "${SIZES[@]}"; do
    for mode in direct pubsync request pubsub pubsubsync; do
        if [ "$mode" = "direct" ]; then
            run_case "$mode" "$CORE_COUNT" "$size" "$CORE_PORT" pub
        else
            run_case "$mode" "$CORE_COUNT" "$size" "$CORE_PORT"
        fi
    done
    run_case batch "$CORE_COUNT" "$size" "$CORE_PORT" -
    run_case jetstream "$JETSTREAM_COUNT" "$size" "$JS_PORT"
    run_case jetstream_msgid "$JETSTREAM_COUNT" "$size" "$JS_PORT"
done

echo
echo "median summary (throughput and latency fields)"
awk '
function remember(key, value, n) {
    n = ++count[key]
    values[key, n] = value
}
function median(key,   i, j, n, t) {
    n = count[key]
    for (i = 1; i <= n; i++) order[i] = values[key, i]
    for (i = 1; i <= n; i++) for (j = i + 1; j <= n; j++) {
        if (order[j] < order[i]) { t = order[i]; order[i] = order[j]; order[j] = t }
    }
    return order[int((n + 1) / 2)]
}
{
    client = mode = ""
    for (i = 1; i <= NF; i++) {
        split($i, pair, "=")
        if (pair[1] == "client") client = pair[2]
        if (pair[1] == "mode") mode = pair[2]
        if (pair[1] == "throughput_msg_s" || pair[1] == "p50_us" || pair[1] == "p90_us" || pair[1] == "p95_us" || pair[1] == "p99_us") {
            remember(client SUBSEP mode SUBSEP pair[1], pair[2])
        }
    }
}
END {
    for (key in count) {
        split(key, parts, SUBSEP)
        printf "%s mode=%s %s=%s\n", parts[1], parts[2], parts[3], median(key)
    }
}' "$RAW_FILE" | sort

echo
mkdir -p "$(dirname "$RESULT_FILE")"
cp "$RAW_FILE" "$RESULT_FILE"
echo "raw results: $RESULT_FILE"
