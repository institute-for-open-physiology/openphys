#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

usage() {
  cat <<'EOF'
Usage: scripts/start_services.sh [options]

Starts the local WAL-era training_sensors service stack:
  - H10 collector (training_sensors collect)
  - Rust WAL live dashboard/API (training_sensors live)
  - periodic one-shot archiver loop (training_sensors archive), enabled by default

CLI flags override environment variables (both --flag value and --flag=value work):
  --target VALUE                 H10 target id/name fragment        (env TARGET, default A1B2C3D4)
  --data-dir DIR                 collector/live/archive data dir    (env DATA_DIR, default data)
  --live-port N                  dashboard port                     (env LIVE_PORT, default 8000)
  --live-window-seconds N        live critical waveform window      (env LIVE_WINDOW_SECONDS, 20)
  --history-window-hours N       event/metrics history window       (env HISTORY_WINDOW_HOURS, 10)
  --scan-timeout-seconds N       collector scan timeout per attempt (env SCAN_TIMEOUT_SECONDS, 30)
  --archive-interval-seconds N   archive-loop sleep between passes  (env ARCHIVE_INTERVAL_SECONDS, 300)
  --bin PATH                     binary path                        (env BIN, target/debug/training_sensors)
  --pid-dir DIR                  pid-file directory                 (env PID_DIR, /tmp)
  --log-dir DIR                  log directory                      (env LOG_DIR, /tmp)
  --collector / --no-collector   start/skip the collector           (env START_COLLECTOR, default on)
  --live / --no-live             start/skip the dashboard           (env START_LIVE, default on)
  --archiver / --no-archiver     start/skip the archive loop        (env START_ARCHIVER, default on)
  --build / --no-build           cargo build before starting        (env BUILD, default on)
  --open-dashboard               open the browser dashboard         (env OPEN_DASHBOARD, default off)
  --allow-existing               skip the duplicate-process check   (env ALLOW_EXISTING_SERVICES)
  -h, --help                     show this help

Keeping a session's data separate (e.g. a different person using the one H10): stop the current
stack, then start it again pointed at a different data dir. The duplicate-collector guard still
prevents two collectors fighting over the single H10. Example:
  scripts/stop_services.sh
  scripts/start_services.sh --data-dir data_iva

Stop with: scripts/stop_services.sh   (pass DATA_DIR=... / --data-dir to it for the final archive pass)
EOF
}

TARGET="${TARGET:-A1B2C3D4}"
DATA_DIR="${DATA_DIR:-data}"
LIVE_PORT="${LIVE_PORT:-8000}"
LIVE_WINDOW_SECONDS="${LIVE_WINDOW_SECONDS:-20}"
HISTORY_WINDOW_HOURS="${HISTORY_WINDOW_HOURS:-10}"
SCAN_TIMEOUT_SECONDS="${SCAN_TIMEOUT_SECONDS:-30}"
START_COLLECTOR="${START_COLLECTOR:-1}"
START_LIVE="${START_LIVE:-1}"
START_ARCHIVER="${START_ARCHIVER:-1}"
ARCHIVE_INTERVAL_SECONDS="${ARCHIVE_INTERVAL_SECONDS:-300}"
BUILD="${BUILD:-1}"
BIN="${BIN:-target/debug/training_sensors}"
PID_DIR="${PID_DIR:-/tmp}"
LOG_DIR="${LOG_DIR:-/tmp}"
OPEN_DASHBOARD="${OPEN_DASHBOARD:-0}"
START_CHECK_DELAY_SECONDS="${START_CHECK_DELAY_SECONDS:-1}"
ALLOW_EXISTING_SERVICES="${ALLOW_EXISTING_SERVICES:-0}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --target) TARGET="${2?--target requires a value}"; shift 2 ;;
    --target=*) TARGET="${1#*=}"; shift ;;
    --data-dir) DATA_DIR="${2?--data-dir requires a value}"; shift 2 ;;
    --data-dir=*) DATA_DIR="${1#*=}"; shift ;;
    --live-port) LIVE_PORT="${2?--live-port requires a value}"; shift 2 ;;
    --live-port=*) LIVE_PORT="${1#*=}"; shift ;;
    --live-window-seconds) LIVE_WINDOW_SECONDS="${2?--live-window-seconds requires a value}"; shift 2 ;;
    --live-window-seconds=*) LIVE_WINDOW_SECONDS="${1#*=}"; shift ;;
    --history-window-hours) HISTORY_WINDOW_HOURS="${2?--history-window-hours requires a value}"; shift 2 ;;
    --history-window-hours=*) HISTORY_WINDOW_HOURS="${1#*=}"; shift ;;
    --scan-timeout-seconds) SCAN_TIMEOUT_SECONDS="${2?--scan-timeout-seconds requires a value}"; shift 2 ;;
    --scan-timeout-seconds=*) SCAN_TIMEOUT_SECONDS="${1#*=}"; shift ;;
    --archive-interval-seconds) ARCHIVE_INTERVAL_SECONDS="${2?--archive-interval-seconds requires a value}"; shift 2 ;;
    --archive-interval-seconds=*) ARCHIVE_INTERVAL_SECONDS="${1#*=}"; shift ;;
    --bin) BIN="${2?--bin requires a value}"; shift 2 ;;
    --bin=*) BIN="${1#*=}"; shift ;;
    --pid-dir) PID_DIR="${2?--pid-dir requires a value}"; shift 2 ;;
    --pid-dir=*) PID_DIR="${1#*=}"; shift ;;
    --log-dir) LOG_DIR="${2?--log-dir requires a value}"; shift 2 ;;
    --log-dir=*) LOG_DIR="${1#*=}"; shift ;;
    --collector) START_COLLECTOR=1; shift ;;
    --no-collector) START_COLLECTOR=0; shift ;;
    --live) START_LIVE=1; shift ;;
    --no-live) START_LIVE=0; shift ;;
    --archiver) START_ARCHIVER=1; shift ;;
    --no-archiver) START_ARCHIVER=0; shift ;;
    --build) BUILD=1; shift ;;
    --no-build) BUILD=0; shift ;;
    --open-dashboard) OPEN_DASHBOARD=1; shift ;;
    --allow-existing) ALLOW_EXISTING_SERVICES=1; shift ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

PID_DIR="${PID_DIR%/}"
LOG_DIR="${LOG_DIR%/}"
COLLECTOR_PID_FILE="${COLLECTOR_PID_FILE:-$PID_DIR/training_sensors_collector.pid}"
LIVE_PID_FILE="${LIVE_PID_FILE:-$PID_DIR/training_sensors_live.pid}"
ARCHIVER_PID_FILE="${ARCHIVER_PID_FILE:-$PID_DIR/training_sensors_archiver.pid}"
COLLECTOR_LOG="${COLLECTOR_LOG:-$LOG_DIR/training_sensors_collector.log}"
LIVE_LOG="${LIVE_LOG:-$LOG_DIR/training_sensors_live.log}"
ARCHIVER_LOG="${ARCHIVER_LOG:-$LOG_DIR/training_sensors_archiver.log}"

require_positive_int() {
  local name="$1"
  local value="$2"
  case "$value" in
    ''|*[!0-9]*)
      echo "$name must be a positive integer, got '$value'" >&2
      exit 2
      ;;
  esac
  if [[ "$value" -le 0 ]]; then
    echo "$name must be > 0, got '$value'" >&2
    exit 2
  fi
}

require_bool() {
  local name="$1"
  local value="$2"
  if [[ "$value" != "0" && "$value" != "1" ]]; then
    echo "$name must be 0 or 1, got '$value'" >&2
    exit 2
  fi
}

require_positive_number() {
  local name="$1"
  local value="$2"
  python3 - "$name" "$value" <<'PY'
import math
import sys
name, value = sys.argv[1], sys.argv[2]
try:
    parsed = float(value)
except ValueError:
    raise SystemExit(f"{name} must be a positive finite number, got {value!r}")
if not math.isfinite(parsed) or parsed <= 0:
    raise SystemExit(f"{name} must be a positive finite number, got {value!r}")
PY
}

require_positive_int LIVE_PORT "$LIVE_PORT"
if [[ "$LIVE_PORT" -gt 65535 ]]; then
  echo "LIVE_PORT must be <= 65535, got '$LIVE_PORT'" >&2
  exit 2
fi
require_positive_int LIVE_WINDOW_SECONDS "$LIVE_WINDOW_SECONDS"
require_positive_int SCAN_TIMEOUT_SECONDS "$SCAN_TIMEOUT_SECONDS"
require_positive_int ARCHIVE_INTERVAL_SECONDS "$ARCHIVE_INTERVAL_SECONDS"
require_positive_int START_CHECK_DELAY_SECONDS "$START_CHECK_DELAY_SECONDS"
require_positive_number HISTORY_WINDOW_HOURS "$HISTORY_WINDOW_HOURS"
require_bool START_COLLECTOR "$START_COLLECTOR"
require_bool START_LIVE "$START_LIVE"
require_bool START_ARCHIVER "$START_ARCHIVER"
require_bool BUILD "$BUILD"
require_bool OPEN_DASHBOARD "$OPEN_DASHBOARD"
require_bool ALLOW_EXISTING_SERVICES "$ALLOW_EXISTING_SERVICES"
if [[ -z "$DATA_DIR" ]]; then
  echo "DATA_DIR/--data-dir must not be empty" >&2
  exit 2
fi

mkdir -p "$PID_DIR" "$LOG_DIR" "$DATA_DIR"

is_pid_running() {
  local pid="${1:-}"
  [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1
}

read_pid_file() {
  local pid_file="$1"
  if [[ -f "$pid_file" ]]; then
    tr -d '[:space:]' <"$pid_file"
  fi
}

STARTED_PIDS=()
STARTED_PID_FILES=()
STARTED_NAMES=()
STARTUP_COMPLETE=0

rollback_started_services() {
  local status=$?
  if [[ "$STARTUP_COMPLETE" == "1" || "$status" -eq 0 ]]; then
    exit "$status"
  fi
  if [[ "${#STARTED_PIDS[@]}" -eq 0 ]]; then
    exit "$status"
  fi

  echo "Startup failed; stopping services started by this invocation..." >&2
  local idx pid pid_file service
  for ((idx = ${#STARTED_PIDS[@]} - 1; idx >= 0; idx--)); do
    pid="${STARTED_PIDS[$idx]}"
    pid_file="${STARTED_PID_FILES[$idx]}"
    service="${STARTED_NAMES[$idx]}"
    if is_pid_running "$pid"; then
      echo "Rollback: SIGINT $service PID $pid" >&2
      kill -INT "$pid" >/dev/null 2>&1 || true
      local loops=20
      local i
      for ((i = 0; i < loops; i++)); do
        if ! is_pid_running "$pid"; then
          break
        fi
        sleep 0.5
      done
      if is_pid_running "$pid"; then
        echo "Rollback: SIGTERM $service PID $pid" >&2
        kill -TERM "$pid" >/dev/null 2>&1 || true
      fi
    fi
    if ! is_pid_running "$pid" && [[ -f "$pid_file" ]]; then
      rm -f "$pid_file"
    fi
  done
  exit "$status"
}
trap rollback_started_services EXIT

assert_pidfile_free() {
  local service="$1"
  local pid_file="$2"
  local pid=""
  pid="$(read_pid_file "$pid_file" || true)"
  if is_pid_running "$pid"; then
    echo "Refusing to start $service: PID file $pid_file points to running PID $pid" >&2
    echo "Use scripts/stop_services.sh first, or remove the PID file if it is truly stale." >&2
    exit 2
  fi
  if [[ -f "$pid_file" ]]; then
    echo "Removing stale $service PID file $pid_file"
    rm -f "$pid_file"
  fi
}

require_no_matching_process() {
  local service="$1"
  local pattern="$2"
  if [[ "$ALLOW_EXISTING_SERVICES" == "1" ]]; then
    return
  fi
  local matches=""
  matches="$(pgrep -fl "$pattern" 2>/dev/null || true)"
  if [[ -n "$matches" ]]; then
    echo "Refusing to start $service: matching process already exists:" >&2
    echo "$matches" >&2
    echo "Set ALLOW_EXISTING_SERVICES=1 / pass --allow-existing to skip this safety check, but duplicate BLE/live services are usually a mistake." >&2
    echo "To collect a different person's session into a separate data dir, stop the current stack first, then start with --data-dir." >&2
    exit 2
  fi
}

require_port_free() {
  local port="$1"
  python3 - "$port" <<'PY'
import socket
import sys
port = int(sys.argv[1])
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.settimeout(0.25)
    busy = sock.connect_ex(("127.0.0.1", port)) == 0
if busy:
    raise SystemExit(1)
PY
}

start_process() {
  local service="$1"
  local pid_file="$2"
  local log_file="$3"
  shift 3

  echo "Starting $service; log: $log_file"
  "$@" >"$log_file" 2>&1 &
  local pid=$!
  echo "$pid" >"$pid_file"

  sleep "$START_CHECK_DELAY_SECONDS"
  if ! is_pid_running "$pid"; then
    local status=0
    wait "$pid" || status=$?
    if [[ "$status" -eq 0 ]]; then
      status=1
    fi
    rm -f "$pid_file"
    echo "$service exited during startup with status $status" >&2
    echo "Last log lines from $log_file:" >&2
    tail -n 80 "$log_file" >&2 || true
    exit "$status"
  fi
  STARTED_PIDS+=("$pid")
  STARTED_PID_FILES+=("$pid_file")
  STARTED_NAMES+=("$service")
  echo "Started $service PID $pid (PID file: $pid_file)"
}

archive_loop() {
  local archive_child_pid=""
  local sleep_child_pid=""
  stop_archive_loop() {
    echo "$(date -Iseconds) archiver loop stopping"
    if is_pid_running "$archive_child_pid"; then
      kill -INT "$archive_child_pid" >/dev/null 2>&1 || true
      wait "$archive_child_pid" >/dev/null 2>&1 || true
    fi
    if is_pid_running "$sleep_child_pid"; then
      kill "$sleep_child_pid" >/dev/null 2>&1 || true
      wait "$sleep_child_pid" >/dev/null 2>&1 || true
    fi
    exit 0
  }
  trap stop_archive_loop INT TERM

  echo "$(date -Iseconds) archiver loop starting; interval=${ARCHIVE_INTERVAL_SECONDS}s data_dir=$DATA_DIR"
  while true; do
    echo "$(date -Iseconds) archive pass starting"
    "$BIN" archive --data-dir "$DATA_DIR" &
    archive_child_pid=$!
    if wait "$archive_child_pid"; then
      echo "$(date -Iseconds) archive pass completed"
    else
      status=$?
      echo "$(date -Iseconds) archive pass failed status=$status" >&2
    fi
    archive_child_pid=""

    sleep "$ARCHIVE_INTERVAL_SECONDS" &
    sleep_child_pid=$!
    wait "$sleep_child_pid" || true
    sleep_child_pid=""
  done
}

COLLECTOR_PATTERN='training_sensors[[:space:]]+collect|cargo[[:space:]]+run.*--[[:space:]]+collect'
LIVE_PATTERN='training_sensors[[:space:]]+live|cargo[[:space:]]+run.*--[[:space:]]+live'

if [[ "$BUILD" == "1" ]]; then
  echo "Building $BIN..."
  cargo build
fi

if [[ ! -x "$BIN" ]]; then
  echo "Binary $BIN is not executable; set BIN=... / --bin, or enable --build" >&2
  exit 2
fi

if [[ "$START_COLLECTOR" == "1" ]]; then
  assert_pidfile_free "collector" "$COLLECTOR_PID_FILE"
  require_no_matching_process "collector" "$COLLECTOR_PATTERN"
fi
if [[ "$START_LIVE" == "1" ]]; then
  assert_pidfile_free "live dashboard" "$LIVE_PID_FILE"
  require_no_matching_process "live dashboard" "$LIVE_PATTERN"
  if ! require_port_free "$LIVE_PORT"; then
    echo "Refusing to start live dashboard: 127.0.0.1:$LIVE_PORT is already accepting connections" >&2
    exit 2
  fi
fi
if [[ "$START_ARCHIVER" == "1" ]]; then
  assert_pidfile_free "archiver loop" "$ARCHIVER_PID_FILE"
fi

if [[ "$START_COLLECTOR" == "1" ]]; then
  start_process \
    "collector" \
    "$COLLECTOR_PID_FILE" \
    "$COLLECTOR_LOG" \
    env RUST_BACKTRACE=full "$BIN" collect \
      --target "$TARGET" \
      --data-dir "$DATA_DIR" \
      --scan-timeout-seconds "$SCAN_TIMEOUT_SECONDS"
fi

if [[ "$START_LIVE" == "1" ]]; then
  start_process \
    "live dashboard" \
    "$LIVE_PID_FILE" \
    "$LIVE_LOG" \
    "$BIN" live \
      --data-dir "$DATA_DIR" \
      --port "$LIVE_PORT" \
      --live-window-seconds "$LIVE_WINDOW_SECONDS" \
      --history-window-hours "$HISTORY_WINDOW_HOURS"
fi

if [[ "$START_ARCHIVER" == "1" ]]; then
  echo "Starting archiver loop; log: $ARCHIVER_LOG"
  archive_loop >"$ARCHIVER_LOG" 2>&1 &
  archiver_pid=$!
  echo "$archiver_pid" >"$ARCHIVER_PID_FILE"
  sleep "$START_CHECK_DELAY_SECONDS"
  if ! is_pid_running "$archiver_pid"; then
    status=0
    wait "$archiver_pid" || status=$?
    if [[ "$status" -eq 0 ]]; then
      status=1
    fi
    rm -f "$ARCHIVER_PID_FILE"
    echo "archiver loop exited during startup with status $status" >&2
    tail -n 80 "$ARCHIVER_LOG" >&2 || true
    exit "$status"
  fi
  STARTED_PIDS+=("$archiver_pid")
  STARTED_PID_FILES+=("$ARCHIVER_PID_FILE")
  STARTED_NAMES+=("archiver loop")
  echo "Started archiver loop PID $archiver_pid (PID file: $ARCHIVER_PID_FILE)"
fi

if [[ "$OPEN_DASHBOARD" == "1" && "$START_LIVE" == "1" ]]; then
  if command -v open >/dev/null 2>&1; then
    open "http://127.0.0.1:$LIVE_PORT/" >/dev/null 2>&1 || true
  fi
fi

pid_or_not_started() {
  local pid_file="$1"
  local pid=""
  pid="$(read_pid_file "$pid_file" || true)"
  if [[ -n "$pid" ]]; then
    echo "$pid"
  else
    echo "not started"
  fi
}

STARTUP_COMPLETE=1
trap - EXIT

cat <<EOF

Services started.
  collector pid: $(pid_or_not_started "$COLLECTOR_PID_FILE")
  live pid:      $(pid_or_not_started "$LIVE_PID_FILE")
  archiver pid:  $(pid_or_not_started "$ARCHIVER_PID_FILE")
  data dir:      $DATA_DIR
  dashboard:     http://127.0.0.1:$LIVE_PORT/

Logs:
  collector: $COLLECTOR_LOG
  live:      $LIVE_LOG
  archiver:  $ARCHIVER_LOG

Stop with:
  scripts/stop_services.sh --data-dir $DATA_DIR
EOF
