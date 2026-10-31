#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

usage() {
  cat <<'EOF'
Usage: scripts/stop_services.sh

Orderly shutdown for the local WAL-era training_sensors service stack.

By default it:
  1. SIGINTs the H10 collector and waits for clean BLE/WAL shutdown.
  2. SIGINTs the Rust live dashboard so manual-marker WAL closes cleanly.
  3. SIGTERMs the periodic archiver loop (its trap stops any child archive pass) and SIGINTs any untracked one-shot archiver.
  4. Runs one final one-shot archive pass over newly closed WAL segments.

Configuration is by environment variables:
  DATA_DIR=data
  BIN=target/debug/training_sensors
  PID_DIR=/tmp
  LOG_DIR=/tmp
  STOP_TIMEOUT_SECONDS=20       wait after SIGINT before escalating
  TERM_TIMEOUT_SECONDS=5        wait after SIGTERM before SIGKILL
  STOP_UNTRACKED=1              also stop matching services without PID files
  FINAL_ARCHIVE=1               run final archive pass after shutdown

Set FINAL_ARCHIVE=0 to only stop processes.
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi
if [[ "$#" -gt 0 ]]; then
  usage >&2
  exit 2
fi

DATA_DIR="${DATA_DIR:-data}"
BIN="${BIN:-target/debug/training_sensors}"
PID_DIR="${PID_DIR:-/tmp}"
LOG_DIR="${LOG_DIR:-/tmp}"
STOP_TIMEOUT_SECONDS="${STOP_TIMEOUT_SECONDS:-20}"
TERM_TIMEOUT_SECONDS="${TERM_TIMEOUT_SECONDS:-5}"
STOP_UNTRACKED="${STOP_UNTRACKED:-1}"
FINAL_ARCHIVE="${FINAL_ARCHIVE:-1}"

COLLECTOR_PID_FILE="${COLLECTOR_PID_FILE:-$PID_DIR/training_sensors_collector.pid}"
LIVE_PID_FILE="${LIVE_PID_FILE:-$PID_DIR/training_sensors_live.pid}"
ARCHIVER_PID_FILE="${ARCHIVER_PID_FILE:-$PID_DIR/training_sensors_archiver.pid}"
ARCHIVER_LOG="${ARCHIVER_LOG:-$LOG_DIR/training_sensors_archiver.log}"

require_nonnegative_int() {
  local name="$1"
  local value="$2"
  case "$value" in
    ''|*[!0-9]*)
      echo "$name must be a non-negative integer, got '$value'" >&2
      exit 2
      ;;
  esac
}

require_bool() {
  local name="$1"
  local value="$2"
  if [[ "$value" != "0" && "$value" != "1" ]]; then
    echo "$name must be 0 or 1, got '$value'" >&2
    exit 2
  fi
}

require_nonnegative_int STOP_TIMEOUT_SECONDS "$STOP_TIMEOUT_SECONDS"
require_nonnegative_int TERM_TIMEOUT_SECONDS "$TERM_TIMEOUT_SECONDS"
require_bool STOP_UNTRACKED "$STOP_UNTRACKED"
require_bool FINAL_ARCHIVE "$FINAL_ARCHIVE"
mkdir -p "$LOG_DIR"

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

pid_command() {
  local pid="$1"
  ps -p "$pid" -o command= 2>/dev/null || true
}

pid_matches_pattern() {
  local pid="$1"
  local pattern="$2"
  local command=""
  command="$(pid_command "$pid")"
  [[ -n "$command" && "$command" =~ $pattern ]]
}

wait_until_exited() {
  local pid="$1"
  local timeout_seconds="$2"
  local loops=$((timeout_seconds * 2))
  local i
  for ((i = 0; i < loops; i++)); do
    if ! is_pid_running "$pid"; then
      return 0
    fi
    sleep 0.5
  done
  ! is_pid_running "$pid"
}

stop_pid() {
  local service="$1"
  local pid="$2"
  local initial_signal="${3:-INT}"
  if ! is_pid_running "$pid"; then
    echo "$service PID $pid is not running"
    return 0
  fi

  echo "Stopping $service PID $pid with SIG$initial_signal"
  kill -"$initial_signal" "$pid" >/dev/null 2>&1 || true
  if wait_until_exited "$pid" "$STOP_TIMEOUT_SECONDS"; then
    echo "$service PID $pid stopped"
    return 0
  fi

  echo "$service PID $pid did not stop after ${STOP_TIMEOUT_SECONDS}s; sending SIGTERM" >&2
  kill -TERM "$pid" >/dev/null 2>&1 || true
  if wait_until_exited "$pid" "$TERM_TIMEOUT_SECONDS"; then
    echo "$service PID $pid stopped after SIGTERM"
    return 0
  fi

  echo "$service PID $pid did not stop after SIGTERM; sending SIGKILL" >&2
  kill -KILL "$pid" >/dev/null 2>&1 || true
  if wait_until_exited "$pid" 2; then
    echo "$service PID $pid killed"
    return 0
  fi

  echo "Failed to stop $service PID $pid" >&2
  return 1
}

stop_pid_file() {
  local service="$1"
  local pid_file="$2"
  local expected_pattern="$3"
  local initial_signal="${4:-INT}"
  local pid=""
  pid="$(read_pid_file "$pid_file" || true)"
  if [[ -z "$pid" ]]; then
    if [[ -f "$pid_file" ]]; then
      echo "Removing empty/stale $service PID file $pid_file"
      rm -f "$pid_file"
    else
      echo "No tracked $service PID file at $pid_file"
    fi
    return 0
  fi

  if is_pid_running "$pid" && ! pid_matches_pattern "$pid" "$expected_pattern"; then
    echo "Not signalling $service PID $pid from $pid_file: command does not match expected service pattern" >&2
    echo "  command: $(pid_command "$pid")" >&2
    echo "Removing stale/suspicious PID file $pid_file; untracked-process scan will still stop real matching services if enabled." >&2
    rm -f "$pid_file"
    return 0
  fi

  stop_pid "$service" "$pid" "$initial_signal" || return $?
  if ! is_pid_running "$pid" && [[ -f "$pid_file" ]]; then
    rm -f "$pid_file"
  fi
}

stop_matching_processes() {
  local service="$1"
  local pattern="$2"
  local pids=""
  pids="$(pgrep -f "$pattern" 2>/dev/null || true)"
  if [[ -z "$pids" ]]; then
    return 0
  fi
  local pid
  for pid in $pids; do
    if [[ "$pid" == "$$" || "$pid" == "${PPID:-}" ]]; then
      continue
    fi
    stop_pid "$service (untracked)" "$pid" || return $?
  done
}

COLLECTOR_PATTERN='training_sensors[[:space:]]+collect|cargo[[:space:]]+run.*--[[:space:]]+collect'
LIVE_PATTERN='training_sensors[[:space:]]+live|cargo[[:space:]]+run.*--[[:space:]]+live'
ARCHIVE_PATTERN='training_sensors[[:space:]]+archive|cargo[[:space:]]+run.*--[[:space:]]+archive'
ARCHIVER_LOOP_PATTERN='start_services[.]sh|training_sensors_archiver_loop'

shutdown_status=0

# Stop the BLE owner first so the H10 gets a clean PMD stop/disconnect and the
# source=chest_h10 WAL segment is closed before the optional final archive pass.
stop_pid_file "collector" "$COLLECTOR_PID_FILE" "$COLLECTOR_PATTERN" || shutdown_status=$?
if [[ "$STOP_UNTRACKED" == "1" ]]; then
  stop_matching_processes "collector" "$COLLECTOR_PATTERN" || shutdown_status=$?
fi

# Stop live next so source=manual_events WAL is also closed before final archive.
stop_pid_file "live dashboard" "$LIVE_PID_FILE" "$LIVE_PATTERN" || shutdown_status=$?
if [[ "$STOP_UNTRACKED" == "1" ]]; then
  stop_matching_processes "live dashboard" "$LIVE_PATTERN" || shutdown_status=$?
fi

# Stop any long-lived archive loop/current one-shot before running the final pass.
stop_pid_file "archiver loop" "$ARCHIVER_PID_FILE" "$ARCHIVER_LOOP_PATTERN" TERM || shutdown_status=$?
if [[ "$STOP_UNTRACKED" == "1" ]]; then
  stop_matching_processes "archiver" "$ARCHIVE_PATTERN" || shutdown_status=$?
fi

final_archive_status=0
if [[ "$FINAL_ARCHIVE" == "1" ]]; then
  if [[ ! -x "$BIN" ]]; then
    echo "Skipping final archive: binary $BIN is not executable" >&2
    final_archive_status=2
  else
    echo "Running final archive pass; log: $ARCHIVER_LOG"
    if "$BIN" archive --data-dir "$DATA_DIR" >>"$ARCHIVER_LOG" 2>&1; then
      echo "Final archive pass completed"
    else
      final_archive_status=$?
      echo "Final archive pass failed with status $final_archive_status; see $ARCHIVER_LOG" >&2
    fi
  fi
fi

if [[ "$shutdown_status" -ne 0 ]]; then
  exit "$shutdown_status"
fi
exit "$final_archive_status"
