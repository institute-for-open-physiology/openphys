#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

TARGET="${TARGET:-${1:-A1B2C3D4}}"
DURATION_SECONDS="${DURATION_SECONDS:-25}"
SCAN_TIMEOUT_SECONDS="${SCAN_TIMEOUT_SECONDS:-20}"
SMOKE_BASE="${SMOKE_BASE:-data/live_smoke}"
ALLOW_EXISTING_COLLECTOR="${ALLOW_EXISTING_COLLECTOR:-0}"
LOG_DIR="${LOG_DIR:-/tmp}"
STAMP="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="$SMOKE_BASE/wal_smoke_$STAMP"
LOG_PATH="$LOG_DIR/training_sensors_live_smoke_$STAMP.log"
LIVE_LOG_PATH="$LOG_DIR/training_sensors_live_dashboard_smoke_$STAMP.log"
LIVE_PID=""

if [[ "$ALLOW_EXISTING_COLLECTOR" != "1" ]]; then
  if pgrep -fl "target/debug/training_sensors collect|cargo run -- collect" >/tmp/training_sensors_live_smoke_pgrep.$$ 2>/dev/null; then
    echo "Refusing to run while another collector appears active:" >&2
    cat /tmp/training_sensors_live_smoke_pgrep.$$ >&2
    rm -f /tmp/training_sensors_live_smoke_pgrep.$$
    echo "Set ALLOW_EXISTING_COLLECTOR=1 to override, but BLE conflicts are likely." >&2
    exit 2
  fi
  rm -f /tmp/training_sensors_live_smoke_pgrep.$$
fi

mkdir -p "$RUN_DIR"

echo "Building collector..."
cargo build

echo "Starting WAL smoke collector for target $TARGET; log: $LOG_PATH"
RUST_BACKTRACE=full target/debug/training_sensors collect \
  --target "$TARGET" \
  --data-dir "$RUN_DIR" \
  --scan-timeout-seconds "$SCAN_TIMEOUT_SECONDS" \
  >"$LOG_PATH" 2>&1 &
PID=$!

cleanup_pid() {
  local pid="$1"
  if [[ -z "$pid" ]]; then
    return
  fi
  if kill -0 "$pid" >/dev/null 2>&1; then
    kill -INT "$pid" >/dev/null 2>&1 || true
    for _ in {1..20}; do
      if ! kill -0 "$pid" >/dev/null 2>&1; then
        return
      fi
      sleep 0.5
    done
    kill "$pid" >/dev/null 2>&1 || true
  fi
}

cleanup() {
  cleanup_pid "$PID"
  cleanup_pid "$LIVE_PID"
}
trap cleanup EXIT

sleep "$DURATION_SECONDS"
cleanup
wait "$PID" || true
trap - EXIT

WAL_SOURCE="$RUN_DIR/wal/participant_id=local/source=chest_h10"
if [[ ! -d "$WAL_SOURCE" ]]; then
  echo "Smoke failed: missing WAL source directory $WAL_SOURCE" >&2
  echo "Collector log: $LOG_PATH" >&2
  exit 1
fi

OPEN_COUNT=$(find "$WAL_SOURCE" -name '*.open' | wc -l | tr -d ' ')
if [[ "$OPEN_COUNT" != "0" ]]; then
  echo "Smoke failed: expected clean Ctrl-C to close all WAL files, found $OPEN_COUNT .open files" >&2
  find "$WAL_SOURCE" -name '*.open' >&2
  echo "Collector log: $LOG_PATH" >&2
  exit 1
fi

CLOSED_WALS=()
while IFS= read -r path; do
  CLOSED_WALS+=("$path")
done < <(find "$WAL_SOURCE" -name '*.closed' | sort)
if [[ "${#CLOSED_WALS[@]}" -lt 1 ]]; then
  echo "Smoke failed: no closed WAL segments under $WAL_SOURCE" >&2
  echo "Collector log: $LOG_PATH" >&2
  exit 1
fi

python3 - "${CLOSED_WALS[@]}" <<'PY'
import json
import struct
import sys
from collections import Counter
from pathlib import Path

POLY = 0x82F63B78


def crc32c(data: bytes) -> int:
    crc = 0xFFFFFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ POLY
            else:
                crc >>= 1
            crc &= 0xFFFFFFFF
    return crc ^ 0xFFFFFFFF


def read_records(path: Path):
    data = path.read_bytes()
    if len(data) < 64 or data[:8] != b"TSWALV1\n":
        raise SystemExit(f"Smoke failed: bad WAL header in {path}")
    offset = 64
    records = []
    while offset < len(data):
        if offset + 8 > len(data):
            raise SystemExit(f"Smoke failed: incomplete frame header in closed WAL {path}")
        payload_len, expected_crc = struct.unpack_from("<II", data, offset)
        offset += 8
        payload = data[offset:offset + payload_len]
        if len(payload) != payload_len:
            raise SystemExit(f"Smoke failed: incomplete payload in closed WAL {path}")
        actual_crc = crc32c(payload)
        if actual_crc != expected_crc:
            raise SystemExit(f"Smoke failed: checksum mismatch in {path}")
        records.append(json.loads(payload.decode("utf-8")))
        offset += payload_len
    return records

records = []
for arg in sys.argv[1:]:
    path = Path(arg)
    segment_records = read_records(path)
    print(f"wal {path} records={len(segment_records)}")
    records.extend(segment_records)

kinds = Counter(record.get("kind") for record in records)
for kind in [
    "segment_started",
    "session_started",
    "scan_attempt",
    "scan_result",
    "connect_attempt",
    "connect_result",
    "service_discovery_summary",
    "subscription_attempt",
    "subscription_result",
    "gatt_read_request",
    "gatt_read_response",
    "gatt_write_request",
    "gatt_write_result",
    "gatt_notification",
    "psftp_set_local_time_attempt",
    "psftp_set_local_time_result",
    "rssi_sample",
    "collector_event",
    "session_ended",
    "segment_closed",
]:
    print(f"kind {kind} count={kinds[kind]}")
    if kinds[kind] < 1:
        raise SystemExit(f"Smoke failed: no WAL records of kind {kind}")

notifications = [record for record in records if record.get("kind") == "gatt_notification"]
descriptions = Counter((record.get("body") or {}).get("description") for record in notifications)
for description in ["pmd_data", "heart_rate_measurement", "pmd_control"]:
    print(f"notification {description} count={descriptions[description]}")
    if descriptions[description] < 1:
        raise SystemExit(f"Smoke failed: no {description} GATT notifications in WAL")

collector_events = [record.get("body") or {} for record in records if record.get("kind") == "collector_event"]
event_names = {body.get("event") for body in collector_events}
for event in ["ctrl_c", "psftp_set_local_time_success", "pmd_stream_start_confirmed"]:
    if event not in event_names:
        raise SystemExit(f"Smoke failed: collector event {event!r} not found in WAL")

PY

echo "Running WAL archiver smoke pass..."
target/debug/training_sensors archive --data-dir "$RUN_DIR" >>"$LOG_PATH" 2>&1
PARQUET_COUNT=$(find "$RUN_DIR/archive/parquet" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')
SIDECAR_COUNT=$(find "$WAL_SOURCE" -name '*.archived.json' 2>/dev/null | wc -l | tr -d ' ')
if [[ "$PARQUET_COUNT" -lt 1 || "$SIDECAR_COUNT" -lt 1 ]]; then
  echo "Smoke failed: archiver did not produce Parquet files and WAL sidecars" >&2
  echo "parquet_count=$PARQUET_COUNT sidecar_count=$SIDECAR_COUNT" >&2
  echo "Collector/archiver log: $LOG_PATH" >&2
  exit 1
fi

echo "Running WAL replay smoke pass..."
REPLAY_SUMMARY="$RUN_DIR/replay_summary.json"
target/debug/training_sensors replay --from-wal "$WAL_SOURCE" --output summary >"$REPLAY_SUMMARY"
python3 - "$REPLAY_SUMMARY" <<'PY'
import json
import sys
from pathlib import Path
summary = json.loads(Path(sys.argv[1]).read_text())
print("replay summary", summary)
if summary.get("segments", 0) < 1:
    raise SystemExit("Smoke failed: replay saw no WAL segments")
if summary.get("replay_events", 0) < 1:
    raise SystemExit("Smoke failed: replay emitted no parser events")
if summary.get("tail_issue_segments", 0) != 0:
    raise SystemExit("Smoke failed: replay reported tail issues for closed WAL")
PY

LIVE_PORT="${LIVE_PORT:-$(python3 - <<'PY'
import socket
with socket.socket() as s:
    s.bind(('127.0.0.1', 0))
    print(s.getsockname()[1])
PY
)}"
echo "Starting Rust WAL live dashboard smoke on port $LIVE_PORT; log: $LIVE_LOG_PATH"
target/debug/training_sensors live --data-dir "$RUN_DIR" --port "$LIVE_PORT" >"$LIVE_LOG_PATH" 2>&1 &
LIVE_PID=$!
trap cleanup EXIT
python3 - "$LIVE_PORT" <<'PY'
import json
import sys
import time
import urllib.error
import urllib.request

port = int(sys.argv[1])
base = f"http://127.0.0.1:{port}"
latest = None
last_error = None
for _ in range(80):
    try:
        with urllib.request.urlopen(base + "/api/latest", timeout=1) as response:
            latest = json.loads(response.read().decode("utf-8"))
        break
    except Exception as exc:  # noqa: BLE001 - diagnostic smoke script
        last_error = exc
        time.sleep(0.25)
if latest is None:
    raise SystemExit(f"Smoke failed: live dashboard did not answer /api/latest: {last_error}")
print("live status", latest.get("status"))
status = latest.get("status") or {}
if not status.get("marker_enabled"):
    raise SystemExit(f"Smoke failed: live marker path disabled: {status}")
if not latest.get("ecg") or not latest.get("acc") or not latest.get("hr"):
    raise SystemExit("Smoke failed: live dashboard did not parse ECG/ACC/HR from WAL")
request = urllib.request.Request(
    base + "/api/marker",
    data=json.dumps({"label": "smoke_marker", "client_id": "live_h10_smoke"}).encode("utf-8"),
    headers={"Content-Type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(request, timeout=3) as response:
    marker = json.loads(response.read().decode("utf-8"))
print("marker response", marker)
if marker.get("label") != "smoke_marker":
    raise SystemExit(f"Smoke failed: unexpected marker response {marker}")
PY
cleanup_pid "$LIVE_PID"
wait "$LIVE_PID" || true
LIVE_PID=""

MANUAL_SOURCE="$RUN_DIR/wal/participant_id=local/source=manual_events"
python3 - "$MANUAL_SOURCE" <<'PY'
import json
import struct
import sys
from pathlib import Path

POLY = 0x82F63B78

def crc32c(data: bytes) -> int:
    crc = 0xFFFFFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = ((crc >> 1) ^ POLY) & 0xFFFFFFFF if (crc & 1) else (crc >> 1) & 0xFFFFFFFF
    return crc ^ 0xFFFFFFFF

def read_records(path: Path):
    data = path.read_bytes()
    if len(data) < 64 or data[:8] != b"TSWALV1\n":
        raise SystemExit(f"Smoke failed: bad manual WAL header in {path}")
    offset = 64
    while offset + 8 <= len(data):
        payload_len, expected_crc = struct.unpack_from("<II", data, offset)
        offset += 8
        payload = data[offset:offset + payload_len]
        if len(payload) != payload_len:
            return
        if crc32c(payload) != expected_crc:
            raise SystemExit(f"Smoke failed: checksum mismatch in manual WAL {path}")
        yield json.loads(payload.decode("utf-8"))
        offset += payload_len

root = Path(sys.argv[1])
records = []
for path in sorted(root.rglob("wal-*.closed")) + sorted(root.rglob("wal-*.open")):
    records.extend(read_records(path))
if not any(r.get("kind") == "manual_marker" and (r.get("body") or {}).get("label") == "smoke_marker" for r in records):
    raise SystemExit("Smoke failed: marker POST did not create a manual_marker WAL record")
PY

echo "Live H10 WAL+archive+replay+dashboard smoke test passed: $RUN_DIR"
echo "Collector/archiver log: $LOG_PATH"
echo "Live dashboard log: $LIVE_LOG_PATH"
