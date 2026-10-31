# Record on a computer

Rust-based Polar H10 collector. Current production `collect` is intentionally narrow: BLE ownership, timestamping, raw WAL capture, watchdog/reconnect, and clean shutdown.

Run these commands from the repository root. Scan to find your H10 ID; use it instead of the placeholder ID below.

## Scan

```bash
cargo run -- scan --seconds 12
```

## Record

`A1B2C3D4` below is a placeholder; pass your own H10's ID with `--target`.

```bash
cargo run -- collect --target A1B2C3D4 --data-dir data
```

New collection writes the first/only durable runtime path as WAL segments:

```text
data/wal/participant_id=local/source=chest_h10/date=YYYY-MM-DD/wal-*.open
data/wal/participant_id=local/source=chest_h10/date=YYYY-MM-DD/wal-*.closed
```

`collect` holds an OS advisory lock at `data/collector.lock` for the lifetime of the process. A second collector using the same `--data-dir` exits before WAL recovery, BLE setup, or scanning; a leftover unlocked lock file does not block restart.

On startup after acquiring the lock, the collector recovers stale `.open` H10 WAL segments and closes them as normal `.closed` files. Stop with Ctrl-C so the collector stops streams/disconnects best-effort and closes the current WAL segment cleanly.

Production `collect` writes raw WAL only. It does not create `data/latest`, `h10_session_*`, collector-owned Parquet output, waveform live JSON, or `live/latest.json`.

The legacy CSV writer is compiled only for Rust tests (`#[cfg(test)]`) so parser/state-machine tests can keep simple fixture assertions; it is not a production runtime sink.

## Archive closed WAL to Parquet

Run a one-shot local archiver pass with:

```bash
cargo run -- archive --data-dir data
```

`archive` holds an OS advisory lock at `data/archive/archiver.lock`, scans only immutable `*.closed` WAL segments, ignores `*.open`, and skips segments with a matching colocated `*.archived.json` sidecar. Derived Parquet files are written via `*.parquet.tmp -> *.parquet` under the WAL-era production layout:

```text
data/archive/parquet/participant_id=local/stream=ecg/date=YYYY-MM-DD/part-START-END-SEGMENT_ID-ecg.parquet
data/archive/parquet/participant_id=local/stream=raw_gatt/date=YYYY-MM-DD/part-START-END-SEGMENT_ID-raw_gatt.parquet
```

There is intentionally no hourly partition in the WAL-era archiver path.

Each successful WAL segment gets a sidecar next to the WAL:

```text
data/wal/participant_id=local/source=chest_h10/date=YYYY-MM-DD/wal-...archived.json
```

The sidecar records the WAL SHA-256/count/time range plus produced Parquet outputs, row counts, sizes, and output checksums. If Parquet finals exist from a previous crash but the sidecar is missing, the archiver verifies row counts and regenerates expected Parquet to compare SHA-256 before writing the sidecar; stale `*.parquet.tmp` files are removed while the archiver lock is held.

## Live dashboard

Serve the WAL-backed Rust dashboard/API with:

```bash
cargo run -- live --data-dir data --port 8000 --live-window-seconds 20 --history-window-hours 10
```

Then open <http://127.0.0.1:8000/>. The live service is optional and separate from `collect`: it reads recent `source=chest_h10` `.open`/`.closed` WAL segments read-only, tails new frames, and never writes, closes, renames, truncates, or recovers sensor WAL files.

The Rust-hosted dashboard intentionally mirrors the previously reviewed Python dashboard: ECG 10 s fixed sweep with R?/S? markers, HR/R-R sweep plots, accelerometer bars/traces with the established axis conventions, RSSI bands, packet delay/drift diagnostics, WAL/live health, manual marker button, and the ~10-hour recent-events pane. Startup is lazy: the server synchronously loads only the critical live window (default 20 s), then loads longer event/metrics history in the background. Browser waveform transport uses incremental SSE batches from `/events`; `/api/latest` remains available for initial state and low-rate/debug snapshots rather than as the main live waveform transport.

The live service may write disposable sparse WAL offset indexes under `data/live_index/...` to make future critical-window startup seek into the newest WAL segment. These indexes are derived caches, can be deleted, and are never canonical archive data.

Marker posting is handled by the live service as canonical WAL records under:

```text
data/wal/participant_id=local/source=manual_events/date=YYYY-MM-DD/wal-*.open
```

When marker posting is enabled, `live` acquires `data/wal/participant_id=local/source=manual_events/writer.lock` and recovers stale `.open` files only for `source=manual_events`. If that lock is already held, the dashboard still runs read-only and `POST /api/marker` is disabled with visible status.

The Python live dashboard has been removed from active scripts. Use `training_sensors live` for WAL-era sessions.

## Replay WAL fixtures/sessions

Replay one WAL file or a directory tree of WAL segments as deterministic JSONL parser events:

```bash
cargo run -- replay --from-wal data/wal/participant_id=local/source=chest_h10 --mode batch
```

Useful variants:

```bash
# One summary JSON object for tests/CI logs.
cargo run -- replay --from-wal data/wal --output summary

# Step mode flushes after each WAL frame, without interactive input.
cargo run -- replay --from-wal path/to/wal-....closed --mode step

# Preserve collector_unix_ns timing, accelerated 20x.
cargo run -- replay --from-wal path/to/wal-dir --mode realtime --speed-factor 20

# Explicitly allow tail-tolerant reading of current .open segments.
cargo run -- replay --from-wal data/wal --allow-open
```

Replay reads `*.closed` in strict mode. It includes `*.open` only when `--allow-open` is passed, and then reads only the valid complete prefix without truncating or closing the file. `--speed-factor` is used by `--mode realtime`; batch/step modes are deterministic and do not sleep. Output event types include `raw_gatt`, parsed `ecg`/`acc`/`hr`/`rr`/`pmd_timing`, `battery`, `health`, and diagnostic `event` rows. Replay does not write archive files, serve the dashboard, recover WAL files, or connect to BLE.

## Check changes

Run the standard local quality gate before reporting collector changes as complete:

```bash
scripts/check.sh
```

If the H10 is available and the change touches collector BLE/protocol behavior, run a short live smoke test too:

```bash
scripts/live_h10_smoke.sh
```

Measure first-party Rust unit-test coverage with:

```bash
scripts/coverage.sh
```

Current coverage targets/exemptions are documented in [test coverage](test_coverage.md). The vendored CoreBluetooth RSSI patch is documented in [CoreBluetooth patch](vendored_btleplug_patch.md).
