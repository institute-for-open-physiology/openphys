# OpenPhys history

A short engineering history of OpenPhys, distilled from the project's private development log. Dates are given to the month.

## May 2026: a Rust collector for the Polar H10

OpenPhys started as a Rust command-line collector on macOS. It started the H10's PMD ECG and accelerometer streams, subscribed to heart rate/R-R notifications and wrote CSV, with a small Python browser dashboard on top. Early findings:

- The H10 has no standard Current Time characteristic. Its clock is set through Polar's proprietary PS-FTP service.
- ECG arrives in bursts of 73-sample frames.
- macOS only reported stale RSSI values, so `btleplug` is vendored with a small patch (`docs/vendored_btleplug_patch.md`).

Range and body-blocking tests showed that CoreBluetooth calls could hang forever after a link timeout. The process stayed alive but recorded nothing. Every BLE await now has a timeout, and per-stream watchdogs end the connection when notifications stop, so the collector reconnects by itself. The same tests showed that **the H10 does not backfill samples missed during a disconnect**.

The code was then split into modules. A quality gate (`scripts/check.sh`), a live hardware smoke test and coverage targets were added. BLE and GATT were moved behind small interfaces so that failure paths could be unit-tested with fakes.

Storage went through three designs in a few weeks. CSV came first, then typed records written straight to Parquet. Parquet produced tiny files until chunks were set to a 30-minute target, and it still lost buffered data on a crash. Then two collectors ran at once and overloaded everything, which led to the current design:

- `collect` only owns the BLE link, timestamps data and appends raw capture records to a write-ahead log (WAL). It holds an OS lock, so a second collector exits at once.
- The WAL format (`docs/wal_format_v1.md`) uses CRC-32C-checked frames. Segments go from `.open` to `.closed`, and a stale `.open` is recovered at startup. Raw GATT traffic is kept, so parser changes can be replayed against real sessions.
- `archive` converts closed segments to Parquet idempotently, guided by a checksummed sidecar. `live` reads the WAL read-only.

The first Rust dashboard lagged because it sent full snapshots. It was reworked to send incremental SSE batches and to load a short live window before longer history.

## June 2026: running it every night

Overnight use exposed operational problems:

- Setting the H10 clock goes through an encrypted characteristic, so the host bonds with the sensor on every connection. When the H10 later drops that bond, reconnects fail until the host forgets the device.
- A shorter connect timeout made reconnects worse and was reverted. Timeout changes are now based on measured connection data.
- The archiver re-hashed every archived segment on each pass. It now skips segments with a valid sidecar.
- A battery-low banner was added after a recording ended because the battery ran out.

A timing study found that the H10's own frame timestamps are the precise clock. The real ECG rate is slightly below the nominal 130 Hz, and the sensor clock drifts by tens of ppm. The right approach is to set the clock once per connection and then measure the offset from the data, not to keep re-setting it.

## August 2026: Android collector and cloud service

A laptop collector only works while the wearer stays near the laptop, so collection moved to the phone. The Kotlin app runs a foreground service, keeps a durable per-record queue and uploads idempotent batches. The Rust cloud service writes each batch to that user's WAL and fsyncs it before acknowledging the phone. It reuses the local dashboard and archiver for each user. Accounts started out passkey-based. During the month they were replaced with invite-only email/password accounts and per-installation device enrollment through an App Link or a QR code.

Beta testing shaped the rest:

- A setup wizard was added, since phone makers handle battery-optimisation exemptions differently.
- An upload-stall watchdog was added.
- A loud dashboard banner separates "data is being lost" from "no data arriving".
- Location collection is consent-gated and off by default.

On the server, keeping a long in-memory history for every user exhausted a small VM after a large backlog upload. The fixes were lazily loaded per-user state, a shorter lookback and bounded ingest: over the limit the server returns HTTP 429, and the phone retries the same batch.

Deployments were split into a guarded lane for schema, identity and configuration changes and a fast lane for app-only changes. Servers must be deployed before clients that send new record kinds. The deployment tooling is not in this repository.

## September 2026

The Android, website and cloud work was merged. The standalone `resting_ecg` workflow was added. Brief experiments with a blood-pressure cuff and with accelerometer-based cardiac timing were carried out separately.

## October 2026: seca CT331 and public release

The seca CT331 protocol was worked out from read-only inspection of the vendor software and verified against raw USB captures with an independent decoder (`docs/seca_ct331_protocol.md`). This led to macOS USB capture tools at 1–32 kHz, a native macOS recorder and Android Bluetooth acquisition.

Sustained high-rate Bluetooth streams lose framing below the app. Periodic host queries help but do not fix it. Missing blocks are therefore recorded explicitly, never reconstructed or silently joined. H10 packets with invalid device timestamps are now flagged and archived instead of blocking uploads. The app also gained native sign-in, because not every phone browser opens enrollment links in the app. The project was then published as OpenPhys.

## Lessons for contributors

- **Only one collector can own the H10's PMD streams.** A second one (say, a laptop next to a phone) gets stream-start errors. Keep one owner per sensor.
- **The H10 does not backfill.** Reconnect fast and record gaps honestly.
- **Fail loudly rather than lose data silently.** Use watchdogs, explicit gap records and visible banners.
- **Keep raw data canonical.** Parquet, indexes and dashboards should all be regenerable from the WAL.
- **Acknowledge only after fsync**, on every hop.
- **Put a timeout on every Bluetooth call**, and test against recorded raw traffic and real hardware.
