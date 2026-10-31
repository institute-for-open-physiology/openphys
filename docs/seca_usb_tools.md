# macOS seca USB capture, replay and upload

These standard-library Python tools provide a developer workflow for the seca ct331.
They are separate from the H10 collector and the Android recorder. The design and
ownership split are in [the proposal](seca_usb_tools_20261021.md).

The capture CLI supports requested rates of 1000, 2000, 4000, 8000, 16000 and
32000 Hz; its default is 32000 Hz. It confirms the device's reported rate, gain 6,
and contact setting before starting. Confirmation establishes the configuration,
not delivered sample rate, effective bandwidth, calibration or pacemaker detection.
This implementation has deterministic and recorded-data validation; its native
32 kHz hardware path still requires the coordinated acceptance test below.

## Capture

Use Python 3.9 or later. USB capture requires macOS and a mounted seca volume under
`/Volumes`. Stop any other seca recorder first and leave the device in standby.
This tool refuses a device reporting another mode. Its cooperative process lock
protects its own instances; the older bridge and temporary probes do not use it.
Never open `ecg.dat` concurrently: reading consumes the device stream.

From the repository root, choose a new output directory:

```sh
mkdir -p data/seca_usb
python3 scripts/seca_usb_capture.py \
  --volume '/Volumes/ct 331' \
  --output-dir data/seca_usb/example_32k \
  --sample-rate 32000 --duration 10
```

The output directory must not exist, and its parent must already exist. The CLI
first reads identity, writes the requested settings with separate paced commands,
requests configuration, confirms the response and then requests acquisition.
Each raw TX/RX record includes Unix and monotonic nanosecond receive clocks.
Each also carries `acquisition`: false for commands and setup responses, true
only for RX after confirmed configuration and a successful `mode=run` write.
Carolina preserves setup bytes but excludes them from calibrated ECG summaries.
Idle and malformed responses remain in the journal. No sample filtering, averaging,
interpolation, resampling or baseline subtraction is applied.

Contact detection defaults to disabled (`openlead=0`), matching the observed
prototype configuration. Use `--contact-detection` to request and confirm
`openlead=1`. Disabled contact detection means zero contact flags cannot establish
good electrode contact. These tools do not interpret pacemaker events.

`session.json` contains requested and observed configuration. `records.jsonl`
contains wire records and lifecycle/error events, with a durable append for each
record. TX records describe attempted commands; a failed write is separately
reported. The device's counter remains in the original frame bytes. Packet receive
timestamps do not establish individual sample acquisition times.

Native device operations run in a child process with a per-operation timeout
(`--io-timeout`, default two seconds). Duration expiry, Ctrl-C, SIGTERM and failures
attempt standby cleanup. A blocked worker is terminated; if this prevents the
standby command, the failure explicitly reports uncertain device state. Check the
device before another recording. No automatic reconnect hides the resulting gap.

A successful capture result describes completion of durable raw transport capture.
Use replay to establish whether valid ECG samples arrived. A missing terminal
`session_end` event means the capture is incomplete, still open or interrupted.
Do not discard a partial session: its raw bytes are useful evidence.

## Replay and full-rate export

```sh
python3 scripts/seca_replay.py data/seca_usb/example_32k \
  --csv data/seca_usb/example_32k_counts.csv \
  --summary data/seca_usb/example_32k_audit.json
```

The decoder independently checks framing, checksums, signed delta compression,
counter wrap, duplicates, regressions and gaps. It accepts a session directory,
legacy probe JSONL or one cloud chunk JSON. A single chunk cannot establish
continuity with adjacent chunks; legacy probes do not establish clean closure.
Requested and observed configuration are reported separately. Conflicting rate
evidence suppresses a nominal duration estimate.

CSV has one row per simultaneous sample, retaining all eight measured integer
channels in order `V1,V2,V3,V4,V5,V6,I,II`, the block counter, sample index within
the block, contact mask and both receive clocks. It preserves samples flagged
for contact problems. An `acquisition` column separates setup from active data;
it is blank for older captures without that evidence. Counter continuity is
checked within each acquisition/setup segment, avoiding false gaps at the boundary.
The derived limb leads can be calculated later from I/II;
this export deliberately preserves measured counts rather than asserting µV
calibration. Receive clocks repeat for every sample in the same packet.

Malformed frames and discontinuities produce diagnostics and a nonzero exit.
Valid frames following a corrupt frame remain exportable; the summary labels
the resulting CSV partial. Export files are created exclusively. Keep large CSVs
in ignored `data/` directories: high-rate uncompressed text can be substantially
larger than the raw recording.

## Prepare and upload

The primary integration adds `POST /api/v1/seca/batches`, accepting explicit
`transport: "usb"` and all six rates. The uploader requires that server version
and an enrolled installation credential. Ordinary website cookies are insufficient.
For a native Mac credential, sign in to Carolina Home and choose **Create Mac
connection link**, then run:

```sh
python3 scripts/seca_enroll.py --label 'Research Mac'
```

Paste the one-use link into the hidden prompt. Tokens never belong in command-line
arguments or logs. The CLI creates a distinct Mac installation, stores owner-only
state under `~/Library/Application Support/TrainingSensors/seca-usb`, and prints
its installation UUID and credential-file path. It does not read phone credentials.
The server records an explicit macOS label/app identity and leaves Android fields
NULL. This requires the server version supporting the Mac exchange endpoint.

After an interrupted/ambiguous exchange, run the same command again: it preserves
the exact pending identity for an idempotent retry. If the pending link expired,
create a new link and run with `--replace-link`. That explicit recovery keeps the
same Mac installation, uses a new exchange ID, and rotates any credential created
by a previous response that was lost. It refuses to replace an already saved
credential. Do not delete pending state to retry; that can orphan an installation.

Actual Mac enrollment is deferred until the primary Android deployment completes;
the resulting installation/credential topology change belongs in the guarded
production workflow.

The credential input is a private JSON file with `installation_id` and
`device_credential` fields supplied by that enrollment flow. Keep it outside Git,
readable only by its owner. Neither the credential nor an enrollment token belongs
in a command argument, project log or report.

Prepare locally, passing the installation's public UUID:

```sh
python3 scripts/seca_upload.py prepare \
  data/seca_usb/example_32k data/seca_usb/example_32k_queue \
  --installation-id YOUR_INSTALLATION_UUID
```

Preparation requires a closed session and a matching configuration-confirmation
event for rate, gain and contact detection. Acquisition-tagged records must follow
the confirmed run boundary; every wire record must carry an explicit boolean.
It creates immutable numbered chunks
with at most 256 records each and a conservative byte limit below the server's
2 MiB limit. A failed but closed capture may be archived with an explicit failure
end reason. A partial preparation has no `ready.json` and cannot be uploaded;
retain it for inspection and prepare again into a new directory.

```sh
python3 scripts/seca_upload.py upload \
  data/seca_usb/example_32k_queue \
  --base-url https://sensors-poc.vineyard-finance.com \
  --credential-file /absolute/private/path/seca-device.json
```

Upload is explicit and HTTPS-only; redirects are rejected. Each request has a
20-second timeout by default. The server must acknowledge the session, contiguous
batch index and SHA-256 of the exact transmitted UTF-8 body. Only then is a local
receipt durably published. Retry the same command after a network failure: verified
receipts are skipped and an ambiguous request sends the identical bytes again.
Receipts also bind the server endpoint, preventing an accidental skip when changing
servers. Authentication or body conflicts remain visible and stop progression.

The uploader preserves the raw journal and all prepared chunks after acknowledgement.
Lifecycle events remain in the local journal; the cloud schema retains wire records,
rate/contact/transport metadata and a final completion/failure reason. Keep the local
journal for richer forensic replay. Concurrent uploads of one queue are rejected.

## Validation and hardware handoff

The standard `scripts/check.sh` gate includes the deterministic suite. To run it
alone without accessing either sensor:

```sh
python3 -m unittest discover -s scripts -p 'test_seca_*.py'
scripts/check.sh
```

Coverage for these Python tools is measured separately from Rust coverage:

```sh
python3 -m coverage run --source=seca_usb_capture,seca_replay,seca_upload \
  -m unittest discover -s scripts -p 'test_seca_*.py'
python3 -m coverage report -m
```

Once the primary session releases the USB device, run short captures at all six
rates. Check reported configuration, decoded sample counts/counter continuity,
receipt-time throughput, idle/malformed counts and final standby. Exercise a cable
disconnect and interrupted stop, confirm bounded failure and retained bytes, then
restore the original recorder if needed. Compare a known input/reference before
claiming rate-dependent gain, useful high-frequency bandwidth or pacemaker detection.

For cloud acceptance, enroll a Mac installation, upload one short capture, retry
the same queue, inspect Carolina's USB/rate/session identity, and download/replay
the archived chunks. Cloud deployment remains the primary session's guarded
transaction because the new archive changes stored-data shape. No production
deployment or live hardware validation was performed by this tooling branch.
