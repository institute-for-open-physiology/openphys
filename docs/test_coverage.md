# Test coverage status

Date: 22 October 2026

Coverage is measured over first-party Rust with:

```bash
scripts/coverage.sh
```

The script uses `-Cinstrument-coverage`, excludes Rust standard-library, registry, vendored and
generated target code, and writes each run to a PID-qualified temporary directory so concurrent
review runs cannot delete one another's artifacts.

Android host-testable coverage is enforced independently with:

```bash
cd android
./gradlew testDebugUnitTest testReleaseUnitTest koverVerify koverXmlReport
```

## Native macOS seca developer app — 21 October 2026

`scripts/check_seca_macos.sh` compiles/runs the deterministic Swift state contracts,
measures LLVM line coverage, and builds the ad-hoc signed native app for macOS 13+.
`RecorderState.swift` measures **96.05% (170/177 lines)**. Tests cover configuration
before acquisition, progress/gaps, user Stop, durable-save versus ACK states,
missing/invalid terminal output, helper errors, restored sessions and enrollment.
The same covered module bounds status lines and supervises progress freshness;
tests drain a real oversized subprocess through EOF and use fake clocks for
stalled recording/upload and continuing preparation progress.

The Python suite now contains **116 passing tests**, including the native bridge,
mixed status/config replies, clean Stop upload metadata, retained recordings,
coalesced output, private identity/secret redaction and exact upload retry. Measured
with coverage.py using `--source=seca_macos_bridge,seca_usb_capture,seca_upload,seca_replay,seca_enroll`:

| File | Lines covered |
| --- | --- |
| `scripts/seca_macos_bridge.py` | 250/255 (98.04%) |
| `scripts/seca_usb_capture.py` | 464/465 (99.78%) |
| `scripts/seca_upload.py` | 278/282 (98.58%) |
| `scripts/seca_replay.py` | 397/400 (99.25%) |
| `scripts/seca_enroll.py` | 199/212 (93.87%) |

`SecaRecorderApp.swift` is an explicit platform-boundary coverage exemption for
SwiftUI/AppKit rendering, Foundation.Process/pipes and window shutdown integration.
The pure lifecycle contract lives in the covered state model. Compile/package checks
exercise framework/API availability. The parent session launched the app and
verified the native window, default 32 kHz and disabled Start without a recorder;
no recording or enrollment was initiated. Stop/close interactions, USB all-rate
variable-signal throughput and separate Mac enrollment still require coordinated
manual/live acceptance. Build/check shell scripts are developer tooling covered by their
successful execution. No Rust source changes are introduced by this native wrapper.
Final `scripts/check.sh` passed 292 Rust tests (one private replay ignored),
116 Python tests and all build/Clippy/browser/auth gates. Independent implementation
review approved after upload cancellation/freshness and pipe-drain corrections.
The follow-up permission/Stop/report changes have separate independent approval:
synthetic permission tests never open `ecg.dat`, real synthetic worker tests retain
standby/close after SIGINT, and a real Swift supervisor/child test verifies that
positive-PID Stop leaves the child alive. Replay tests prove nested/default-network
secrets never appear in returned reports, stdout or summary files while private
source bytes remain exact, and intentional Stop cannot mask actual cleanup errors.

## seca integration gates — 21 October 2026

- Independent decoder tests include mixed signed boundaries at every delta width,
  12-bit padding, counters/wrap, framing/checksum failures, lead derivation and
  battery validity. A separately decoded private on-body fixture is additionally
  tested with `SECA_GOLDEN_PATH=… cargo test seca:: -- --include-ignored`; personal
  samples stay outside git. The ordinary suite skips that one private-fixture test.
- Cloud regression tests cover auth/tenant isolation, exact-request digest ACKs,
  conflicting retries, orphan recovery and failure between rename/directory fsync,
  all rate/transport contracts, bounded previews and status-vs-ECG freshness.
- Android deterministic seca framing, journal recovery, watchdog, keepalive and
  uploader tests cover 100% of their source lines. Android `SecaActivity`,
  `SecaService`, `SecaSettings`, `SecaStatus`, `SecaSnapshot`, `SecaUploadWorker`,
  `SecaBootReceiver`, `SecaTiming` diagnostic logging and directory-fsync syscall adapter
  are platform-boundary exemptions, verified through lint/build and live signed
  Galaxy tests. Their protocol, queue and ACK logic remain included in Kover.
- `scripts/check.sh` passed 289 Rust tests (one private-fixture test skipped),
  build/format/strict Clippy and browser/auth/Python checks. The private replay
  passed separately. `scripts/coverage.sh` passed and measured 88.88% overall
  (28,745/32,341 lines), with new `src/seca.rs` at 90.26%, `src/cloud/seca.rs`
  at 95.93%, and extended `src/cloud/web.rs` at 90.76%. Existing lower-coverage
  platform/process boundary exemptions below remain applicable. Report:
  `/var/folders/tj/4d_kt9015k5d7rsgp9g1mrdr0000gn/T/training_sensors_coverage.9705/report_first_party.txt`.
- Android final candidate: **283 JVM tests**, zero failures/errors/skips;
  `lintDebug`, release assembly, `koverVerifyDebug` and `koverXmlReportDebug` pass.
  Pure seca protocol 72/72, queue 109/109, liveness 37/37, keepalive 3/3 and uploader
  20/20 measured source lines are covered. Regression tests include retaining all
  coalesced frames before reporting corruption, process/torn-tail recovery,
  immutable export and retry bodies, exact ACK hashes, RX-only acquisition flags,
  status-only ECG stalls, and periodic-query cancellation. Independent source and
  artifact review approved APK SHA-256
  `4bc4a30c7ee6446880e31ae7c3f73cbba5b2d25a5d6f94c4fdd499d649c93905`:
  version0.3.0/code11, expected production signing certificate and valid alignment.
- Live Galaxy Bluetooth 4 kHz handshake/readback, journaling, stop and USB export
  succeeded. Timing isolated the initial roughly 480 ms delivery stall to socket
  reads, while fsync remained fast. Periodic read-only `R` queries at approximately
  2.2-second intervals removed that repeated stall in an 88-second A/B recording.
  Independent replay found 11,042 valid RX packets and 352,064 samples/channel,
  with **four missing 32-sample groups**; this is not a gap-free acceptance result.
  See `seca_android_bluetooth_keepalive_20261021.md` for evidence and remaining tests.
  Production upload and end-to-end replay validation remain pending. No H10 protocol
  code changed; a disruptive H10 hardware smoke test is not required.
## Native macOS enrollment — 21 October 2026

Independent review approved the separate native enrollment endpoint and private CLI.
`scripts/check.sh` passes **292 Rust tests**, with one
private replay fixture ignored, **99 Python tests**, and build/format/strict Clippy,
browser/authentication and syntax gates. The enrollment CLI contributes 11 tests,
including ambiguous-response retry, explicit expired-link replacement, private
storage, process exclusion, hidden-prompt fallback, partial HTTP responses and
ancestor synchronization. It measures **93.87% Python line coverage (199/212)**.

The full `scripts/coverage.sh` run passes and measures **88.98% overall Rust line
coverage (29,050/32,646)**. Changed Rust files remain above the per-file target:
`src/cloud/auth.rs` **97.81%**, `src/cloud/db.rs` **93.19%**, and
`src/cloud/web.rs` **90.89%**. Existing platform-boundary exemptions below remain
applicable. Regression tests preserve Android enrollment, reject cross-owner and
cross-platform identity reuse, deny consumed-token retries after Home disconnect,
and allow a fresh enrollment after disconnect. Report:
`/var/folders/tj/4d_kt9015k5d7rsgp9g1mrdr0000gn/T/training_sensors_coverage.16094/report_first_party.txt`.

Actual Mac enrollment/production activation is deferred to the root session after
the first Android deployment; deterministic checks do not claim that live result.

## seca macOS tooling — 21 October 2026

The isolated USB capture/replay/upload branch adds Python tools and no Rust source.
All **88 deterministic Python tests** pass, including a spawned fake-worker timeout,
partial-byte retention, storage faults, all compression widths and rates, setup versus
acquisition boundaries, an exact-byte cloud-ACK HTTP fixture, and resumable uploads.
Independent review approved capture, replay, upload and the integration fixtures.

Measured with coverage.py 7.10.7 (all three modules exceed 90%):

| File | Python line coverage |
| --- | --- |
| `scripts/seca_replay.py` | 99.23% (385/388) |
| `scripts/seca_upload.py` | 98.17% (268/273) |
| `scripts/seca_usb_capture.py` | 99.78% (460/461) |

Run `python3 -m coverage run --source=seca_usb_capture,seca_replay,seca_upload
-m unittest discover -s scripts -p 'test_seca_*.py'` as one command, followed by
`python3 -m coverage report -m`. Entry-point guards and a few defensive error paths
are the remaining uncovered lines. Native USB calls are exercised through deterministic
fakes; this does not replace live device acceptance. Live 32 kHz throughput, rate-dependent
calibration and bandwidth remain unverified because the primary session owned the hardware.
Two previously closed personal 4 kHz probes decoded locally without checksum/counter
errors (32,608 and 32,576 samples/channel); personal samples are never committed.

`scripts/check.sh` passes the unchanged Rust baseline (278 tests) plus formatting,
build, Clippy, dashboard/auth checks and current Python syntax. `scripts/coverage.sh`
was not repeated because this branch changes no Rust source. No H10/CoreBluetooth
behavior changed, so an H10 interruption would not validate these tools.

## Deployment startup grace — 21 October 2026

The later guarded-release readiness follow-up has **21 application-helper tests**
and **18 smoke tests**, plus coordinator readiness, full coherence, application
activation and worker-health fixtures. They cover strict read-only health waits,
provenance drift, delayed health, deadline/probe/sleep budgets, early credential/tool
failure without parent-environment token leakage, SSH response loss, checkpoint-
specific retry proofs and failed backup/SQLite operations hidden by older evidence.
These shell runtime files (`deploy/deploy-release.sh`, `deploy/smoke.sh`,
`deploy/lib/worker-health.sh`, and `deploy/remote/training-sensors-app-activate`)
have an explicit integration-fixture exemption from numeric Rust/Python line
coverage: fake clocks and mocked SSH/systemd execute their actual shell paths, and
the complete protocol fixtures exercise activation, recovery and finalization.
Each implementation slice received independent review by another agent.
Private validation reports are under `data/release_dependency_audit/`.

The subsequent pre-snapshot readiness extension passes **29 focused tests** (10 new)
and the complete preflight fixture, including all 28 seca guard tests. The five
readiness/configuration functions now measure **98.67% line coverage (74/75)**.
The remaining unrecorded line is a `break` after late health success; both late
response and post-health provenance-cost rejection are explicitly exercised.
Snapshot boundary tests prove timeout and provenance drift cause no stop, restart,
backup or state writes. Private evidence is in
`data/pre_snapshot_readiness/focused_coverage.json`. The integration exemption below
still covers the larger preflight helper; no Rust or hardware code changed.

The separate startup-grace change adds no Rust or sensor-runtime code. All 19
focused Python tests pass; the five changed readiness/configuration functions
measure **100% line coverage (67/67)**. Fake monotonic clocks cover delayed health,
deadline exhaustion, clipped probes/sleeps, stale provenance, invalid configuration
before mutation, repeated resume/abort without restarting an initializing process,
and exact health-response requirements. Private measured evidence is in
`data/startup_health_grace/focused_coverage.json` in the isolated tooling worktree.

All 14 shell startup tests in `deploy/test_app_startup_grace.py` pass, registered
in the application activation fixture. Root/systemd/protocol boundaries retain their
integration exemption and are exercised through the full preflight and application
activation fixtures; both complete fixtures pass on the final source. These local
synthetic tests do not claim production startup
validation. Rust coverage and H10 live tests are inapplicable to this helper change.

## seca guarded-storage checks — 21 October 2026

The seca preservation extension adds no Rust or sensor-runtime source. All 28
focused Python tests pass, along with the complete deployment preflight fixture.
The five added helper functions have **99.53% line coverage (210/211)** from the
focused tests: inventory validation, exact prior-chunk preservation, old-manifest
rejection and timing callbacks are exercised with synthetic files and transactions.
The remaining uncovered line is a defensive directory-replacement race rejection.

The preexisting root/systemd/process-control portions of the larger preflight file
retain an integration-test exemption from a whole-file Python line threshold:
`deploy/release-preflight-test.sh` exercises the real CLI/state machine through
mocked service/process boundaries. It covers both legacy H10 invariants and seca
corruption, append, atomic-summary races, recovery and terminal-state shortcuts.
The off-host evidence fixture checks stderr progress without changing stdout or
skipping hash/durability checks. These tests do not constitute live deployment or
raw-data restore validation. Rust coverage and H10 live tests are inapplicable to
this deployment-tooling change; the owning session performs production integration.

The private measured report is
`data/seca_storage_guard/focused_coverage.json` in the isolated tooling worktree.
Run `python3 deploy/test_seca_storage_guard.py` for the focused suite and
`deploy/release-preflight-test.sh` for its complete CLI integration fixture.

## Current verified gates — 23 September 2026

The dashboard browser-handoff and admin ECG startup fixes have been checked on top of
the main integration of `a4a107b`:

- `scripts/check.sh` passed **278 Rust tests**, formatting, build, Clippy and the
  embedded JavaScript/auth/link/Python checks.
- `scripts/coverage.sh` passed **278 tests** and measured **88.71% first-party line
  coverage (27,611/31,126)**. Every file below 90% retains its explicit boundary exemption below.
- Browser regression checks cover Strict-cookie recovery in real Chrome (valid/expired/
  signed-out and fragment links), 13 recovery-script cases, full-ring SSE startup/reconnect
  and stale HTTP/SSE interactions.
- Android v0.2.8/code 10: **267 JVM tests per variant** (debug and release), no failures,
  errors or skips; Kover **98.18% lines (1567/1596)**, every included file >=90%.
- `lintDebug`: zero errors, 112 warnings; `assembleDebug`, `koverVerify` and
  `koverXmlReport` passed. No production APK was built/released as part of the merge.
- Today's Galaxy A54/Android 14 acceptance passed the v0.2.8 eight-step wizard, fresh
  ECG/ACC/HR/RR/contact check, short offline queue/catch-up and locked-screen upload
  tests. This was a short USB-connected check, not overnight or battery-only validation.
  See `customer_onboarding_readiness_20260923.md` for limits and evidence.

## Historical verified gates — 6 August 2026

- The integrated Rust source passes **264 tests**.
- The version-code-4 Android source passes **183 JVM tests** in each build variant.
- Android Kover reports **98.61% line coverage (1066/1081)** across the included first-party source.
  Every included file is at or above 90%.
- Android lint passes with zero errors and 43 warnings. The version-code-4 debug and Android-test
  APKs were assembled and executed only as distinct-package connected instrumentation; the protected
  signed v4 release remains deliberately uninstalled. Independent post-merge Android review approved
  the underlying corrective candidate at **9.6/10** with no high-, medium- or new low-severity
  finding. Independent review of the exact
  version-code-4 enrollment and Samsung-guidance change found no high-, medium- or low-severity
  issue; signed-v4 installation and physical acceptance remain pending.
- On the authorized Galaxy A54 / Android 14, exact `connectedDebugAndroidTest` passed **3/3** tests
  with zero failure, error or skip (XML duration **1.247 s**; Gradle **20 s**). The isolated packages
  were `com.vineyardfinance.sensors.debug` and `com.vineyardfinance.sensors.debug.test`, with the
  test package targeting only the debug app.
- Repository formatting, lint, build and deterministic deployment-script checks are separate release
  gates; coverage does not replace them.

## Current Rust line coverage

Measured on 22 October 2026 after the H10 invalid-device-timestamp recovery fix.
`scripts/coverage.sh` passes with 297 tests passed and one ignored; total first-party
line coverage is **88.98%**. All files below 90% retain the documented boundary
exemptions below. The new parser/ingest/archive/replay/live regressions pass;
raw sensor evidence is retained while invalid clocks cannot advance sample freshness.

| File | Line coverage | Status |
| --- | ---: | --- |
| `src/archive.rs` | 90.75% | Meets target |
| `src/ble.rs` | 77.44% | Live adapter/collector-loop exemption below |
| `src/cloud/auth.rs` | 97.81% | Meets target |
| `src/cloud/db.rs` | 93.19% | Meets target |
| `src/cloud/ingest.rs` | 96.91% | Meets target |
| `src/cloud/web.rs` | 90.89% | Meets target (residual browser/Resend/server-lifecycle notes below) |
| `src/cloud/seca.rs` | 95.93% | Meets target |
| `src/config.rs` | 100.00% | Meets target |
| `src/csv.rs` | 96.37% | Meets target; test-only compatibility module |
| `src/gatt.rs` | 26.98% | Real `btleplug` platform boundary exemption below |
| `src/hr.rs` | 100.00% | Meets target |
| `src/live_server.rs` | 88.29% | Browser/long-running dashboard boundary exemption below |
| `src/main.rs` | 0.00% | Thin CLI/process entrypoint exemption below |
| `src/pmd.rs` | 95.12% | Meets target |
| `src/process_lock.rs` | 90.85% | Meets target |
| `src/psftp.rs` | 81.49% | Live encrypted-GATT boundary exemption below |
| `src/records.rs` | 95.65% | Meets target |
| `src/replay.rs` | 92.00% | Meets target |
| `src/seca.rs` | 90.26% | Meets target |
| `src/sink.rs` | 58.61% | Live record-adapter boundary exemption below |
| `src/util.rs` | 98.31% | Meets target |
| `src/wal.rs` | 94.10% | Meets target |
| **First-party total** | **88.71% (27,611/31,126)** | Reduced by the documented platform/process boundaries |

The current measurements supersede the earlier August figures. In particular, password hashing and rate
limits, invitation/password-reset state, single-use enrollment exchange, hash-only device
credentials, browser/device credential separation, tenant authorization, revocation, idempotent
ingest and current schema migration paths are now deterministically exercised. There is no coverage
exemption for `auth.rs`, `db.rs` or `ingest.rs`.

The current Rust coverage report SHA-256 is
`4b9a010d371b034af8665d9b84c8187d66cfcc2847ce15d4505430255446c3a4`.

## Cloud web boundary exemption

`src/cloud/web.rs` now meets the 90% line target (90.71% after the 23 September browser-handoff/admin-startup
work); the notes below describe its residual uncovered lines. They are narrowly
confined to generated browser HTML/JavaScript rendering variants, the external Resend network
success/failure boundary, and defensive lifecycle branches in the long-running TCP/SSE/background
server. Those branches require a real browser, provider or process lifecycle and remain part of the
deployed acceptance run.

The exemption does not include the security or durability contract. Deterministic route/database
tests cover:

- exact-email, seven-day, single-use invitations and registration;
- password login/logout, generic password-reset requests, one-hour single-use reset completion,
  session rotation and CSRF enforcement;
- password-work concurrency/rate limits and bounded accepted password encodings;
- ten-minute, single-use enrollment creation and exchange, including stable ambiguous-retry and
  terminal reuse/expiry behavior;
- separation of website cookies from exact `Authorization: Device ...` ingestion credentials;
- permanent credential hash verification, installation binding, revocation and the stable
  `device_reconnect_required` response;
- user-only dashboard/data access and admin cross-user views, invitation controls, installation
  listing and credential revocation;
- the authenticated private-beta APK route's exact immutable bytes and GET/HEAD/Range metadata,
  active wearer/admin access, denial for every non-browser or inactive identity, onboarding order,
  and fail-closed invalid/substituted startup artifacts;
- stable installation/batch identifiers, contiguous sequence validation, idempotent retries,
  fsync-before-ACK, receipt reconstruction and checkpoint/reader/writer contention;
- bounded/redacted telemetry and absence of credentials or reset/enrollment tokens from logs and
  response URLs.

The deployed HTTPS/browser, Resend/Gmail and long-lived service checks remain required before this
exemption can be accepted for release.

## Android measured surface and boundary exemptions

The 5 August Kover report covers the version-code-4 deterministic Android source at **98.61%
(1066/1081)** with **183 passing JVM tests in each build variant**. Included source families cover
App-Link parsing and intent scrubbing, enrollment exchange/retry state, the exchange-only success
event and bounded recreation-safe success-notice presentation, Keystore-store orchestration,
post-enrollment recovery, collector admission/runtime and cleanup policy, the process-lifetime
durable-backlog monitor,
setup-checklist evaluation, upload-work policy, H10 protocol parsing, durable
identity/queue/record serialization, HTTP handling and idempotent upload. The lowest included file
is 91.67%; all included files meet the repository's 90% threshold. The changed
`EnrollmentManager.kt`, `EnrollmentSuccessNotice.kt`, `EnrollmentSuccessBannerUi.kt` and
`SetupChecklist.kt` sources each have 100% line coverage. The exact report SHA-256 is
`4668974dabfe950c7c392dd309ab65840f8c11653bf12c0fb7cb94614c03c27a`.

Deterministic Android tests prove that only a real successful enrollment exchange creates the
prominent notice; an existing credential cannot create it, exact-event acknowledgement cannot clear
a newer state, recreation preserves it only for the original bounded interval without a second
announcement, and pending/failure states clear it. Robolectric tests exercise the static
secret-free copy, visual/accessibility properties, scroll, focus and announcement boundary. The
Samsung setup regression also inflates the production layout and requires both literal navigation
paths, the truthful manual-confirmation warning, checkbox copy and safe generic settings-button
label; checklist policy independently proves missing confirmation remains the first incomplete
pre-start gate.

They also specifically prove that the low-storage stop remains a typed
archive-critical failure, escapes the recoverable BLE reconnect loop after cleanup and reaches the
persistent action-required service latch even if recording the critical event also fails. Network
callback cleanup selects the exact token, calls platform unregister and clears successful ownership
under one serialized owner boundary shared with service destruction. A failure propagates without
clearing the callback, including after owner close, so the competing lifecycle path can retry it;
stale callbacks cannot invoke cleanup or detach replacements. A deliberate Stop cannot turn cleanup
failure into a persistent fatal latch. The tests also prove that process-default backlog zero is
explicitly unknown, the application observer remains authoritative after service stop and publishes
the residual worker's measured zero, stream failure/completion is visible and retried sequentially
without overlapping subscriptions, and queue-monitor errors do not overwrite archive-critical
collector failure state. `BacklogStatusMonitor.kt` has 92.86% line coverage (13/14); the uncovered
line is a compiler/coroutine boundary and the file remains above the repository threshold.

Kover excludes only these named boundaries:

- generated `BuildConfig`, resource, Room and view-binding code;
- Activity/application graph and Android settings-intent wiring;
- the Android Keystore provider cipher implementation (the surrounding credential-store logic is
  included and tested);
- foreground service, boot receiver, notification-channel and live status/snapshot wiring;
- real `BluetoothGatt`, H10 collector callbacks and raw-record writer;
- Room database/entity framework adapters and WorkManager entrypoint wiring;
- the Android permission/battery/manufacturer inspector, whose pure checklist policy is included.

Connected debug instrumentation now covers the real Room WAL / `synchronous=FULL` configuration,
the real Android Keystore encrypted credential round-trip and compare-and-set clear, and the store's
injected invalid-alias recovery preserving the debug installation identity and queued record. The
remaining exclusions require further instrumentation and physical Galaxy/H10 validation. They cover Android
framework/provider behavior, not enrollment-token rules, credential scoping, setup policy,
fatal-action-required policy, queue idempotence or upload acknowledgement logic.

The prior Android reviewer approved the exact underlying corrective commit at **9.6/10** with no
high-, medium- or new low-severity finding. The earlier **LOW** observability note remains: if multiple independent
cleanup actions fail during the same terminal pass, the service preserves the first failure for
operator-facing status and may not report every secondary cleanup failure separately. The primary
failure remains visible and unregister ownership is retained for retry. The connected storage and
Keystore-provider slice now passes; App Link, foreground-service/settings, BLE/GATT and full
Galaxy/H10 behavior remain live acceptance boundaries rather than deterministic-review blockers.

The independent version-code-4 reviewer found no high-, medium- or low-severity issue in the scoped
guidance/version/docs diff. It reproduced 6/6 affected tests in each build variant, all 183 tests in
30 suites per variant, lint at zero errors / 43 warnings and Kover at 1066/1081 (98.61%) with the
recorded report digest. This scoped clean review does not erase the inherited low observability note
above. Exact clean and pushed commit `fc07da631e4ec1aee90e0c35002397d746860dd8` now also has a
protected, independently approved, v2-only signed versionCode 4 / `0.2.2` artifact; the unsigned
Gradle output was rejected. The artifact remains deliberately uninstalled, the Galaxy remains on
versionCode 2 / `0.2.0`, and physical Galaxy A54/One UI route validation remains an excluded live
boundary.

The connected result XML is
`android/app/build/outputs/androidTest-results/connected/debug/TEST-SM-A546B - 14-_app-.xml`; its
adjacent device directory contains the textproto and per-test logs, and the rendered report
is `android/app/build/reports/androidTests/connected/debug/index.html`. The Galaxy stayed
locked/dozing with no prompt. The production v2 package hash, PID, foreground service, connected H10,
near-zero queue, wake lock, radios and public health were unchanged; the Gradle lifecycle left no
debug/test package installed and no manual cleanup was performed. This closes only the connected
storage/Keystore instrumentation portion, not signed-v4 installation or live-v4 acceptance.

## Other Rust boundary exemptions

### `src/ble.rs`

Fake-backed tests cover parser/control decisions, bounded awaits, reconnect policy, stream
freshness, gap/error recording and pairing-loss classification. Remaining lines wire those pure
decisions into the real adapter, Ctrl-C and long-running collector loop. Replay and
`scripts/live_h10_smoke.sh` cover the real boundary.

### `src/gatt.rs`

This module deliberately adapts `btleplug::platform::{Adapter, Peripheral}` to project-owned
traits. Project-owned characteristic conversion is unit-tested and the traits are exercised by BLE
and PS-FTP fakes. Connect/discover/read/write/subscribe/notification/RSSI implementations require a
real adapter and H10 and are covered by live smoke.

### `src/live_server.rs`

Deterministic WAL fixtures cover startup windows/index repair, ECG/ACC/HR/RR/status restoration,
cloud envelopes, quarantine, marker writing, dashboard authorization, archive health and SSE batch
semantics. Executable Node harnesses additionally cover the embedded browser/SharedWorker generation
replacement, same-generation progress watchdog, snapshot retry ownership, multi-tab restart
deduplication, direct-EventSource fallback and stale-callback races. Those JavaScript paths are not
attributed by Rust source instrumentation. Remaining lines are browser drawing variants and
long-running listener/connection lifecycle branches, validated by replay and deployed dashboard
smoke.

### `src/main.rs`

This is intentionally a thin `clap`/Tokio entrypoint that delegates to tested modules. `cargo build`,
replay and live smoke exercise the binary; direct unit tests would primarily retest `clap` or launch
long-lived services.

### `src/psftp.rs`

Protocol construction, packet splitting, response matching, success/error metadata and timeout
behavior are fake-tested. Remaining lines are encrypted live-GATT wrapper/error branches; the live
smoke requires PS-FTP request/result evidence before accepting a run.

### `src/sink.rs`

Tests cover raw GATT-to-WAL capture, stale-open recovery, idempotent close, canonical filenames and
service-discovery summaries. Remaining lines are simple record-kind adapters and compatibility
defaults reached through the full H10 collection path; live smoke and replay validate their output.

## Retained test-only support

`src/csv.rs` is retained only as a `#[cfg(test)]` legacy `RecordSink` for parser and BLE
state-machine tests. It is not compiled into the production collector and is not a runtime
archive/live transport.


## Resting ECG research prototype — 25 September 2026

`resting_ecg/` is isolated archive-analysis/operator-report code. No first-party
Rust, collector state machine, production schema or hardware behavior changed;
`scripts/check.sh`, Rust coverage and live H10 smoke are exempt for this change.
No collector restart or deployment occurred.

Deterministic Python regression coverage (pytest-cov, line coverage):

| Module | Coverage | Evidence |
|---|---:|---|
| `core.py` | 97% | Exact timing, acquisition selection, gaps/contact/motion failures, abnormal morphology retained, missing context |
| `measures.py` | 95% | Four QT formulas, RR and units, missing/contradictory marks, invalid sentinel, uncertainty overlap, voltage baseline |
| `astra.py` | 95% | Schema/index validation, in-view validity, streaming completion/delta/item handling, explicit authorization/model, expired credentials, no secret in artifacts |

Run `python -m pytest resting_ecg/tests -q --cov=resting_ecg.core
--cov=resting_ecg.measures --cov=resting_ecg.astra` from the repository root.
50 tests, including numerical Octave adapter integration, passed. The bundled SWT
adapter agrees with pinned PyWavelets for impulse/ramp/noise, three filter banks
and six levels (54 tested transform levels); endpoint-extension fixtures passed.
This is not evidence of MATLAB or clinical equivalence.

Per-file exemptions from source-line instrumentation:

- `delineators.py` and `octave_adapter.m`: real local NeuroKit, WFDB/Fortran and
  Octave archive replay covers the adapters. Dedicated synthetic wave-group tests
  verify a T endpoint beyond an RR midpoint, biphasic peaks and repeated wave
  groups; raw-output review found/corrected upstream sentinel/association issues.
  One optional ECGdeli positive-R variant fails inside the upstream Octave
  synchronization path and is explicitly shown as failed. Default ECGdeli works.
- `octave_compat/*.m`: numerical integration fixtures above, plus actual ECGdeli
  replay; MATLAB line coverage tooling is unavailable. No equivalence claim.
- `figures.py`, `report.py`, `report.css`, `report.js`, `check_report.py`: actual
  artifact generation, visual desktop/mobile inspection, offline Chrome checks,
  all operator decision branches, JSON export and letter preparation, zero browser
  errors/external requests, one-page A4 PDF and exact SVG physical-width checks.
  Rendering code is not adequately validated by implementation-mirroring mocks.
- `__main__.py`: actual end-to-end archive runs with fresh external Astra
  annotation and separate interpretation requests, and an actual no-eligible-
  window run that produces a failure report without requesting an annotation.
  No clinical diagnostic performance or clearance safety validation is claimed.
- `repeatability.py`: deterministic matching-input/distinct-response regression
  tests plus actual three-run comparison and browser validation of its persistent
  gate/report attachment; HTML mutation and CLI glue are integration-covered.
- `__init__.py`: a version declaration only.

Independent methodological/code review passed before commit, including the final
report-binding guard and actual-run hashes. This is not clinical adjudication. Personal replay
inputs, native samples, prompts with data, responses, screenshots and reports stay
in ignored local `data/analysis/`; none are test fixtures committed to Git.

### Plotly review and shorter prompts — same-day follow-up

The suite now has 55 passing tests. Line coverage remains core 97%, measurements
95% and Astra client 95%; the new `interactive.py` has 90% coverage. Include
`--cov=resting_ecg.interactive` in the command above. Added checks preserve native
waveform/landmark coordinates, missing annotations, source immutability and report
binding. Cross-prompt overlays require matching waveform, image and model inputs;
they are labelled separately from identical-prompt repeatability evidence.

`interactive.js` is exempt from source-line instrumentation: actual offline Chrome
checks exercise default annotation visibility, method/overlay selection, hover,
uncertainty, beat navigation, QT rulers, full-strip reset, mobile layout and
printing. Existing operator gates, exports and letter preparation also pass.
No browser errors or external requests occurred. The print-only SVG retains a
250 mm ten-second trace. Visual review and independent artifact review verified
the native coordinates and comparison endpoints.

Two fresh archive runs exercise the shorter annotation/interpretation prompts.
A further interpretation request with the same seven unlabelled waveform panels
exercises the image-aware interpretation path; its earlier numeric-only response
is preserved. These runs demonstrate execution and provenance, not clinical
accuracy. Independent review approved the final source and private artifacts,
including the unresolved repeatability gate. Collector-specific exemptions above
still apply; no hardware interruption, runtime change or deployment occurred.

### Machine-informed Astra annotation — 30 September 2026

65 tests pass. Line coverage: core 97%, measurements 95%, Astra client 95%,
interactive 90%, and the new `informed.py` 100%. Include
`--cov=resting_ecg.informed` in the command above. New tests use a fake Astra to
check that every machine method, including failed tools, reaches the machine
review; that only beats inside the supplied samples are sent; that each stage
gets the replayed history and images it should; that the final revision, never
the draft, is measured; and that there is no fallback to the draft when plot
review fails. They also cover beat-plot widening for long PR/QT, per-stage
repeatability signatures, labelled display overlays, and HTML escaping of the
report's annotation-conversation section.

`repeatability.py` is 69% covered by source-line instrumentation. Its uncovered
HTML attachment/CLI glue keeps the integration exemption above. `figures.py` and
`report.py` rendering remain exempt as above, with the pure `beat_window` and
`astra_stages` logic now unit-tested. Actual end-to-end replays (run08, run09)
exercise all three annotation stages, interpretation, report rendering and the
browser check. No collector, runtime or hardware change: `scripts/check.sh`,
Rust coverage and the live H10 smoke test remain exempt.

## Experimental BP5S cuff scripts exemption (30 Sep 2026)

`experiments/ihealth_bp5s/` is a Python feasibility experiment outside the Rust collector and outside
`scripts/coverage.sh`. Its protocol core (framing, identify crypto, pressure scale) is covered by the
deterministic replay tests in `experiments/ihealth_bp5s/test_bp5s.py` against a recorded, accepted
handshake. BLE session handling, the analysis and the report builder were validated live
(`docs/ihealth_bp5s_feasibility_20260930.md`). If the cuff is integrated into the collector, it must
meet the normal Rust coverage gate.

## Experimental SCG ejection-timing analysis exemption (1 Oct 2026)

`experiments/scg_lvet/` is offline research analysis in Python, outside the Rust collector and outside
`scripts/coverage.sh`. Its core (`scg_core.py`: polarity choice, QRS-energy gate, beat guards, posture,
cross-correlation shift, feature tracking, end-to-end segment processing) is covered by synthetic tests in
`experiments/scg_lvet/test_scg_core.py`. The generalised extractor was regression-checked against the
30 Sep 2026 sums (identical with the new gate off). Data adapters, fetchers and the report builder are
validated by running them on the real datasets and by a misaligned-accelerometer negative control
(`docs/scg_population_reproducibility_20261001.md`). Not collector code; no runtime coverage gate applies.

## seca Android receive/archive pipeline — 21 October 2026

The bounded independent-reader candidate passes 290 JVM tests, Android lint and
Kover verification. `SecaIngress.kt` is 70/70 lines covered (100%): fragment and
buffer ownership, conservative run transition, TX ordering/durable ACK, overflow
reservation, partial tail, consumer cancellation before startup, disk failure,
drain-before-seal and fresh connection state. `SecaService.kt` remains the existing
Android foreground-service/Bluetooth boundary exemption; new ownership/drain
logic is exercised through the pure ingress tests and independent review, with
signed varying-input live acceptance still pending. This candidate retains fsync
per record and does not claim that radio gaps or framing failures are fixed.

## seca default-off LED traffic experiment — 21 October 2026

297 Android JVM tests pass, with lint/Kover verification. The pure typed status
parser, raw-O transformation, per-connection freshness and command selection in
`SecaLedStatus.kt` cover45/45lines (100%); cadence helper3/3. Tests cover all256
raw values, reviewed single-bit/shared-fault vectors, complementary banks/default
override, mixed fields and every compression width, embedded O bytes, malformed
whole frames, status aging after delayed durable ACK, normal R behavior and cancellation. Android service/
Bluetooth remains the existing manual/live boundary exemption. This default-off
candidate is an experiment; real LED effect and transport acceptance remain open.


Android seca upload fairness (21 October 2026): all 304 unit tests, lintDebug,
Kover report and verification pass. `SecaUploadSchedule.kt` covers 14/14 lines
(100%). Tests cover per-session lowest-index ordering, three-active/one-history
fairness, historical round-robin insertion/removal, active reconnects, retry
without budget advancement, absent active sessions, and exact ACK followed by
failed directory synchronization. Mixed-session MockWebServer tests exercise the
real durable queue and uploader. The narrow runtime wiring uses existing tested
HTTP/ACK boundaries; phone live freshness remains a root-owned acceptance check.
Independent review by seca_review approved the scheduler and durability boundary.


The subsequent LED selection-clock race regression passes with all 305 Android
unit tests, lint and Kover verification. `SecaLedStatus.kt` remains 45/45 lines
covered. A real thread waits behind the observation monitor while a newer O is
published; its clock supplier asserts monitor ownership and then sees current
time. Negative ages, exact 3,000 ms acceptance, 3,001 ms rejection, and immutable
post-journal freshness checks remain covered. Parent owns long live acceptance.


Android bounded framing-recovery experiment and fresh Stop upload (21 October2026):
318 unit tests pass with the private HCI fixture enabled, zero failures/errors/
skips; ordinary and explicit LED/recovery builds pass lint and Kover verification.
`SecaRecovery.kt` covers135/135lines (100%); `SecaIngress.kt`81/81 (100%). The eight
private cases exercise2,672 split variants with exact byte/frame-hash conservation,
3,007 retained corrupt bytes and114 visible missing groups. No private waveform
fixture is committed; set `SECA_RECOVERY_CASES` to the ignored local JSON fixture
for that optional test (otherwise it is explicitly skipped). Synthetic tests
exercise typed parsing, every recovery bound, counter ambiguities, TX ordering,
Stop and overflow. Independent cloud review approved recovery; Android-agent
review approved the Stop work policy and cancellation/retry regression. Android
WorkManager scheduling, Bluetooth I/O and experimental live acceptance remain
platform-boundary exemptions covered by root's controlled phone trials; unit tests
do not claim lossless4kHz behavior.

## 23 October 2026 — seca checksum recovery and reconnect view

Android debug and release: **319 tests each**, no failures/errors/skips, including
private eight-stream HCI replay and a new 2 kHz checksum-error/all-splits regression.
Kover verification passes: **2087/2116 lines (98.63%)**, `SecaRecovery.kt`135/135,
`SecaIngress.kt`81/81. Existing Android UI/service boundary exemptions apply;
phone install and live cloud validation are recorded in the repair proposal.
Lint passes. Dashboard regressions cover same-source reconnect selection,
manual selection and seek, browser range clamping, stale requests, capped-list
omission and error deduplication. `scripts/check.sh` passes (297 Rust tests,
one ignored, 116 Python tests, Clippy and JavaScript checks). No Rust source
coverage changed; the embedded HTML is validated with its executed JS tests.

## seca GPS collection — 23 October 2026

Android debug/release suites each run 323 tests: 322 pass and the optional private
HCI-fixture test skips when its environment variable is absent. Lint and Kover
verification pass; measured lines 2091/2120 (98.63%). New `LocationOwnership.kt`
is 3/3; changed `QueueRepository.kt` is 79/81. Deterministic regressions exercise
exclusive listener ownership, cancellation/cleanup handover, disabled waiters and
interleaved H10/phone epochs with stable retries and ACK-by-ACK draining.

Existing LocationCollector, foreground-service, application graph and UI boundary
exemptions apply. Signed Galaxy acceptance validates seca alone, H10 service absent,
location foreground type, real GPS/fused fixes, GNSS metadata, matching seca session
and phone-only provenance, canonical CRC-checked cloud WAL and screen-off receipt.
`scripts/check.sh` passes. Rust coverage/live H10 smoke are inapplicable: no Rust or
H10 BLE-boundary changes; do not introduce a second H10 collector for this test.

## Native Android login/logout — 23 October 2026

Debug and release each pass 334 JVM tests (one optional private HCI fixture skip),
with lint, signed release assembly and Kover verification. Coverage is 2159/2188
lines (98.67%). `PasswordEnrollmentClient.kt`36/36, `EnrollmentManager.kt`67/67,
`DeviceLogout.kt`8/8 and `SecaQueue.kt`110/110 are fully covered. Regressions cover
cookie/Origin/CSRF isolation, path/expiry filtering, redirect refusal, failed login,
foreign enrollment URLs, duplicate submission, exact exchange retry, refusal to
log out with running acquisition/pending uploads/unreadable queues, and unfinished
seca journals. The native `SignInActivity` is a UI/platform exemption like
`MainActivity`, covered by lint/build and the signed Vivo screen check. Password
entry and actual account connection require the wearer on the phone. Confirmed
Android app-data reset is intentionally not executed on the user's installation.
No Rust source or BLE changes; Rust coverage/live H10 gates are exempt. Full
`scripts/check.sh` passes.

## iOS client

Swift is outside the Rust coverage gate. `ios/H10Kit` has 78 tests covering protocol parsing, restored sensor identity, independent queues, upload recovery, enrollment origins, history, Health export, and Watch data. Storage tests cover large plain/compressed backlogs, exact retry bytes, dictionary resets, header/payload corruption, truncated frames, nonempty compaction, restart, disk-full recovery, and warning thresholds. The iOS test target covers navigation, chart interaction, events, contact freshness, Health permissions, and the low-storage alert. Simulator previews use synthetic data; CoreBluetooth and power use require device checks.

## Native mobile parity (9 October 2026)

`Mobile parity` CI runs Android unit, screenshot, lint, build, and Kover checks.
The iOS workflow runs shared packet fixtures and light/dark screenshot comparisons.
Both UI tests discover screen sections from `shared/screens/` and verify they are reachable.

- Compose drawing, UI lifecycle adapters, and SetupActivity are exempt from line coverage. Native UI tests, screenshots, lint, and builds cover those boundaries.
- Signal decoding, beat detection, display buffers, chart selection, and history storage remain in measured Android coverage.
- Queue tests cover atomic event edits, reboot epochs, and the non-destructive Room migration.
- SwiftUI drawing and generated design values use screenshot and contract checks. Rust is unchanged.
- Physical Android BLE, background, and upgrade acceptance remain unverified for this build.
