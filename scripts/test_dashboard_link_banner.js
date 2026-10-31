'use strict';

// Pins renderLinkBanner: the red "DATA IS BEING LOST" banner may only fire for a live
// sensor->phone link failure (device-clock ordered, upload path alive), the amber
// buffering banner only for genuine upload silence, and replay never banners.

const assert = require('assert');
const fs = require('fs');
const vm = require('vm');

const source = fs.readFileSync('src/live_server.rs', 'utf8');
const start = source.indexOf('function renderActionBanner(');
const end = source.indexOf('function render()', start);
assert(start >= 0 && end > start, 'dashboard banner functions not found');

const elements = {};
function element(id) {
  if (!elements[id]) elements[id] = {textContent: '', innerHTML: '', className: '', style: {}};
  return elements[id];
}

const context = {
  Date,
  // Module-scope latch trackers are declared outside the sliced region.
  lastPairingBreakdownNs: 0,
  lastStreamingProgressNs: 0,
  lastLinkFailureNs: 0,
  lastLinkRecoveryNs: 0,
  document: {getElementById: element, cookie: ''},
  // Byte-for-byte the dashboard's real asNumber semantics (Number(null) === 0).
  asNumber(value, fallback = 0) {
    const number = Number(value);
    return Number.isFinite(number) ? number : fallback;
  },
  fmtNsDateTime() { return 'formatted-time'; },
  fmtNs() { return 'now'; },
  htmlText(value) { return String(value); },
};
vm.createContext(context);
vm.runInContext(source.slice(start, end), context);

function resetLatches() {
  context.lastLinkFailureNs = 0;
  context.lastLinkRecoveryNs = 0;
}

const banner = element('linkBanner');

function run(status, events) {
  context.renderLinkBanner({status, events: events || []});
}

// 1. Live BLE loss: failure events newer than any ECG, uploads alive, ECG stale -> red.
resetLatches();
run(
  {latest_ecg_unix_ns: 1_000, ecg_freshness_age_ms: 45_000, upload_receive_freshness_age_ms: 3_000, latest_upload_received_unix_ns: 99},
  [{event: 'connection_cycle_failed', collector_unix_ns: 5_000}],
);
assert.strictEqual(banner.style.display, 'block', 'red banner must show for live link loss');
assert.strictEqual(banner.className, '', 'link-loss banner is the loud red variant');
assert(banner.innerHTML.includes('DATA IS BEING LOST'), 'red banner copy');

// 2. Streaming recovery event newer than the failure clears it.
run(
  {latest_ecg_unix_ns: 1_000, ecg_freshness_age_ms: 45_000, upload_receive_freshness_age_ms: 3_000, latest_upload_received_unix_ns: 99},
  [{event: 'pmd_stream_start_confirmed', collector_unix_ns: 6_000}],
);
assert.strictEqual(banner.style.display, 'none', 'recovery event must clear the red banner');

// 3. Fresh ECG clears it even while old failure events remain latched.
resetLatches();
run(
  {latest_ecg_unix_ns: 1_000, ecg_freshness_age_ms: 2_000, upload_receive_freshness_age_ms: 3_000, latest_upload_received_unix_ns: 99},
  [{event: 'connection_cycle_failed', collector_unix_ns: 5_000}],
);
assert.strictEqual(banner.style.display, 'none', 'fresh ECG must suppress the red banner');

// 4. Catch-up drain immunity: failure events are OLDER than the newest ECG row.
resetLatches();
run(
  {latest_ecg_unix_ns: 9_000, ecg_freshness_age_ms: 300_000, upload_receive_freshness_age_ms: 2_000, latest_upload_received_unix_ns: 99},
  [{event: 'connection_cycle_failed', collector_unix_ns: 5_000}],
);
assert.strictEqual(banner.style.display, 'none', 'old failure events during catch-up must not banner');

// 5. Upload stall: no live upload path -> the red banner may not claim live loss...
resetLatches();
run(
  {latest_ecg_unix_ns: 1_000, ecg_freshness_age_ms: 300_000, upload_receive_freshness_age_ms: 400_000, latest_upload_received_unix_ns: 99},
  [{event: 'connection_cycle_failed', collector_unix_ns: 5_000}],
);
assert.strictEqual(banner.style.display, 'block', 'upload silence must show the buffering banner');
assert.strictEqual(banner.className, 'buffering', 'upload silence is the amber variant');
assert(banner.innerHTML.includes('NO DATA ARRIVING'), 'buffering banner copy');
assert(banner.innerHTML.includes('buffered on the phone'), 'buffering banner must reassure about the durable queue');
assert(banner.innerHTML.includes('formatted-time'), 'buffering banner shows the last-upload time');

// 6. Grace band between 30 s and 60 s of upload silence: neither banner.
resetLatches();
run(
  {latest_ecg_unix_ns: 1_000, ecg_freshness_age_ms: 45_000, upload_receive_freshness_age_ms: 45_000, latest_upload_received_unix_ns: 99},
  [{event: 'connection_cycle_failed', collector_unix_ns: 5_000}],
);
assert.strictEqual(banner.style.display, 'none', 'grace band must not banner');

// 7. Replay suppresses everything.
resetLatches();
run(
  {replay: true, latest_ecg_unix_ns: 1_000, ecg_freshness_age_ms: 300_000, upload_receive_freshness_age_ms: 400_000, latest_upload_received_unix_ns: 99},
  [{event: 'connection_cycle_failed', collector_unix_ns: 5_000}],
);
assert.strictEqual(banner.style.display, 'none', 'replay must never banner');

// 8. A user who has never uploaded gets no buffering banner.
resetLatches();
run({latest_ecg_unix_ns: null, ecg_freshness_age_ms: null, upload_receive_freshness_age_ms: null, latest_upload_received_unix_ns: null}, []);
assert.strictEqual(banner.style.display, 'none', 'never-uploaded user must not banner');

// 9. Null upload age must never count as "uploads alive": the local macOS dashboard has
// no phone/upload path (both upload fields permanently null) and must never show the
// phone-specific red banner, even with a watchdog event latched and ECG stale.
resetLatches();
run(
  {latest_ecg_unix_ns: 1_000, ecg_freshness_age_ms: 300_000, upload_receive_freshness_age_ms: null, latest_upload_received_unix_ns: null},
  [{event: 'ecg_watchdog_timeout', collector_unix_ns: 5_000}],
);
assert.strictEqual(banner.style.display, 'none', 'null upload age must suppress the red banner');

// 10. Null ECG age (server restarted mid-outage, no ECG row seen yet) still counts as
// stale, so a live link loss is not missed.
resetLatches();
run(
  {latest_ecg_unix_ns: null, ecg_freshness_age_ms: null, upload_receive_freshness_age_ms: 3_000, latest_upload_received_unix_ns: 99},
  [{event: 'connection_cycle_failed', collector_unix_ns: 5_000}],
);
assert.strictEqual(banner.style.display, 'block', 'null ECG age is stale, red banner must show');
assert.strictEqual(banner.className, '', 'null-ECG-age loss is the loud red variant');

// 11. Watchdog timeouts and adapter-off also arm the red latch.
for (const eventName of ['ecg_watchdog_timeout', 'acc_watchdog_timeout', 'bluetooth_adapter_off']) {
  resetLatches();
  run(
    {latest_ecg_unix_ns: 1_000, ecg_freshness_age_ms: 45_000, upload_receive_freshness_age_ms: 3_000, latest_upload_received_unix_ns: 99},
    [{event: eventName, collector_unix_ns: 5_000}],
  );
  assert.strictEqual(banner.style.display, 'block', `${eventName} must arm the red banner`);
  assert.strictEqual(banner.className, '', `${eventName} arms the loud red variant`);
}

console.log('dashboard link-loss banner tests passed');
