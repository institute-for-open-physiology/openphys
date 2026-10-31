// Exercise the shipped seca dashboard with deterministic requests and timers.
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const html = fs.readFileSync('src/cloud/seca.html', 'utf8');
const source = html.match(/<script>([\s\S]*?)<\/script>/)[1];
const requests = [], timers = new Map(), elements = new Map();
let nextTimer = 0;
function element() {
  return {value: '', checked: false, textContent: '', children: [],
    append(...items) { this.children.push(...items); },
    replaceChildren(...items) { this.children = items; }, setAttribute() {}};
}
const context = vm.createContext({
  console, AbortController, Date, Error,
  document: {
    getElementById(id) {
      if (!elements.has(id)) {
        const node = element();
        if (id === 'position') {
          let maximum = 0, value = '0';
          Object.defineProperties(node, {
            max: {get: () => String(maximum), set: x => {maximum = Number(x); value = String(Math.min(Number(value), maximum));}},
            value: {get: () => value, set: x => {value = String(Math.max(0, Math.min(Number(x), maximum)));}},
          });
        }
        elements.set(id, node);
      }
      return elements.get(id);
    },
    createElement: element, querySelectorAll() { return []; },
  },
  window: {addEventListener() {}},
  setTimeout(fn, ms) { const id = ++nextTimer; timers.set(id, {fn, ms}); return id; },
  clearTimeout(id) { timers.delete(id); }, setInterval() {},
  fetch(path, options) {
    return new Promise((resolve, reject) => {
      options.signal.addEventListener('abort', () => {
        const error = new Error('aborted'); error.name = 'AbortError'; reject(error);
      });
      requests.push({path, resolve: value => resolve({ok: true, status: 200, json: async () => value})});
    });
  },
});
function state(expression) { return vm.runInContext(expression, context); }
async function settle() { for (let i = 0; i < 12; i++) await Promise.resolve(); }
const a = {last_batch_index: 7, session_id: 'a', user_id: 'user', device_id: 'seca', installation_id: 'phone', sample_rate_hz: 4000, started_unix_ms: 0};
const b = {...a, session_id: 'b', started_unix_ms: 1000, last_batch_index: 10};
function snapshot(session, end) {
  return {summary: {...session, sample_count: 0, packet_count: 0, last_batch_index: 10,
    received_unix_ms: Date.now(), last_capture_unix_ns: 0, last_ecg_capture_unix_ns: null,
    battery_percent: null, missing_blocks: 0, sequence_errors: 0, decode_errors: 0},
    end_batch_index: end, rows: [], errors: [], stride: 8, gap_count: 0, preview: 'test'};
}
(async () => {
  vm.runInContext(source, context);
  assert.equal(requests.length, 1);
  const firstTimer = [...timers.values()][0];
  assert.equal(firstTimer.ms, 10000);
  firstTimer.fn(); await settle();
  assert.equal(state('busy'), false, 'timeout releases the single-request gate');
  assert.match(elements.get('error').textContent, /timed out/);
  assert.equal(timers.size, 0, 'timeout is always cleared');

  elements.get('live').checked = true;
  elements.get('refresh').onclick();
  requests[1].resolve({sessions: [a, b]}); await settle();
  assert.match(requests[2].path, /seca\/a\/snapshot$/);
  elements.get('position').max = 2; // A shorter previous archive clamps range values.
  elements.get('recordings').value = 'b';
  elements.get('recordings').onchange();
  assert.equal(Number(elements.get('position').value), 10, 'new recording opens at its own newest chunk');
  assert.equal(elements.get('live').checked, false, 'manual recording selection exits follow mode');
  requests[2].resolve(snapshot(a, 10)); await settle();
  assert.equal(state('snapshot'), null, 'old response cannot repopulate the selected view');
  assert.match(requests[3].path, /seca\/b\/snapshot\?end=/);
  requests[3].resolve(snapshot(b, 10)); await settle();
  assert.equal(state('snapshot.summary.session_id'), 'b');

  state('void update()'); await settle();
  const previous = requests[4];
  elements.get('position').value = '3';
  elements.get('position').onchange();
  previous.resolve(snapshot(b, 10)); await settle();
  assert.equal(state('snapshot'), null, 'in-flight live response cannot overwrite a manual seek');
  assert.match(requests[5].path, /snapshot\?end=3$/);
  requests[5].resolve(snapshot(b, 3)); await settle();
  assert.equal(state('snapshot.end_batch_index'), 3);
  assert.equal(state('busy'), false);
  assert.equal(timers.size, 0);
  // Follow reconnects only within the chosen owner's installation and device.
  const c = {...b, session_id: 'c', started_unix_ms: 2000};
  const otherPhone = {...c, session_id: 'other-phone', installation_id: 'different', started_unix_ms: 9000};
  const otherDevice = {...c, session_id: 'other-device', device_id: 'different', started_unix_ms: 10000};
  const otherOwner = {...c, session_id: 'other-owner', user_id: 'different', started_unix_ms: 11000};
  elements.get('live').checked = true;
  elements.get('live').onchange();
  requests[6].resolve({sessions: [otherOwner, otherDevice, otherPhone, c, b, a]}); await settle();
  assert.equal(state('selected'), 'c');
  assert.match(requests[7].path, /seca\/c\/snapshot$/);
  const recovered = snapshot(c, 10);
  recovered.summary.last_error = 'checksum mismatch';
  recovered.errors = ['checksum mismatch', 'other error'];
  requests[7].resolve(recovered); await settle();
  assert.match(elements.get('status').textContent, /Following a newer recording/);
  assert.equal(elements.get('error').textContent, 'checksum mismatch\nother error');

  // An explicit historical choice survives refresh, even when newer sessions exist.
  elements.get('recordings').value = 'a';
  elements.get('recordings').onchange();
  requests[8].resolve(snapshot(a, 10)); await settle();
  elements.get('refresh').onclick();
  requests[9].resolve({sessions: [c, b, a]}); await settle();
  assert.equal(state('selected'), 'a');
  requests[10].resolve(snapshot(a, 10)); await settle();
  assert.doesNotMatch(elements.get('status').textContent, /Following a newer recording/);

  // A manual selection while a relist is in flight invalidates its auto-follow.
  elements.get('live').checked = true; elements.get('live').onchange();
  elements.get('recordings').value = 'b'; elements.get('recordings').onchange();
  requests[11].resolve({sessions: [c, b, a]}); await settle();
  assert.equal(state('selected'), 'b');
  requests[12].resolve(snapshot(b, 10)); await settle();
  assert.equal(state('snapshot.summary.session_id'), 'b');
  assert.equal(timers.size, 0);
  // A selected archive omitted by the capped list must not jump to another owner.
  elements.get('live').checked = true; elements.get('live').onchange();
  requests[13].resolve({sessions: [otherOwner, otherDevice, otherPhone]}); await settle();
  assert.equal(state('selected'), 'b');
  assert.match(requests[14].path, /seca\/b\/snapshot$/);
  requests[14].resolve(snapshot(b, 10)); await settle();
  assert.match(html, /value="5000" selected/);
  console.log('seca dashboard timeout, session-switch and seek recovery checks passed');
})().catch(error => { console.error(error); process.exitCode = 1; });
