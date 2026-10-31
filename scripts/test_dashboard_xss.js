'use strict';

const assert = require('assert');
const fs = require('fs');
const vm = require('vm');

const source = fs.readFileSync('src/live_server.rs', 'utf8');
const start = source.indexOf('function htmlText(value)');
const end = source.indexOf('function csrfToken()', start);
assert(start >= 0 && end > start, 'dashboard escaping/rendering functions not found');

const context = {
  Date,
  latestSeq: 7,
  state: {ecg: [], acc: [], hr: [], rr: [], events: []},
  asNumber(value, fallback = 0) {
    const number = Number(value);
    return Number.isFinite(number) ? number : fallback;
  },
  fmtMaybe(value, decimals = 0, suffix = '') {
    return value === null || value === undefined ? '—' : `${Number(value).toFixed(decimals)}${suffix}`;
  },
  fmtNs() { return 'now'; },
};
vm.createContext(context);
vm.runInContext(source.slice(start, end), context);

const fields = [
  'connection_state',
  'latest_source',
  'last_disconnect',
  'last_upload_ack',
  'last_upload_error',
  'last_collector_error',
  'marker_status',
  'critical_load_index_state',
  'critical_load_start_path',
  'history_load_state',
  'index_build_state',
  'index_last_error',
  'last_tail_error',
  'history_last_error',
  'sse_last_write_error',
];
const status = {};
for (const field of fields) {
  status[field] = `<img data-field="${field}" src=x onerror="globalThis.pwned=true">&'`;
}
status.archive = {
  state: '<img data-field="archive_state" src=x onerror="globalThis.pwned=true">',
  error: '<img data-field="archive_error" src=x onerror="globalThis.pwned=true">',
  last_success_unix_ms: null,
  last_failure_unix_ms: null,
};
const html = context.walHealthHtml(status);

for (const field of fields) {
  assert(
    !html.includes(`<img data-field="${field}"`),
    `untrusted ${field} reached innerHTML without escaping`,
  );
  assert(
    html.includes(`&lt;img data-field=&quot;${field}&quot;`),
    `escaped ${field} is missing from rendered diagnostics`,
  );
}
for (const field of ['archive_state', 'archive_error']) {
  assert(
    !html.includes(`<img data-field="${field}"`),
    `untrusted ${field} reached innerHTML without escaping`,
  );
  assert(
    html.includes(`&lt;img data-field=&quot;${field}&quot;`),
    `escaped ${field} is missing from rendered archive diagnostics`,
  );
}
assert.strictEqual(context.pwned, undefined, 'malicious status payload executed');
console.log('dashboard malicious status/detail escaping test passed');
