'use strict';

// Execute the shipped page script, including the cookie-free external-navigation
// recovery path. Rust tests separately pin which routes can serve this public shell.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const source = fs.readFileSync('src/cloud/web.rs', 'utf8');
const html = source.match(/const SESSION_BOOTSTRAP_HTML: &str = r#"([\s\S]*?)"#;/)?.[1];
assert(html, 'session recovery HTML exists');
const script = html.match(/<script>([\s\S]*?)<\/script>/)?.[1];
assert(script, 'session recovery script exists');

async function scenario(cookie, responses, expectedPaths, destination, timeout = false) {
  const requests = [], navigations = [], timers = new Map();
  const original = 'https://sensors.example/dashboard/own-user?view=live#trace';
  let timerId = 0;
  const context = {
    document: {cookie},
    location: {href: original, replace: url => navigations.push(url), reload: () => navigations.push(original)},
    AbortController,
    setTimeout(fn, ms) {
      assert.equal(ms, 10000, 'network recovery has a finite timeout');
      timers.set(++timerId, fn);
      return timerId;
    },
    clearTimeout: id => timers.delete(id),
    async fetch(url, options) {
      requests.push({url, options});
      assert.equal(options.credentials, 'same-origin');
      assert.equal(options.cache, 'no-store');
      assert(options.signal instanceof AbortSignal);
      const result = responses.shift();
      if (result === 'hang') {
        return new Promise((_, reject) => {
          options.signal.addEventListener('abort', () => reject(new Error('aborted')));
        });
      }
      if (result instanceof Error) throw result;
      assert.equal(typeof result, 'number', 'unexpected authentication request');
      return {ok: result >= 200 && result < 300, status: result};
    },
  };
  const done = vm.runInNewContext(script, context);
  if (timeout) {
    for (let turn = 0; requests.length < expectedPaths.length && turn < 20; turn++) await Promise.resolve();
    assert.equal(timers.size, 1);
    [...timers.values()][0]();
  }
  await done;
  assert.deepEqual(requests.map(r => r.url), expectedPaths);
  assert.deepEqual(navigations, [destination === 'original' ? original : '/login']);
  assert.equal(timers.size, 0, 'completed requests release timeout timers');
  for (const {url, options} of requests) {
    if (url.endsWith('browser-refresh')) {
      assert.equal(options.method, 'POST');
      assert.equal(options.headers['x-csrf-token'], 'browser-csrf');
    } else {
      assert.equal(options.method, undefined, 'valid sessions need only a read');
    }
  }
}

(async () => {
  const marker = 'unrelated=value; sensors_csrf=browser-csrf';
  const me = '/api/v1/me', refresh = '/api/v1/auth/browser-refresh';
  await scenario('', [], [], 'login');
  await scenario('sensors_csrf=', [], [], 'login');
  await scenario('sensors_csrf=%ZZ', [], [], 'login');
  // The initial request may have had no cookies; document.cookie is available
  // after the recovery page loads on our origin. A valid session is not rotated.
  await scenario(marker, [200], [me], 'original');
  await scenario(marker, [401, 200], [me, refresh], 'original');
  await scenario(marker, [401, 401], [me, refresh], 'login');
  await scenario(marker, [401, 403], [me, refresh], 'login');
  await scenario(marker, [403], [me], 'login');
  await scenario(marker, [500], [me], 'login');
  await scenario(marker, [new Error('offline')], [me], 'login');
  await scenario(marker, [401, new Error('offline')], [me, refresh], 'login');
  await scenario(marker, ['hang'], [me], 'login', true);
  await scenario(marker, [401, 'hang'], [me, refresh], 'login', true);
  console.log('Session recovery: 13 scenarios passed.');
})().catch(error => { console.error(error); process.exitCode = 1; });
