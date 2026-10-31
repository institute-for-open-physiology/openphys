'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const rust = fs.readFileSync('src/cloud/web.rs', 'utf8');
const start = rust.indexOf('const nsms=');
const end = rust.indexOf('startOverview().catch', start);
assert(start >= 0 && end > start, 'extract admin overview transport functions');

function snapshot(streamId, latestSeq, users = ['user-1']) {
  return {
    schema_version: 1,
    archive: {state: 'ok'},
    users: users.map(id => ({
      user: {id},
      status: {stream_id: streamId, latest_sequence: latestSeq},
      ecg: [],
      latest_sequence: latestSeq,
    })),
  };
}

function overview(streamId, fromSeq, toSeq, users = ['user-1']) {
  return {
    schema_version: 1,
    archive: {state: 'ok'},
    users: users.map((userId, userIndex) => ({
      user_id: userId,
      batch: {
        schema_version: 1,
        stream_id: streamId,
        from_seq: fromSeq,
        to_seq: toSeq,
        latest_seq: toSeq,
        overflow: false,
        status: {stream_id: streamId, latest_sequence: toSeq},
        items: [{
          seq: toSeq,
          kind: 'ecg',
          row: {collector_unix_ns: toSeq, device_ts_ns: toSeq, microvolts: 100 + userIndex},
        }],
      },
    })),
  };
}

function multiplex(...payloads) {
  return {
    schema_version: 1,
    archive: {state: 'ok'},
    users: payloads.flatMap(payload => payload.users),
  };
}

function response(value, ok = true, status = 200) {
  return {ok, status, async json() { return value; }};
}

function deferred() {
  let resolve;
  const promise = new Promise(yes => { resolve = yes; });
  return {promise, resolve};
}

function makeHarness() {
  const fetchQueue = [];
  const fetchCalls = [];
  const timers = [];
  const sources = [];
  const context = {
    console: {error() {}},
    document: {querySelector() { return null; }, querySelectorAll() { return []; }},
    EventSource: class {
      constructor(url) { this.url = url; this.listeners = new Map(); sources.push(this); }
      addEventListener(name, callback) { this.listeners.set(name, callback); }
      emit(name, payload) { this.listeners.get(name)({data: JSON.stringify(payload)}); }
    },
    async fetch(url, options) {
      fetchCalls.push({url, options});
      assert(fetchQueue.length, `unexpected fetch ${url}`);
      return fetchQueue.shift();
    },
    setTimeout(callback, delay) {
      const timer = {callback, delay, active: true};
      timers.push(timer);
      return timer;
    },
  };
  vm.createContext(context);
  vm.runInContext(`${rust.slice(start, end)}
this.adminOverviewTest = {
  refreshOverview,
  applyOverview,
  startOverview,
  requestOverviewRefresh,
  state(userId) { return live.get(userId); },
  pending() { return overviewPending.length; },
};`, context, {filename: 'admin-overview-stream-recovery.js'});
  return {
    api: context.adminOverviewTest,
    enqueue(value) { fetchQueue.push(value); },
    fetchCalls,
    timers,
    sources,
    runNextTimer() {
      const timer = timers.find(candidate => candidate.active);
      assert(timer, 'active overview retry timer');
      timer.active = false;
      timer.callback();
      return timer;
    },
  };
}

async function flushAsync() {
  for (let index = 0; index < 6; index += 1) await Promise.resolve();
  await new Promise(resolve => setImmediate(resolve));
}

(async () => {
  const startup = makeHarness();
  await startup.api.startOverview();
  assert.equal(startup.fetchCalls.length, 0, 'initial snapshot and cursor come from one SSE connection');
  assert.equal(startup.sources.length, 1);
  const feed = startup.sources[0];
  assert.equal(feed.url, '/api/v1/admin/overview/events');
  feed.emit('overview_snapshot', snapshot('stream-a', 20000));
  feed.emit('overview', overview('stream-a', 20000, 20001));
  assert.equal(startup.api.state('user-1').latestSeq, 20001, 'new data applies immediately after full-ring startup');
  assert.deepEqual(Array.from(startup.api.state('user-1').ecg, row => row.device_ts_ns), [20001]);
  feed.emit('overview_snapshot', snapshot('stream-a', 30000));
  feed.emit('overview', overview('stream-a', 30000, 30001));
  assert.deepEqual(Array.from(startup.api.state('user-1').ecg, row => row.device_ts_ns), [30001], 'reconnect replaces prior data and starts at current cursor');
  for (const ok of [true, false]) {
    const stale = deferred();
    startup.enqueue(stale.promise);
    startup.api.requestOverviewRefresh();
    feed.emit('overview', overview('stream-a', 30001, 30002));
    feed.emit('overview_snapshot', snapshot('stream-b', 40000));
    feed.emit('overview', overview('stream-b', 40000, 40001));
    assert.equal(startup.api.pending(), 0, 'fresh SSE data is not gated by superseded HTTP, even if it hangs');
    assert.equal(startup.api.state('user-1').latestSeq, 40001);
    stale.resolve(response(snapshot('stream-a', 30001), ok, ok ? 200 : 503));
    await flushAsync();
    assert.equal(startup.api.state('user-1').streamId, 'stream-b', 'stale HTTP cannot rewind SSE snapshot');
    assert.equal(startup.api.state('user-1').latestSeq, 40001, 'superseded request completion drains later queued data, including on HTTP failure');
    assert.equal(startup.api.pending(), 0);
    feed.emit('overview', overview('stream-b', 40001, 40002));
    assert.deepEqual(Array.from(startup.api.state('user-1').ecg, row => row.device_ts_ns), [40001, 40002]);
  }

  const lower = makeHarness();
  lower.enqueue(Promise.resolve(response(snapshot('stream-a', 100, ['user-1', 'user-2']))));
  await lower.api.refreshOverview();
  const lowerSnapshot = deferred();
  lower.enqueue(lowerSnapshot.promise);
  lower.api.applyOverview(overview('stream-b', 5, 6, ['user-1', 'user-2']));
  lower.api.applyOverview(overview('stream-b', 6, 7, ['user-1', 'user-2']));
  assert.equal(lower.fetchCalls.length, 2, 'all users share one generation resnapshot');
  assert.equal(lower.api.pending(), 2, 'new-generation batches are buffered while snapshot is in flight');
  lowerSnapshot.resolve(response(snapshot('stream-b', 5, ['user-1', 'user-2'])));
  await flushAsync();
  for (const userId of ['user-1', 'user-2']) {
    const current = lower.api.state(userId);
    assert.equal(current.streamId, 'stream-b');
    assert.equal(current.latestSeq, 7, 'buffered batches advance from the replacement snapshot');
    assert.deepEqual(Array.from(current.ecg, row => row.device_ts_ns), [6, 7], 'first replacement ECG row is not skipped');
  }

  const higherSnapshot = deferred();
  lower.enqueue(higherSnapshot.promise);
  lower.api.applyOverview(overview('stream-c', 200, 201, ['user-1', 'user-2']));
  assert.equal(lower.fetchCalls.length, 3, 'stream identity triggers recovery even when replacement sequence is higher');
  higherSnapshot.resolve(response(snapshot('stream-c', 200, ['user-1', 'user-2'])));
  await flushAsync();
  for (const userId of ['user-1', 'user-2']) {
    const current = lower.api.state(userId);
    assert.equal(current.streamId, 'stream-c');
    assert.equal(current.latestSeq, 201);
    assert.deepEqual(Array.from(current.ecg, row => row.device_ts_ns), [201]);
  }

  const overlapping = makeHarness();
  overlapping.enqueue(Promise.resolve(response(snapshot('stream-a', 100))));
  await overlapping.api.refreshOverview();
  const overlappingSnapshot = deferred();
  overlapping.enqueue(overlappingSnapshot.promise);
  overlapping.api.applyOverview(overview('stream-b', 5, 6));
  overlapping.api.applyOverview(overview('stream-c', 200, 201));
  assert.equal(overlapping.api.pending(), 2, 'both restart generations are bounded while one snapshot is in flight');
  overlappingSnapshot.resolve(response(snapshot('stream-c', 200)));
  await flushAsync();
  const converged = overlapping.api.state('user-1');
  assert.equal(converged.streamId, 'stream-c');
  assert.equal(converged.latestSeq, 201, 'first row from the snapshot generation survives an overlapping older restart');
  assert.deepEqual(Array.from(converged.ecg, row => row.device_ts_ns), [201]);
  assert.equal(overlapping.api.pending(), 0, 'stale queued generation is discarded instead of requeued forever');
  assert(!overlapping.timers.some(candidate => candidate.active), 'overlapping B-to-C restart converges without a retry loop');
  assert.equal(overlapping.fetchCalls.length, 2, 'one shared snapshot resolves both overlapping generations');

  const mixed = makeHarness();
  mixed.enqueue(Promise.resolve(response({
    schema_version: 1,
    archive: {state: 'ok'},
    users: [
      ...snapshot('stream-a', 100, ['user-1']).users,
      ...snapshot('stream-x', 50, ['user-2']).users,
    ],
  })));
  await mixed.api.refreshOverview();
  const mixedSnapshot = deferred();
  mixed.enqueue(mixedSnapshot.promise);
  mixed.api.applyOverview(overview('stream-b', 5, 6, ['user-1']));
  mixed.api.applyOverview(multiplex(
    overview('stream-a', 100, 101, ['user-1']),
    overview('stream-x', 50, 51, ['user-2']),
  ));
  assert.equal(mixed.api.pending(), 2, 'mixed-generation multiplex payload waits behind one shared snapshot');
  mixedSnapshot.resolve(response({
    schema_version: 1,
    archive: {state: 'ok'},
    users: [
      ...snapshot('stream-b', 5, ['user-1']).users,
      ...snapshot('stream-x', 50, ['user-2']).users,
    ],
  }));
  await flushAsync();
  assert.equal(mixed.api.state('user-1').latestSeq, 6, 'replacement tenant row applies while its stale queued generation is discarded');
  assert.deepEqual(Array.from(mixed.api.state('user-1').ecg, row => row.device_ts_ns), [6]);
  assert.equal(mixed.api.state('user-2').latestSeq, 51, 'matching tenant keeps its first post-snapshot row');
  assert.deepEqual(Array.from(mixed.api.state('user-2').ecg, row => row.device_ts_ns), [51]);
  assert.equal(mixed.api.pending(), 0, 'mixed-user pending payload converges without requeue');
  assert(!mixed.timers.some(candidate => candidate.active), 'mixed-user recovery does not create a retry loop');
  assert.equal(mixed.fetchCalls.length, 2, 'mixed-user recovery remains globally single-flight');

  const retry = makeHarness();
  retry.enqueue(Promise.resolve(response(snapshot('stream-a', 100))));
  await retry.api.refreshOverview();
  retry.enqueue(Promise.resolve(response({}, false, 503)));
  retry.api.applyOverview(overview('stream-b', 5, 6));
  await flushAsync();
  const timer = retry.timers.find(candidate => candidate.active);
  assert(timer, 'failed admin snapshot schedules retry');
  assert.equal(timer.delay, 1000, 'admin retry starts at bounded one-second backoff');
  retry.api.applyOverview(overview('stream-b', 6, 7));
  assert.equal(retry.fetchCalls.length, 2, 'SSE events cannot bypass pending admin retry backoff');
  retry.enqueue(Promise.resolve(response(snapshot('stream-b', 5))));
  retry.runNextTimer();
  await flushAsync();
  assert.equal(retry.fetchCalls.length, 3);
  assert.equal(retry.api.state('user-1').latestSeq, 7);
  assert.deepEqual(Array.from(retry.api.state('user-1').ecg, row => row.device_ts_ns), [6, 7]);

  console.log('Admin overview stream recovery checks passed (snapshot startup/reconnect, stale HTTP success/failure/hang, lower/higher/overlap/mixed generation, buffering, single-flight retry).');
})().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
