'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const rust = fs.readFileSync('src/live_server.rs', 'utf8');
const start = rust.indexOf('const LIMITS =');
const end = rust.indexOf('function prepareData()', start);
assert(start >= 0 && end > start, 'extract dashboard transport functions');

class FakeEventSource {
  static instances = [];

  constructor(url) {
    this.url = url;
    this.listeners = new Map();
    this.closed = false;
    FakeEventSource.instances.push(this);
  }

  addEventListener(name, listener) {
    this.listeners.set(name, listener);
  }

  close() {
    this.closed = true;
  }

  emit(name, data) {
    const listener = this.listeners.get(name);
    assert(listener, `listener for ${name}`);
    listener({data});
  }
}

function snapshot(streamId, latestSeq, ecg = []) {
  return {
    schema_version: 1,
    stream_id: streamId,
    latest_seq: latestSeq,
    status: {schema_version: 1, stream_id: streamId, latest_sequence: latestSeq},
    ecg,
    acc: [],
    hr: [],
    rr: [],
    health: [],
    timing: [],
    events: [],
  };
}

function batch(streamId, fromSeq, toSeq, items = [], latestSeq = toSeq) {
  return {
    schema_version: 1,
    stream_id: streamId,
    from_seq: fromSeq,
    to_seq: toSeq,
    latest_seq: latestSeq,
    overflow: false,
    items,
    status: {schema_version: 1, stream_id: streamId, latest_sequence: latestSeq},
  };
}

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return {promise, resolve, reject};
}

function response(value, ok = true, status = 200) {
  return {ok, status, async json() { return value; }};
}

function makeHarness() {
  const fetchQueue = [];
  const fetchCalls = [];
  const timers = [];
  const elements = new Map();
  const context = {
    EventSource: FakeEventSource,
    SharedWorker: function SharedWorker() {},
    clearTimeout(timer) { if (timer) timer.active = false; },
    console: {error() {}},
    document: {
      getElementById(id) {
        if (!elements.has(id)) elements.set(id, {textContent: ''});
        return elements.get(id);
      },
    },
    async fetch(url, options) {
      fetchCalls.push({url, options});
      assert(fetchQueue.length > 0, `unexpected fetch ${url}`);
      return fetchQueue.shift();
    },
    setTimeout(callback, delay) {
      const timer = {callback, delay, active: true};
      timers.push(timer);
      return timer;
    },
    window: {addEventListener() {}},
  };
  vm.createContext(context);
  vm.runInContext(`${rust.slice(start, end)}
this.dashboardTest = {
  resetFromSnapshot,
  applyBatch,
  refreshStatus,
  refreshSnapshotAndResubscribe,
  handleTransportMessage,
  startDirectSse,
  setShared(port) { liveTransport = {kind:'shared_worker', port}; },
  setDirect(source) { eventSource = source; liveTransport = {kind:'direct_sse'}; },
  latestSeq() { return latestSeq; },
  currentStreamId() { return currentStreamId; },
  ecg() { return state.ecg; },
  hasPendingLagTimer() { return !!streamLagTimer; },
};`, context, {filename: 'dashboard-stream-recovery.js'});
  return {
    api: context.dashboardTest,
    enqueue(value) { fetchQueue.push(value); },
    fetchCalls,
    timers,
    runNextTimer() {
      const timer = timers.find(candidate => candidate.active);
      assert(timer, 'active retry timer');
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
  const shared = makeHarness();
  const port = {messages: [], postMessage(message) { this.messages.push(structuredClone(message)); }};
  shared.api.resetFromSnapshot(snapshot('stream-a', 100));
  shared.api.setShared(port);
  const pending = deferred();
  shared.enqueue(pending.promise);
  shared.api.applyBatch(batch('stream-b', 100, 100, [], 5));
  shared.api.applyBatch(batch('stream-b', 100, 100, [], 5));
  assert.equal(shared.fetchCalls.length, 1, 'generation resnapshot is single-flight');
  pending.resolve(response(snapshot('stream-b', 5)));
  await flushAsync();
  assert.deepEqual(port.messages, [{type: 'subscribe', since: 5, streamId: 'stream-b'}]);
  shared.api.applyBatch(batch('stream-b', 5, 6, [
    {seq: 6, kind: 'ecg', row: {collector_unix_ns: 1, device_ts_ns: 1, microvolts: 321}},
  ]));
  assert.equal(shared.api.latestSeq(), 6, 'new generation replaces the larger old cursor');
  assert.equal(shared.api.ecg().length, 1, 'first post-restart waveform row is not filtered');

  const retrying = makeHarness();
  const retryPort = {messages: [], postMessage(message) { this.messages.push(structuredClone(message)); }};
  retrying.api.resetFromSnapshot(snapshot('stream-a', 100));
  retrying.api.setShared(retryPort);
  retrying.enqueue(Promise.resolve(response({}, false, 503)));
  retrying.api.applyBatch(batch('stream-b', 100, 100, [], 5));
  await flushAsync();
  const retryTimer = retrying.timers.find(timer => timer.active);
  assert(retryTimer, 'failed resnapshot schedules a retry');
  assert.equal(retryTimer.delay, 1000, 'resnapshot retry starts with bounded one-second backoff');
  retrying.api.applyBatch(batch('stream-b', 100, 100, [], 5));
  retrying.api.applyBatch(batch('stream-b', 100, 100, [], 5));
  assert.equal(retrying.fetchCalls.length, 1, 'repeated SSE events cannot bypass the pending retry backoff');
  retrying.enqueue(Promise.resolve(response(snapshot('stream-b', 5))));
  retrying.runNextTimer();
  await flushAsync();
  assert.equal(retrying.fetchCalls.length, 2, 'scheduled resnapshot retries after service recovery');
  assert.deepEqual(retryPort.messages, [{type: 'subscribe', since: 5, streamId: 'stream-b'}]);

  const direct = makeHarness();
  direct.api.resetFromSnapshot(snapshot('stream-a', 100));
  direct.api.startDirectSse('test');
  const oldDirectSource = FakeEventSource.instances.at(-1);
  direct.enqueue(Promise.resolve(response(snapshot('stream-b', 5))));
  oldDirectSource.emit('batch', JSON.stringify(batch('stream-b', 100, 100, [], 5)));
  await flushAsync();
  assert(oldDirectSource.closed, 'direct fallback closes its old-generation EventSource');
  const replacement = FakeEventSource.instances.at(-1);
  assert.equal(replacement.url, '/events?since=5', 'direct fallback reopens from the replacement snapshot');
  oldDirectSource.emit('batch', JSON.stringify(batch('stream-a', 100, 101, [
    {seq: 101, kind: 'ecg', row: {collector_unix_ns: 3, device_ts_ns: 3, microvolts: 999}},
  ])));
  await flushAsync();
  assert.equal(direct.fetchCalls.length, 1, 'queued callbacks from the replaced direct source are ignored');
  assert(!replacement.closed, 'stale direct callback cannot disturb the replacement source');
  replacement.emit('batch', JSON.stringify(batch('stream-b', 5, 6, [
    {seq: 6, kind: 'ecg', row: {collector_unix_ns: 2, device_ts_ns: 2, microvolts: 654}},
  ])));
  assert.equal(direct.api.latestSeq(), 6);
  assert.equal(direct.api.ecg().length, 1, 'direct fallback accepts the first new-generation waveform row');

  const stalledShared = makeHarness();
  const stalledPort = {messages: [], postMessage(message) { this.messages.push(structuredClone(message)); }};
  stalledShared.api.resetFromSnapshot(snapshot('stream-stable', 100));
  stalledShared.api.setShared(stalledPort);
  stalledShared.enqueue(Promise.resolve(response({
    schema_version: 1,
    stream_id: 'stream-stable',
    latest_sequence: 5000,
  })));
  await stalledShared.api.refreshStatus();
  assert.equal(stalledShared.api.latestSeq(), 100, 'status alone reproduces the old same-generation stalled cursor');
  stalledShared.enqueue(Promise.resolve(response({
    schema_version: 1,
    stream_id: 'stream-stable',
    latest_sequence: 5001,
  })));
  await stalledShared.api.refreshStatus();
  assert.equal(stalledShared.timers.filter(timer => timer.active).length, 1, 'repeated high-water evidence remains single-flight');
  const sharedGrace = stalledShared.timers.find(timer => timer.active);
  assert(sharedGrace, 'authoritative cursor divergence starts a grace timer');
  assert.equal(sharedGrace.delay, 1500, 'stall recovery allows in-flight SSE delivery to win first');
  stalledShared.enqueue(Promise.resolve(response(snapshot('stream-stable', 5002, [
    {collector_unix_ns: 5_002, device_ts_ns: 5_002, microvolts: 777},
  ]))));
  stalledShared.runNextTimer();
  await flushAsync();
  assert.equal(stalledShared.api.latestSeq(), 5002, 'a gap beyond the delta window is recovered by one bounded snapshot');
  assert.deepEqual(Array.from(stalledShared.api.ecg(), row => row.microvolts), [777], 'the first resumed live row survives while old backfill stays absent');
  assert.deepEqual(stalledPort.messages, [{
    type: 'recover',
    since: 5002,
    streamId: 'stream-stable',
    observedLatestSequence: 5002,
  }], 'the shared upstream is explicitly reopened from the recovered snapshot');
  stalledShared.api.applyBatch(batch('stream-stable', 5002, 5003, [
    {seq: 5003, kind: 'ecg', row: {collector_unix_ns: 5_003, device_ts_ns: 5_003, microvolts: 778}},
  ]));
  assert.equal(stalledShared.api.latestSeq(), 5003);
  assert.deepEqual(Array.from(stalledShared.api.ecg(), row => row.microvolts), [777, 778], 'incremental delivery continues without a page reload');

  const stalledRetry = makeHarness();
  const stalledRetryPort = {messages: [], postMessage(message) { this.messages.push(structuredClone(message)); }};
  stalledRetry.api.resetFromSnapshot(snapshot('stream-retry', 40));
  stalledRetry.api.setShared(stalledRetryPort);
  stalledRetry.enqueue(Promise.resolve(response({
    schema_version: 1,
    stream_id: 'stream-retry',
    latest_sequence: 4000,
  })));
  await stalledRetry.api.refreshStatus();
  stalledRetry.enqueue(Promise.resolve(response({}, false, 503)));
  stalledRetry.runNextTimer();
  await flushAsync();
  assert.equal(stalledRetryPort.messages.length, 0, 'a failed snapshot cannot move the shared upstream past missing data');
  const stalledRetryTimer = stalledRetry.timers.find(timer => timer.active);
  assert(stalledRetryTimer, 'same-generation snapshot failure schedules bounded retry');
  assert.equal(stalledRetryTimer.delay, 1000);
  stalledRetry.enqueue(Promise.resolve(response(snapshot('stream-retry', 4001, [
    {collector_unix_ns: 4_001, device_ts_ns: 4_001, microvolts: 401},
  ]))));
  stalledRetry.runNextTimer();
  await flushAsync();
  assert.deepEqual(stalledRetryPort.messages, [{
    type: 'recover',
    since: 4001,
    streamId: 'stream-retry',
    observedLatestSequence: 4001,
  }], 'the retry retains the exact forced-recovery episode');

  const retryCaughtUp = makeHarness();
  const retryCaughtUpPort = {messages: [], postMessage(message) { this.messages.push(structuredClone(message)); }};
  retryCaughtUp.api.resetFromSnapshot(snapshot('stream-retry-caught-up', 50));
  retryCaughtUp.api.setShared(retryCaughtUpPort);
  retryCaughtUp.enqueue(Promise.resolve(response({
    schema_version: 1,
    stream_id: 'stream-retry-caught-up',
    latest_sequence: 5000,
  })));
  await retryCaughtUp.api.refreshStatus();
  retryCaughtUp.enqueue(Promise.resolve(response({}, false, 503)));
  retryCaughtUp.runNextTimer();
  await flushAsync();
  const caughtUpRetryTimer = retryCaughtUp.timers.find(timer => timer.active);
  assert(caughtUpRetryTimer && caughtUpRetryTimer.delay === 1000, 'failed stall snapshot owns its retry timer');
  retryCaughtUp.api.applyBatch(batch('stream-retry-caught-up', 50, 5000, [
    {seq: 5000, kind: 'ecg', row: {collector_unix_ns: 5_000, device_ts_ns: 5_000, microvolts: 500}},
  ]));
  assert(!caughtUpRetryTimer.active, 'normal same-generation progress cancels the stall-owned retry');
  assert.equal(retryCaughtUp.timers.filter(timer => timer.active).length, 0, 'no snapshot retry storm survives after SSE catches up');
  assert.equal(retryCaughtUpPort.messages.length, 0, 'caught-up SSE does not force a worker restart');

  const retryGeneration = makeHarness();
  const retryGenerationPort = {messages: [], postMessage(message) { this.messages.push(structuredClone(message)); }};
  retryGeneration.api.resetFromSnapshot(snapshot('stream-old', 60));
  retryGeneration.api.setShared(retryGenerationPort);
  retryGeneration.enqueue(Promise.resolve(response({
    schema_version: 1,
    stream_id: 'stream-old',
    latest_sequence: 6000,
  })));
  await retryGeneration.api.refreshStatus();
  retryGeneration.enqueue(Promise.resolve(response({}, false, 503)));
  retryGeneration.runNextTimer();
  await flushAsync();
  const generationRetryTimer = retryGeneration.timers.find(timer => timer.active);
  retryGeneration.api.applyBatch(batch('stream-new', 0, 1, [
    {seq: 1, kind: 'ecg', row: {collector_unix_ns: 1, device_ts_ns: 1, microvolts: 1}},
  ], 1));
  assert(generationRetryTimer.active, 'generation replacement upgrades rather than cancels a pending stall retry');
  retryGeneration.enqueue(Promise.resolve(response(snapshot('stream-new', 5, [
    {collector_unix_ns: 5, device_ts_ns: 5, microvolts: 5},
  ]))));
  retryGeneration.runNextTimer();
  await flushAsync();
  assert.deepEqual(retryGenerationPort.messages, [{type: 'subscribe', since: 5, streamId: 'stream-new'}], 'replacement generation cannot inherit the old forced-recovery request');
  retryGeneration.api.applyBatch(batch('stream-new', 5, 6, [
    {seq: 6, kind: 'ecg', row: {collector_unix_ns: 6, device_ts_ns: 6, microvolts: 6}},
  ]));
  assert.equal(retryGeneration.api.latestSeq(), 6);

  const inFlightGeneration = makeHarness();
  const inFlightPort = {messages: [], postMessage(message) { this.messages.push(structuredClone(message)); }};
  inFlightGeneration.api.resetFromSnapshot(snapshot('stream-inflight-old', 70));
  inFlightGeneration.api.setShared(inFlightPort);
  inFlightGeneration.enqueue(Promise.resolve(response({
    schema_version: 1,
    stream_id: 'stream-inflight-old',
    latest_sequence: 7000,
  })));
  await inFlightGeneration.api.refreshStatus();
  const oldSnapshot = deferred();
  inFlightGeneration.enqueue(oldSnapshot.promise);
  inFlightGeneration.runNextTimer();
  inFlightGeneration.api.applyBatch(batch('stream-inflight-new', 0, 1, [], 1));
  inFlightGeneration.enqueue(Promise.resolve(response(snapshot('stream-inflight-new', 8, [
    {collector_unix_ns: 8, device_ts_ns: 8, microvolts: 8},
  ]))));
  oldSnapshot.resolve(response(snapshot('stream-inflight-old', 7001)));
  await flushAsync();
  assert.deepEqual(inFlightPort.messages, [{type: 'subscribe', since: 8, streamId: 'stream-inflight-new'}], 'a general generation follow-up suppresses the stale in-flight recovery action');
  assert.equal(inFlightGeneration.api.currentStreamId(), 'stream-inflight-new');

  const healthyIdle = makeHarness();
  const idlePort = {messages: [], postMessage(message) { this.messages.push(structuredClone(message)); }};
  healthyIdle.api.resetFromSnapshot(snapshot('stream-idle', 70));
  healthyIdle.api.setShared(idlePort);
  for (let index = 0; index < 3; index += 1) {
    healthyIdle.enqueue(Promise.resolve(response({
      schema_version: 1,
      stream_id: 'stream-idle',
      latest_sequence: 70,
    })));
    await healthyIdle.api.refreshStatus();
  }
  assert.equal(healthyIdle.timers.filter(timer => timer.active).length, 0, 'a long legitimate no-data interval never schedules recovery');
  assert.equal(idlePort.messages.length, 0);

  const advancing = makeHarness();
  const advancingPort = {messages: [], postMessage(message) { this.messages.push(structuredClone(message)); }};
  advancing.api.resetFromSnapshot(snapshot('stream-advancing', 80));
  advancing.api.setShared(advancingPort);
  advancing.enqueue(Promise.resolve(response({
    schema_version: 1,
    stream_id: 'stream-advancing',
    latest_sequence: 90,
  })));
  await advancing.api.refreshStatus();
  const originalAdvancingTimer = advancing.timers.find(timer => timer.active);
  advancing.api.applyBatch(batch('stream-advancing', 80, 85, [
    {seq: 85, kind: 'ecg', row: {collector_unix_ns: 85, device_ts_ns: 85, microvolts: 85}},
  ], 90));
  assert(!originalAdvancingTimer.active, 'incremental progress resets the stall grace instead of forcing a restart');
  assert.equal(advancing.timers.filter(timer => timer.active).length, 1, 'advancing catch-up retains one bounded watchdog');
  advancing.api.applyBatch(batch('stream-advancing', 85, 90, [
    {seq: 90, kind: 'ecg', row: {collector_unix_ns: 90, device_ts_ns: 90, microvolts: 90}},
  ], 90));
  assert.equal(advancing.timers.filter(timer => timer.active).length, 0, 'catching up to the witness cancels recovery entirely');
  assert.equal(advancingPort.messages.length, 0, 'continuous catch-up cannot create a recovery storm');

  const overflowed = makeHarness();
  const overflowPort = {messages: [], postMessage(message) { this.messages.push(structuredClone(message)); }};
  overflowed.api.resetFromSnapshot(snapshot('stream-overflow', 10));
  overflowed.api.setShared(overflowPort);
  overflowed.enqueue(Promise.resolve(response(snapshot('stream-overflow', 5000, [
    {collector_unix_ns: 5_000, device_ts_ns: 5_000, microvolts: 500},
  ]))));
  overflowed.api.applyBatch({...batch('stream-overflow', 10, 4000, [], 5000), overflow: true});
  await flushAsync();
  assert.equal(overflowed.api.latestSeq(), 5000, 'delta-buffer overflow converges through the bounded snapshot');
  assert.deepEqual(Array.from(overflowed.api.ecg(), row => row.microvolts), [500], 'overflow recovery preserves the snapshot first live row');
  assert.deepEqual(overflowPort.messages, [{type: 'subscribe', since: 5000, streamId: 'stream-overflow'}]);
  overflowed.api.applyBatch(batch('stream-overflow', 5000, 5001, [
    {seq: 5001, kind: 'ecg', row: {collector_unix_ns: 5_001, device_ts_ns: 5_001, microvolts: 501}},
  ]));
  assert.deepEqual(Array.from(overflowed.api.ecg(), row => row.microvolts), [500, 501], 'first incremental row after overflow is accepted once');

  const graceWinner = makeHarness();
  const gracePort = {messages: [], postMessage(message) { this.messages.push(structuredClone(message)); }};
  graceWinner.api.resetFromSnapshot(snapshot('stream-grace', 20));
  graceWinner.api.setShared(gracePort);
  graceWinner.enqueue(Promise.resolve(response({
    schema_version: 1,
    stream_id: 'stream-grace',
    latest_sequence: 21,
  })));
  await graceWinner.api.refreshStatus();
  assert(graceWinner.api.hasPendingLagTimer());
  graceWinner.api.applyBatch(batch('stream-grace', 20, 21, [
    {seq: 21, kind: 'ecg', row: {collector_unix_ns: 21, device_ts_ns: 21, microvolts: 21}},
  ]));
  assert(!graceWinner.api.hasPendingLagTimer(), 'normal SSE progress cancels the recovery grace timer');
  assert.equal(gracePort.messages.length, 0, 'healthy catch-up never restarts the shared source');

  const stalledDirect = makeHarness();
  stalledDirect.api.resetFromSnapshot(snapshot('stream-direct', 300));
  stalledDirect.api.startDirectSse('test stall');
  const stalledDirectSource = FakeEventSource.instances.at(-1);
  stalledDirect.enqueue(Promise.resolve(response({
    schema_version: 1,
    stream_id: 'stream-direct',
    latest_sequence: 9000,
  })));
  await stalledDirect.api.refreshStatus();
  stalledDirect.enqueue(Promise.resolve(response(snapshot('stream-direct', 9001, [
    {collector_unix_ns: 9_001, device_ts_ns: 9_001, microvolts: 901},
  ]))));
  stalledDirect.runNextTimer();
  await flushAsync();
  assert(stalledDirectSource.closed, 'same-generation direct SSE stall closes the old source');
  const stalledDirectReplacement = FakeEventSource.instances.at(-1);
  assert.equal(stalledDirectReplacement.url, '/events?since=9001');
  stalledDirectReplacement.emit('batch', JSON.stringify(batch('stream-direct', 9001, 9002, [
    {seq: 9002, kind: 'ecg', row: {collector_unix_ns: 9_002, device_ts_ns: 9_002, microvolts: 902}},
  ])));
  assert.equal(stalledDirect.api.latestSeq(), 9002, 'direct SSE resumes after the same-generation gap without reload');

  console.log('Dashboard stream recovery checks passed (generation, same-stream stall, grace, retry, SharedWorker and direct SSE).');
})().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
