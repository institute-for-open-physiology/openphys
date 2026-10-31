'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const rust = fs.readFileSync('src/live_server.rs', 'utf8');
const match = rust.match(/const LIVE_WORKER_JS: &str = r##"([\s\S]*?)"##;/);
assert(match, 'extract embedded LIVE_WORKER_JS');

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

  open() {
    if (this.onopen) this.onopen();
  }

  emit(name, data, id) {
    const listener = this.listeners.get(name);
    assert(listener, `listener for ${name}`);
    listener({data, lastEventId: String(id)});
  }

  fail() {
    assert(this.onerror, 'EventSource error handler');
    this.onerror();
  }
}

class FakePort {
  constructor() {
    this.messages = [];
    this.closed = false;
  }

  postMessage(message) {
    this.messages.push(structuredClone(message));
  }

  start() {}

  close() {
    this.closed = true;
  }

  send(message) {
    assert(this.onmessage, 'worker port message handler');
    this.onmessage({data: message});
  }
}

const timers = [];
const errors = [];
const context = {
  EventSource: FakeEventSource,
  clearTimeout() {},
  console: {error: (...args) => errors.push(args)},
  setTimeout(callback) {
    timers.push(callback);
    return timers.length;
  },
};
context.self = context;
vm.createContext(context);
vm.runInContext(match[1], context, {filename: 'live-worker.js'});

function connect(since, streamId = 'stream-a') {
  const port = new FakePort();
  context.self.onconnect({ports: [port]});
  port.send({type: 'subscribe', since, streamId});
  return port;
}

const ports = Array.from({length: 8}, () => connect(100));
assert.equal(FakeEventSource.instances.length, 1, 'eight tabs share one EventSource');
assert.equal(FakeEventSource.instances[0].url, '/events?since=100');

FakeEventSource.instances[0].open();
for (const port of ports) {
  assert(port.messages.some(message => message.type === 'transport' && message.state === 'connected'));
}

const batch = JSON.stringify({stream_id: 'stream-a', from_seq: 100, to_seq: 110, latest_seq: 110, overflow: false, items: []});
FakeEventSource.instances[0].emit('batch', batch, 110);
for (const port of ports) {
  assert(port.messages.some(message => message.type === 'batch' && message.toSeq === 110));
}

const joiningPort = connect(105);
assert.equal(FakeEventSource.instances.length, 1, 'joining tab does not create another EventSource');
assert(joiningPort.messages.some(message => message.type === 'batch' && message.toSeq === 110), 'joining tab receives worker catchup');

const beforeMalformed = ports[0].messages.filter(message => message.type === 'batch').length;
FakeEventSource.instances[0].emit('batch', 'not JSON', 111);
assert.equal(ports[0].messages.filter(message => message.type === 'batch').length, beforeMalformed, 'malformed batch is dropped');
assert.equal(errors.length, 1, 'malformed batch is logged');

const laterPort = connect(150);
const mismatchedPort = connect(110, 'stale-tab-stream');
assert.equal(mismatchedPort.messages.filter(message => message.type === 'resync').length, 1, 'a mismatched joining tab is told to resnapshot');
mismatchedPort.send({type: 'subscribe', since: 110, streamId: 'stale-tab-stream'});
assert.equal(mismatchedPort.messages.filter(message => message.type === 'resync').length, 1, 'repeated mismatched subscriptions are bounded until upstream generation is known');
FakeEventSource.instances[0].fail();
assert.equal(timers.length, 1, 'failed EventSource schedules one reconnect');
timers.shift()();
assert.equal(FakeEventSource.instances.length, 2, 'worker reconnect creates one replacement EventSource');
assert.equal(FakeEventSource.instances[1].url, '/events?since=110', 'reconnect resumes after the last delivered shared event');

const resyncCount = port => port.messages.filter(message => message.type === 'resync').length;
const lowerResetPorts = [...ports, joiningPort, laterPort, mismatchedPort];
const beforeLowerReset = lowerResetPorts.map(resyncCount);
const lowerReset = JSON.stringify({
  stream_id: 'stream-b',
  from_seq: 110,
  to_seq: 110,
  latest_seq: 5,
  overflow: false,
  items: [],
});
FakeEventSource.instances[1].emit('batch', lowerReset, 110);
assert(FakeEventSource.instances[1].closed, 'old-generation EventSource closes immediately');
assert.equal(FakeEventSource.instances.length, 3, 'generation reset creates one replacement EventSource');
assert.equal(FakeEventSource.instances[2].url, '/events?since=5', 'lower new-generation high-water replaces the stale cursor');
lowerResetPorts.forEach((port, index) => assert.equal(resyncCount(port), beforeLowerReset[index] + 1, 'every tab resnapshots exactly once for a lower generation reset'));

const freshLowerBatch = JSON.stringify({
  stream_id: 'stream-b',
  from_seq: 5,
  to_seq: 6,
  latest_seq: 6,
  overflow: false,
  items: [{seq: 6, kind: 'ecg', row: {microvolts: 123}}],
});
FakeEventSource.instances[2].emit('batch', freshLowerBatch, 6);
const postLowerSnapshot = connect(5, 'stream-b');
const lowerCatchup = postLowerSnapshot.messages.filter(message => message.type === 'batch');
assert.equal(lowerCatchup.length, 1, 'post-reset snapshot receives one buffered new-generation batch');
assert.equal(lowerCatchup[0].toSeq, 6);
assert(!lowerCatchup.some(message => message.toSeq === 110), 'old-generation catch-up was discarded');

const higherResetPorts = [...lowerResetPorts, postLowerSnapshot];
const beforeHigherReset = higherResetPorts.map(resyncCount);
const higherReset = JSON.stringify({
  stream_id: 'stream-c',
  from_seq: 6,
  to_seq: 200,
  latest_seq: 200,
  overflow: false,
  items: [{seq: 200, kind: 'ecg', row: {microvolts: 456}}],
});
FakeEventSource.instances[2].emit('batch', higherReset, 200);
assert(FakeEventSource.instances[2].closed, 'second old-generation EventSource closes immediately');
assert.equal(FakeEventSource.instances.length, 4, 'higher generation reset also creates one replacement EventSource');
assert.equal(FakeEventSource.instances[3].url, '/events?since=200', 'generation identity wins even when the new counter is higher');
higherResetPorts.forEach((port, index) => {
  assert.equal(resyncCount(port), beforeHigherReset[index] + 1, 'every tab resnapshots exactly once for a higher generation reset');
});

const freshHigherBatch = JSON.stringify({
  stream_id: 'stream-c',
  from_seq: 200,
  to_seq: 201,
  latest_seq: 201,
  overflow: false,
  items: [{seq: 201, kind: 'acc', row: {raw_x: 1}}],
});
FakeEventSource.instances[3].emit('batch', freshHigherBatch, 201);
const postHigherSnapshot = connect(200, 'stream-c');
assert(postHigherSnapshot.messages.some(message => message.type === 'batch' && message.toSeq === 201), 'higher-generation catch-up resumes without skipping its first row');

FakeEventSource.instances[2].fail();
assert.equal(FakeEventSource.instances.length, 4, 'late error from a replaced EventSource cannot close the current upstream');
assert(!FakeEventSource.instances[3].closed, 'current upstream survives a stale source callback');

const statusResetPorts = [...higherResetPorts, postHigherSnapshot];
const beforeStatusReset = statusResetPorts.map(resyncCount);
FakeEventSource.instances[3].emit('status', JSON.stringify({
  stream_id: 'stream-d',
  latest_sequence: 2,
}), 2);
assert(FakeEventSource.instances[3].closed, 'status-only local feed also closes its old generation');
assert.equal(FakeEventSource.instances.length, 5, 'status generation change creates one replacement EventSource');
assert.equal(FakeEventSource.instances[4].url, '/events?since=2');
statusResetPorts.forEach((port, index) => {
  assert.equal(resyncCount(port), beforeStatusReset[index] + 1, 'status generation change resnapshots every tab once');
});
FakeEventSource.instances[4].emit('batch', JSON.stringify({
  stream_id: 'stream-d',
  from_seq: 2,
  to_seq: 3,
  latest_seq: 3,
  overflow: false,
  items: [{seq: 3, kind: 'hr', row: {bpm: 70}}],
}), 3);
const postStatusSnapshot = connect(2, 'stream-d');
assert(postStatusSnapshot.messages.some(message => message.type === 'batch' && message.toSeq === 3), 'status-triggered reset preserves new-generation catch-up');

const stallRecoveryPorts = [...statusResetPorts, postStatusSnapshot];
const beforeStallRecovery = stallRecoveryPorts.map(resyncCount);
postStatusSnapshot.send({
  type: 'recover',
  streamId: 'stream-d',
  since: 5000,
  observedLatestSequence: 5000,
});
assert(FakeEventSource.instances[4].closed, 'same-generation stall closes the silent upstream');
assert.equal(FakeEventSource.instances.length, 6, 'one recovery request creates one replacement EventSource');
assert.equal(FakeEventSource.instances[5].url, '/events?since=5000', 'recovery resumes from the requesting tab snapshot');
stallRecoveryPorts.forEach((port, index) => {
  const expected = port === postStatusSnapshot ? beforeStallRecovery[index] : beforeStallRecovery[index] + 1;
  assert.equal(resyncCount(port), expected, 'other tabs resnapshot exactly once while the initiating tab keeps its new snapshot');
});
const batchesBeforeStaleCallback = postStatusSnapshot.messages.filter(message => message.type === 'batch').length;
FakeEventSource.instances[4].emit('batch', JSON.stringify({
  stream_id: 'stream-d',
  from_seq: 3,
  to_seq: 4999,
  latest_seq: 4999,
  overflow: false,
  items: [],
}), 4999);
assert.equal(postStatusSnapshot.messages.filter(message => message.type === 'batch').length, batchesBeforeStaleCallback, 'a late callback from the force-replaced source is ignored');
for (const port of stallRecoveryPorts) {
  port.send({type: 'recover', streamId: 'stream-d', since: 5000, observedLatestSequence: 5000});
}
assert.equal(FakeEventSource.instances.length, 6, 'eight tabs with the same stall evidence cannot create a restart storm');
FakeEventSource.instances[5].emit('batch', JSON.stringify({
  stream_id: 'stream-d',
  from_seq: 5000,
  to_seq: 5001,
  latest_seq: 5001,
  overflow: false,
  items: [{seq: 5001, kind: 'ecg', row: {microvolts: 789}}],
}), 5001);
for (const port of stallRecoveryPorts) {
  assert(port.messages.some(message => message.type === 'batch' && message.toSeq === 5001), 'fresh batches resume through the one recovered upstream');
}

for (const port of stallRecoveryPorts) port.send({type: 'disconnect'});
assert(FakeEventSource.instances[5].closed, 'last tab closes the recovered shared EventSource');

console.log('SharedWorker lifecycle checks passed (8 tabs, catchup, generation reset, same-stream recovery, reconnect, cleanup).');
