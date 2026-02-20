const test = require('node:test');
const assert = require('node:assert/strict');

const {
  getProgressStore,
  getCheckpointKey,
  readCheckpoint,
  writeCheckpoint,
} = require('../lib/checkpoint');

function makeLogger() {
  const warnings = [];
  const debugs = [];
  return {
    warnings,
    debugs,
    warn: (...args) => warnings.push(args),
    debug: (...args) => debugs.push(args),
  };
}

test('getProgressStore returns cached store', async () => {
  const store = { marker: 'cached' };
  const job = {
    progressStore: store,
    getProgressStore: async () => {
      throw new Error('should not be called');
    },
  };

  const result = await getProgressStore(job, makeLogger());
  assert.equal(result, store);
});

test('getProgressStore calls getter and caches', async () => {
  const store = { marker: 'from-getter' };
  const job = {
    getProgressStore: async () => store,
  };

  const result = await getProgressStore(job, makeLogger());
  assert.equal(result, store);
  assert.equal(job.progressStore, store);
});

test('getProgressStore returns null on error and logs warning', async () => {
  const logger = makeLogger();
  const job = {
    getProgressStore: async () => {
      throw new Error('boom');
    },
  };

  const result = await getProgressStore(job, logger);
  assert.equal(result, null);
  assert.equal(logger.warnings.length, 1);
});

test('getCheckpointKey uses default and trims custom suffix', () => {
  assert.equal(getCheckpointKey({ bucket: 'b' }), 'aliyun-oss::b::default');
  assert.equal(
    getCheckpointKey({ bucket: 'b', checkpointKey: '  custom  ' }),
    'aliyun-oss::b::custom',
  );
});

test('readCheckpoint returns null when store is missing or fails', async () => {
  const logger = makeLogger();
  const job = {};
  const config = { bucket: 'b' };

  const noStore = await readCheckpoint(job, config, logger);
  assert.equal(noStore, null);

  const badStoreJob = {
    progressStore: {
      get: async () => { throw new Error('fail'); },
    },
  };
  const failed = await readCheckpoint(badStoreJob, config, logger);
  assert.equal(failed, null);
  assert.equal(logger.debugs.length, 1);
});

test('readCheckpoint returns stored value', async () => {
  const logger = makeLogger();
  const job = {
    progressStore: {
      get: async () => ({ lastKey: 'k', lastModified: '2025-01-01T00:00:00Z' }),
    },
  };
  const config = { bucket: 'b', checkpointKey: 'suffix' };
  const result = await readCheckpoint(job, config, logger);
  assert.equal(result.lastKey, 'k');
});

test('writeCheckpoint writes using set and skips older checkpoints', async () => {
  const logger = makeLogger();
  const calls = [];
  const store = {
    get: async () => ({ lastModified: '2025-02-01T00:00:00Z' }),
    set: async (key, payload) => calls.push({ key, payload }),
  };
  const job = { progressStore: store };
  const config = { bucket: 'b', checkpointKey: 'suffix' };

  await writeCheckpoint(job, config, {
    lastKey: 'older',
    lastModified: '2025-01-01T00:00:00Z',
  }, logger);
  assert.equal(calls.length, 0);

  await writeCheckpoint(job, config, {
    lastKey: 'newer',
    lastModified: '2025-03-01T00:00:00Z',
  }, logger);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].key, 'aliyun-oss::b::suffix');
  assert.equal(calls[0].payload.lastKey, 'newer');
});

test('writeCheckpoint supports put/write and warns on failure', async () => {
  const logger = makeLogger();
  const putCalls = [];
  const writeCalls = [];

  const putStore = {
    get: async () => null,
    put: async (key, payload) => putCalls.push({ key, payload }),
  };
  await writeCheckpoint({ progressStore: putStore }, { bucket: 'b' }, {
    lastKey: 'k',
    lastModified: '2025-01-01T00:00:00Z',
  }, logger);
  assert.equal(putCalls.length, 1);

  const writeStore = {
    get: async () => null,
    write: async (key, payload) => writeCalls.push({ key, payload }),
  };
  await writeCheckpoint({ progressStore: writeStore }, { bucket: 'b' }, {
    lastKey: 'k',
    lastModified: '2025-01-01T00:00:00Z',
  }, logger);
  assert.equal(writeCalls.length, 1);

  const badStore = {
    get: async () => null,
    set: async () => { throw new Error('boom'); },
  };
  await writeCheckpoint({ progressStore: badStore }, { bucket: 'b' }, {
    lastKey: 'k',
    lastModified: '2025-01-01T00:00:00Z',
  }, logger);
  assert.ok(logger.warnings.length >= 1);
});

test('writeCheckpoint warns when store does not support writes', async () => {
  const logger = makeLogger();
  const store = { get: async () => null };
  await writeCheckpoint({ progressStore: store }, { bucket: 'b' }, {
    lastKey: 'k',
    lastModified: '2025-01-01T00:00:00Z',
  }, logger);
  assert.equal(logger.warnings.length, 1);
});
