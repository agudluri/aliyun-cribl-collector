const test = require('node:test');
const assert = require('node:assert/strict');
const zlib = require('zlib');

const {
  buildOssClient,
  downloadObject,
  maybeGunzip,
  buildOutputStream,
} = require('../lib/oss');

function makeLogger() {
  return {
    error: () => {},
  };
}

test('buildOssClient returns injected test client', () => {
  const injected = { marker: 'oss' };
  const config = { _testOssClient: injected };
  const client = buildOssClient(config, 'bucket');
  assert.equal(client, injected);
});

test('buildOssClient returns injected client from _testClients', () => {
  const injected = { marker: 'oss-2' };
  const config = { _testClients: { oss: injected } };
  const client = buildOssClient(config, 'bucket');
  assert.equal(client, injected);
});

test('buildOssClient constructs a real client when not injected', () => {
  const client = buildOssClient({
    bucket: 'demo-bucket',
    ossEndpoint: 'https://oss-cn-test.aliyuncs.com',
    accessKeyId: 'id',
    accessKeySecret: 'secret',
  }, 'override-bucket');
  assert.ok(client);
});

test('downloadObject returns a Buffer for both buffer and string content', async () => {
  const logger = makeLogger();
  const job = { reportError: () => {} };

  const clientBuffer = { get: async () => ({ content: Buffer.from('abc') }) };
  const clientString = { get: async () => ({ content: 'abc' }) };

  const buf1 = await downloadObject(clientBuffer, 'key', logger, job);
  const buf2 = await downloadObject(clientString, 'key', logger, job);

  assert.ok(Buffer.isBuffer(buf1));
  assert.ok(Buffer.isBuffer(buf2));
  assert.equal(buf1.toString('utf8'), 'abc');
  assert.equal(buf2.toString('utf8'), 'abc');
});

test('downloadObject reports errors to job and rethrows', async () => {
  const errors = [];
  const logger = makeLogger();
  const job = { reportError: (err) => errors.push(err) };
  const client = { get: async () => { throw new Error('boom'); } };

  await assert.rejects(() => downloadObject(client, 'key', logger, job), /boom/);
  assert.equal(errors.length, 1);
});

test('maybeGunzip respects config and decompresses gzip content', () => {
  const logger = makeLogger();
  const job = { reportError: () => {} };
  const original = Buffer.from('line1\nline2\n', 'utf8');
  const gzipped = zlib.gzipSync(original);

  const passthrough = maybeGunzip(gzipped, 'file.gz', { decompressGzip: false }, logger, job);
  assert.equal(passthrough, gzipped);

  const decoded = maybeGunzip(gzipped, 'file.gz', { decompressGzip: true }, logger, job);
  assert.equal(decoded.toString('utf8'), original.toString('utf8'));
});

test('maybeGunzip skips non-gzip content', () => {
  const logger = makeLogger();
  const job = { reportError: () => {} };
  const buffer = Buffer.from('x');

  const result = maybeGunzip(buffer, 'file.txt', { decompressGzip: true }, logger, job);
  assert.equal(result, buffer);
});

test('maybeGunzip reports and throws on invalid gzip payloads', () => {
  const errors = [];
  const logger = makeLogger();
  const job = { reportError: (err) => errors.push(err) };
  const bad = Buffer.from('not-gzip');

  assert.throws(
    () => maybeGunzip(bad, 'file.gz', { decompressGzip: true }, logger, job),
    /incorrect header|unexpected end|invalid/,
  );
  assert.equal(errors.length, 1);
});

test('buildOutputStream emits raw and json formats', async () => {
  const lines = ['a', 'b'];
  const meta = { bucket: 'b', key: 'k' };

  const rawStream = buildOutputStream(lines, meta, 'raw');
  let raw = '';
  for await (const chunk of rawStream) {
    raw += chunk;
  }
  assert.equal(raw, 'a\n' + 'b\n');

  const jsonStream = buildOutputStream(lines, meta, 'json');
  let jsonText = '';
  for await (const chunk of jsonStream) {
    jsonText += chunk;
  }
  const parsed = jsonText.trim().split('\n').map((line) => JSON.parse(line));
  assert.equal(parsed.length, 2);
  assert.equal(parsed[0].message, 'a');
  assert.equal(parsed[0].oss.bucket, 'b');
});
