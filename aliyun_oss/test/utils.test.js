const test = require('node:test');
const assert = require('node:assert/strict');

const { clamp, toInt, tryDecodeBase64 } = require('../lib/utils');

test('clamp returns fallback for non-finite values', () => {
  assert.equal(clamp(NaN, 0, 10, 5), 5);
  assert.equal(clamp(Infinity, 0, 10, 7), 7);
});

test('clamp enforces min/max bounds', () => {
  assert.equal(clamp(-1, 0, 10, 5), 0);
  assert.equal(clamp(11, 0, 10, 5), 10);
  assert.equal(clamp(7, 0, 10, 5), 7);
});

test('toInt parses numeric values and returns undefined for invalid input', () => {
  assert.equal(toInt('42'), 42);
  assert.equal(toInt(3.14), 3.14);
  assert.equal(toInt('nope'), undefined);
  assert.equal(toInt(undefined), undefined);
});

test('tryDecodeBase64 decodes valid base64 and rejects JSON-like strings', () => {
  const payload = 'hello-world';
  const encoded = Buffer.from(payload, 'utf8').toString('base64');

  assert.equal(tryDecodeBase64(encoded), payload);
  assert.equal(tryDecodeBase64('  {"a":1} '), null);
  assert.equal(tryDecodeBase64('not-base64'), null);
});
