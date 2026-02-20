const test = require('node:test');
const assert = require('node:assert/strict');

const {
  buildQueueClient,
  deriveQueueAccountId,
  recvMessages,
  normalizeQueueMessages,
  isMessageNotExist,
  parseQueueMessage,
  decodeObjectKey,
  getReceiptHandle,
  extendVisibilityTimeout,
  deleteQueueMessage,
} = require('../lib/queue');

function makeLogger() {
  const warnings = [];
  const errors = [];
  return {
    warnings,
    errors,
    warn: (...args) => warnings.push(args),
    error: (...args) => errors.push(args),
    debug: () => {},
  };
}

test('buildQueueClient returns injected test client', () => {
  const injected = { marker: 'queue' };
  const config = { _testQueueClient: injected };
  const client = buildQueueClient(config);
  assert.equal(client, injected);
});

test('buildQueueClient returns injected client from _testClients', () => {
  const injected = { marker: 'queue-2' };
  const config = { _testClients: { queue: injected } };
  const client = buildQueueClient(config);
  assert.equal(client, injected);
});

test('buildQueueClient constructs client with endpoint', () => {
  const client = buildQueueClient({
    accessKeyId: 'id',
    accessKeySecret: 'secret',
    queueEndpoint: 'https://1577260716295338.mns.cn-test.aliyuncs.com',
  });
  assert.ok(client);
});

test('buildQueueClient returns null when endpoint is missing', () => {
  const client = buildQueueClient({
    accessKeyId: 'id',
    accessKeySecret: 'secret',
  });
  assert.equal(client, null);
});

test('deriveQueueAccountId extracts first host label from endpoint', () => {
  const accountId = deriveQueueAccountId('https://1577260716295338.mns.cn-hangzhou.aliyuncs.com');
  assert.equal(accountId, '1577260716295338');
});

test('deriveQueueAccountId throws on invalid endpoint', () => {
  assert.throws(
    () => deriveQueueAccountId('not-a-url'),
    /Invalid queueEndpoint URL/,
  );
});

test('normalizeQueueMessages handles common response shapes', () => {
  const msg = { MessageBody: '{}' };

  assert.deepEqual(normalizeQueueMessages([msg]), [msg]);
  assert.deepEqual(normalizeQueueMessages({ body: [msg] }), [msg]);
  assert.deepEqual(normalizeQueueMessages({ body: msg }), [msg]);
  assert.deepEqual(normalizeQueueMessages({ body: { Messages: { Message: [msg] } } }), [msg]);
  assert.deepEqual(normalizeQueueMessages({ body: { Messages: { Message: msg } } }), [msg]);
  assert.deepEqual(normalizeQueueMessages({ Messages: { Message: [msg] } }), [msg]);
  assert.deepEqual(normalizeQueueMessages({ Messages: { Message: msg } }), [msg]);
  assert.deepEqual(normalizeQueueMessages({ Message: msg }), [msg]);
  assert.deepEqual(normalizeQueueMessages(null), []);
  assert.deepEqual(normalizeQueueMessages({}), []);
});

test('isMessageNotExist recognizes MNS empty-queue errors', () => {
  assert.equal(isMessageNotExist({ name: 'MNSMessageNotExistError' }), true);
  assert.equal(isMessageNotExist({ code: 'MessageNotExist' }), true);
  assert.equal(isMessageNotExist(new Error('Message not exist')), true);
  assert.equal(isMessageNotExist(new Error('Other error')), false);
});

test('parseQueueMessage parses JSON bodies and decodes object keys', () => {
  const logger = makeLogger();
  const message = {
    MessageId: 'mid-1',
    ReceiptHandle: 'rh-1',
    MessageBody: JSON.stringify({
      events: [
        {
          eventTime: '2025-01-01T00:00:00Z',
          oss: {
            bucket: { name: 'demo-bucket' },
            object: { key: 'logs%2Fapp.log', size: '42', eTag: '"etag"' },
          },
        },
      ],
    }),
  };

  const result = parseQueueMessage(message, { bucket: 'fallback' }, logger);
  assert.ok(result);
  assert.equal(result.bucket, 'demo-bucket');
  assert.equal(result.key, 'logs/app.log');
  assert.equal(result.size, 42);
  assert.equal(result.receiptHandle, 'rh-1');
  assert.equal(result.messageId, 'mid-1');
});

test('parseQueueMessage handles base64-encoded JSON bodies', () => {
  const logger = makeLogger();
  const payload = JSON.stringify({ key: 'logs/base64.log' });
  const message = {
    MessageId: 'mid-2',
    ReceiptHandle: 'rh-2',
    MessageBody: Buffer.from(payload).toString('base64'),
  };

  const result = parseQueueMessage(message, { bucket: 'fallback-bucket' }, logger);
  assert.ok(result);
  assert.equal(result.bucket, 'fallback-bucket');
  assert.equal(result.key, 'logs/base64.log');
});

test('parseQueueMessage warns on invalid payloads', () => {
  const logger = makeLogger();
  const result = parseQueueMessage(
    { MessageId: 'mid-3', ReceiptHandle: 'rh-3', MessageBody: 'not-json' },
    { bucket: 'fallback' },
    logger,
  );

  assert.equal(result, null);
  assert.equal(logger.warnings.length, 1);
});

test('parseQueueMessage warns when receipt handle is missing', () => {
  const logger = makeLogger();
  const result = parseQueueMessage(
    { MessageId: 'mid-0', MessageBody: JSON.stringify({ key: 'logs/app.log' }) },
    { bucket: 'fallback' },
    logger,
  );

  assert.equal(result, null);
  assert.equal(logger.warnings.length, 1);
});

test('parseQueueMessage warns on base64 payloads that are not JSON', () => {
  const logger = makeLogger();
  const payload = Buffer.from('not-json', 'utf8').toString('base64');
  const result = parseQueueMessage(
    { MessageId: 'mid-4', ReceiptHandle: 'rh-4', MessageBody: payload },
    { bucket: 'fallback' },
    logger,
  );

  assert.equal(result, null);
  assert.equal(logger.warnings.length, 1);
});

test('parseQueueMessage warns when bucket/key are missing', () => {
  const logger = makeLogger();
  const result = parseQueueMessage(
    { MessageId: 'mid-5', ReceiptHandle: 'rh-5', MessageBody: JSON.stringify({ foo: 'bar' }) },
    {},
    logger,
  );

  assert.equal(result, null);
  assert.equal(logger.warnings.length, 1);
});

test('decodeObjectKey handles invalid encoding', () => {
  const bad = '%E0%A4%A';
  assert.equal(decodeObjectKey(bad), bad);
});

test('decodeObjectKey returns input for non-string values', () => {
  assert.equal(decodeObjectKey(null), null);
  assert.equal(decodeObjectKey(''), '');
});

test('decodeObjectKey does not convert plus to space', () => {
  assert.equal(decodeObjectKey('a+b'), 'a+b');
});

test('getReceiptHandle prefers explicit receipt handles', () => {
  assert.equal(getReceiptHandle({ receiptHandle: 'a', queueReceiptHandle: 'b' }), 'a');
  assert.equal(getReceiptHandle({ queueReceiptHandle: 'b' }), 'b');
  assert.equal(getReceiptHandle({ queue: { receiptHandle: 'c' } }), 'c');
  assert.equal(getReceiptHandle({ ReceiptHandle: 'd' }), 'd');
  assert.equal(getReceiptHandle({ queue: { ReceiptHandle: 'e' } }), 'e');
  assert.equal(getReceiptHandle({}), undefined);
});

test('extendVisibilityTimeout no-ops when timeout is not set', async () => {
  const logger = makeLogger();
  let called = 0;
  const queue = {
    changeMessageVisibility: async () => {
      called += 1;
      return { body: { ReceiptHandle: 'new' } };
    },
  };
  const config = { queueName: 'q', queueVisibilityTimeout: 0 };
  const receipt = await extendVisibilityTimeout(queue, config, 'rh', logger);
  assert.equal(receipt, 'rh');
  assert.equal(called, 0);
});

test('extendVisibilityTimeout returns updated receipt handle', async () => {
  const logger = makeLogger();
  const queue = {
    changeMessageVisibility: async () => ({ body: { ReceiptHandle: 'rh-new' } }),
  };
  const config = { queueName: 'q', queueVisibilityTimeout: 60 };
  const receipt = await extendVisibilityTimeout(queue, config, 'rh-old', logger);
  assert.equal(receipt, 'rh-new');
});

test('extendVisibilityTimeout returns original handle on error', async () => {
  const logger = makeLogger();
  const queue = {
    changeMessageVisibility: async () => { throw new Error('boom'); },
  };
  const config = { queueName: 'q', queueVisibilityTimeout: 60 };
  const receipt = await extendVisibilityTimeout(queue, config, 'rh-old', logger);
  assert.equal(receipt, 'rh-old');
});

test('extendVisibilityTimeout falls back when response has no receipt handle', async () => {
  const logger = makeLogger();
  const queue = {
    changeMessageVisibility: async () => ({ body: {} }),
  };
  const config = { queueName: 'q', queueVisibilityTimeout: 60 };
  const receipt = await extendVisibilityTimeout(queue, config, 'rh-old', logger);
  assert.equal(receipt, 'rh-old');
});

test('deleteQueueMessage uses queue override and logs on failure', async () => {
  const logger = makeLogger();
  const queue = {
    deleteMessage: async () => { throw new Error('fail'); },
  };

  await deleteQueueMessage({ queueName: 'q' }, 'rh', logger, queue);
  assert.equal(logger.warnings.length, 1);
});

test('deleteQueueMessage logs at info on success', async () => {
  const logger = makeLogger();
  let infoCalled = false;
  logger.info = () => { infoCalled = true; };
  const queue = {
    deleteMessage: async () => {},
  };

  await deleteQueueMessage({ queueName: 'q' }, 'rh', logger, queue);
  assert.equal(infoCalled, true);
});

test('recvMessages returns empty array on MessageNotExist', async () => {
  const logger = makeLogger();
  const queue = {
    batchReceiveMessage: async () => {
      const err = new Error('Message not exist');
      err.code = 'MessageNotExist';
      throw err;
    },
  };

  const result = await recvMessages(queue, 1, 1, { queueName: 'q' }, {}, logger);
  assert.deepEqual(result, []);
  assert.equal(logger.errors.length, 0);
});

test('recvMessages reports and rethrows unexpected errors', async () => {
  const logger = makeLogger();
  const reported = [];
  const job = {
    reportError: (err) => reported.push(err),
  };
  const queue = {
    batchReceiveMessage: async () => {
      throw new Error('boom');
    },
  };

  await assert.rejects(
    () => recvMessages(queue, 1, 1, { queueName: 'q' }, job, logger),
    /boom/,
  );
  assert.equal(reported.length, 1);
  assert.equal(logger.errors.length, 1);
});
