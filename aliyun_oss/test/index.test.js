const test = require('node:test');
const assert = require('node:assert/strict');

const collector = require('../index.js');
const {
  normalizeQueueMessages,
  parseQueueMessage,
  isMessageNotExist,
  tryDecodeBase64,
  getReceiptHandle,
  shouldInclude,
  deleteQueueMessage,
} = collector._test;

// Exercise init() path to keep baseConfig coverage without affecting tests.
collector.init({ conf: {} });

const previousGlobalC = globalThis.C;
globalThis.C = {
  Secret: (name) => {
    const values = {
      testAccessKeyId: 'id',
      testAccessKeySecret: 'secret',
      testSecurityToken: 'token',
    };
    return { value: values[name] || '' };
  },
};
test.after(() => {
  globalThis.C = previousGlobalC;
});

function makeLogger() {
  const warnings = [];
  const infos = [];
  return {
    warnings,
    infos,
    warn: (...args) => warnings.push(args),
    info: (...args) => infos.push(args),
    debug: () => {},
    error: () => {},
  };
}

async function readStream(stream) {
  let data = '';
  for await (const chunk of stream) {
    data += chunk;
  }
  return data;
}

test('normalizeQueueMessages handles common response shapes', () => {
  const msg = { MessageBody: '{}' };

  assert.deepEqual(normalizeQueueMessages([msg]), [msg]);
  assert.deepEqual(normalizeQueueMessages({ body: [msg] }), [msg]);
  assert.deepEqual(normalizeQueueMessages({ body: msg }), [msg]);
  assert.deepEqual(normalizeQueueMessages({ Messages: { Message: [msg] } }), [msg]);
  assert.deepEqual(normalizeQueueMessages({ Messages: { Message: msg } }), [msg]);
  assert.deepEqual(normalizeQueueMessages({ Message: msg }), [msg]);
  assert.deepEqual(normalizeQueueMessages(null), []);
});

test('tryDecodeBase64 decodes valid payloads and ignores JSON strings', () => {
  const payload = JSON.stringify({ a: 1, b: 'two' });
  const b64 = Buffer.from(payload).toString('base64');

  assert.equal(tryDecodeBase64(' {"a":1} '), null);
  assert.equal(tryDecodeBase64(b64), payload);
  assert.equal(tryDecodeBase64('not-base64'), null);
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

test('isMessageNotExist recognizes MNS empty-queue errors', () => {
  assert.equal(isMessageNotExist({ name: 'MNSMessageNotExistError' }), true);
  assert.equal(isMessageNotExist({ code: 'MessageNotExist' }), true);
  assert.equal(isMessageNotExist(new Error('Message not exist')), true);
  assert.equal(isMessageNotExist(new Error('Other error')), false);
});

test('getReceiptHandle prefers explicit receipt handles', () => {
  assert.equal(getReceiptHandle({ receiptHandle: 'a', queueReceiptHandle: 'b' }), 'a');
  assert.equal(getReceiptHandle({ queueReceiptHandle: 'b' }), 'b');
  assert.equal(getReceiptHandle({ queue: { receiptHandle: 'c' } }), 'c');
  assert.equal(getReceiptHandle({ ReceiptHandle: 'd' }), 'd');
  assert.equal(getReceiptHandle({ queue: { ReceiptHandle: 'e' } }), 'e');
  assert.equal(getReceiptHandle({}), undefined);
});

test('shouldInclude honors include and exclude filters', () => {
  assert.equal(shouldInclude('abc', { excludeFilters: ['b'] }), false);
  assert.equal(shouldInclude('abc', { includeFilters: ['b'] }), true);
  assert.equal(shouldInclude('abc', { includeFilters: ['x'] }), false);
  assert.equal(shouldInclude('abc', {}), true);
});

test('deleteQueueMessage calls deleteMessage with queue name and receipt handle', async () => {
  const logger = makeLogger();
  const calls = [];
  const queue = {
    deleteMessage: async (queueName, receiptHandle) => {
      calls.push({ queueName, receiptHandle });
    },
  };

  await deleteQueueMessage({ queueName: 'queue-a' }, 'rh-1', logger, queue);

  assert.deepEqual(calls, [{ queueName: 'queue-a', receiptHandle: 'rh-1' }]);
});

test('deleteQueueMessage no-ops when receipt handle is missing', async () => {
  const logger = makeLogger();
  const calls = [];
  const queue = {
    deleteMessage: async () => {
      calls.push('called');
    },
  };

  await deleteQueueMessage({ queueName: 'queue-a' }, '', logger, queue);
  await deleteQueueMessage({ queueName: 'queue-a' }, null, logger, queue);

  assert.deepEqual(calls, []);
});

test('discover + collect end-to-end deletes the message and emits events', async () => {
  const logger = makeLogger();
  const deleted = [];
  let recvCalls = 0;

  const queue = {
    batchReceiveMessage: async (queueName, numOfMessages, waitSeconds) => {
      recvCalls += 1;
      if (recvCalls > 1) {
        return [];
      }
      return [{
        MessageId: 'mid-100',
        ReceiptHandle: 'rh-100',
        MessageBody: JSON.stringify({
          bucket: 'demo-bucket',
          key: 'logs/one.log',
          size: 12,
        }),
      }];
    },
    changeMessageVisibility: async (queueName, receiptHandle, timeoutSeconds) => ({
      body: { ReceiptHandle: `rh-100-extended-${timeoutSeconds}` },
    }),
    deleteMessage: async (queueName, receiptHandle) => {
      deleted.push({ queueName, receiptHandle });
    },
  };

  const oss = {
    useBucket: () => {},
    get: async () => ({ content: Buffer.from('line1\nline2\n') }),
  };

  const config = {
    queueName: 'queue-a',
    queueEndpoint: 'https://acc-1.mns.cn-test.aliyuncs.com',
    queueVisibilityTimeout: 120,
    accessKeyIdSecretName: 'testAccessKeyId',
    accessKeySecretSecretName: 'testAccessKeySecret',
    ossEndpoint: 'https://oss-cn-test.aliyuncs.com',
    bucket: 'demo-bucket',
    _testQueueClient: queue,
    _testOssClient: oss,
  };

  const job = {
    collectorConfig: { conf: config },
    logger: () => logger,
    reportError: () => {},
    addResults: async (results) => {
      job._results = results;
    },
  };

  await collector.discover(job);
  assert.ok(job._results);
  assert.equal(job._results.length, 1);

  const stream = await collector.collect(job._results[0], job);
  const output = await readStream(stream);
  const lines = output.trim().split('\n').map((line) => JSON.parse(line));

  assert.equal(lines.length, 2);
  assert.equal(lines[0].message, 'line1');
  assert.equal(lines[0].oss.bucket, 'demo-bucket');
  assert.equal(lines[0].oss.key, 'logs/one.log');
  assert.deepEqual(deleted, [{ queueName: 'queue-a', receiptHandle: 'rh-100-extended-120' }]);
});

test('collect fails when receipt handle is missing', async () => {
  const logger = makeLogger();
  const config = {
    queueName: 'queue-a',
    queueEndpoint: 'https://acc-1.mns.cn-test.aliyuncs.com',
    accessKeyIdSecretName: 'testAccessKeyId',
    accessKeySecretSecretName: 'testAccessKeySecret',
    ossEndpoint: 'https://oss-cn-test.aliyuncs.com',
    bucket: 'demo-bucket',
    _testQueueClient: { deleteMessage: async () => {} },
    _testOssClient: { useBucket: () => {}, get: async () => ({ content: Buffer.from('line1\n') }) },
  };
  const job = {
    collectorConfig: { conf: config },
    logger: () => logger,
    reportError: () => {},
  };

  await assert.rejects(
    () => collector.collect({ bucket: 'demo-bucket', key: 'logs/missing.log' }, job),
    /Missing SMQ receipt handle/,
  );
});

test('discover returns without results when queue is empty', async () => {
  const queue = {
    batchReceiveMessage: async () => [],
  };

  const config = {
    queueName: 'queue-a',
    queueEndpoint: 'https://acc-1.mns.cn-test.aliyuncs.com',
    accessKeyIdSecretName: 'testAccessKeyId',
    accessKeySecretSecretName: 'testAccessKeySecret',
    ossEndpoint: 'https://oss-cn-test.aliyuncs.com',
    bucket: 'demo-bucket',
    _testQueueClient: queue,
  };

  const job = {
    collectorConfig: { conf: config },
    addResults: async () => {
      throw new Error('should not be called');
    },
  };

  await collector.discover(job);
});

test('discover uses logger object form (not only logger function)', async () => {
  const logger = makeLogger();
  const queue = {
    batchReceiveMessage: async () => [],
  };
  const config = {
    queueName: 'queue-a',
    queueEndpoint: 'https://acc-1.mns.cn-test.aliyuncs.com',
    accessKeyIdSecretName: 'testAccessKeyId',
    accessKeySecretSecretName: 'testAccessKeySecret',
    ossEndpoint: 'https://oss-cn-test.aliyuncs.com',
    bucket: 'demo-bucket',
    _testQueueClient: queue,
  };
  const job = {
    collectorConfig: { conf: config },
    logger,
  };

  await collector.discover(job);
  assert.ok(logger.infos.some((entry) => String(entry[0]).includes('Aliyun SMQ discover starting')));
});

test('discover uses job.log function when present', async () => {
  const queue = {
    batchReceiveMessage: async () => [],
  };
  const config = {
    queueName: 'queue-a',
    queueEndpoint: 'https://acc-1.mns.cn-test.aliyuncs.com',
    accessKeyIdSecretName: 'testAccessKeyId',
    accessKeySecretSecretName: 'testAccessKeySecret',
    ossEndpoint: 'https://oss-cn-test.aliyuncs.com',
    bucket: 'demo-bucket',
    _testQueueClient: queue,
  };
  const logs = [];
  const job = {
    collectorConfig: { conf: config },
    log: (level, message) => logs.push({ level, message }),
  };

  await collector.discover(job);
  assert.ok(logs.some((entry) => entry.message.includes('Aliyun collector logger binding')));
  assert.ok(logs.some((entry) => entry.message.includes('Aliyun SMQ discover starting')));
});

test('discover skips invalid and filtered messages and stops at max', async () => {
  const logger = makeLogger();
  const queue = {
    batchReceiveMessage: async () => ([
      { MessageId: 'mid-a', ReceiptHandle: 'rh-a', MessageBody: 'not-json' },
      {
        MessageId: 'mid-b',
        ReceiptHandle: 'rh-b',
        MessageBody: JSON.stringify({ bucket: 'demo-bucket', key: 'skip.log' }),
      },
      {
        MessageId: 'mid-c',
        ReceiptHandle: 'rh-c',
        MessageBody: JSON.stringify({ bucket: 'demo-bucket', key: 'keep.log' }),
      },
    ]),
  };

  const config = {
    queueName: 'queue-a',
    queueEndpoint: 'https://acc-1.mns.cn-test.aliyuncs.com',
    queueMaxMessages: 1,
    accessKeyIdSecretName: 'testAccessKeyId',
    accessKeySecretSecretName: 'testAccessKeySecret',
    ossEndpoint: 'https://oss-cn-test.aliyuncs.com',
    bucket: 'demo-bucket',
    includeFilters: ['keep'],
    _testQueueClient: queue,
  };

  const job = {
    collectorConfig: { conf: config },
    logger: () => logger,
    addResults: async (results) => {
      job._results = results;
    },
  };

  await collector.discover(job);
  assert.ok(job._results);
  assert.equal(job._results.length, 1);
  assert.equal(job._results[0].key, 'keep.log');
  assert.ok(logger.infos.some((entry) => String(entry[0]).includes('received message')));
});

test('collect works with oss client that lacks useBucket', async () => {
  const logger = makeLogger();
  const config = {
    queueName: 'queue-a',
    queueEndpoint: 'https://acc-1.mns.cn-test.aliyuncs.com',
    accessKeyIdSecretName: 'testAccessKeyId',
    accessKeySecretSecretName: 'testAccessKeySecret',
    ossEndpoint: 'https://oss-cn-test.aliyuncs.com',
    bucket: 'demo-bucket',
    _testQueueClient: { deleteMessage: async () => {} },
    _testOssClient: { get: async () => ({ content: Buffer.from('line1\n') }) },
  };
  const job = {
    collectorConfig: { conf: config },
    logger: () => logger,
    reportError: () => {},
  };

  const stream = await collector.collect({
    bucket: 'demo-bucket',
    key: 'logs/one.log',
    receiptHandle: 'rh-1',
  }, job);

  const output = await readStream(stream);
  assert.ok(output.includes('line1'));
});
