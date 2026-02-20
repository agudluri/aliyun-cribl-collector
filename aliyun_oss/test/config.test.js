const test = require('node:test');
const assert = require('node:assert/strict');

const { getConfig, validateConfig } = require('../lib/config');

function makeJob() {
  return {
    errors: [],
    reportError: function (err) {
      this.errors.push(err);
    },
  };
}

test('getConfig merges sources with later entries overriding earlier ones', () => {
  const baseConfig = { a: 1, bucket: 'base' };
  const job = {
    collectorConfig: { conf: { a: 2 } },
    config: { a: 3, b: 4 },
    params: { a: 5 },
  };

  const merged = getConfig(baseConfig, job);
  assert.equal(merged.a, 5);
  assert.equal(merged.b, 4);
  assert.equal(merged.bucket, 'base');
});

test('getConfig trims whitespace for known string fields and filters', () => {
  const baseConfig = {
    ossEndpoint: ' https://oss-cn-test.aliyuncs.com ',
    queueEndpoint: ' https://acc-1.mns.cn-test.aliyuncs.com ',
    queueName: ' queue-a ',
    bucket: ' bucket ',
    accessKeyIdSecretName: ' keyIdName ',
    accessKeySecretSecretName: ' keySecretName ',
    securityTokenSecretName: ' tokenName ',
    checkpointKey: ' key ',
    outputFormat: ' json ',
    includeFilters: [' logs ', ' ', 'audit'],
    excludeFilters: [' tmp ', '', null],
  };

  const oldC = globalThis.C;
  globalThis.C = {
    Secret: (name) => {
      const values = {
        keyIdName: ' id ',
        keySecretName: ' secret ',
        tokenName: ' token ',
      };
      return { value: values[name] };
    },
  };
  try {
    const merged = getConfig(baseConfig, {});
    assert.equal(merged.ossEndpoint, 'https://oss-cn-test.aliyuncs.com');
    assert.equal(merged.queueEndpoint, 'https://acc-1.mns.cn-test.aliyuncs.com');
    assert.equal(merged.queueName, 'queue-a');
    assert.equal(merged.bucket, 'bucket');
    assert.equal(merged.accessKeyId, 'id');
    assert.equal(merged.accessKeySecret, 'secret');
    assert.equal(merged.securityToken, 'token');
    assert.equal(merged.checkpointKey, 'key');
    assert.equal(merged.outputFormat, 'json');
    assert.deepEqual(merged.includeFilters, ['logs', 'audit']);
    assert.deepEqual(merged.excludeFilters, ['tmp']);
  } finally {
    globalThis.C = oldC;
  }
});

test('getConfig resolves credentials from secret-name fields', () => {
  const oldC = globalThis.C;
  globalThis.C = {
    Secret: (name) => {
      const values = {
        keyIdName: ' resolved-id ',
        keySecretName: ' resolved-secret ',
        tokenName: ' resolved-token ',
      };
      return { value: values[name] };
    },
  };

  try {
    const merged = getConfig({
      accessKeyIdSecretName: ' keyIdName ',
      accessKeySecretSecretName: ' keySecretName ',
      securityTokenSecretName: ' tokenName ',
    }, {});

    assert.equal(merged.accessKeyId, 'resolved-id');
    assert.equal(merged.accessKeySecret, 'resolved-secret');
    assert.equal(merged.securityToken, 'resolved-token');
  } finally {
    globalThis.C = oldC;
  }
});

test('getConfig throws when secret-name fields are set but Cribl secret API is unavailable', () => {
  const oldC = globalThis.C;
  delete globalThis.C;

  assert.throws(
    () => getConfig({ accessKeyIdSecretName: 'myKeyId' }, {}),
    /Cribl Secret API is not available/,
  );

  globalThis.C = oldC;
});

test('validateConfig accepts queueEndpoint', () => {
  const job = makeJob();
  const config = {
    accessKeyIdSecretName: 'keyIdName',
    accessKeySecretSecretName: 'keySecretName',
    bucket: 'bucket',
    ossEndpoint: 'https://oss-cn-test.aliyuncs.com',
    queueName: 'queue-a',
    queueEndpoint: 'https://acc-1.mns.cn-test.aliyuncs.com',
  };

  assert.doesNotThrow(() => validateConfig(config, job));
  assert.equal(job.errors.length, 0);
});

test('validateConfig reports and throws on missing required fields', () => {
  const job = makeJob();
  const config = {
    bucket: 'bucket',
    ossEndpoint: 'https://oss-cn-test.aliyuncs.com',
    queueName: 'queue-a',
  };

  assert.throws(() => validateConfig(config, job), /Missing required collector configuration/);
  assert.equal(job.errors.length, 1);
});

test('validateConfig reports missing bucket and ossEndpoint', () => {
  const job = makeJob();
  const config = {
    accessKeyIdSecretName: 'keyIdName',
    accessKeySecretSecretName: 'keySecretName',
    queueEndpoint: 'https://acc-1.mns.cn-test.aliyuncs.com',
    queueName: 'queue-a',
  };

  assert.throws(() => validateConfig(config, job), /bucket, ossEndpoint/);
  assert.equal(job.errors.length, 1);
});

test('validateConfig requires queueEndpoint', () => {
  const job = makeJob();
  const config = {
    accessKeyIdSecretName: 'keyIdName',
    accessKeySecretSecretName: 'keySecretName',
    bucket: 'bucket',
    ossEndpoint: 'https://oss-cn-test.aliyuncs.com',
    queueName: 'queue-a',
  };

  assert.throws(() => validateConfig(config, job), /queueEndpoint/);
  assert.equal(job.errors.length, 1);
});

test('validateConfig reports missing queueName', () => {
  const job = makeJob();
  const config = {
    accessKeyIdSecretName: 'keyIdName',
    accessKeySecretSecretName: 'keySecretName',
    bucket: 'bucket',
    ossEndpoint: 'https://oss-cn-test.aliyuncs.com',
    queueEndpoint: 'https://acc-1.mns.cn-test.aliyuncs.com',
  };

  assert.throws(() => validateConfig(config, job), /queueName/);
  assert.equal(job.errors.length, 1);
});
