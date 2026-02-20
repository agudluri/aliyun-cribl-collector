const MNSClient = require('@alicloud/mns');
const { tryDecodeBase64 } = require('./utils');

// Derive MNS accountId from endpoint host, e.g.:
// https://1577260716295338.mns.cn-hangzhou.aliyuncs.com -> 1577260716295338
function deriveQueueAccountId(queueEndpoint) {
  const endpoint = typeof queueEndpoint === 'string' ? queueEndpoint.trim() : '';
  if (!endpoint) {
    throw new Error('queueEndpoint is required to derive SMQ account ID.');
  }

  let parsed;
  try {
    parsed = new URL(endpoint);
  } catch (error) {
    throw new Error(`Invalid queueEndpoint URL "${queueEndpoint}".`);
  }

  const [accountId] = (parsed.hostname || '').split('.');
  if (!accountId) {
    throw new Error(`Unable to derive SMQ account ID from queueEndpoint "${queueEndpoint}".`);
  }

  return accountId;
}

// Create an MNS client (or use injected test client).
function buildQueueClient(config) {
  // Tests can inject a fake client to avoid real network calls.
  if (config?._testQueueClient) {
    return config._testQueueClient;
  }
  if (config?._testClients?.queue) {
    return config._testClients.queue;
  }
  if (!config?.queueEndpoint) {
    return null;
  }
  const accountId = deriveQueueAccountId(config.queueEndpoint);
  const accessKeyId = config.accessKeyId;
  const accessKeySecret = config.accessKeySecret;

  // Base options for the MNS SDK.
  const options = {
    accessKeyId,
    accessKeySecret,
    securityToken: config.securityToken || undefined,
  };

  // Endpoint is provided explicitly (e.g., VPC/private or public endpoint).
  options.endpoint = config.queueEndpoint;

  return new MNSClient(accountId, options);
}

// Pull a batch of messages and normalize the response shape.
async function recvMessages(queue, waitSeconds, count, config, job, logger) {
  try {
    // batchReceiveMessage returns multiple messages when available.
    const response = await queue.batchReceiveMessage(
      config.queueName,
      count,
      waitSeconds,
    );
    return normalizeQueueMessages(response);
  } catch (error) {
    // Empty queue -> not an error; just return no messages.
    if (isMessageNotExist(error)) {
      return [];
    }
    logger.error('Aliyun SMQ receive failed: %s', error.message);
    if (job.reportError) {
      job.reportError(error);
    }
    throw error;
  }
}

// Optionally extend message visibility to give collect more time to run.
async function extendVisibilityTimeout(queue, config, receiptHandle, logger) {
  const visibilityTimeout = Number(config.queueVisibilityTimeout);
  if (!receiptHandle || !Number.isFinite(visibilityTimeout) || visibilityTimeout <= 0) {
    return receiptHandle;
  }

  try {
    const response = await queue.changeMessageVisibility(
      config.queueName,
      receiptHandle,
      visibilityTimeout,
    );
    const newHandle = response?.body?.ReceiptHandle || response?.body?.receiptHandle;
    return newHandle || receiptHandle;
  } catch (error) {
    logger.warn(
      'Aliyun SMQ visibility update failed for receiptHandle=%s: %s',
      receiptHandle,
      error.message,
    );
    return receiptHandle;
  }
}

// Handle multiple shapes returned by MNS SDKs.
function normalizeQueueMessages(response) {
  // The SDK may return an array directly.
  if (!response) {
    return [];
  }

  if (Array.isArray(response)) {
    return response;
  }

  // Some versions wrap results in a "body" field.
  if (Array.isArray(response.body)) {
    return response.body;
  }

  // Some responses wrap Messages under body.
  if (Array.isArray(response.body?.Messages?.Message)) {
    return response.body.Messages.Message;
  }

  if (response.body?.Messages?.Message) {
    return [response.body.Messages.Message];
  }

  if (response.body) {
    return [response.body];
  }

  // Legacy or alternative shape: response.Messages.Message
  if (Array.isArray(response.Messages?.Message)) {
    return response.Messages.Message;
  }

  if (response.Messages?.Message) {
    return [response.Messages.Message];
  }

  // Single message fallback.
  if (response.Message) {
    return [response.Message];
  }

  return [];
}

// Detect the "queue empty" condition across SDK error shapes.
function isMessageNotExist(error) {
  if (!error) {
    return false;
  }
  // Try to locate an error code in several possible places.
  const code = [
    error.code,
    error.Code,
    error?.Error?.Code,
    error?.body?.Error?.Code,
    error.name,
  ].find((value) => value);

  if (code && String(code).toLowerCase().includes('messagenotexist')) {
    return true;
  }

  // Fall back to message text.
  const message = (
    error.message
    || error?.Message
    || error?.Error?.Message
    || ''
  ).toLowerCase();

  return message.includes('message not exist');
}

// Parse and normalize SMQ/MNS notification payloads.
function parseQueueMessage(message, config, logger) {
  if (!message) {
    return null;
  }

  // ReceiptHandle is required later to delete the message.
  const receiptHandle = message.ReceiptHandle || message.receiptHandle;
  const messageId = message.MessageId || message.messageId;
  if (!receiptHandle) {
    logger.warn(
      'Aliyun SMQ message %s missing receipt handle; skipping message.',
      messageId || '<unknown>',
    );
    return null;
  }

  let payload = message.MessageBody;
  if (typeof payload === 'string') {
    payload = payload.trim();
  }

  // data is the parsed JSON payload (if possible).
  let data = payload;
  if (typeof payload === 'string' && payload.length) {
    try {
      data = JSON.parse(payload);
    } catch (error) {
      // Some MNS SDKs return base64 payloads; try decoding before failing.
      const decoded = tryDecodeBase64(payload);
      if (decoded) {
        try {
          data = JSON.parse(decoded);
        } catch (innerError) {
          logger.warn(
            'Aliyun SMQ message %s is not valid JSON: %s',
            messageId || '<unknown>',
            innerError.message,
          );
          return null;
        }
      } else {
        logger.warn(
          'Aliyun SMQ message %s is not valid JSON: %s',
          messageId || '<unknown>',
          error.message,
        );
        return null;
      }
    }
  }

  const event = Array.isArray(data?.events) && data.events.length
    ? data.events[0]
    : data?.event || data;
  // Some notifications use "records" instead of "events".
  const record = Array.isArray(data?.records) && data.records.length
    ? data.records[0]
    : null;
  const oss = event?.oss || data?.oss || record?.oss;
  const objectMetadata = oss?.object || data?.object || record?.object;

  // Bucket/key can appear in different fields depending on emitter.
  let bucket = oss?.bucket?.name
    || data?.bucket
    || data?.bucketName
    || record?.bucketName
    || config.bucket;
  let key = objectMetadata?.key
    || data?.objectKey
    || data?.key
    || record?.objectKey
    || record?.oss?.object?.key;

  if (typeof key === 'string') {
    // OSS sometimes URL-encodes keys.
    key = decodeObjectKey(key);
  }

  // If we cannot identify bucket+key, skip this message.
  if (!bucket || !key) {
    logger.warn(
      'Aliyun SMQ message missing bucket/key. messageId=%s',
      messageId || '<unknown>',
    );
    return null;
  }

  // Normalize common metadata fields when present.
  const size = Number(objectMetadata?.size ?? data?.size);
  const lastModified = event?.eventTime
    || objectMetadata?.lastModified
    || data?.eventTime
    || data?.lastModified;
  const etag = objectMetadata?.eTag || objectMetadata?.etag || data?.etag;

  return {
    bucket,
    key,
    size: Number.isFinite(size) ? size : undefined,
    lastModified,
    etag,
    receiptHandle,
    messageId,
    queue: {
      receiptHandle,
      messageId,
      dequeueCount: message.DequeueCount,
      nextVisibleTime: message.NextVisibleTime,
    },
  };
}

// Decode URL-encoded OSS object keys.
function decodeObjectKey(key) {
  if (typeof key !== 'string' || !key.length) {
    return key;
  }

  try {
    // decodeURIComponent does not translate '+' into space, which avoids corrupting literal plus signs.
    return decodeURIComponent(key);
  } catch (error) {
    return key;
  }
}

// Receipt handle can appear in multiple places; pick the first available.
function getReceiptHandle(collectible) {
  return collectible?.receiptHandle
    || collectible?.queueReceiptHandle
    || collectible?.queue?.receiptHandle
    || collectible?.ReceiptHandle
    || collectible?.queue?.ReceiptHandle;
}

// Delete (ack) a message so it is not reprocessed.
async function deleteQueueMessage(config, receiptHandle, logger, queueOverride) {
  if (!receiptHandle) {
    return;
  }

  const queue = queueOverride || buildQueueClient(config);
  if (!queue) {
    logger.warn('Aliyun SMQ delete skipped: queue client not configured.');
    return;
  }

  try {
    await queue.deleteMessage(config.queueName, receiptHandle);
    if (typeof logger.info === 'function') {
      logger.info('Aliyun SMQ deleted message %s', receiptHandle);
    }
  } catch (error) {
    logger.warn(
      'Aliyun SMQ delete failed for receiptHandle=%s: %s',
      receiptHandle,
      error.message,
    );
  }
}

module.exports = {
  buildQueueClient,
  deriveQueueAccountId,
  recvMessages,
  extendVisibilityTimeout,
  normalizeQueueMessages,
  isMessageNotExist,
  parseQueueMessage,
  decodeObjectKey,
  getReceiptHandle,
  deleteQueueMessage,
};
