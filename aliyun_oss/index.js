// Core helper modules.
const { format } = require('util');
const { getConfig, validateConfig } = require('./lib/config');
const { buildOssClient, downloadObject, maybeGunzip, buildOutputStream } = require('./lib/oss');
const {
  buildQueueClient,
  recvMessages,
  extendVisibilityTimeout,
  parseQueueMessage,
  getReceiptHandle,
  deleteQueueMessage,
  normalizeQueueMessages,
  isMessageNotExist,
  decodeObjectKey,
} = require('./lib/queue');
const { shouldInclude } = require('./lib/filters');
const { clamp, toInt, tryDecodeBase64 } = require('./lib/utils');
const { writeCheckpoint } = require('./lib/checkpoint');

// Metadata that Cribl reads when loading the collector.
exports.name = 'Aliyun OSS Bucket';
exports.version = '0.1.0';
exports.disabled = false;
exports.destroyable = false;
exports.hidden = false;

// Prefer Cribl's task logger (function or object form) so messages appear in Job Inspector.
// Fall back to console only when the job logger is unavailable.
function getLogger(job) {
  // In some Cribl runtimes, this is the canonical task-log writer.
  const jobLogFn = job && typeof job.log === 'function'
    ? job.log.bind(job)
    : null;

  if (jobLogFn) {
    return {
      source: 'job.log(level,message)',
      info: (...args) => jobLogFn('info', format(...args)),
      warn: (...args) => jobLogFn('warn', format(...args)),
      error: (...args) => jobLogFn('error', format(...args)),
      debug: (...args) => jobLogFn('debug', format(...args)),
    };
  }

  let runtimeLogger = null;
  let loggerSource = 'console';

  // Some Cribl runtimes expose logger as a function.
  if (job && typeof job.logger === 'function') {
    try {
      runtimeLogger = job.logger();
      loggerSource = 'job.logger()';
    } catch (error) {
      runtimeLogger = null;
      loggerSource = 'console';
    }
  // Other runtimes expose logger directly as an object.
  } else if (job && job.logger && typeof job.logger === 'object') {
    runtimeLogger = job.logger;
    loggerSource = 'job.logger';
  } else if (job && job.log && typeof job.log === 'object') {
    runtimeLogger = job.log;
    loggerSource = 'job.log';
  }

  const fallback = console;
  const bindMethod = (method, fallbackMethod) => {
    if (runtimeLogger && typeof runtimeLogger[method] === 'function') {
      return runtimeLogger[method].bind(runtimeLogger);
    }
    return fallbackMethod.bind(fallback);
  };

  return {
    info: bindMethod('info', fallback.info || fallback.log),
    warn: bindMethod('warn', fallback.warn || fallback.log),
    error: bindMethod('error', fallback.error || fallback.log),
    debug: bindMethod('debug', fallback.debug || fallback.log),
    source: loggerSource,
  };
}

// Stored base config from init(). Used as a fallback for later jobs.
let baseConfig = {};
// Capture base config once at initialization time.
exports.init = async (opts = {}) => {
  // Defensive copy so we don't mutate Cribl's object.
  baseConfig = opts.conf ? { ...opts.conf } : {};
};

// ----------------------------------------------------------------------
// DISCOVER phase: read queue messages and report objects to collect.
// ----------------------------------------------------------------------
exports.discover = async (job) => {
  // Merge config from all known sources on the job object.
  const config = getConfig(baseConfig, job);
  // Validate required fields before making any network calls.
  validateConfig(config, job);
  const logger = getLogger(job);
  logger.info('Aliyun collector logger binding: phase=discover source=%s', logger.source);
  await discoverFromQueue(job, config, logger);
};

async function discoverFromQueue(job, config, logger) {
  // Build the MNS client using the shared credentials.
  const queue = buildQueueClient(config);
  if (!queue) {
    logger.warn('Aliyun SMQ discover skipped: queue client not configured.');
    return;
  }

  // Bound discovery and batch sizes to stay within MNS limits.
  const maxMessages = Math.max(
    1,
    toInt(config.queueMaxMessages)
      || 256,
  );
  const batchSize = Math.max(1, Math.min(toInt(config.queueBatchSize) || 10, 16));
  const waitSeconds = clamp(Number(config.queueWaitSeconds), 0, 30, 20);
  const results = [];

  logger.info(
    'Aliyun SMQ discover starting: queue=%s maxMessages=%d batchSize=%d waitSeconds=%d',
    config.queueName,
    maxMessages,
    batchSize,
    waitSeconds,
  );

  // Keep polling until we hit the per-run maximum.
  while (results.length < maxMessages) {
    const remaining = Math.min(batchSize, maxMessages - results.length);
    const messages = await recvMessages(queue, waitSeconds, remaining, config, job, logger);
    // Empty response -> stop discovery for this run.
    if (!messages.length) break;

    for (const message of messages) {
      // Normalize the MNS notification into { bucket, key, ... }.
      const parsed = parseQueueMessage(message, config, logger);
      if (!parsed) {
        continue;
      }

      // Apply include/exclude filters on the object key.
      if (!shouldInclude(parsed.key, config)) {
        continue;
      }

      // Optionally extend visibility and capture the new receipt handle.
      const updatedHandle = await extendVisibilityTimeout(
        queue,
        config,
        parsed.receiptHandle,
        logger,
      );
      if (updatedHandle && updatedHandle !== parsed.receiptHandle) {
        parsed.receiptHandle = updatedHandle;
        if (parsed.queue) {
          parsed.queue.receiptHandle = updatedHandle;
        }
      }

      logger.info(
        'Aliyun SMQ received message %s for oss://%s/%s',
        parsed.messageId || '<unknown>',
        parsed.bucket,
        parsed.key,
      );

      // Push into results for the collect phase.
      results.push(parsed);

      if (results.length >= maxMessages) {
        break;
      }
    }
  }

  if (!results.length) {
    logger.info('Aliyun SMQ discover finished: no messages available.');
    return;
  }

  logger.info(
    'Aliyun SMQ discover reporting %d objects from queue %s.',
    results.length,
    config.queueName,
  );
  // Hand results back to Cribl so it can schedule collect() for each object.
  await job.addResults(results);
}

// ----------------------------------------------------------------------
// COLLECT phase: download object, emit events, checkpoint, and delete the message.
// ----------------------------------------------------------------------
exports.collect = async (collectible, job) => {
  // Same config merge/validation as discovery.
  const config = getConfig(baseConfig, job);
  validateConfig(config, job);

  const bucket = collectible.bucket || config.bucket;
  const logger = getLogger(job);
  logger.info('Aliyun collector logger binding: phase=collect source=%s', logger.source);
  const client = buildOssClient(config, bucket);
  const key = collectible.key;
  const receiptHandle = getReceiptHandle(collectible);
  const queue = buildQueueClient(config);

  // Without a receipt handle we cannot delete the message; fail fast.
  if (!receiptHandle) {
    const error = new Error(
      `Missing SMQ receipt handle for oss://${bucket}/${key}. ` +
      'Message cannot be deleted and will be reprocessed.',
    );
    logger.error(error.message);
    if (job?.reportError) {
      job.reportError(error);
    }
    throw error;
  }

  // Some OSS client versions allow switching buckets after construction.
  if (bucket && typeof client.useBucket === 'function') {
    client.useBucket(bucket);
  }

  logger.info('Collecting oss://%s/%s', bucket, key);

  // Download object, optionally decompress, then split into non-empty lines.
  const buffer = await downloadObject(client, key, logger, job);
  const payload = maybeGunzip(buffer, key, config, logger, job).toString('utf8');
  const lines = payload.split(/\r?\n/).filter((line) => line.trim().length);

  const emitMetadata = {
    bucket,
    key,
    size: collectible.size || buffer.length,
    lastModified: collectible.lastModified,
    etag: collectible.etag,
  };

  // Create the output stream for Cribl (raw or JSON-wrapped).
  const stream = buildOutputStream(lines, emitMetadata, config.outputFormat || 'json');

  // Checkpoint is informational; queue is the source of truth.
  await writeCheckpoint(job, config, {
    lastKey: key,
    lastModified: collectible.lastModified || new Date().toISOString(),
  }, logger);

  // Delete the queue message only after a successful download/emit.
  // Reuse the queue client built for this collect run.
  await deleteQueueMessage(config, receiptHandle, logger, queue);

  return stream;
};

// Export helpers for unit tests (not used at runtime by Cribl).
exports._test = {
  normalizeQueueMessages,
  parseQueueMessage,
  isMessageNotExist,
  tryDecodeBase64,
  getReceiptHandle,
  decodeObjectKey,
  shouldInclude,
  deleteQueueMessage,
};
