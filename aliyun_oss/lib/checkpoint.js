// Simple logging helpers to avoid hard dependency on config.js.
function logWarn(logger, message, ...args) {
  if (logger && typeof logger.warn === 'function') {
    logger.warn(message, ...args);
  } else {
    console.warn(message, ...args);
  }
}

function logDebug(logger, message, ...args) {
  if (logger && typeof logger.debug === 'function') {
    logger.debug(message, ...args);
  } else {
    // Avoid noisy output if debug is not available.
  }
}

// Fetch Cribl progress store if configured.
async function getProgressStore(job, logger) {
  if (!job) {
    return null;
  }

  // Reuse a cached store on the job object if already fetched.
  if (job.progressStore) {
    return job.progressStore;
  }

  // Some Cribl runtimes expose a method to fetch the store lazily.
  if (typeof job.getProgressStore === 'function') {
    try {
      const store = await job.getProgressStore();
      // Cache the store for subsequent calls.
      job.progressStore = store;
      return store;
    } catch (error) {
      logWarn(logger, 'Unable to obtain progress store: %s', error.message);
      return null;
    }
  }

  return null;
}

// Namespace checkpoints per bucket and optional suffix.
function getCheckpointKey(config) {
  // checkpointKey lets multiple schedules write separate checkpoints.
  const suffix = config.checkpointKey ? String(config.checkpointKey).trim() : 'default';
  return `aliyun-oss::${config.bucket}::${suffix}`;
}

// Read checkpoint (currently unused by collection flow).
async function readCheckpoint(job, config, logger) {
  const store = await getProgressStore(job, logger);
  // If there is no store or no "get" method, return null (no checkpoint).
  if (!store || typeof store.get !== 'function') {
    return null;
  }

  try {
    return await store.get(getCheckpointKey(config));
  } catch (error) {
    logDebug(logger, 'Unable to read checkpoint: %s', error.message);
    return null;
  }
}

// Write last processed object to progress store for observability.
async function writeCheckpoint(job, config, checkpoint, logger) {
  const store = await getProgressStore(job, logger);
  if (!store) {
    return;
  }

  // Key is stable across runs for the same bucket + suffix.
  const key = getCheckpointKey(config);
  const payload = {
    lastKey: checkpoint.lastKey,
    lastModified: checkpoint.lastModified,
  };

  try {
    // Avoid overwriting with older timestamps.
    if (typeof store.get === 'function') {
      const existing = await store.get(key);
      if (existing?.lastModified && new Date(existing.lastModified) > new Date(payload.lastModified)) {
        return;
      }
    }

    // Support multiple store APIs.
    if (typeof store.set === 'function') {
      await store.set(key, payload);
    } else if (typeof store.put === 'function') {
      await store.put(key, payload);
    } else if (typeof store.write === 'function') {
      await store.write(key, payload);
    } else {
      logWarn(logger, 'Progress store does not support writing checkpoints.');
    }
  } catch (error) {
    logWarn(logger, 'Unable to write checkpoint: %s', error.message);
  }
}

module.exports = {
  getProgressStore,
  getCheckpointKey,
  readCheckpoint,
  writeCheckpoint,
};
