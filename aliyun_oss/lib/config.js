const { readCriblSecret } = require('./utils');

// Merge config from various Cribl-provided locations plus baseConfig.
// Later entries override earlier ones.
function getConfig(baseConfig, job) {
  // Cribl jobs can surface config in different places depending on runtime.
  // We scan all common locations and merge them into one object.
  const sources = [
    baseConfig,
    job?.collectorConfig?.conf,
    job?.collectorConfig,
    job?.collector?.conf,
    job?.collector,
    job?.config?.collector?.conf,
    job?.config?.collector,
    job?.config?.conf,
    job?.config,
    job?.params,
    job?.options,
  ];

  const merged = sources.reduce((acc, source) => {
    if (source && typeof source === 'object') {
      // Shallow merge: copy top-level fields into the accumulator.
      Object.assign(acc, source);
    }
    return acc;
  }, {});

  // Trim whitespace before resolving secrets.
  const sanitized = sanitizeConfig(merged);
  return resolveCredentialSecrets(sanitized, job);
}

// Trim plain string fields, otherwise keep original value type. (Helper to sanitizeConfig.)
function sanitizeString(value) {
  return typeof value === 'string' ? value.trim() : value;
}

// Trim each string in an array and drop empty entries.
// This is mainly used for include/exclude filter arrays. (Helper to sanitizeConfig.)
function sanitizeStringArray(values) {
  if (!Array.isArray(values)) {
    return values;
  }
  return values
    .map((value) => (typeof value === 'string' ? value.trim() : value))
    .filter((value) => value);
}

// The main function to sanitize config values.
function sanitizeConfig(config) {
  if (!config || typeof config !== 'object') {
    return config;
  }
  // Normalize known string fields that are commonly edited in UI.
  // Work on a copy to avoid mutating Cribl's original config object.
  const sanitized = { ...config };
  const stringFields = [
    'ossEndpoint',
    'queueEndpoint',
    'queueName',
    'bucket',
    'accessKeyIdSecretName',
    'accessKeySecretSecretName',
    'securityTokenSecretName',
    'checkpointKey',
    'outputFormat',
  ];

  for (const field of stringFields) {
    if (field in sanitized) {
      sanitized[field] = sanitizeString(sanitized[field]);
    }
  }

  sanitized.includeFilters = sanitizeStringArray(sanitized.includeFilters);
  sanitized.excludeFilters = sanitizeStringArray(sanitized.excludeFilters);

  return sanitized;
}

// Helper to resolveCredentialSecrets.
function resolveSecretField(config, valueField, secretNameField, defaultType = 'text') {
  if (!config || typeof config !== 'object') {
    return;
  }
  if (config[secretNameField]) {
    config[valueField] = readCriblSecret(config[secretNameField], defaultType);
  }
}

// Map all credential secret-name fields into runtime credential values.
function resolveCredentialSecrets(config, job) {
  if (!config || typeof config !== 'object') {
    return config;
  }

  try {
    resolveSecretField(config, 'accessKeyId', 'accessKeyIdSecretName');
    resolveSecretField(config, 'accessKeySecret', 'accessKeySecretSecretName');
    resolveSecretField(config, 'securityToken', 'securityTokenSecretName');
    return config;
  } catch (error) {
    // Surface secret-resolution issues in Job Inspector for troubleshooting.
    if (job?.reportError) {
      job.reportError(error);
    }
    throw error;
  }
}

// Ensure required OSS and SMQ fields are present.
function validateConfig(config, job) {
  // access keys must come from named Cribl secrets.
  const missing = [];
  if (!config.accessKeyIdSecretName) missing.push('accessKeyIdSecretName');
  if (!config.accessKeySecretSecretName) missing.push('accessKeySecretSecretName');
  if (!config.bucket) missing.push('bucket');
  if (!config.ossEndpoint) missing.push('ossEndpoint');

  if (missing.length) {
    // Report to Cribl UI if possible, then abort the run.
    const message = `Missing required collector configuration: ${missing.join(', ')}`;
    if (job && job.reportError) {
      job.reportError(new Error(message));
    }
    throw new Error(message);
  }

  // SMQ/MNS requirements (queue access).
  const missingQueue = [];
  if (!config.queueName) missingQueue.push('queueName');
  if (!config.queueEndpoint) {
    missingQueue.push('queueEndpoint');
  }
  if (missingQueue.length) {
    const message = `Missing required SMQ configuration: ${missingQueue.join(', ')}`;
    if (job && job.reportError) {
      job.reportError(new Error(message));
    }
    throw new Error(message);
  }
}

module.exports = {
  getConfig,
  validateConfig,
};
