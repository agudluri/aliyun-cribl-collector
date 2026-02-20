const OSS = require('ali-oss');
const { Readable } = require('stream');
const zlib = require('zlib');

// Create an OSS client (or use injected test client).
function buildOssClient(config, bucketOverride) {
  // In tests we inject a fake OSS client through config.
  if (config?._testOssClient) {
    return config._testOssClient;
  }
  if (config?._testClients?.oss) {
    return config._testClients.oss;
  }
  // Real OSS SDK client using shared credentials.
  return new OSS({
    bucket: bucketOverride || config.bucket,
    endpoint: config.ossEndpoint,
    accessKeyId: config.accessKeyId,
    accessKeySecret: config.accessKeySecret,
    stsToken: config.securityToken || undefined,
  });
}

// Download a full OSS object into memory.
async function downloadObject(client, key, logger, job) {
  try {
    // ali-oss returns { content } which may already be a Buffer.
    const response = await client.get(key);
    return Buffer.isBuffer(response.content)
      ? response.content
      : Buffer.from(response.content);
  } catch (error) {
    // Surface the error in Cribl and rethrow to fail the run.
    logger.error('Failed to download object %s: %s', key, error.message);
    if (job.reportError) {
      job.reportError(error);
    }
    throw error;
  }
}

function shouldGunzip(key, buffer) {
  // Fast path: file extension.
  if (key.toLowerCase().endsWith('.gz')) {
    return true;
  }
  // Fallback: inspect gzip magic bytes.
  if (!buffer || buffer.length < 2) {
    return false;
  }
  // gzip magic header 0x1f8b
  return buffer[0] === 0x1f && buffer[1] === 0x8b;
}

// Decompress gzip if enabled and applicable.
function maybeGunzip(buffer, key, config, logger, job) {
  // Respect config and skip if not gzip.
  if (config.decompressGzip === false || !shouldGunzip(key, buffer)) {
    return buffer;
  }
  try {
    // Synchronous gunzip is fine for small/medium objects.
    return zlib.gunzipSync(buffer);
  } catch (error) {
    logger.error('Failed to gunzip object %s: %s', key, error.message);
    if (job.reportError) {
      job.reportError(error);
    }
    throw error;
  }
}

// Produce output stream in either raw or JSON-wrapped format.
function buildOutputStream(lines, metadata, format) {
  if (format === 'raw') {
    // Raw mode: emit plain lines.
    return Readable.from(lines.map((line) => `${line}\n`), { encoding: 'utf8' });
  }
  // JSON mode: wrap each line with OSS metadata.
  return Readable.from(
    lines.map((line) => `${JSON.stringify({ message: line, oss: metadata })}\n`),
    { encoding: 'utf8' },
  );
}

module.exports = {
  buildOssClient,
  downloadObject,
  maybeGunzip,
  buildOutputStream,
};
