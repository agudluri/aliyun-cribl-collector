// Generic helpers shared across modules.

// Clamp a value to a numeric range, or return a fallback if invalid.
function clamp(value, min, max, fallback) {
  if (!Number.isFinite(value)) {
    return fallback;
  }
  if (value < min) {
    return min;
  }
  if (value > max) {
    return max;
  }
  return value;
}

// Parse a number and return undefined if it is not finite.
function toInt(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

// Conservative base64 decoder to avoid false positives.
function tryDecodeBase64(value) {
  // Only attempt base64 on non-empty strings.
  if (typeof value !== 'string') {
    return null;
  }
  const trimmed = value.trim();
  // If it already looks like JSON, do not decode.
  if (!trimmed || trimmed.startsWith('{') || trimmed.startsWith('[')) {
    return null;
  }
  const compact = trimmed.replace(/\s+/g, '');
  // Base64 strings should have length divisible by 4.
  if (!compact.length || compact.length % 4 !== 0) {
    return null;
  }
  // Base64 alphabet check.
  if (!/^[A-Za-z0-9+/=]+$/.test(compact)) {
    return null;
  }
  try {
    return Buffer.from(compact, 'base64').toString('utf8');
  } catch (error) {
    return null;
  }
}

// Resolve a Cribl secret value by name.
function readCriblSecret(secretName, secretType = 'text') {
  const runtimeC = globalThis.C;
  if (!runtimeC || typeof runtimeC.Secret !== 'function') {
    throw new Error(
      `Cannot resolve secret "${secretName}": Cribl Secret API is not available in this runtime.`,
    );
  }

  // Ask Cribl's expression runtime for the secret reference object.
  const secretRef = runtimeC.Secret(secretName, secretType);
  // Cribl runtimes can expose value in different shapes (`value`, `val`, callable).
  const candidates = [secretRef?.value, secretRef?.val, secretRef];

  for (const candidate of candidates) {
    let resolved = candidate;
    if (typeof resolved === 'function') {
      try {
        resolved = resolved.call(secretRef);
      } catch (error) {
        continue;
      }
    }

    // Return first non-empty string value; trim UI-entered whitespace.
    if (typeof resolved === 'string' && resolved.trim()) {
      return resolved.trim();
    }
  }

  throw new Error(`Unable to resolve non-empty value for secret "${secretName}".`);
}

module.exports = {
  clamp,
  toInt,
  tryDecodeBase64,
  readCriblSecret,
};
