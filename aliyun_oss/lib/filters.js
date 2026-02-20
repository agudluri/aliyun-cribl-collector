// Include/exclude filters for OSS keys.

function shouldInclude(key, config) {
  const { includeFilters = [], excludeFilters = [] } = config;

  // Exclude filters win: any match skips the object.
  if (Array.isArray(excludeFilters) && excludeFilters.some((filter) => filter && key.includes(filter))) {
    return false;
  }

  // If include filters are provided, at least one must match.
  if (Array.isArray(includeFilters) && includeFilters.length) {
    return includeFilters.some((filter) => filter && key.includes(filter));
  }

  // No filters configured -> accept everything.
  return true;
}

module.exports = {
  shouldInclude,
};
