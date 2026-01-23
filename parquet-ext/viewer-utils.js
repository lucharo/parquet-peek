// Utility functions for the Parquet Peek viewer
// Extracted for testability and reuse

export function escapeHtml(str) {
  if (str == null) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#x27;');
}

export function escapeSource(source) {
  return source.replace(/'/g, "''");
}

export function escapeLikePattern(str) {
  return str
    .replace(/\\/g, '\\\\')
    .replace(/%/g, '\\%')
    .replace(/_/g, '\\_');
}

export function escapeColumnName(name) {
  return `"${name.replace(/"/g, '""')}"`;
}

export function truncateColumnName(name, maxLen = 30) {
  if (name.length <= maxLen) return name;
  return name.slice(0, maxLen - 1) + '\u2026';
}

export function buildFilterClauses(filterMap, colMeta) {
  return Object.entries(filterMap)
    .filter(([_, val]) => {
      if (typeof val === 'object') {
        return val.min !== '' || val.max !== '';
      }
      return val && val.trim();
    })
    .map(([col, val]) => {
      const meta = colMeta[col] || {};
      if (meta.filterType === 'range') {
        const parts = [];
        const minNum = parseFloat(val.min);
        const maxNum = parseFloat(val.max);
        if (!isNaN(minNum)) parts.push(`${escapeColumnName(col)} >= ${minNum}`);
        if (!isNaN(maxNum)) parts.push(`${escapeColumnName(col)} <= ${maxNum}`);
        return parts.length > 0 ? `(${parts.join(' AND ')})` : null;
      } else if (meta.filterType === 'date') {
        const parts = [];
        if (val.min && /^\d{4}-\d{2}-\d{2}$/.test(val.min)) {
          parts.push(`${escapeColumnName(col)} >= '${val.min}'`);
        }
        if (val.max && /^\d{4}-\d{2}-\d{2}$/.test(val.max)) {
          const isTimestamp = /^TIMESTAMP/i.test(meta.type || '');
          if (isTimestamp) {
            // For TIMESTAMP columns, use exclusive upper bound (< next day)
            // to include all times within the selected date
            parts.push(`${escapeColumnName(col)} < '${val.max}'::DATE + INTERVAL '1 day'`);
          } else {
            parts.push(`${escapeColumnName(col)} <= '${val.max}'`);
          }
        }
        return parts.length > 0 ? `(${parts.join(' AND ')})` : null;
      } else if (meta.filterType === 'select') {
        if (!val || val === '') return null;
        const escapedVal = val.replace(/'/g, "''");
        return `${escapeColumnName(col)} = '${escapedVal}'`;
      } else {
        const escapedVal = escapeLikePattern(val.replace(/'/g, "''"));
        return `CAST(${escapeColumnName(col)} AS VARCHAR) ILIKE '%${escapedVal}%' ESCAPE '\\'`;
      }
    })
    .filter(Boolean);
}
