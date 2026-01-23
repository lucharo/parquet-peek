// DuckDB-wasm for Parquet parsing
import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm/+esm';
import { escapeHtml, escapeSource, escapeLikePattern, escapeColumnName, truncateColumnName, buildFilterClauses } from './viewer-utils.js';

// DOM elements
const status = document.getElementById('status');
const schemaDiv = document.getElementById('schema');
const tableDiv = document.getElementById('table');
const buttons = document.getElementById('buttons');
const moreBtn = document.getElementById('more');
const loadAllBtn = document.getElementById('loadAll');
const downloadBtn = document.getElementById('download');
const dropZone = document.getElementById('drop');
const urlBar = document.getElementById('url-bar');

// URL parameters - viewer.html?url=...
const params = new URLSearchParams(location.search);
const fileUrl = params.get('url');

// State
let db, conn;
let offset = 0;
const CHUNK = 100;        // Rows per page
const MAX_COLS = 100;     // Column cap for wide tables
const TIMEOUT_MS = 30000; // 30s timeout for network requests
let totalRows = 0;
let columns = [];
let currentSource = null;
let rowOffset = 0;        // For row number display

// Sort state
let sortColumn = null;
let sortDirection = null; // 'ASC' or 'DESC'

// Filter state
let filters = {};         // { columnName: filterValue or { min, max } }
let columnMeta = {};      // { columnName: { filterType, values?, type } }

// === Error Detection & Handling ===

function isCorsError(error) {
  const msg = error.message?.toLowerCase() || '';
  return msg.includes('cors') ||
         msg.includes('cross-origin') ||
         msg.includes('network error') ||
         (msg.includes('failed to fetch') && fileUrl?.startsWith('http'));
}

function isNetworkError(error) {
  const msg = error.message?.toLowerCase() || '';
  return msg.includes('failed to fetch') ||
         msg.includes('networkerror') ||
         msg.includes('network request failed') ||
         msg.includes('net::');
}

function isInvalidParquet(error) {
  const msg = error.message?.toLowerCase() || '';
  return msg.includes('parquet') ||
         msg.includes('invalid') ||
         msg.includes('not a valid') ||
         msg.includes('magic number') ||
         msg.includes('corrupted');
}

// NB: `suggestion` is trusted static HTML (contains <br>, ▸).
// Never pass user-controlled data as `suggestion`.
function showError(title, details, suggestion) {
  const html = `
    <div class="error">
      <strong>${escapeHtml(title)}</strong><br>
      ${details ? `<code>${escapeHtml(details)}</code><br>` : ''}
      ${suggestion || ''}
    </div>
  `;
  status.innerHTML = html;
  status.classList.remove('loading');
}

function handleError(error) {
  console.error('Parquet Viewer Error:', error);

  if (isCorsError(error)) {
    showError(
      'CORS Error - Cannot access this file',
      error.message,
      `The server doesn't allow cross-origin requests. Try:<br>
       ▸ Download the file and drag it here<br>
       ▸ Use a URL that supports CORS (e.g., raw GitHub URLs)`
    );
  } else if (isNetworkError(error)) {
    showError(
      'Network Error',
      error.message,
      `Could not fetch the file. Try:<br>
       ▸ Check if the URL is correct<br>
       ▸ Check your internet connection<br>
       ▸ The server might be down - try again later`
    );
  } else if (isInvalidParquet(error)) {
    showError(
      'Invalid Parquet File',
      error.message,
      `The file doesn't appear to be a valid Parquet file. Try:<br>
       ▸ Verify the file is actually in Parquet format<br>
       ▸ The file may be corrupted - try re-downloading`
    );
  } else {
    showError(
      'Error',
      error.message,
      'An unexpected error occurred.'
    );
  }
}

// === DuckDB Initialization ===

async function initDuckDB() {
  status.textContent = 'Initializing DuckDB...';
  status.classList.add('loading');

  const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

  // Create worker from CDN bundle
  const workerUrl = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
  );

  const worker = new Worker(workerUrl);
  const logger = new duckdb.ConsoleLogger();
  db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(workerUrl);

  conn = await db.connect();
  status.classList.remove('loading');
}

// === Parquet Query Functions ===

async function getSchema(source) {
  const result = await conn.query(`DESCRIBE SELECT * FROM read_parquet('${escapeSource(source)}')`);
  return result.toArray().map(row => ({
    name: row.column_name,
    type: row.column_type
  }));
}

async function getRowCount(source) {
  const result = await conn.query(`SELECT COUNT(*) as cnt FROM read_parquet('${escapeSource(source)}')`);
  return Number(result.toArray()[0].cnt);
}

async function getRows(source, cols, limit, off, sortCol = null, sortDir = null, filterMap = {}, colMeta = {}) {
  const colList = cols.map(c => escapeColumnName(c.name)).join(', ');
  let query = `SELECT ${colList} FROM read_parquet('${escapeSource(source)}')`;

  // Add WHERE clause for filters
  const clauses = buildFilterClauses(filterMap, colMeta);
  if (clauses.length > 0) {
    query += ` WHERE ${clauses.join(' AND ')}`;
  }

  // Add ORDER BY clause for sorting
  if (sortCol) {
    query += ` ORDER BY ${escapeColumnName(sortCol)} ${sortDir}`;
  }

  query += ` LIMIT ${limit} OFFSET ${off}`;
  const result = await conn.query(query);
  return result.toArray();
}

async function getFilteredRowCount(source, filterMap = {}, colMeta = {}) {
  let query = `SELECT COUNT(*) as cnt FROM read_parquet('${escapeSource(source)}')`;
  const clauses = buildFilterClauses(filterMap, colMeta);
  if (clauses.length > 0) {
    query += ` WHERE ${clauses.join(' AND ')}`;
  }
  const result = await conn.query(query);
  return Number(result.toArray()[0].cnt);
}


// Detect if column is categorical (low cardinality)
const CATEGORICAL_THRESHOLD = 20;
async function getColumnMeta(source, cols) {
  const meta = {};
  for (const col of cols) {
    const isNumeric = /^(INTEGER|BIGINT|DOUBLE|FLOAT|DECIMAL|NUMERIC|REAL|SMALLINT|TINYINT)/i.test(col.type);
    const isDate = /^(DATE|TIMESTAMP|TIME)/i.test(col.type);

    if (isNumeric) {
      meta[col.name] = { filterType: 'range', type: col.type };
    } else if (!isDate) {
      // Check cardinality for potential categorical
      try {
        const result = await conn.query(
          `SELECT COUNT(DISTINCT ${escapeColumnName(col.name)}) as cnt FROM read_parquet('${escapeSource(source)}')`
        );
        const distinctCount = Number(result.toArray()[0].cnt);
        if (distinctCount <= CATEGORICAL_THRESHOLD && distinctCount > 0) {
          // Get distinct values
          const valsResult = await conn.query(
            `SELECT DISTINCT ${escapeColumnName(col.name)} as val FROM read_parquet('${escapeSource(source)}') ORDER BY val LIMIT ${CATEGORICAL_THRESHOLD}`
          );
          const values = valsResult.toArray().map(r => r.val);
          meta[col.name] = { filterType: 'select', type: col.type, values };
        } else {
          meta[col.name] = { filterType: 'text', type: col.type };
        }
      } catch (e) {
        meta[col.name] = { filterType: 'text', type: col.type };
      }
    } else if (isDate) {
      meta[col.name] = { filterType: 'date', type: col.type };
    } else {
      meta[col.name] = { filterType: 'text', type: col.type };
    }
  }
  return meta;
}

// === Rendering Functions ===

function renderSchema(cols, totalCols) {
  if (cols.length === 0) {
    schemaDiv.innerHTML = '<b>No columns</b> - This file has no columns.';
    return;
  }

  const truncated = totalCols > MAX_COLS ? ` (showing ${MAX_COLS} of ${totalCols})` : '';
  const colCount = cols.length;
  const isOpen = colCount <= 10;

  schemaDiv.innerHTML = `
    <details${isOpen ? ' open' : ''}>
      <summary>${colCount} columns${truncated}</summary>
      <div class="schema-content">(<br>  ${cols.map(c => `${escapeHtml(c.name)} <span class="type">${escapeHtml(c.type)}</span>`).join(',<br>  ')}<br>)</div>
    </details>
  `;
}

function getSortIndicator(colName) {
  if (sortColumn !== colName) return '<span class="sort-indicator">⇅</span>';
  return sortDirection === 'ASC'
    ? '<span class="sort-indicator">▲</span>'
    : '<span class="sort-indicator">▼</span>';
}

function renderRows(rows, cols, append = false) {
  if (cols.length === 0) {
    tableDiv.innerHTML = '<p>No columns to display.</p>';
    return;
  }

  if (!append) {
    // Create table with row number column, sortable headers, and filter row
    let html = '<table><thead><tr>' +
      '<th class="row-num">#</th>' +
      cols.map(c => {
        const isSorted = sortColumn === c.name;
        const sortHint = isSorted
          ? `Sorted ${sortDirection}. Click to toggle.`
          : 'Click to sort (re-queries full dataset)';
        const escapedName = escapeHtml(c.name);
        const escapedType = escapeHtml(c.type);
        return `<th class="sortable${isSorted ? ' sorted' : ''}" data-column="${escapedName}" title="${escapedName} (${escapedType})\n${sortHint}">` +
          `<span class="col-name">${escapeHtml(truncateColumnName(c.name))}</span>${getSortIndicator(c.name)}</th>`;
      }).join('') +
      '</tr>' +
      '<tr class="filter-row">' +
      '<td class="row-num"></td>' +
      cols.map(c => {
        const meta = columnMeta[c.name] || { filterType: 'text' };
        const eName = escapeHtml(c.name);
        if (meta.filterType === 'select' && meta.values) {
          const currentVal = filters[c.name] || '';
          const options = ['<option value="">All</option>']
            .concat(meta.values.map(v => {
              const ev = escapeHtml(v);
              return `<option value="${ev}"${v === currentVal ? ' selected' : ''}>${ev}</option>`;
            }));
          return `<td><select data-filter="${eName}" data-type="select">${options.join('')}</select></td>`;
        } else if (meta.filterType === 'range') {
          const rangeVal = filters[c.name] || { min: '', max: '' };
          return `<td class="range-filter">
            <input type="number" placeholder="Min" data-filter="${eName}" data-range="min" value="${escapeHtml(rangeVal.min || '')}" step="any">
            <input type="number" placeholder="Max" data-filter="${eName}" data-range="max" value="${escapeHtml(rangeVal.max || '')}" step="any">
          </td>`;
        } else if (meta.filterType === 'date') {
          const rangeVal = filters[c.name] || { min: '', max: '' };
          return `<td class="range-filter">
            <input type="date" title="From" data-filter="${eName}" data-range="min" value="${escapeHtml(rangeVal.min || '')}">
            <input type="date" title="To" data-filter="${eName}" data-range="max" value="${escapeHtml(rangeVal.max || '')}">
          </td>`;
        } else {
          return `<td><input type="text" placeholder="Filter..." data-filter="${eName}" data-type="text" value="${escapeHtml(filters[c.name] || '')}"></td>`;
        }
      }).join('') +
      '</tr></thead><tbody id="tbody"></tbody></table>';
    tableDiv.innerHTML = html;
    rowOffset = 0;

    // Setup sort click handlers
    tableDiv.querySelectorAll('th.sortable').forEach(th => {
      th.addEventListener('click', () => handleSort(th.dataset.column));
    });

    // Setup filter input handlers with debounce
    let filterTimeout;
    // Text inputs
    tableDiv.querySelectorAll('.filter-row input[data-type="text"]').forEach(input => {
      input.addEventListener('input', (e) => {
        clearTimeout(filterTimeout);
        filterTimeout = setTimeout(() => handleFilter(e.target.dataset.filter, e.target.value), 300);
      });
    });
    // Range inputs (number and date)
    tableDiv.querySelectorAll('.filter-row input[data-range]').forEach(input => {
      const handler = (e) => {
        clearTimeout(filterTimeout);
        filterTimeout = setTimeout(() => handleRangeFilter(e.target.dataset.filter, e.target.dataset.range, e.target.value), 300);
      };
      input.addEventListener('input', handler);
      input.addEventListener('change', handler);
    });
    // Select dropdowns
    tableDiv.querySelectorAll('.filter-row select').forEach(select => {
      select.addEventListener('change', (e) => handleFilter(e.target.dataset.filter, e.target.value));
    });
  }

  const tbody = document.getElementById('tbody');
  const html = rows.map((row, i) => {
    const rowNum = rowOffset + i + 1;
    return '<tr>' +
      `<td class="row-num">${rowNum.toLocaleString()}</td>` +
      cols.map(c => {
        const val = row[c.name];
        const display = val === null ? '' : String(val);
        const escaped = escapeHtml(display);
        return `<td title="${escapeHtml(`[${c.type}] ${display}`)}" data-value="${escaped}">${escaped}</td>`;
      }).join('') + '</tr>';
  }).join('');

  rowOffset += rows.length;
  tbody.insertAdjacentHTML('beforeend', html);
}

// Sort handler
async function handleSort(colName) {
  // Toggle sort direction
  if (sortColumn === colName) {
    sortDirection = sortDirection === 'ASC' ? 'DESC' : 'ASC';
  } else {
    sortColumn = colName;
    sortDirection = 'ASC';
  }

  // Reset offset and reload data
  offset = 0;
  await reloadData();
}

// Filter handler
async function handleFilter(colName, value) {
  filters[colName] = value;
  offset = 0;
  await reloadData();
}

// Range filter handler
async function handleRangeFilter(colName, rangeType, value) {
  if (!filters[colName] || typeof filters[colName] !== 'object') {
    filters[colName] = { min: '', max: '' };
  }
  filters[colName][rangeType] = value;
  offset = 0;
  await reloadData();
}

// Reload data with current sort/filter state
async function reloadData() {
  status.classList.add('loading');
  status.textContent = 'Loading...';

  try {
    const rows = await getRows(currentSource, columns, CHUNK, 0, sortColumn, sortDirection, filters, columnMeta);
    offset = rows.length;

    // Update filtered row count for pagination
    const hasFilters = Object.values(filters).some(v => {
      if (typeof v === 'object') return v.min !== '' || v.max !== '';
      return v && v.trim && v.trim();
    });
    const filteredCount = hasFilters
      ? await getFilteredRowCount(currentSource, filters, columnMeta)
      : totalRows;

    renderRows(rows, columns, false);

    const filename = currentSource.split('/').pop().split('?')[0];
    const filterNote = hasFilters ? ` (filtered: ${filteredCount.toLocaleString()})` : '';
    status.textContent = `${filename} — ${totalRows.toLocaleString()} rows × ${columns.length} cols${filterNote}`;
    status.classList.remove('loading');

    // Update button with filtered count
    const remaining = filteredCount - offset;
    moreBtn.textContent = `Load next ${CHUNK} (${remaining.toLocaleString()} remaining)`;
    moreBtn.disabled = remaining <= 0;
  } catch (e) {
    handleError(e);
  }
}

// Click-to-copy functionality
function setupClickToCopy() {
  tableDiv.addEventListener('click', async (e) => {
    const td = e.target.closest('td:not(.row-num)');
    if (!td) return;

    const value = td.dataset.value || td.textContent;
    try {
      await navigator.clipboard.writeText(value);
      td.classList.add('copied');
      setTimeout(() => td.classList.remove('copied'), 300);
    } catch (err) {
      console.warn('Copy failed:', err);
    }
  });
}

function updateButtons() {
  buttons.classList.remove('hidden');
  const remaining = totalRows - offset;
  moreBtn.textContent = `Load next ${CHUNK} (${remaining.toLocaleString()} remaining)`;
  moreBtn.disabled = remaining <= 0;
}

// === File Size Fetching ===

async function getFileSize(url) {
  try {
    const response = await fetch(url, { method: 'HEAD' });
    const size = response.headers.get('content-length');
    if (size) {
      const bytes = parseInt(size, 10);
      if (bytes < 1024) return `${bytes} B`;
      if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
      return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
    }
  } catch (e) {
    // Ignore - file size is optional
  }
  return null;
}

// === Main Load Functions ===

// URL bar click-to-copy
function setupUrlBar() {
  urlBar.addEventListener('click', async () => {
    try {
      await navigator.clipboard.writeText(urlBar.textContent);
      urlBar.classList.add('copied');
      setTimeout(() => urlBar.classList.remove('copied'), 300);
    } catch (err) {
      console.warn('URL copy failed:', err);
    }
  });
}

async function loadParquet(source, isUrl = true) {
  currentSource = source;

  // Show URL bar for URL sources
  if (isUrl) {
    urlBar.textContent = source;
    urlBar.title = 'Click to copy URL';
    urlBar.classList.remove('hidden');
  }

  // Reset sort/filter state for new file
  sortColumn = null;
  sortDirection = null;
  filters = {};

  // Show loading state
  status.textContent = 'Reading schema...';
  status.classList.add('loading');

  // Get schema
  const allCols = await getSchema(source);
  columns = allCols.slice(0, MAX_COLS);
  renderSchema(columns, allCols.length);

  // Handle empty schema (0 columns)
  if (allCols.length === 0) {
    status.textContent = 'This file has no columns.';
    status.classList.remove('loading');
    return;
  }

  status.textContent = 'Counting rows...';
  totalRows = await getRowCount(source);

  // Handle empty file (0 rows)
  if (totalRows === 0) {
    const filename = isUrl ? source.split('/').pop().split('?')[0] : source;
    status.textContent = `${filename} — Empty file (0 rows × ${allCols.length} cols)`;
    status.classList.remove('loading');
    renderRows([], columns);
    return;
  }

  // Get column metadata for smart filters
  status.textContent = 'Analyzing columns...';
  columnMeta = await getColumnMeta(source, columns);

  // Build status with optional file size
  const filename = isUrl ? source.split('/').pop().split('?')[0] : source;
  let statusText = `${filename} — ${totalRows.toLocaleString()} rows × ${allCols.length} cols`;

  let fileSizeStr = null;
  if (isUrl) {
    fileSizeStr = await getFileSize(source);
    if (fileSizeStr) statusText += ` — ${fileSizeStr}`;
  }

  // Update Load all button tooltip
  const loadAllTooltip = `Load all ${totalRows.toLocaleString()} rows.\n` +
    (fileSizeStr ? `File size: ${fileSizeStr}\n` : '') +
    `This may be slow for large datasets.`;
  loadAllBtn.title = loadAllTooltip;

  status.textContent = statusText + ' — Loading first rows...';

  // Load first chunk
  const rows = await getRows(source, columns, CHUNK, 0, sortColumn, sortDirection, filters, columnMeta);
  offset = rows.length;

  renderRows(rows, columns);
  status.textContent = statusText;
  status.classList.remove('loading');
  updateButtons();

  // Download button for URL sources
  downloadBtn.onclick = () => window.open(source, '_blank');
  downloadBtn.style.display = isUrl ? 'inline-block' : 'none';
}

async function loadMore() {
  moreBtn.disabled = true;
  moreBtn.textContent = 'Loading...';

  try {
    const rows = await getRows(currentSource, columns, CHUNK, offset, sortColumn, sortDirection, filters, columnMeta);
    offset += rows.length;

    renderRows(rows, columns, true);

    // Update button with correct count based on filters
    const hasFilters = Object.values(filters).some(v => {
      if (typeof v === 'object') return v.min !== '' || v.max !== '';
      return v && v.trim && v.trim();
    });
    const filteredCount = hasFilters
      ? await getFilteredRowCount(currentSource, filters, columnMeta)
      : totalRows;
    const remaining = filteredCount - offset;
    moreBtn.textContent = `Load next ${CHUNK} (${remaining.toLocaleString()} remaining)`;
    moreBtn.disabled = remaining <= 0;
  } catch (e) {
    handleError(e);
  }
}

async function loadAll() {
  // Get current filtered count
  const hasFilters = Object.values(filters).some(v => {
    if (typeof v === 'object') return v.min !== '' || v.max !== '';
    return v && v.trim && v.trim();
  });
  const filteredCount = hasFilters
    ? await getFilteredRowCount(currentSource, filters, columnMeta)
    : totalRows;
  const remaining = filteredCount - offset;

  if (remaining <= 0) return;

  // Warning for large loads
  const WARN_THRESHOLD = 10000;
  if (remaining > WARN_THRESHOLD) {
    const confirmed = confirm(
      `Load ${remaining.toLocaleString()} rows?\n\n` +
      `This may be slow and use significant memory.\n` +
      `Consider using filters to reduce the dataset.`
    );
    if (!confirmed) return;
  }

  loadAllBtn.disabled = true;
  moreBtn.disabled = true;
  loadAllBtn.textContent = '⚠ Loading all...';

  try {
    // Load in batches to show progress
    const BATCH = 1000;
    while (offset < filteredCount) {
      const rows = await getRows(currentSource, columns, BATCH, offset, sortColumn, sortDirection, filters, columnMeta);
      if (rows.length === 0) break;
      offset += rows.length;
      renderRows(rows, columns, true);
      loadAllBtn.textContent = `Loading... ${offset.toLocaleString()}/${filteredCount.toLocaleString()}`;
    }

    loadAllBtn.textContent = 'All loaded';
    moreBtn.textContent = 'All rows loaded';
    moreBtn.disabled = true;
    loadAllBtn.disabled = true;
  } catch (e) {
    handleError(e);
  }
}

// === Local File Handling ===

async function handleLocalFile(file) {
  status.textContent = `Registering ${file.name}...`;
  status.classList.add('loading');

  const buffer = await file.arrayBuffer();
  await db.registerFileBuffer(file.name, new Uint8Array(buffer));
  await loadParquet(file.name, false);
}

function setupDragDrop() {
  dropZone.classList.remove('hidden');

  dropZone.ondragover = e => { e.preventDefault(); dropZone.classList.add('drag'); };
  dropZone.ondragleave = () => dropZone.classList.remove('drag');
  dropZone.ondrop = async e => {
    e.preventDefault();
    dropZone.classList.remove('drag');
    const file = e.dataTransfer.files[0];
    if (file?.name.endsWith('.parquet')) {
      try {
        await handleLocalFile(file);
      } catch (err) {
        handleError(err);
      }
    } else {
      showError('Invalid file', '', 'Please drop a .parquet file.');
    }
  };
}

// === Keyboard Shortcuts ===

function setupKeyboardShortcuts() {
  document.addEventListener('keydown', (e) => {
    // Space to load more (when not in input/select)
    if (e.code === 'Space' && !e.target.matches('input, textarea, select')) {
      e.preventDefault();
      if (!moreBtn.disabled && !buttons.classList.contains('hidden')) {
        loadMore();
      }
    }

    // Escape: blur input if focused, otherwise close
    if (e.code === 'Escape') {
      if (e.target.matches('input, textarea, select')) {
        e.target.blur();
      } else {
        window.close();
      }
    }
  });
}

// === Main Execution ===

try {
  await initDuckDB();
  setupClickToCopy();
  setupKeyboardShortcuts();
  setupUrlBar();

  if (fileUrl) {
    status.textContent = `Loading ${fileUrl}...`;
    status.classList.add('loading');

    // Add timeout wrapper for URL loading
    const timeoutPromise = new Promise((_, reject) => {
      setTimeout(() => reject(new Error('Request timed out - the server took too long to respond')), TIMEOUT_MS);
    });

    try {
      await Promise.race([loadParquet(fileUrl, true), timeoutPromise]);
    } catch (e) {
      handleError(e);
    }
  } else {
    status.textContent = 'Drop a .parquet file or open a .parquet URL';
    setupDragDrop();
  }

  moreBtn.onclick = loadMore;
  loadAllBtn.onclick = loadAll;

} catch (e) {
  handleError(e);
}
