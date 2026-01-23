import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { escapeColumnName, truncateColumnName, buildFilterClauses } from '../parquet-ext/viewer-utils.js';

describe('escapeColumnName', () => {
  it('wraps column name in double quotes', () => {
    assert.equal(escapeColumnName('name'), '"name"');
  });

  it('escapes embedded double quotes', () => {
    assert.equal(escapeColumnName('col"umn'), '"col""umn"');
  });

  it('handles spaces in names', () => {
    assert.equal(escapeColumnName('my column'), '"my column"');
  });

  it('handles special chars', () => {
    assert.equal(escapeColumnName("col'umn"), '"col\'umn"');
  });
});

describe('truncateColumnName', () => {
  it('returns short names unchanged', () => {
    assert.equal(truncateColumnName('short'), 'short');
  });

  it('truncates long names with ellipsis', () => {
    const name = 'a'.repeat(40);
    const result = truncateColumnName(name, 30);
    assert.equal(result.length, 30);
    assert.equal(result.endsWith('\u2026'), true);
  });

  it('handles exact length', () => {
    const name = 'a'.repeat(30);
    assert.equal(truncateColumnName(name, 30), name);
  });

  it('uses default maxLen of 30', () => {
    const name = 'a'.repeat(31);
    const result = truncateColumnName(name);
    assert.equal(result.length, 30);
  });
});

describe('buildFilterClauses', () => {
  it('builds text ILIKE clause with escaped wildcards', () => {
    const clauses = buildFilterClauses(
      { name: 'john' },
      { name: { filterType: 'text' } }
    );
    assert.equal(clauses.length, 1);
    assert.match(clauses[0], /ILIKE/);
    assert.match(clauses[0], /john/);
  });

  it('escapes single quotes in text filter values', () => {
    const clauses = buildFilterClauses(
      { name: "O'Brien" },
      { name: { filterType: 'text' } }
    );
    assert.equal(clauses.length, 1);
    assert.match(clauses[0], /O''Brien/);
  });

  it('builds range clause for numeric filters', () => {
    const clauses = buildFilterClauses(
      { age: { min: '18', max: '65' } },
      { age: { filterType: 'range' } }
    );
    assert.equal(clauses.length, 1);
    assert.match(clauses[0], />= 18/);
    assert.match(clauses[0], /<= 65/);
  });

  it('builds date clause with exclusive upper bound for TIMESTAMP', () => {
    const clauses = buildFilterClauses(
      { created: { min: '2024-01-01', max: '2024-01-31' } },
      { created: { filterType: 'date', type: 'TIMESTAMP' } }
    );
    assert.equal(clauses.length, 1);
    assert.match(clauses[0], />=\s*'2024-01-01'/);
    // TIMESTAMP max should use < date + INTERVAL, not <= date
    assert.match(clauses[0], /< '2024-01-31'::DATE \+ INTERVAL '1 day'/);
    assert.doesNotMatch(clauses[0], /<= '2024-01-31'/);
  });

  it('builds date clause with inclusive upper bound for DATE type', () => {
    const clauses = buildFilterClauses(
      { created: { min: '2024-01-01', max: '2024-01-31' } },
      { created: { filterType: 'date', type: 'DATE' } }
    );
    assert.equal(clauses.length, 1);
    assert.match(clauses[0], /<= '2024-01-31'/);
  });

  it('builds select clause with escaped value', () => {
    const clauses = buildFilterClauses(
      { status: "it's done" },
      { status: { filterType: 'select' } }
    );
    assert.equal(clauses.length, 1);
    assert.match(clauses[0], /it''s done/);
  });

  it('skips empty filter values', () => {
    const clauses = buildFilterClauses(
      { name: '', age: { min: '', max: '' } },
      { name: { filterType: 'text' }, age: { filterType: 'range' } }
    );
    assert.equal(clauses.length, 0);
  });

  it('escapes ILIKE wildcards in text filter', () => {
    const clauses = buildFilterClauses(
      { name: '100%' },
      { name: { filterType: 'text' } }
    );
    assert.equal(clauses.length, 1);
    // Should escape % so it's treated literally
    assert.match(clauses[0], /100\\%/);
    assert.match(clauses[0], /ESCAPE/);
  });
});
