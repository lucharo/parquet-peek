import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { escapeHtml, escapeSource, escapeLikePattern } from '../parquet-ext/viewer-utils.js';

describe('escapeHtml – XSS prevention', () => {
  it('escapes < and > in cell values', () => {
    assert.equal(escapeHtml('<script>alert(1)</script>'), '&lt;script&gt;alert(1)&lt;/script&gt;');
  });

  it('escapes & in cell values', () => {
    assert.equal(escapeHtml('a & b'), 'a &amp; b');
  });

  it('escapes double quotes in attributes', () => {
    assert.equal(escapeHtml('"onmouseover="alert(1)"'), '&quot;onmouseover=&quot;alert(1)&quot;');
  });

  it('escapes single quotes', () => {
    assert.equal(escapeHtml("it's"), "it&#x27;s");
  });

  it('handles null gracefully', () => {
    assert.equal(escapeHtml(null), '');
  });

  it('handles undefined gracefully', () => {
    assert.equal(escapeHtml(undefined), '');
  });

  it('converts numbers to string', () => {
    assert.equal(escapeHtml(42), '42');
  });

  it('handles empty string', () => {
    assert.equal(escapeHtml(''), '');
  });

  it('passes through safe text unchanged', () => {
    assert.equal(escapeHtml('hello world'), 'hello world');
  });
});

describe('escapeSource – SQL injection in file paths', () => {
  it('escapes single quotes in source path', () => {
    assert.equal(escapeSource("file'; DROP TABLE--"), "file''; DROP TABLE--");
  });

  it('handles normal URLs unchanged', () => {
    assert.equal(escapeSource('https://example.com/data.parquet'), 'https://example.com/data.parquet');
  });

  it('escapes multiple single quotes', () => {
    assert.equal(escapeSource("a'b'c"), "a''b''c");
  });

  it('handles empty string', () => {
    assert.equal(escapeSource(''), '');
  });
});

describe('escapeLikePattern – ILIKE wildcard injection', () => {
  it('escapes percent wildcard', () => {
    assert.equal(escapeLikePattern('100%'), '100\\%');
  });

  it('escapes underscore wildcard', () => {
    assert.equal(escapeLikePattern('a_b'), 'a\\_b');
  });

  it('escapes backslashes', () => {
    assert.equal(escapeLikePattern('path\\file'), 'path\\\\file');
  });

  it('escapes all special chars together', () => {
    assert.equal(escapeLikePattern('50%_off\\sale'), '50\\%\\_off\\\\sale');
  });

  it('handles normal text unchanged', () => {
    assert.equal(escapeLikePattern('hello'), 'hello');
  });

  it('handles empty string', () => {
    assert.equal(escapeLikePattern(''), '');
  });
});
