# Privacy Policy — Parquet Peek

**Last updated:** 2025-01-23

## Data Collection

Parquet Peek does **not** collect, transmit, or store any user data. All file processing happens locally in your browser using bundled WebAssembly (DuckDB-wasm).

## Permissions

The extension requests the following permissions solely to intercept and display `.parquet` files:

- **declarativeNetRequest** — Removes `Content-Disposition` headers from `.parquet` responses to enable in-browser viewing.
- **downloads** — Cancels `.parquet` file downloads locally so they can be redirected to the built-in viewer.
- **Host permissions (`<all_urls>`)** — Parquet files can be hosted on any domain. The extension only pattern-matches `.parquet` URLs; it does not read page content or DOM.
- **webNavigation** — Detects navigation to `.parquet` file URLs to redirect them to the viewer.

## Remote Code

No remote code is used. All code, including DuckDB-wasm, is bundled locally within the extension package.

## Third-Party Services

Parquet Peek does not communicate with any external servers or third-party services.

## Contact

If you have questions about this policy, open an issue at https://github.com/lucharo/parquet-peek/issues.
