set dotenv-load

# Run tests
test:
    node --test tests/*.test.js

# Download and bundle DuckDB-wasm for the extension
build:
    #!/usr/bin/env bash
    set -euo pipefail
    ROOT="$(pwd)"
    mkdir -p "$ROOT/parquet-ext/lib"
    echo "Downloading @duckdb/duckdb-wasm..."
    TARBALL=$(cd /tmp && npm pack @duckdb/duckdb-wasm --quiet 2>/dev/null)
    tar xzf "/tmp/$TARBALL" -C /tmp
    cp /tmp/package/dist/duckdb-mvp.wasm "$ROOT/parquet-ext/lib/"
    cp /tmp/package/dist/duckdb-browser-mvp.worker.js "$ROOT/parquet-ext/lib/"
    echo "Bundling duckdb-browser.mjs with dependencies..."
    cd /tmp/package && npm install --quiet 2>/dev/null
    npx esbuild /tmp/package/dist/duckdb-browser.mjs \
        --bundle --format=esm --platform=browser \
        --outfile="$ROOT/parquet-ext/lib/duckdb-bundle.mjs"
    rm -rf /tmp/package "/tmp/$TARBALL"
    echo "Done. parquet-ext/lib/ ready:"
    ls -lh "$ROOT/parquet-ext/lib/"

# Bundle extension for Chrome Web Store upload
bundle-for-chrome: build
    zip -r parquet-peek.zip parquet-ext -x "*.DS_Store" -x "*/_metadata/*"
    @echo "Created parquet-peek.zip"
    @ls -lh parquet-peek.zip

# Upload to Chrome Web Store (fill .env with your credentials first)
publish: bundle-for-chrome
    npx chrome-webstore-upload-cli upload \
        --source parquet-peek.zip \
        --extension-id $CWS_EXTENSION_ID \
        --client-id $CWS_CLIENT_ID \
        --client-secret $CWS_CLIENT_SECRET \
        --refresh-token $CWS_REFRESH_TOKEN \
        --auto-publish
