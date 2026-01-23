set dotenv-load

# Run tests
test:
    node --test tests/*.test.js

# Bundle extension for Chrome Web Store upload
bundle-for-chrome:
    zip -r parquet-peek.zip parquet-ext -x "*.DS_Store"
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
