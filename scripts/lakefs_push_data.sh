#!/bin/bash

# Load environment variables safely
if [ -f .env ]; then
    set -o allexport
    source .env
    set +o allexport
else
    echo ".env file not found!"
    exit 1
fi

# Debug: Print environment variables (without exposing secrets)
echo "Using LakeFS endpoint: $LAKEFS_ENDPOINT"
echo "Access key length: ${#LAKEFS_ADMIN_ACCESS_KEY}"
echo "Secret key length: ${#LAKEFS_ADMIN_SECRET_KEY}"

# Create lakectl config file
docker exec lakefs /bin/sh -c "cat > /root/.lakectl.yaml << EOF
credentials:
  access_key_id: $LAKEFS_ADMIN_ACCESS_KEY
  secret_access_key: $LAKEFS_ADMIN_SECRET_KEY
server:
  endpoint_url: http://localhost:8000
EOF"

REPO="spotify-repo"
SRC_BRANCH="splited-data"
DST_BRANCH="development"
FILE_PATH="year=2001/data.csv"

echo "Creating temporary directory..."
docker exec lakefs mkdir -p /tmp/lakefs_transfer

echo "Downloading from source branch..."
docker exec lakefs lakectl fs download \
    "lakefs://$REPO/$SRC_BRANCH/$FILE_PATH" \
    /tmp/lakefs_transfer/data.csv

echo "Uploading to destination branch..."
docker exec lakefs lakectl fs upload \
    --source /tmp/lakefs_transfer/data.csv \
    "lakefs://$REPO/$DST_BRANCH/$FILE_PATH"

echo "Cleaning up..."
docker exec lakefs rm -rf /tmp/lakefs_transfer
