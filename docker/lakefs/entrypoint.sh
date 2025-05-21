#!/bin/bash
set -e

# Check if setup has already been completed
MARKER_FILE="/var/lakefs_setup/setup_done"

if [ -f "$MARKER_FILE" ]; then
    echo "LakeFS setup already completed. Skipping."
    exit 0
fi

# # Wait for LakeFS to be ready
# echo "Waiting for LakeFS to be ready..."
# for i in {1..30}; do
#     if curl -s -f http://lakefs:8000/_health > /dev/null; then
#         echo "LakeFS is ready!"
#         break
#     fi
#     echo "LakeFS not ready yet, waiting... ($i/30)"
#     sleep 5
#     if [ $i -eq 30 ]; then
#         echo "Timeout waiting for LakeFS"
#         exit 1
#     fi
# done

# Make directory for marker file
mkdir -p "$(dirname "$MARKER_FILE")"

# Run the setup script
echo "Running LakeFS setup script..."
python /app/setup_lakefs.py

# If setup was successful, create marker file
if [ $? -eq 0 ]; then
    echo "Setup completed successfully. Creating marker file."
    touch "$MARKER_FILE"
    echo "$(date) - Setup completed" > "$MARKER_FILE"
else
    echo "Setup failed. Will not create marker file."
    exit 1
fi
