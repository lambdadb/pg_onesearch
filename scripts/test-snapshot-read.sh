#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
if [ "${1:-}" != --reuse-transport-image ]; then
    ./scripts/test-remote-read.sh
fi
docker build --platform linux/arm64 -f spikes/snapshot_read/Dockerfile -t pg_onesearch:snapshot-read .
docker run --rm --platform linux/arm64 --network none --entrypoint /bin/sh \
    pg_onesearch:snapshot-read spikes/snapshot_read/run.sh
