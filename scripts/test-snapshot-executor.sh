#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
if [ "${1:-}" != --reuse-snapshot-image ]; then
    ./scripts/test-snapshot-read.sh
fi
docker build --platform linux/arm64 -f spikes/snapshot_executor/Dockerfile -t pg_onesearch:snapshot-executor .
docker run --rm --platform linux/arm64 --network none --entrypoint /bin/sh \
    pg_onesearch:snapshot-executor spikes/snapshot_executor/run.sh
