#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
if [ "${1:-}" != --reuse-executor-image ]; then
    ./scripts/test-snapshot-executor.sh
fi
docker run --rm --platform linux/arm64 --network none --entrypoint /bin/sh \
    pg_onesearch:snapshot-executor spikes/snapshot_executor/run.sh retention
