#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
if [ "${1:-}" != --reuse-capture-image ]; then
    ./scripts/test-change-capture.sh
fi
docker build --platform linux/arm64 -f spikes/batch_replay/Dockerfile -t pg_onesearch:batch-replay .
docker run --rm --platform linux/arm64 --network none --entrypoint /bin/sh \
    pg_onesearch:batch-replay spikes/batch_replay/run.sh
