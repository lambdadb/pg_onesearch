#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
if [ "${1:-}" != --reuse-lifecycle-image ]; then
    ./scripts/test-index-lifecycle.sh
fi
docker build --platform linux/arm64 -f spikes/change_capture/Dockerfile -t pg_onesearch:change-capture .
docker run --rm --platform linux/arm64 --network none --entrypoint /bin/sh \
    pg_onesearch:change-capture spikes/change_capture/run.sh
