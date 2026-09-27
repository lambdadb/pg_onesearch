#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
if [ "${1:-}" != --reuse-client-image ]; then
    ./scripts/test-client-contract.sh
fi
docker build --platform linux/arm64 -f spikes/index_lifecycle/Dockerfile -t pg_onesearch:index-lifecycle .
docker run --rm --platform linux/arm64 --network none --entrypoint /bin/sh \
    pg_onesearch:index-lifecycle spikes/index_lifecycle/run.sh
