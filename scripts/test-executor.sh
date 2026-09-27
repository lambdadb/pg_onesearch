#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
# Build the shared transport and its pinned dependencies from this checkout.
if [ "${1:-}" != --reuse-transport-image ]; then
  ./scripts/test-remote-read.sh
fi
docker build --platform linux/arm64 -f spikes/executor/Dockerfile -t pg_onesearch:executor .
docker run --rm --platform linux/arm64 --network none --entrypoint /bin/sh \
  pg_onesearch:executor spikes/executor/run.sh
