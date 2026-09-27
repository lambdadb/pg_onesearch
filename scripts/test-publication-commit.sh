#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
if [ "${1:-}" != --reuse-executor-image ]; then
    ./scripts/test-snapshot-executor.sh
fi
docker build --platform linux/arm64 -f spikes/publication_commit/Dockerfile -t pg_onesearch:publication-commit .
docker run --rm --platform linux/arm64 --network none --entrypoint /bin/sh \
    pg_onesearch:publication-commit spikes/publication_commit/run.sh
