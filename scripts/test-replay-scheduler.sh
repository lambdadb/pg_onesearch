#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
if [ "${1:-}" != --reuse-completion-image ]; then
    ./scripts/test-publication-commit.sh
fi
docker build --platform linux/arm64 -f spikes/replay_scheduler/Dockerfile -t pg_onesearch:replay-scheduler .
docker run --rm --platform linux/arm64 --network none --entrypoint /bin/sh \
    pg_onesearch:replay-scheduler spikes/replay_scheduler/run.sh
