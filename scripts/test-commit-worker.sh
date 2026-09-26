#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
docker build --platform linux/arm64 -t pg_onesearch:commit-worker-spike .
docker run --rm --platform linux/arm64 --network none --entrypoint /bin/sh \
    pg_onesearch:commit-worker-spike spikes/commit_worker/run.sh
