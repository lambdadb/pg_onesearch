#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
docker build --platform linux/arm64 -t pg_onesearch:0.1.0-dev .
docker run --rm --platform linux/arm64 --network none --user postgres \
    --entrypoint /bin/sh pg_onesearch:0.1.0-dev scripts/test-container.sh
