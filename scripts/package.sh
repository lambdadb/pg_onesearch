#!/bin/sh
# Internal evaluation bundle, not a release or a system package manager package.
set -eu
cd "$(dirname "$0")/.."
mkdir -p artifacts
docker build --platform linux/arm64 -t pg_onesearch:0.1.0-dev .
docker run --rm --platform linux/arm64 --network none --entrypoint sh \
    pg_onesearch:0.1.0-dev -c \
    'make install DESTDIR=/tmp/package >/dev/null && tar -czf - -C /tmp/package .' \
    > artifacts/pg_onesearch-0.1.0-dev-pg18.6-debian12-arm64.tar.gz
(cd artifacts && shasum -a 256 pg_onesearch-0.1.0-dev-pg18.6-debian12-arm64.tar.gz > SHA256SUMS)
