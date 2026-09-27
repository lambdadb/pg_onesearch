#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
docker build --platform linux/arm64 -t pg_onesearch:client-contract-base .
docker build --platform linux/arm64 -f spikes/client_contract/Dockerfile -t pg_onesearch:client-contract .
docker build --platform linux/arm64 -f spikes/remote_read/Dockerfile -t pg_onesearch:remote-read .
docker run --rm --platform linux/arm64 --network none --entrypoint /bin/sh \
    pg_onesearch:remote-read spikes/remote_read/run.sh
