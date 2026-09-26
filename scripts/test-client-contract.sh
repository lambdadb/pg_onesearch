#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
docker build --platform linux/arm64 -t pg_onesearch:client-contract-base .
docker build --platform linux/arm64 -f spikes/client_contract/Dockerfile \
    -t pg_onesearch:client-contract .
docker run --rm --platform linux/arm64 --network none --entrypoint /bin/sh \
    pg_onesearch:client-contract spikes/commit_worker/run.sh \
    /opt/client-venv/bin/python spikes/client_contract/test.py
