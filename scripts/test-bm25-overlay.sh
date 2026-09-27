#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
docker build --platform linux/arm64 -f spikes/bm25_overlay/Dockerfile -t pg_onesearch:bm25-overlay .
python3 spikes/bm25_overlay/run.py "$@"
