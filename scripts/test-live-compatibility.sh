#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
exec python3 -u spikes/live_compatibility/run.py "$@"
