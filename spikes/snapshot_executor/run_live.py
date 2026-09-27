#!/usr/bin/env python3
"""Opt-in live Custom Scan test, sharing the verified snapshot harness cleanup gate."""
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'snapshot_read'))
from run_live import main, ProbeError
if __name__ == '__main__':
    try:
        sys.exit(main(executor=True))
    except ProbeError as exc:
        print('FAIL: ' + str(exc), file=sys.stderr)
        sys.exit(1)
