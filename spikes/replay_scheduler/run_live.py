#!/usr/bin/env python3
"""Opt-in automatic replay, commit wait and SQL search against owned live fixtures."""
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'snapshot_read'))
from run_live import main, ProbeError
if __name__ == '__main__':
    try:
        sys.exit(main(scheduler=True))
    except ProbeError as exc:
        print('FAIL: ' + str(exc), file=sys.stderr)
        sys.exit(1)
