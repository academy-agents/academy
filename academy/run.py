from __future__ import annotations

import argparse
import pickle
import sys
from collections.abc import Sequence
from typing import Any

from academy.manager import _run_agent_on_worker
from academy.manager import _RunSpec


def _main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Use Academy debug mode',
    )
    parser.add_argument('--spec', default=None, help='Path to spec file')

    argv = sys.argv[1:] if argv is None else argv
    args = parser.parse_args(argv)

    spec: _RunSpec[Any, Any]
    if args.spec is None:
        spec_bytes = sys.stdin.buffer.read()
        spec = pickle.loads(spec_bytes)
    else:
        with open(args.spec, 'rb') as fp:
            spec = pickle.load(fp)

    _run_agent_on_worker(spec, args.debug)

    return 0


if __name__ == '__main__':
    raise SystemExit(_main())
