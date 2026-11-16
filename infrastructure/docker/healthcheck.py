"""Container health checks (stdlib only, no curl needed).

  python healthcheck.py http <url>                       -> healthy if HTTP 200
  python healthcheck.py file <path> <max_age_seconds>     -> healthy if the heartbeat file is fresh
  python healthcheck.py spark <path> <max_age_seconds>    -> fresh Spark progress file AND every
                                                             streaming query active without exception
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.request


def main(argv: list[str]) -> int:
    kind = argv[1]
    if kind == "http":
        try:
            with urllib.request.urlopen(argv[2], timeout=4) as r:   # noqa: S310 - fixed internal URL
                return 0 if r.status == 200 else 1
        except Exception as exc:  # noqa: BLE001
            print(f"unhealthy: {exc}")
            return 1
    path, max_age = argv[2], float(argv[3])
    try:
        age = time.time() - os.path.getmtime(path)
    except OSError:
        print("unhealthy: no heartbeat file yet")
        return 1
    if age > max_age:
        print(f"unhealthy: heartbeat {age:.0f}s old")
        return 1
    if kind == "spark":
        with open(path, encoding="utf-8") as fh:
            snap = json.load(fh)
        bad = [q["name"] for q in snap.get("queries", []) if not q.get("active") or q.get("exception")]
        if not snap.get("queries") or bad:
            print(f"unhealthy: inactive/failed queries {bad}")
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
