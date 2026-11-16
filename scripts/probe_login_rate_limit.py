"""Probe the login rate limit through nginx, with and without a client-supplied X-Forwarded-For (Phase 12).

    python scripts/probe_login_rate_limit.py --base http://127.0.0.1:18080 --attempts 15

Uses a deliberately WRONG password for a non-existent user, so the only possible answers are 401 (counted against the
limit) or 429 (limit reached); no session is ever created. Waits for the start of a new rate-limit window first.
Run only against the isolated stack: each attempt writes a LOGIN_FAILED audit row.
"""
from __future__ import annotations

import argparse
import secrets
import time
from collections import Counter

import httpx


def run(base: str, attempts: int, spoof: bool) -> Counter:
    time.sleep(61 - time.time() % 60)                       # fresh fixed window
    codes: Counter = Counter()
    for i in range(attempts):
        h = {"X-Forwarded-For": f"198.51.100.{i + 1}"} if spoof else {}
        r = httpx.post(f"{base}/api/v1/auth/login", headers=h, timeout=15,
                       json={"username": f"probe-{secrets.token_hex(3)}", "password": "wrong-Password-1"})
        codes[r.status_code] += 1
    return codes


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True)
    ap.add_argument("--attempts", type=int, default=15)
    a = ap.parse_args()
    if ":8080" in a.base:
        print("refusing to probe the live dashboard port; use the isolated stack")
        return 2
    plain = run(a.base, a.attempts, spoof=False)
    spoofed = run(a.base, a.attempts, spoof=True)
    print(f"no X-Forwarded-For : {dict(plain)}")
    print(f"spoofed per attempt: {dict(spoofed)}")
    bypass = spoofed.get(429, 0) == 0 and plain.get(429, 0) > 0
    print("RESULT:", "BYPASS - a client-chosen X-Forwarded-For gets a fresh login bucket" if bypass
          else "limit enforced for spoofed headers too")
    return 1 if bypass else 0


if __name__ == "__main__":
    raise SystemExit(main())
