"""Turn JUnit XML (pytest / Vitest / Playwright) into a short Markdown summary.

    python scripts/ci/junit_summary.py "Backend pytest" reports/pytest.xml >> "$GITHUB_STEP_SUMMARY"

Lists totals plus every failed/errored test and every skip reason. Exit code is always 0: the test step itself decides
success or failure; this only reports.
"""
from __future__ import annotations

import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def main() -> int:
    title, path = sys.argv[1], Path(sys.argv[2])
    if not path.exists():
        print(f"### {title}\n\nNo result file ({path.name}) - the test step did not run to completion.\n")
        return 0
    root = ET.parse(path).getroot()
    cases = list(root.iter("testcase"))
    failed, skipped = [], []
    for c in cases:
        name = f"{c.get('classname', '')}::{c.get('name', '')}".strip(":")
        for tag in ("failure", "error"):
            el = c.find(tag)
            if el is not None:
                failed.append((name, (el.get("message") or "").splitlines()[0][:200] if el.get("message") else tag))
        sk = c.find("skipped")
        if sk is not None:
            skipped.append((name, (sk.get("message") or "")[:200]))
    passed = len(cases) - len(failed) - len(skipped)
    icon = "FAIL" if failed else "PASS"
    print(f"### {icon}: {title}\n\n| passed | failed | skipped | total |\n|---|---|---|---|\n"
          f"| {passed} | {len(failed)} | {len(skipped)} | {len(cases)} |\n")
    if failed:
        print("**Failed**\n")
        print("\n".join(f"- `{n}` — {m}" for n, m in failed) + "\n")
    if skipped:
        print("<details><summary>Skipped</summary>\n")
        print("\n".join(f"- `{n}` — {m}" for n, m in skipped) + "\n\n</details>\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
