"""Gate dependency-vulnerability scans against a reviewed baseline.

    python scripts/ci/audit_gate.py --pip-json pip-audit.json --npm-json npm-audit.json \
        --baseline .github/security/audit-baseline.json [--summary out.md]
    python scripts/ci/audit_gate.py ... --write-baseline        # (re)write the baseline from the current findings

Inputs are the JSON outputs of ``pip-audit -f json`` and ``npm audit --json``. Every finding is keyed as
``<ecosystem>:<package>==<version>:<advisory id>`` (npm: the installed version is not reported, so
``npm:<package>:<GHSA id>``).

* Exit 1 if a finding is NOT in the baseline: a new advisory, or a dependency change that brought one in.
* Baseline entries that no longer occur are listed as resolved (remove them from the baseline in the same change).
* The baseline is NOT an exemption list: it records known, unfixed findings so that CI can block new ones without
  silently upgrading pinned dependencies. Each entry is listed in SECURITY.md as an open item for the owner.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path


def pip_findings(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for dep in json.loads(path.read_text(encoding="utf-8"))["dependencies"]:
        for v in dep.get("vulns", []):
            fix = ",".join(v.get("fix_versions", [])[:1]) or "no fix"
            out[f"pip:{dep['name'].lower()}=={dep['version']}:{v['id']}"] = f"fixed in {fix}"
    return out


def npm_findings(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for name, vul in json.loads(path.read_text(encoding="utf-8")).get("vulnerabilities", {}).items():
        for via in vul.get("via", []):
            if isinstance(via, dict):          # direct advisory (strings are "via another package" references)
                m = re.search(r"GHSA-[\w-]+", via.get("url", ""))
                key = m.group(0) if m else str(via.get("source"))
                out[f"npm:{via.get('name', name)}:{key}"] = f"{via.get('severity', '?')}: {via.get('title', '')[:90]}"
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pip-json", type=Path)
    ap.add_argument("--npm-json", type=Path)
    ap.add_argument("--baseline", type=Path, required=True)
    ap.add_argument("--summary", type=Path)
    ap.add_argument("--write-baseline", action="store_true")
    a = ap.parse_args()
    found: dict[str, str] = {}
    if a.pip_json:
        found |= pip_findings(a.pip_json)
    if a.npm_json:
        found |= npm_findings(a.npm_json)
    scopes = {s for s, f in (("pip", a.pip_json), ("npm", a.npm_json)) if f}

    if a.write_baseline:
        base = json.loads(a.baseline.read_text(encoding="utf-8")) if a.baseline.exists() else {}
        kept = {k: v for k, v in base.get("accepted", {}).items() if k.split(":", 1)[0] not in scopes}
        base.update(reviewed=date.today().isoformat(), accepted=dict(sorted({**kept, **found}.items())))
        a.baseline.parent.mkdir(parents=True, exist_ok=True)
        a.baseline.write_text(json.dumps(base, indent=2) + "\n", encoding="utf-8")
        print(f"baseline written: {len(base['accepted'])} accepted findings")
        return 0

    accepted = json.loads(a.baseline.read_text(encoding="utf-8")).get("accepted", {})
    new = sorted(k for k in found if k not in accepted)
    known = sorted(k for k in found if k in accepted)
    resolved = sorted(k for k in accepted if k not in found and k.split(":", 1)[0] in scopes)
    lines = [f"### {'FAIL' if new else 'PASS'}: dependency audit ({', '.join(sorted(scopes))})", "",
             f"- **new (blocking): {len(new)}**", f"- known, accepted in the baseline: {len(known)}",
             f"- baseline entries no longer found (resolved): {len(resolved)}", ""]
    if new:
        lines += ["**New findings**", "", *[f"- `{k}` — {found[k]}" for k in new], ""]
    if resolved:
        lines += ["**Resolved (remove from the baseline)**", "", *[f"- `{k}`" for k in resolved], ""]
    pkgs: dict[str, int] = {}
    for k in known:
        pkg = k.rsplit(":", 1)[0]
        pkgs[pkg] = pkgs.get(pkg, 0) + 1
    if pkgs:
        lines += ["<details><summary>Known findings by package</summary>", "",
                  *[f"- `{p}`: {n}" for p, n in sorted(pkgs.items())], "", "</details>", ""]
    text = "\n".join(lines)
    print(text)
    if a.summary:
        with a.summary.open("a", encoding="utf-8") as fh:
            fh.write(text + "\n")
    return 1 if new else 0


if __name__ == "__main__":
    sys.exit(main())
