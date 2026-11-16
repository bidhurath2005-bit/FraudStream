"""Build the distributable source archive FraudStream.zip from an explicit, checked file list (Phase 15).

    python scripts/package_release.py --out ../FraudStream-release            # staging dir + ZIP + .sha256
    python scripts/package_release.py --out ../FraudStream-release --list-only

What goes in:
  * every file Git would publish (``.gitignore`` rules applied), listed with ``git ls-files --others
    --exclude-standard`` against a throw-away bare Git directory, so no repository is created in the project and the
    project's own Git state (if any) is not touched; and
  * EXTRA_INCLUDES: files that are deliberately gitignored but required to run the documented application without
    regenerating them (the synthetic dataset; ``scripts/ci/prepare_dataset.py`` can still recreate and verify it).

What never goes in (checked again after listing, build fails on any hit): env files except ``.env.example``,
credentials, databases and dumps, backups, keys, Argon2 hashes, PEM private keys, PostgreSQL dump headers
(the rules of ``scripts/ci/check_repo_hygiene.py``), plus derived runtime output and caches.

The archive has one top-level ``FraudStream/`` folder, sorted entries, fixed timestamps (so the same input files
give a byte-identical ZIP), regular files only, and ``FraudStream/RELEASE_MANIFEST.sha256`` with the SHA-256 of
every other file. ``<out>/FraudStream.zip.sha256`` holds the archive checksum (``sha256sum -c`` format).
The staging copy is ``<out>/staging/FraudStream`` (must not exist yet, or ``--force`` to replace it).
"""
from __future__ import annotations

import argparse
import hashlib
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path, PurePosixPath

sys.path.insert(0, str(Path(__file__).resolve().parent / "ci"))
from check_repo_hygiene import CONTENT, FORBIDDEN_NAME  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
TOP = "FraudStream"
MANIFEST = "RELEASE_MANIFEST.sha256"
EXTRA_INCLUDES = ("data/synthetic/synthetic-fraudstream-v1.0.jsonl.gz",
                  "data/synthetic/synthetic-fraudstream-v1.0.meta.json")
NEVER = ("ml/artifacts/runtime_config.json",)          # derived at start-up; also gitignored
EXECUTABLE_SUFFIXES = (".sh",)
ZIP_TIME = (2026, 11, 16, 0, 0, 0)


def gitignore_respecting_files() -> list[str]:
    with tempfile.TemporaryDirectory(prefix="fs-pkg-") as tmp:
        gd = Path(tmp) / "probe.git"
        subprocess.run(["git", "init", "-q", "--bare", str(gd)], check=True)
        r = subprocess.run(["git", f"--git-dir={gd}", f"--work-tree={ROOT}", "ls-files", "-z", "--others",
                            "--exclude-standard"], cwd=ROOT, capture_output=True, check=True)
    return sorted({p for p in r.stdout.decode("utf-8").split("\0") if p})


def check(rel: str, data: bytes) -> list[str]:
    problems = [f"{rel}: {why}" for rx, why in FORBIDDEN_NAME if rx.search(rel)]
    for rx, why in CONTENT:
        if rx.pattern == rb"PGDMP" and not data.startswith(b"PGDMP"):
            continue
        if rx.search(data):
            problems.append(f"{rel}: contains {why}")
    p = PurePosixPath(rel)
    if p.is_absolute() or ".." in p.parts or "\\" in rel:
        problems.append(f"{rel}: unsafe path")
    return problems


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True, help="output directory (outside the repository)")
    ap.add_argument("--list-only", action="store_true")
    ap.add_argument("--force", action="store_true", help="replace an existing staging dir / ZIP in --out")
    a = ap.parse_args()
    out = Path(a.out).resolve()
    if out == ROOT or ROOT in out.parents:
        sys.exit("--out must be outside the repository (no archive inside the archive)")

    files = [f for f in gitignore_respecting_files() if f not in NEVER]
    for extra in EXTRA_INCLUDES:
        if not (ROOT / extra).is_file():
            sys.exit(f"required file missing: {extra}")
        files.append(extra)
    files = sorted(set(files))
    if a.list_only:
        print("\n".join(files))
        print(f"{len(files)} files", file=sys.stderr)
        return 0

    problems, payload = [], {}
    for rel in files:
        src = ROOT / rel
        if src.is_symlink() or not src.is_file():
            problems.append(f"{rel}: not a regular file")
            continue
        data = src.read_bytes()
        problems += check(rel, data)
        payload[rel] = data
    if problems:
        print("REFUSING TO PACKAGE:", *problems, sep="\n  ")
        return 1

    stage_root, zpath = out / "staging", out / f"{TOP}.zip"
    stage = stage_root / TOP
    if (stage_root.exists() or zpath.exists()) and not a.force:
        sys.exit(f"{stage_root} or {zpath} exists (use --force to replace)")
    if stage_root.exists():
        shutil.rmtree(stage_root)
    zpath.unlink(missing_ok=True)

    manifest = "".join(f"{hashlib.sha256(payload[r]).hexdigest()}  {r}\n" for r in files)
    payload[MANIFEST] = manifest.encode("utf-8")
    for rel, data in payload.items():
        dst = stage / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(data)

    with zipfile.ZipFile(zpath, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as z:
        for rel in sorted(payload):
            info = zipfile.ZipInfo(f"{TOP}/{rel}", date_time=ZIP_TIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            mode = 0o755 if rel.endswith(EXECUTABLE_SUFFIXES) else 0o644
            info.external_attr = (0o100000 | mode) << 16
            info.create_system = 3          # Unix, so the mode bits above are honoured on extraction
            z.writestr(info, payload[rel])

    digest = hashlib.sha256(zpath.read_bytes()).hexdigest()
    (out / f"{TOP}.zip.sha256").write_text(f"{digest}  {TOP}.zip\n", encoding="utf-8", newline="\n")
    size = zpath.stat().st_size
    print(f"staged {len(payload)} files ({len(files)} + {MANIFEST}) in {stage}")
    print(f"wrote {zpath} ({size:,} bytes)\nsha256 {digest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
