"""Regenerate the synthetic training dataset deterministically and prove it is the one the models were trained on.

    python scripts/ci/prepare_dataset.py [--data-dir data]

`data/` is gitignored, so a fresh checkout (CI) has no dataset. The generator is seeded (seed 2026), so the EVENTS are
reproducible: the ``content_sha256`` of the regenerated dataset must equal the one recorded in the stored experiment
(``ml/artifacts/experiments/*.json``). The .jsonl.gz *file bytes* differ from the original (gzip header time) and
so does ``created_at`` in the meta file - only the content hash is comparable.

Refuses to touch an existing dataset: if ``<data-dir>/synthetic`` already has the meta file it is only verified.
Never trains or writes anything under ml/artifacts.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=str(ROOT / "data"))
    a = ap.parse_args()
    os.environ["FRAUDSTREAM_DATA_DIR"] = str(Path(a.data_dir).resolve())
    sys.path[:0] = [str(ROOT), str(ROOT / "backend")]
    # imported here: ml.training.train reads FRAUDSTREAM_DATA_DIR at import time
    from ml.training.train import prepare_synthetic

    experiments = sorted((ROOT / "ml" / "artifacts" / "experiments").glob("*.json"))
    if not experiments:
        print("no stored experiment in ml/artifacts/experiments", file=sys.stderr)
        return 1
    expected = json.loads(experiments[-1].read_text(encoding="utf-8"))["dataset"]["content_sha256"]
    _, meta = prepare_synthetic()          # generates only if the meta file is missing
    got = meta["content_sha256"]
    print(f"dataset {meta['name']} v{meta['version']}: {meta['rows']} rows, content_sha256 {got}")
    if got != expected:
        print(f"MISMATCH: experiment {experiments[-1].name} was trained on content_sha256 {expected}", file=sys.stderr)
        return 1
    print(f"matches the training dataset recorded in {experiments[-1].name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
