"""CLI: python -m ml.training.train [--dataset synthetic|paysim|ieee] [--csv PATH] [--quick]

Synthetic data is generated deterministically if missing. Public datasets must be placed
locally first (see docs/DATA_AND_ML.md section 1); nothing is downloaded automatically.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import replace
from pathlib import Path

from ml.datasets.public import ieee_to_events, paysim_to_events, read_csv_checked, validate_frame
from ml.datasets.synthetic import SyntheticDatasetSpec, load_events, write_dataset
from ml.features.engineering import build_feature_frame
from ml.training.pipeline import TrainingConfig, train_all

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ARTIFACTS = Path(os.environ.get("FRAUDSTREAM_ARTIFACTS_DIR", ROOT / "ml" / "artifacts"))
DATA_DIR = Path(os.environ.get("FRAUDSTREAM_DATA_DIR", ROOT / "data"))


def prepare_synthetic(quick: bool = False) -> tuple[list[dict], dict]:
    spec = SyntheticDatasetSpec()
    if quick:  # small dataset for CI/tests; results are NOT comparable to the full run
        spec = replace(spec, name="synthetic-fraudstream-quick", customer_count=600, duration_seconds=6 * 3600,
                       tps=1.2, suspicious_pct=3.0)
    out = DATA_DIR / "synthetic"
    meta_path = out / f"{spec.name}-v{spec.version}.meta.json"
    if not meta_path.exists():
        write_dataset(spec, out)
    meta = json.loads(meta_path.read_text())
    return load_events(out / meta["path"]), meta


def run(dataset: str = "synthetic", csv: str | None = None, quick: bool = False,
        artifacts_dir: Path = DEFAULT_ARTIFACTS, nrows: int | None = None) -> dict:
    if dataset == "synthetic":
        events, meta = prepare_synthetic(quick)
    else:
        if not csv:
            raise SystemExit(f"--csv is required for dataset '{dataset}'")
        df = read_csv_checked(Path(csv), dataset, nrows=nrows)
        profile = validate_frame(df, dataset)
        events = paysim_to_events(df) if dataset == "paysim" else ieee_to_events(df)
        meta = {"name": dataset, "version": "imported", "source": dataset, "rows": len(df), "profile": profile,
                "provenance": f"user-supplied CSV {Path(csv).name}"}
    frame = build_feature_frame(events)
    return train_all(frame, meta, artifacts_dir, TrainingConfig())


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="synthetic", choices=["synthetic", "paysim", "ieee"])
    ap.add_argument("--csv")
    ap.add_argument("--nrows", type=int)
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--artifacts-dir", default=str(DEFAULT_ARTIFACTS))
    a = ap.parse_args()
    summary = run(a.dataset, a.csv, a.quick, Path(a.artifacts_dir), a.nrows)
    for m in summary["models"]:
        t = m["metrics"]["test"]
        print(f"{m['model_name']:<20} test PR-AUC={t['pr_auc']:.4f} P={t['precision']:.3f} R={t['recall']:.3f} "
              f"F1={t['f1']:.3f} FPR={t['false_positive_rate']:.4f} p50={m['inference_latency']['p50_ms']:.2f}ms")
    print("recommended:", summary["recommended_supervised"], "| experiment:", summary["experiment_version"])


if __name__ == "__main__":
    main()
