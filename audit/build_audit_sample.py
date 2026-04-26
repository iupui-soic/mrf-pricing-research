#!/usr/bin/env python3
"""
build_audit_sample.py
=====================
Generates `audit/audit_sample_200.csv` — a 200-row stratified random
sample from `cdm_all.parquet` for human-auditor labeling. Supports the
PROJECT_PLAN.md §6 Week 1 deliverable: precision (and recall via a
parallel sweep) numbers for the paper §Methods.

Stratification: 200 rows allocated by `code_type` proportional to corpus
volume, with a floor of 10 rows per code type so the rare types (DRG,
NDC) are represented. Within each stratum: simple random.

Human auditor labels three columns and writes notes; `score_audit.py`
(separate script) computes per-row precision metrics.
"""

from __future__ import annotations

import random
from pathlib import Path

import pandas as pd

CORPUS = Path("/data0/hcai-chargemasters/ingest/cdm_all.parquet")
OUT = Path(__file__).resolve().parent / "audit_sample_200.csv"
SEED = 20260426  # date-stamped for reproducibility
N = 200
FLOOR_PER_TYPE = 10


def main() -> None:
    df = pd.read_parquet(CORPUS, columns=[
        "year", "oshpd_id", "hospital_folder", "file_source", "sheet_name",
        "header_row", "code_type", "procedure_code", "description",
        "charge_column", "charge_raw", "charge", "setting",
    ])
    print(f"[load] cdm_all.parquet: {len(df):,} rows")

    # Allocate sample size per code_type (proportional with floor)
    counts = df["code_type"].value_counts()
    total = counts.sum()
    types = counts.index.tolist()
    raw_alloc = {t: max(FLOOR_PER_TYPE, round(N * counts[t] / total))
                 for t in types}
    # Trim to exactly N (largest type absorbs the slack)
    diff = N - sum(raw_alloc.values())
    if diff != 0:
        biggest = max(types, key=lambda t: counts[t])
        raw_alloc[biggest] += diff
    print(f"[alloc] {raw_alloc}")

    rng = random.Random(SEED)
    samples = []
    for ct, k in raw_alloc.items():
        sub = df[df["code_type"] == ct]
        idx = rng.sample(range(len(sub)), min(k, len(sub)))
        samples.append(sub.iloc[idx])
    out = pd.concat(samples, ignore_index=True)
    out["row_id"] = range(1, len(out) + 1)

    # Auditor-facing columns (blank — human fills in)
    out["audit_correct_code_type"] = ""        # Y / N / ?
    out["audit_correct_procedure_code"] = ""   # Y / N / ?
    out["audit_correct_charge"] = ""           # Y / N / ?
    out["audit_correct_setting"] = ""          # Y / N / ?
    out["audit_notes"] = ""

    cols = [
        "row_id",
        "year", "oshpd_id", "hospital_folder", "file_source", "sheet_name",
        "header_row", "code_type", "procedure_code", "description",
        "charge_column", "charge_raw", "charge", "setting",
        "audit_correct_code_type",
        "audit_correct_procedure_code",
        "audit_correct_charge",
        "audit_correct_setting",
        "audit_notes",
    ]
    out[cols].to_csv(OUT, index=False)
    print(f"[out] {OUT} ({len(out)} rows × {len(cols)} cols, seed={SEED})")


if __name__ == "__main__":
    main()
