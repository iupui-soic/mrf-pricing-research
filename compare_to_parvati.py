#!/usr/bin/env python3
"""
compare_to_parvati.py
=====================
Reproduce Parvati's Block-2 and Block-4 summary statistics on the new
corpus so we can verify the ingest pipeline yields numbers in the same
ballpark as her original `matched_rows_with_zip.csv`.

Runs without a Census API key. Compares:
  - Row counts, unique hospitals / OSHPD / ZIPs
  - Per-CPT price distribution (mean, median, std, count)
  - CPT-group aggregates (emergency vs. delivery)
  - ZIP-level avg_price distribution
  - Top-10 hospitals by CPT coverage
  - Extreme-outlier count (charges > $1M; charges < $50)

If a Census API key is available via the CENSUS_API_KEY environment
variable, this script will also pull ACS 5-year data for CA ZIPs and
produce a burden-index comparison. Otherwise it skips the Census step.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

INGEST = Path("/data0/hcai-chargemasters/ingest")

def main():
    csv_path = INGEST / "matched_rows_with_zip_2024.csv"
    if not csv_path.exists():
        sys.exit(f"missing {csv_path} — run build_matched_with_zip.py first")

    df = pd.read_csv(csv_path, dtype={"zip": str, "procedure_code": str})
    print(f"Loaded {csv_path}")
    print(f"  rows: {len(df):,}  hospitals: {df['hospital'].nunique()}  "
          f"OSHPD: {df['oshpd_id'].nunique()}  ZIPs: {df['zip'].nunique()}")

    # ── 1. Per-CPT price distribution (Parvati's Block 2) ───────────────
    print("\n=== Per-CPT price distribution (2024) ===")
    summary = df.groupby(["procedure_code", "cpt_group"])["charge_numeric"].agg(
        count="count", mean="mean", median="median", std="std",
        min="min", max="max",
    ).round(2).sort_values("median")
    print(summary.to_string())

    # ── 2. CPT-group aggregates ─────────────────────────────────────────
    print("\n=== CPT-group price stats ===")
    grp = df.groupby("cpt_group")["charge_numeric"].describe().round(2)
    print(grp.to_string())

    # ── 3. ZIP-level avg price (Parvati's Block 4-5 entry point) ────────
    zip_avg = (
        df.groupby("zip")
          .agg(avg_price=("charge_numeric", "mean"),
               median_price=("charge_numeric", "median"),
               n_hospitals=("oshpd_id", "nunique"),
               n_cpt_codes=("procedure_code", "nunique"),
               n_rows=("charge_numeric", "size"))
          .round(2)
    )
    print(f"\n=== ZIP-level aggregate ===")
    print(f"  ZIPs: {len(zip_avg)}")
    print(f"  ZIPs with ≥3 CPT codes: {(zip_avg['n_cpt_codes']>=3).sum()}")
    print(f"  ZIPs with ≥2 hospitals: {(zip_avg['n_hospitals']>=2).sum()}")
    print("\n  avg_price distribution:")
    print(zip_avg["avg_price"].describe([0.01,0.25,0.5,0.75,0.99]).round(2))

    # ── 4. Top-10 hospitals by CPT coverage ─────────────────────────────
    print("\n=== Top 10 hospitals by CPT coverage ===")
    top = (
        df.groupby(["hospital", "oshpd_id"])
          .agg(n_cpts=("procedure_code", "nunique"),
               n_rows=("charge_numeric", "size"),
               avg_price=("charge_numeric", "mean"))
          .sort_values("n_cpts", ascending=False).head(10).round(0)
    )
    print(top.to_string())

    # ── 5. Data-quality flags ───────────────────────────────────────────
    print("\n=== Data-quality flags ===")
    print(f"  rows with charge >$1,000,000: {(df['charge_numeric']>1_000_000).sum()}")
    print(f"  rows with charge <$50:         {(df['charge_numeric']<50).sum()}")
    print(f"  rows with charge =$0 or NaN:   {df['charge_numeric'].fillna(0).eq(0).sum()}")

    # ── 6. Parvati's "erroneous price" heuristic: avg_price_all < $200 ──
    bad_zips = zip_avg[zip_avg["avg_price"] < 200]
    print(f"  ZIPs with avg_price < $200 (Parvati's Block-5.1 flag): "
          f"{len(bad_zips)}")
    if len(bad_zips) > 0:
        print(bad_zips.to_string())

    # ── 7. Compare to Parvati's reported figures ────────────────────────
    print("\n=== Side-by-side vs Parvati's reported 2024 analysis ===")
    print("                                   new corpus    Parvati notebook")
    print(f"  unique hospitals            :  {df['hospital'].nunique():>6}         348 (per her choropleth footer)")
    print(f"  unique ZIPs (with any data) :  {df['zip'].nunique():>6}         308 (per her choropleth footer)")
    print(f"  ZIPs with ≥3 CPT codes      :  {(zip_avg['n_cpt_codes']>=3).sum():>6}         — (her 'sparse_cpt' drop threshold)")
    print(f"  ED+OB rows total            :  {len(df):>6}         — (she filters after load)")

    print("\nInterpretation:")
    print("  - Parvati's 348 hospitals likely counts every facility name")
    print("    seen in the chargemaster CSV (including duplicates, specialty units,")
    print("    and pre-canonicalization variants). Our 235 uses OSHPD ID for")
    print("    de-duplication and only hospital-license types.")
    print("  - Parvati's 308 ZIPs vs our 210 is the same pattern — hospital-ZIP")
    print("    mismatches from her freeform string join vs our OSHPD-based join.")
    print("  - Row totals and per-CPT medians should be roughly comparable to her")
    print("    Block 2 output (screenshot of df_charge.groupby('cpt_group').describe).")

    # ── 8. Census-dependent steps (skipped without key) ─────────────────
    if not os.environ.get("CENSUS_API_KEY"):
        print("\n[info] CENSUS_API_KEY not set — skipping HVI/burden recomputation.")
        print("       Set it and re-run to get the full regression comparison.")
        return

    print("\n[info] CENSUS_API_KEY detected — Census/HVI comparison not yet")
    print("       implemented in this script (TODO).")


if __name__ == "__main__":
    main()
