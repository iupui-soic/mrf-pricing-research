#!/usr/bin/env python3
"""
analysis/wang_correlations.py
=============================
Replicates Wang, Bai & Anderson (Health Affairs 2023) within-hospital
cross-price-type correlation analysis on the new CA + IN MRF corpus.

Wang et al. found, for shoppable services, that chargemaster, cash, and
minimum-negotiated prices are highly positively correlated within hospital
across procedures — i.e., hospitals that price one service high tend to
price others high across all four price types, with the structure stable
within hospital. We test this on our 528-hospital CA + IN sample.

Inputs:
  /data0/mrf-pricing-research/analysis/ratios_hospital_code.parquet

Outputs:
  /data0/mrf-pricing-research/analysis/wang_per_hospital.parquet
      ccn, state, n_codes, median_cash_discount, discounter,
      r_gross_cash, r_gross_negmin, r_cash_negmin
  /data0/mrf-pricing-research/analysis/wang_state_summary.parquet
      state, segment, pair, n_hospitals, p25, p50, p75, mean

Methodology:
  - For each hospital × code in the ratio panel where the relevant pair
    of price types is populated, compute Pearson r across codes within
    that hospital.
  - Require ≥10 codes per hospital for a stable correlation.
  - Pool to state level: distribution of within-hospital r's.
  - Robustness segmentation: a hospital is a "discounter" if its median
    cash_discount = (gross - cash) / gross across paired codes is at
    least DISCOUNT_THRESHOLD. Wang et al. analyzed only hospitals that
    publish a meaningful cash discount; non-discounters trivially get
    r(gross,cash)=1 because cash is just a copy of gross.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

RATIOS_PQ = "/data0/mrf-pricing-research/analysis/ratios_hospital_code.parquet"
OUT_DIR   = Path("/data0/mrf-pricing-research/analysis")
OUT_HOSP  = OUT_DIR / "wang_per_hospital.parquet"
OUT_SUM   = OUT_DIR / "wang_state_summary.parquet"

MIN_CODES_PER_HOSPITAL = 10
# Hospitals with median cash_discount ≥ 5% are treated as real discounters.
# Below this, cash is essentially a copy of gross and r(gross,cash) is
# mechanically 1 — uninformative.
DISCOUNT_THRESHOLD = 0.05


def main():
    con = duckdb.connect()
    df = con.execute(f"""
        SELECT ccn, state, code, gross, cash, neg_min
        FROM '{RATIOS_PQ}'
        WHERE state IN ('CA', 'IN')
    """).df()
    print(f"[load] {len(df):,} hospital × code rows")

    pairs = [
        ("gross_cash",   "gross",  "cash"),
        ("gross_negmin", "gross",  "neg_min"),
        ("cash_negmin",  "cash",   "neg_min"),
    ]

    rows = []
    for ccn, sub in df.groupby("ccn", sort=False):
        state = sub["state"].iloc[0]

        # Cash-discount diagnostic: median (gross - cash) / gross across
        # codes where both populated and gross > 0.
        gc = sub[["gross", "cash"]].dropna()
        gc = gc[gc["gross"] > 0]
        if len(gc) >= MIN_CODES_PER_HOSPITAL:
            disc = ((gc["gross"] - gc["cash"]) / gc["gross"]).median()
        else:
            disc = np.nan

        rec = {
            "ccn": ccn, "state": state, "n_codes": len(sub),
            "median_cash_discount": disc,
            "discounter": (disc >= DISCOUNT_THRESHOLD) if pd.notna(disc) else None,
        }
        for label, a, b in pairs:
            paired = sub[[a, b]].dropna()
            paired = paired[(paired[a] > 0) & (paired[b] > 0)]
            if len(paired) >= MIN_CODES_PER_HOSPITAL:
                # Pearson r on log to dampen heavy tails
                la = np.log(paired[a])
                lb = np.log(paired[b])
                if la.std() > 0 and lb.std() > 0:
                    rec[f"r_{label}"] = float(np.corrcoef(la, lb)[0, 1])
                    rec[f"n_{label}"] = len(paired)
                else:
                    rec[f"r_{label}"] = np.nan
                    rec[f"n_{label}"] = len(paired)
            else:
                rec[f"r_{label}"] = np.nan
                rec[f"n_{label}"] = len(paired)
        rows.append(rec)

    per_hosp = pd.DataFrame(rows)
    per_hosp.to_parquet(OUT_HOSP, index=False)
    print(f"[out] {OUT_HOSP}  ({len(per_hosp)} hospitals)")

    # Discounter prevalence
    print("\n  Cash-discount prevalence by state "
          f"(threshold ≥ {DISCOUNT_THRESHOLD:.0%}):")
    ph = per_hosp.dropna(subset=["discounter"]).copy()
    ph["discounter_int"] = ph["discounter"].astype(int)
    disc_summary = ph.groupby("state").agg(
        n=("ccn", "count"),
        n_discounters=("discounter_int", "sum"),
        median_discount=("median_cash_discount", "median"),
    )
    disc_summary["pct_discounters"] = (
        disc_summary["n_discounters"].astype(float) / disc_summary["n"] * 100
    ).round(1)
    print(disc_summary.to_string())

    # State × segment × pair summary
    summary_rows = []
    segments = [
        ("all",            lambda d: d),
        ("discounters",    lambda d: d[d["discounter"] == True]),
        ("nondiscounters", lambda d: d[d["discounter"] == False]),
    ]
    for state in ("CA", "IN"):
        st = per_hosp[per_hosp["state"] == state]
        for seg_name, seg_filter in segments:
            seg = seg_filter(st)
            for label, _, _ in pairs:
                col = f"r_{label}"
                vals = seg[col].dropna()
                if len(vals) > 0:
                    summary_rows.append({
                        "state": state,
                        "segment": seg_name,
                        "pair": label,
                        "n_hospitals": len(vals),
                        "p25":  vals.quantile(0.25),
                        "p50":  vals.quantile(0.50),
                        "p75":  vals.quantile(0.75),
                        "mean": vals.mean(),
                    })
    summary = pd.DataFrame(summary_rows)
    summary.to_parquet(OUT_SUM, index=False)
    print(f"\n[out] {OUT_SUM}  ({len(summary)} rows)")

    print("\n" + "=" * 72)
    print("Within-hospital cross-price-type correlations — DISCOUNTERS ONLY")
    print(f"(hospitals with median cash discount ≥ {DISCOUNT_THRESHOLD:.0%})")
    print("=" * 72)
    disc_only = summary[summary["segment"] == "discounters"]
    pivot = disc_only.pivot(index="pair", columns="state", values="p50").round(3)
    pivot = pivot.reindex(["gross_cash", "gross_negmin", "cash_negmin"])
    print(pivot.to_string())

    print("\n" + "=" * 72)
    print("Within-hospital cross-price-type correlations — ALL HOSPITALS")
    print("=" * 72)
    all_seg = summary[summary["segment"] == "all"]
    pivot_all = all_seg.pivot(index="pair", columns="state", values="p50").round(3)
    pivot_all = pivot_all.reindex(["gross_cash", "gross_negmin", "cash_negmin"])
    print(pivot_all.to_string())

    print("\nFull distribution (state × segment × pair):")
    print(summary.round(3).to_string(index=False))

    print("\nWang et al. 2023 reported (discounter sample): gross-cash r≈0.85, "
          "gross-negmin r≈0.78, cash-negmin r≈0.83.")


if __name__ == "__main__":
    main()
