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
      ccn, state, n_codes, r_gross_cash, r_gross_negmin, r_cash_negmin
  /data0/mrf-pricing-research/analysis/wang_state_summary.parquet
      state, pair, n_hospitals, p25, p50, p75, mean

Methodology:
  - For each hospital × code in the ratio panel where the relevant pair
    of price types is populated, compute Pearson r across codes within
    that hospital.
  - Require ≥10 codes per hospital for a stable correlation.
  - Pool to state level: distribution of within-hospital r's.
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
        rec = {"ccn": ccn, "state": state, "n_codes": len(sub)}
        for label, a, b in pairs:
            paired = sub[[a, b]].dropna()
            if len(paired) >= MIN_CODES_PER_HOSPITAL:
                # Pearson r on log to dampen heavy tails
                la = np.log(paired[a])
                lb = np.log(paired[b])
                if la.std() > 0 and lb.std() > 0:
                    rec[f"r_{label}"] = np.corrcoef(la, lb)[0, 1]
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

    # State × pair summary
    summary_rows = []
    for state in ("CA", "IN"):
        st = per_hosp[per_hosp["state"] == state]
        for label, _, _ in pairs:
            col = f"r_{label}"
            vals = st[col].dropna()
            if len(vals) > 0:
                summary_rows.append({
                    "state": state,
                    "pair": label,
                    "n_hospitals": len(vals),
                    "p25":  vals.quantile(0.25),
                    "p50":  vals.quantile(0.50),
                    "p75":  vals.quantile(0.75),
                    "mean": vals.mean(),
                })
    summary = pd.DataFrame(summary_rows)
    summary.to_parquet(OUT_SUM, index=False)
    print(f"[out] {OUT_SUM}  ({len(summary)} rows)")

    print("\n" + "=" * 64)
    print("Within-hospital cross-price-type correlations (Pearson r on log)")
    print("=" * 64)
    pivot = summary.pivot(index="pair", columns="state", values="p50").round(3)
    pivot = pivot.reindex(["gross_cash", "gross_negmin", "cash_negmin"])
    print(pivot.to_string())

    print("\nFull distribution:")
    print(summary.round(3).to_string(index=False))

    print("\nWang et al. 2023 reported: gross-cash r≈0.85, gross-negmin r≈0.78, "
          "cash-negmin r≈0.83 across their shoppable-services sample.")


if __name__ == "__main__":
    main()
