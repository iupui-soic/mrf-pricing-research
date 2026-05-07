#!/usr/bin/env python3
"""
analysis/wang_discounter_sensitivity.py
=======================================
Sensitivity of the within-hospital gross↔cash log-correlation finding to the
discounter-classification threshold.

A flat-fraction-encoding hospital that publishes cash = 0.95·gross would
register as a discounter under our default 5% threshold even though its
cash schedule is mechanically derived from chargemaster. If the within-hospital
r ≈ 1.0 finding holds even at substantively higher thresholds (e.g. ≥30%
discount), the encoding-artifact interpretation becomes much harder to
sustain — only hospitals with genuine, large fractional discounts are in
the sample.

For each threshold θ ∈ {0.05, 0.15, 0.30}, we re-classify hospitals as
discounters iff median_cash_discount ≥ θ, then compute discounter share
and the median Pearson r (gross↔cash, gross↔neg_min, cash↔neg_min) within
the discounter subset, by state.

Inputs:
  /data0/mrf-pricing-research/analysis/wang_per_hospital.parquet

Outputs:
  /data0/mrf-pricing-research/analysis/wang_discounter_sensitivity.parquet
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd

INP = "/data0/mrf-pricing-research/analysis/wang_per_hospital.parquet"
OUT = Path("/data0/mrf-pricing-research/analysis/wang_discounter_sensitivity.parquet")

THRESHOLDS = [0.05, 0.15, 0.30]
PAIRS = [("r_gross_cash", "n_gross_cash"),
         ("r_gross_negmin", "n_gross_negmin"),
         ("r_cash_negmin", "n_cash_negmin")]


def main():
    df = pd.read_parquet(INP)
    rows = []
    for threshold in THRESHOLDS:
        for state in ("CA", "IN"):
            sub = df[df["state"] == state].copy()
            sub_eval = sub[sub["r_gross_cash"].notna()]  # gross-cash evaluable
            n_eval = len(sub_eval)
            disc = sub_eval[sub_eval["median_cash_discount"] >= threshold]
            n_disc = len(disc)
            row = {
                "threshold": threshold,
                "state": state,
                "n_evaluable_gross_cash": n_eval,
                "n_discounters": n_disc,
                "discounter_share": (n_disc / n_eval) if n_eval else float("nan"),
                "median_cash_discount_in_disc": disc["median_cash_discount"].median(),
            }
            for r_col, _ in PAIRS:
                vals = disc[r_col].dropna()
                row[f"p50_{r_col}"] = vals.median() if len(vals) else float("nan")
                row[f"n_{r_col}"]   = len(vals)
            rows.append(row)

    out = pd.DataFrame(rows).sort_values(["threshold", "state"]).reset_index(drop=True)
    out.to_parquet(OUT, index=False)
    print(f"[out] {OUT}")
    pd.set_option("display.width", 180)
    pd.set_option("display.max_columns", 20)
    print(out.round(4).to_string(index=False))


if __name__ == "__main__":
    main()
