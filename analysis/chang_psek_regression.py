#!/usr/bin/env python3
"""
analysis/chang_psek_regression.py
=================================
Extends Chang & Psek (BMC Health Services Research 2024) — which
regressed regional hospital prices on community social risk factors
at HSA level nationally — to ZIP resolution within a CA + IN frame.

For each ZIP with at least one hospital, compute the median hospital
price-to-Medicare ratio (gross, cash, negotiated_min), then regress
log(ratio) on ZIP-level socioeconomic features:

    log(ratio) ~ poverty_rate + log(median_income) + pct_uninsured*

For CA we use the existing ACS 2024 5-year cache built by
compare_to_parvati.py, which has poverty + income + uninsured + disability
+ elderly. For IN we use the lighter ACS pull from census/pull_census_in.py
(income + poverty only — no subject-table uninsured/disability columns).
The combined regression therefore restricts to features available in both.

Inputs:
  /data0/mrf-pricing-research/analysis/ratios_hospital_code.parquet
  /data0/mrf-pricing-research/crosswalk/facilities_crosswalk.parquet
  /data0/mrf-pricing-research/hcai-chargemasters/ingest/cache_census_zip_2024.csv
  /data0/mrf-pricing-research/census/in_zip_demographics.parquet

Outputs:
  /data0/mrf-pricing-research/analysis/chang_psek_zip_panel.parquet
      zip, state, gross_ratio, cash_ratio, neg_min_ratio,
      median_income, poverty_rate, n_hospitals
  /data0/mrf-pricing-research/analysis/chang_psek_models.txt
      OLS summaries for each ratio × state combination
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

RATIOS_PQ    = "/data0/mrf-pricing-research/analysis/ratios_hospital_code.parquet"
XW_PQ        = "/data0/mrf-pricing-research/crosswalk/facilities_crosswalk.parquet"
CA_CENSUS    = "/data0/mrf-pricing-research/hcai-chargemasters/ingest/cache_census_zip_2024.csv"
IN_CENSUS_PQ = "/data0/mrf-pricing-research/census/in_zip_demographics.parquet"

OUT_DIR    = Path("/data0/mrf-pricing-research/analysis")
OUT_PANEL  = OUT_DIR / "chang_psek_zip_panel.parquet"
OUT_MODELS = OUT_DIR / "chang_psek_models.txt"


def load_ca_census() -> pd.DataFrame:
    df = pd.read_csv(CA_CENSUS, dtype={"zip": str})
    keep = ["zip", "median_income", "poverty_rate", "total_pop"]
    df = df[keep].copy()
    df["state"] = "CA"
    return df


def load_in_census() -> pd.DataFrame:
    df = pd.read_parquet(IN_CENSUS_PQ)
    df = df.rename(columns={
        "median_household_income": "median_income",
        "pct_poverty":             "poverty_rate",
        "total_population":        "total_pop",
    })
    keep = ["zip", "median_income", "poverty_rate", "total_pop", "state"]
    df = df[[c for c in keep if c in df.columns]].copy()
    df["state"] = "IN"
    return df


def main():
    import statsmodels.formula.api as smf

    con = duckdb.connect()
    print("[load] hospital × code ratios + crosswalk …")

    # ZIP-level median ratio per state. Use median across (hospital × code)
    # to dampen single-procedure outliers, then median across hospitals
    # within the ZIP.
    zip_ratios = con.execute(f"""
        WITH hosp AS (
            SELECT
                r.ccn, xw.zip, xw.state,
                MEDIAN(r.gross_ratio)   AS gross_ratio_h,
                MEDIAN(r.cash_ratio)    AS cash_ratio_h,
                MEDIAN(r.neg_min_ratio) AS neg_min_ratio_h
            FROM '{RATIOS_PQ}' r
            INNER JOIN '{XW_PQ}' xw ON xw.ccn = r.ccn
            WHERE r.state IN ('CA','IN')
              AND xw.zip IS NOT NULL
              AND (r.gross_ratio IS NULL OR r.gross_ratio < 200)
              AND (r.cash_ratio IS NULL OR r.cash_ratio < 200)
              AND (r.neg_min_ratio IS NULL OR r.neg_min_ratio < 200)
            GROUP BY r.ccn, xw.zip, xw.state
        )
        SELECT
            state,
            CAST(zip AS VARCHAR) AS zip,
            COUNT(DISTINCT ccn)    AS n_hospitals,
            MEDIAN(gross_ratio_h)   AS gross_ratio,
            MEDIAN(cash_ratio_h)    AS cash_ratio,
            MEDIAN(neg_min_ratio_h) AS neg_min_ratio
        FROM hosp
        GROUP BY state, zip
    """).df()
    zip_ratios["zip"] = zip_ratios["zip"].astype(str).str.zfill(5)
    print(f"[zip] {len(zip_ratios):,} hospital ZIPs across CA + IN")

    # Census harmonization
    ca = load_ca_census()
    in_ = load_in_census()
    ca["zip"] = ca["zip"].astype(str).str.zfill(5)
    in_["zip"] = in_["zip"].astype(str).str.zfill(5)
    census = pd.concat([ca, in_], ignore_index=True)
    print(f"[census] CA {len(ca):,} ZIPs + IN {len(in_):,} ZIPs = {len(census):,}")

    panel = zip_ratios.merge(census.drop(columns=["state"]), on="zip", how="inner")
    panel["log_median_income"] = np.log(panel["median_income"])
    panel["poverty_rate_pct"]  = panel["poverty_rate"] * 100
    panel = panel.dropna(subset=["log_median_income", "poverty_rate_pct"])
    print(f"[panel] {len(panel):,} ZIPs after census join")

    panel.to_parquet(OUT_PANEL, index=False)
    print(f"[out] {OUT_PANEL}")

    # Regressions: log(ratio) ~ poverty + log(income), per state per ratio
    out_lines = []

    def banner(s):
        line = "=" * 72
        return f"\n{line}\n{s}\n{line}"

    out_lines.append(banner("Chang & Psek 2024 ZIP-level extension — CA + IN, 2026 corpus"))
    out_lines.append(f"n_zip = {len(panel)}  (CA={int((panel.state=='CA').sum())}, IN={int((panel.state=='IN').sum())})")
    out_lines.append(f"min hospitals per ZIP: {panel.n_hospitals.min()}, "
                     f"median: {int(panel.n_hospitals.median())}, "
                     f"max: {panel.n_hospitals.max()}")

    for ratio_col in ["gross_ratio", "cash_ratio", "neg_min_ratio"]:
        for state_filter, label in [(("CA", "IN"), "POOLED"), (("CA",), "CA"), (("IN",), "IN")]:
            sub = panel[panel.state.isin(state_filter)].copy()
            sub = sub[sub[ratio_col] > 0].copy()
            sub["log_r"] = np.log(sub[ratio_col])
            sub = sub.dropna(subset=["log_r", "log_median_income", "poverty_rate_pct"])
            if len(sub) < 15:
                out_lines.append(f"\n[{ratio_col} | {label}]  n={len(sub)} — too few, skipped")
                continue
            try:
                m = smf.ols(
                    "log_r ~ poverty_rate_pct + log_median_income",
                    data=sub
                ).fit(cov_type="HC3")
                out_lines.append(f"\n[{ratio_col} | {label}]  n={len(sub)}  R²={m.rsquared:.3f}")
                for name in ["Intercept", "poverty_rate_pct", "log_median_income"]:
                    b = m.params.get(name, np.nan)
                    p = m.pvalues.get(name, np.nan)
                    out_lines.append(f"  {name:25s}: {b:+.4f}  (p={p:.4f})")
            except Exception as e:
                out_lines.append(f"\n[{ratio_col} | {label}]  fit failed: {e}")

    text = "\n".join(out_lines)
    OUT_MODELS.write_text(text)
    print(f"[out] {OUT_MODELS}")
    print(text)


if __name__ == "__main__":
    main()
