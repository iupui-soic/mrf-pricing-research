#!/usr/bin/env python3
"""
analysis/chang_psek_outlier_sensitivity.py
==========================================
Reports the pooled cash-ratio income-coefficient sensitivity panel exactly as
shown in the Results §3.4 table of the manuscript. Five outlier policies are
evaluated on the same 165-ZIP analytic panel; only the trimming rule varies.

The published headline finding is the first row (0.01 floor + 99.5%
state-specific upper trim, the panel-construction policy). The remaining rows
are sensitivities. The single material shift (β = -0.96 → -1.77) appears only
when the lower 0.01 floor is dropped, which is the principled finding: token,
sentinel, and per-vial-mismatched entries below 0.01 drive the difference, not
the upper-tail policy.

Inputs:
  /data0/mrf-pricing-research/mrf/parsed/mrf_gross.parquet
  /data0/mrf-pricing-research/crosswalk/facilities_crosswalk.parquet
  /data0/mrf-pricing-research/medicare/medicare_cpt_2026.parquet
  /data0/mrf-pricing-research/hcai-chargemasters/ingest/cache_census_zip_2024.csv
  /data0/mrf-pricing-research/census/in_zip_demographics.parquet

Output:
  Stdout table matching Results §3.4 sensitivity panel.
"""
from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf

GROSS  = "/data0/mrf-pricing-research/mrf/parsed/mrf_gross.parquet"
XW     = "/data0/mrf-pricing-research/crosswalk/facilities_crosswalk.parquet"
BENCH  = "/data0/mrf-pricing-research/medicare/medicare_cpt_2026.parquet"
CA_CEN = "/data0/mrf-pricing-research/hcai-chargemasters/ingest/cache_census_zip_2024.csv"
IN_CEN = "/data0/mrf-pricing-research/census/in_zip_demographics.parquet"


def load_census() -> pd.DataFrame:
    ca = pd.read_csv(CA_CEN, dtype={"zip": str})[["zip","median_income","poverty_rate"]]
    ca["zip"]   = ca["zip"].str.zfill(5)
    ca["state"] = "CA"
    ind = (pd.read_parquet(IN_CEN)
              .rename(columns={"median_household_income":"median_income",
                                "pct_poverty":"poverty_rate"}))
    ind["zip"] = ind["zip"].astype(str).str.zfill(5)
    ind = ind[["zip","median_income","poverty_rate"]].copy()
    ind["state"] = "IN"
    return pd.concat([ca, ind], ignore_index=True)


def fit(con: duckdb.DuckDBPyConnection, panel_sql: str, label: str,
        census: pd.DataFrame) -> dict:
    p = con.execute(panel_sql).df()
    p["zip"] = p["zip"].astype(str).str.zfill(5)
    p = p.merge(census.drop(columns=["state"]), on="zip", how="inner")
    p["log_median_income"] = np.log(p["median_income"])
    p["poverty_rate_pct"]  = p["poverty_rate"] * 100
    p = p[p["cash_ratio"] > 0].dropna(
        subset=["log_median_income","poverty_rate_pct"])
    p["log_r"] = np.log(p["cash_ratio"])
    m = smf.ols("log_r ~ poverty_rate_pct + log_median_income",
                data=p).fit(cov_type="HC3")
    return {
        "policy": label,
        "n":      len(p),
        "beta":   float(m.params["log_median_income"]),
        "p":      float(m.pvalues["log_median_income"]),
        "r2":     float(m.rsquared),
    }


def main() -> None:
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='12GB'")
    con.execute("PRAGMA threads=8")

    con.execute(f"""
        CREATE TEMP TABLE g AS
        SELECT ccn, UPPER(TRIM(code)) AS code,
               MEDIAN(gross_charge)    AS gross,
               MEDIAN(discounted_cash) AS cash
        FROM '{GROSS}'
        WHERE code_type IN ('CPT','HCPCS') AND gross_charge > 0
        GROUP BY ccn, UPPER(TRIM(code))
    """)
    con.execute(f"""
        CREATE TEMP TABLE hc AS
        SELECT g.ccn, xw.state, g.code, g.cash,
               CASE WHEN g.cash > 0 AND mc.medicare_allowable > 0
                    THEN g.cash / mc.medicare_allowable END AS cash_ratio_raw
        FROM g
        LEFT JOIN '{XW}' xw ON xw.ccn = g.ccn
        INNER JOIN '{BENCH}' mc ON mc.code = g.code
        WHERE mc.medicare_allowable > 0
          AND NOT (mc.medicare_allowable < 10.00 AND mc.source = 'opps')
          AND xw.state IN ('CA','IN')
    """)

    census = load_census()

    def panel(filter_clause: str) -> str:
        return f"""
            WITH hosp AS (
              SELECT r.ccn, xw.zip, xw.state, MEDIAN(
                CASE WHEN {filter_clause} THEN r.cash_ratio_raw END
              ) AS cash_ratio_h
              FROM hc r
              INNER JOIN '{XW}' xw ON xw.ccn = r.ccn
              WHERE xw.zip IS NOT NULL
              GROUP BY r.ccn, xw.zip, xw.state
            )
            SELECT state, CAST(zip AS VARCHAR) AS zip,
                   COUNT(DISTINCT ccn) AS n_hospitals,
                   MEDIAN(cash_ratio_h) AS cash_ratio
            FROM hosp GROUP BY state, zip
        """

    cap_995 = """
        WITH caps AS (
          SELECT state, QUANTILE_CONT(cash_ratio_raw, 0.995) AS cap
          FROM hc GROUP BY state
        ),
        hosp AS (
          SELECT r.ccn, xw.zip, xw.state, MEDIAN(
            CASE WHEN r.cash_ratio_raw >= 0.01
                  AND r.cash_ratio_raw <= c.cap
                 THEN r.cash_ratio_raw END
          ) AS cash_ratio_h
          FROM hc r JOIN caps c ON c.state = r.state
          INNER JOIN 'PLACEHOLDER_XW' xw ON xw.ccn = r.ccn
          WHERE xw.zip IS NOT NULL
          GROUP BY r.ccn, xw.zip, xw.state
        )
        SELECT state, CAST(zip AS VARCHAR) AS zip,
               COUNT(DISTINCT ccn) AS n_hospitals,
               MEDIAN(cash_ratio_h) AS cash_ratio
        FROM hosp GROUP BY state, zip
    """.replace("PLACEHOLDER_XW", XW)

    cap_98 = cap_995.replace("0.995", "0.98")

    rows = [
        fit(con, cap_995, "0.01 floor + 99.5% trim (PRIMARY)", census),
        fit(con, cap_98,  "0.01 floor + 98% winsor (Chang & Psek)", census),
        fit(con, panel("r.cash_ratio_raw >= 0.01 AND r.cash_ratio_raw <= 500"),
                       "0.01 floor + 500x hard cap", census),
        fit(con, panel("r.cash_ratio_raw >= 0.01"),
                       "0.01 floor, no upper trim", census),
        fit(con, panel("r.cash_ratio_raw IS NOT NULL"),
                       "Fully uncapped (no floor, no upper trim)", census),
    ]
    df = pd.DataFrame(rows)
    print(df.to_string(index=False,
        formatters={"beta":"{:+.4f}".format,
                    "p":"{:.4f}".format,
                    "r2":"{:.4f}".format}))


if __name__ == "__main__":
    main()
