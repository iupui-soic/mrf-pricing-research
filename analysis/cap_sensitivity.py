#!/usr/bin/env python3
"""
analysis/cap_sensitivity.py
===========================
Tests whether the headline state x price-type medians are robust to the choice
of upper-tail trimming policy. Reproduces the panel construction in
build_ratios.py but substitutes alternative caps in place of the published
state-specific 99.5th-percentile cap, then re-tabulates the medians.

Caps tested:
  A_current  : per-state, per-price-type 99.5th percentile (the published policy;
               1,117x for IN gross, 215x for CA gross)
  B_cap_500  : hard cap at 500x Medicare allowable (all states, all price types)
  C_cap_200  : hard cap at 200x
  D_cap_100  : hard cap at 100x

The 0.01 floor and the OPPS <$10 drug-code exclusion remain in place across
all four policies (they are independent of upper-tail trimming).

Outputs:
  /data0/mrf-pricing-research/analysis/cap_sensitivity.parquet
"""
from pathlib import Path

import duckdb
import pandas as pd

GROSS = "/data0/mrf-pricing-research/mrf/parsed/mrf_gross.parquet"
NEG   = "/data0/mrf-pricing-research/mrf/parsed/mrf_negotiated.parquet"
XW    = "/data0/mrf-pricing-research/crosswalk/facilities_crosswalk.parquet"
BENCH = "/data0/mrf-pricing-research/medicare/medicare_cpt_2026.parquet"

OUT_PQ  = Path("/data0/mrf-pricing-research/analysis/cap_sensitivity.parquet")
OUT_TXT = Path("/data0/mrf-pricing-research/analysis/cap_sensitivity.txt")


def main():
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
        CREATE TEMP TABLE n AS
        SELECT ccn, UPPER(TRIM(code)) AS code,
               MIN(negotiated_dollar)    AS neg_min,
               MEDIAN(negotiated_dollar) AS neg_median
        FROM '{NEG}'
        WHERE code_type IN ('CPT','HCPCS') AND negotiated_dollar > 0
        GROUP BY ccn, UPPER(TRIM(code))
    """)
    con.execute(f"""
        CREATE TEMP TABLE hc AS
        SELECT COALESCE(g.ccn, n.ccn)  AS ccn,
               xw.state,
               COALESCE(g.code, n.code) AS code,
               g.gross, g.cash, n.neg_min, n.neg_median,
               mc.medicare_allowable, mc.source AS bench_source
        FROM g FULL OUTER JOIN n USING (ccn, code)
        LEFT JOIN '{XW}' xw ON xw.ccn = COALESCE(g.ccn, n.ccn)
        INNER JOIN '{BENCH}' mc ON mc.code = COALESCE(g.code, n.code)
        WHERE mc.medicare_allowable > 0
          AND NOT (mc.medicare_allowable < 10.00 AND mc.source = 'opps')
          AND xw.state IN ('CA','IN')
    """)

    policies = [
        ("A_current_99.5pct", None),
        ("B_hard_cap_500x",   500.0),
        ("C_hard_cap_200x",   200.0),
        ("D_hard_cap_100x",   100.0),
    ]
    price_cols = [
        ("gross",      "gross"),
        ("cash",       "cash"),
        ("neg_min",    "neg_min"),
        ("neg_median", "neg_median"),
    ]

    rows = []
    for policy, hard_cap in policies:
        for price_type, col in price_cols:
            if hard_cap is None:
                cap_expr = (f"QUANTILE_CONT(CASE WHEN {col} > 0 "
                            f"THEN {col}/medicare_allowable END, 0.995)")
            else:
                cap_expr = str(hard_cap)
            sql = f"""
                WITH caps AS (
                    SELECT state, {cap_expr} AS cap FROM hc GROUP BY state
                )
                SELECT
                    h.state, '{price_type}' AS price_type,
                    COUNT(*)                                          AS n_pairs,
                    QUANTILE_CONT(h.{col}/h.medicare_allowable, 0.50) AS p50_ratio,
                    QUANTILE_CONT(h.{col}/h.medicare_allowable, 0.75) AS p75_ratio,
                    AVG(c.cap)                                        AS effective_cap
                FROM hc h JOIN caps c ON c.state = h.state
                WHERE h.{col} > 0
                  AND h.medicare_allowable > 0
                  AND h.{col}/h.medicare_allowable >= 0.01
                  AND h.{col}/h.medicare_allowable <= c.cap
                GROUP BY h.state
            """
            df = con.execute(sql).df()
            df["policy"] = policy
            rows.append(df)

    out = pd.concat(rows, ignore_index=True)[
        ["policy","state","price_type","n_pairs","p50_ratio","p75_ratio","effective_cap"]
    ]
    out.to_parquet(OUT_PQ, index=False)
    print(f"[out] {OUT_PQ}")

    # Pretty headline table
    lines = []
    lines.append("Cap-sensitivity: median price-to-Medicare ratio by policy / state / price type")
    lines.append("=" * 78)
    pivot = out.pivot_table(
        index=["price_type","state"], columns="policy", values="p50_ratio"
    ).round(3)
    lines.append(pivot.to_string())
    lines.append("")
    lines.append("Effective per-state cap (x Medicare allowable):")
    cap_pivot = out.pivot_table(
        index=["price_type","state"], columns="policy", values="effective_cap"
    ).round(1)
    lines.append(cap_pivot.to_string())
    lines.append("")
    lines.append("n_pairs by policy:")
    n_pivot = out.pivot_table(
        index=["price_type","state"], columns="policy", values="n_pairs"
    )
    lines.append(n_pivot.to_string())
    txt = "\n".join(lines)
    OUT_TXT.write_text(txt)
    print(txt)
    print(f"[out] {OUT_TXT}")


if __name__ == "__main__":
    main()
