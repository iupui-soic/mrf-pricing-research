#!/usr/bin/env python3
"""
analysis/build_ratios.py
========================
Computes price-to-Medicare ratios for chargemaster, cash, and negotiated
prices across the CA + IN MRF corpus, then summarizes by state.

Inputs:
  /data0/mrf-pricing-research/mrf/parsed/mrf_gross.parquet
  /data0/mrf-pricing-research/mrf/parsed/mrf_negotiated.parquet
  /data0/mrf-pricing-research/crosswalk/facilities_crosswalk.parquet
  /data0/mrf-pricing-research/medicare/medicare_cpt_2026.parquet

Outputs:
  /data0/mrf-pricing-research/analysis/ratios_hospital_code.parquet
      ccn, state, code, gross, cash, neg_min, neg_median, neg_n_payers,
      medicare_allowable, gross_ratio, cash_ratio, neg_min_ratio,
      neg_median_ratio
  /data0/mrf-pricing-research/analysis/ratios_state_summary.parquet
      state, price_type, n_pairs, p25, p50, p75, mean
  /data0/mrf-pricing-research/analysis/ratios_payer_state.parquet
      state, payer_name, n_pairs, p50_neg_ratio
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

GROSS_PQ      = "/data0/mrf-pricing-research/mrf/parsed/mrf_gross.parquet"
NEG_PQ        = "/data0/mrf-pricing-research/mrf/parsed/mrf_negotiated.parquet"
CROSSWALK_PQ  = "/data0/mrf-pricing-research/crosswalk/facilities_crosswalk.parquet"
BENCHMARK_PQ  = "/data0/mrf-pricing-research/medicare/medicare_cpt_2026.parquet"

OUT_DIR  = Path("/data0/mrf-pricing-research/analysis")
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_HC   = OUT_DIR / "ratios_hospital_code.parquet"
OUT_SUM  = OUT_DIR / "ratios_state_summary.parquet"
OUT_PAY  = OUT_DIR / "ratios_payer_state.parquet"


def main():
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='12GB'")
    con.execute("PRAGMA threads=8")

    print("[setup] registering parquet sources …")
    con.execute(f"CREATE VIEW gross AS SELECT * FROM '{GROSS_PQ}'")
    con.execute(f"CREATE VIEW neg AS SELECT * FROM '{NEG_PQ}'")
    con.execute(f"CREATE VIEW xw  AS SELECT * FROM '{CROSSWALK_PQ}'")
    con.execute(f"CREATE VIEW mc  AS SELECT * FROM '{BENCHMARK_PQ}'")

    print("[gross] aggregating to ccn × code …")
    con.execute("""
        CREATE TEMP TABLE g AS
        SELECT
            ccn,
            UPPER(TRIM(code)) AS code,
            MIN(gross_charge)         AS gross,
            MIN(discounted_cash)      AS cash
        FROM gross
        WHERE code_type IN ('CPT','HCPCS')
          AND gross_charge IS NOT NULL
          AND gross_charge > 0
        GROUP BY ccn, UPPER(TRIM(code))
    """)
    n_g = con.execute("SELECT COUNT(*) FROM g").fetchone()[0]
    print(f"[gross] {n_g:,} hospital × code pairs")

    print("[neg] aggregating to ccn × code …")
    con.execute("""
        CREATE TEMP TABLE n AS
        SELECT
            ccn,
            UPPER(TRIM(code)) AS code,
            MIN(negotiated_dollar) AS neg_min,
            MEDIAN(negotiated_dollar) AS neg_median,
            COUNT(DISTINCT payer_name) AS neg_n_payers
        FROM neg
        WHERE code_type IN ('CPT','HCPCS')
          AND negotiated_dollar IS NOT NULL
          AND negotiated_dollar > 0
        GROUP BY ccn, UPPER(TRIM(code))
    """)
    n_n = con.execute("SELECT COUNT(*) FROM n").fetchone()[0]
    print(f"[neg] {n_n:,} hospital × code pairs")

    print("[join] gross ⨝ neg ⨝ crosswalk ⨝ medicare …")
    con.execute("""
        CREATE TEMP TABLE hc AS
        SELECT
            COALESCE(g.ccn, n.ccn) AS ccn,
            xw.state,
            COALESCE(g.code, n.code) AS code,
            g.gross,
            g.cash,
            n.neg_min,
            n.neg_median,
            n.neg_n_payers,
            mc.medicare_allowable,
            mc.source AS bench_source
        FROM g
        FULL OUTER JOIN n USING (ccn, code)
        LEFT JOIN xw ON xw.ccn = COALESCE(g.ccn, n.ccn)
        INNER JOIN mc ON mc.code = COALESCE(g.code, n.code)
        WHERE mc.medicare_allowable > 0
        AND NOT (mc.medicare_allowable < 10.00 AND mc.source = 'opps')
    """)
    n_hc = con.execute("SELECT COUNT(*) FROM hc").fetchone()[0]
    print(f"[join] {n_hc:,} hospital × code rows with Medicare benchmark")

# Compute 99.5th percentile winsorization caps by state × price type
    con.execute("""
        CREATE TEMP TABLE caps AS
        SELECT
            state,
            QUANTILE_CONT(gross      / medicare_allowable, 0.995) AS cap_gross,
            QUANTILE_CONT(cash       / medicare_allowable, 0.995) AS cap_cash,
            QUANTILE_CONT(neg_min    / medicare_allowable, 0.995) AS cap_neg_min,
            QUANTILE_CONT(neg_median / medicare_allowable, 0.995) AS cap_neg_median
        FROM hc
        WHERE state IN ('CA','IN')
          AND gross > 0
          AND medicare_allowable > 0
        GROUP BY state
    """)
    caps_df = con.execute("SELECT * FROM caps").df()
    print("[winsorize] 99.5th percentile caps by state:")
    print(caps_df.to_string(index=False))

    con.execute("""
        CREATE TEMP TABLE ratios AS
        SELECT
            h.ccn, h.state, h.code, h.gross, h.cash, h.neg_min, h.neg_median,
            h.neg_n_payers, h.medicare_allowable, h.bench_source,
            CASE WHEN h.gross      > 0
                 AND h.gross      / h.medicare_allowable <= c.cap_gross
                 THEN h.gross      / h.medicare_allowable END AS gross_ratio,
            CASE WHEN h.cash       > 0
                 AND h.cash       / h.medicare_allowable <= c.cap_cash
                 THEN h.cash       / h.medicare_allowable END AS cash_ratio,
            CASE WHEN h.neg_min    > 0
                 AND h.neg_min    / h.medicare_allowable <= c.cap_neg_min
                 THEN h.neg_min    / h.medicare_allowable END AS neg_min_ratio,
            CASE WHEN h.neg_median > 0
                 AND h.neg_median / h.medicare_allowable <= c.cap_neg_median
                 THEN h.neg_median / h.medicare_allowable END AS neg_median_ratio
        FROM hc h
        JOIN caps c ON c.state = h.state
        WHERE h.state IN ('CA','IN')
    """)

    print(f"[out] {OUT_HC} …")
    con.execute(f"COPY ratios TO '{OUT_HC}' (FORMAT PARQUET)")
    n_out = con.execute("SELECT COUNT(*) FROM ratios").fetchone()[0]
    print(f"      wrote {n_out:,} rows")

    # ── State × price-type summary ────────────────────────────────────────
    summary = con.execute("""
        WITH long AS (
            SELECT state, 'gross'      AS price_type, gross_ratio      AS r FROM ratios WHERE gross_ratio      IS NOT NULL
            UNION ALL
            SELECT state, 'cash'       AS price_type, cash_ratio       AS r FROM ratios WHERE cash_ratio       IS NOT NULL
            UNION ALL
            SELECT state, 'neg_min'    AS price_type, neg_min_ratio    AS r FROM ratios WHERE neg_min_ratio    IS NOT NULL
            UNION ALL
            SELECT state, 'neg_median' AS price_type, neg_median_ratio AS r FROM ratios WHERE neg_median_ratio IS NOT NULL
        )
        SELECT
            state,
            price_type,
            COUNT(*)                              AS n_pairs,
            QUANTILE_CONT(r, 0.25)                AS p25,
            QUANTILE_CONT(r, 0.50)                AS p50,
            QUANTILE_CONT(r, 0.75)                AS p75,
            AVG(r)                                AS mean
        FROM long
        WHERE r is not null  -- drop pathological ratios
        GROUP BY state, price_type
        ORDER BY state, price_type
    """).df()
    summary.to_parquet(OUT_SUM, index=False)
    print(f"[out] {OUT_SUM}  ({len(summary)} rows)")

    # ── Payer × state negotiated ratio summary ────────────────────────────
    payer = con.execute("""
        WITH per_pair AS (
            SELECT
                xw.state, n.payer_name,
                MIN(n.negotiated_dollar) AS neg_min,
                ANY_VALUE(mc.medicare_allowable) AS allow
            FROM neg n
            INNER JOIN mc ON mc.code = UPPER(TRIM(n.code))
            INNER JOIN xw ON xw.ccn  = n.ccn
            WHERE n.code_type IN ('CPT','HCPCS')
              AND n.negotiated_dollar > 0
              AND mc.medicare_allowable > 0
              AND xw.state IN ('CA','IN')
              AND n.payer_name IS NOT NULL
              AND TRIM(n.payer_name) <> ''
            GROUP BY xw.state, n.payer_name, n.ccn, UPPER(TRIM(n.code))
        )
        SELECT
            state,
            payer_name,
            COUNT(*)                                          AS n_pairs,
            QUANTILE_CONT(neg_min/allow, 0.50)                AS p50_neg_ratio,
            QUANTILE_CONT(neg_min/allow, 0.25)                AS p25_neg_ratio,
            QUANTILE_CONT(neg_min/allow, 0.75)                AS p75_neg_ratio
        FROM per_pair
        WHERE neg_min/allow BETWEEN 0.01 AND 1000
        GROUP BY state, payer_name
        HAVING COUNT(*) >= 100
        ORDER BY state, p50_neg_ratio
    """).df()
    payer.to_parquet(OUT_PAY, index=False)
    print(f"[out] {OUT_PAY}  ({len(payer)} payers with ≥100 pairs)")

    # ── Headline summary to stdout ────────────────────────────────────────
    print("\n" + "=" * 72)
    print("STATE × PRICE-TYPE  median ratio to Medicare allowable")
    print("=" * 72)
    pivot = summary.pivot(index="price_type", columns="state", values="p50")
    pivot = pivot.reindex(["gross", "cash", "neg_min", "neg_median"])
    print(pivot.round(2).to_string())

    print("\n" + "=" * 72)
    print("STATE × PRICE-TYPE  full distribution")
    print("=" * 72)
    print(summary.to_string(index=False))

    print("\n" + "=" * 72)
    print("Top 10 payers by lowest negotiated ratio (median), by state")
    print("=" * 72)
    for st in ("CA", "IN"):
        sub = payer[payer["state"] == st].nsmallest(10, "p50_neg_ratio")
        print(f"\n  {st}:")
        print(sub[["payer_name","n_pairs","p50_neg_ratio"]].to_string(index=False))

    con.close()


if __name__ == "__main__":
    main()
