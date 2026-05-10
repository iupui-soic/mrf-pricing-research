#!/usr/bin/env python3
"""
analysis/corpus_figure.py
=========================
Renders Figure 1 (corpus coverage + schema-version distribution) from raw
inputs so the figure cannot drift from the manuscript's numbers.

Panel (a): grouped bars for CA and IN across four pipeline stages —
universe (CCN) → valid §180 MRFs → parsed gross → parsed negotiated.
Panel (b): pie chart of MRF schema-version bucket distribution across the
478 hospitals contributing rows to the unified gross parquet (the natural
denominator for schema_version, which is only assigned to hospitals with
gross rows). Panel (b) title prints `n=478 hospitals with parsed gross
prices` directly so the denominator is unambiguous.

Inputs:
  /data0/mrf-pricing-research/mrf/parsed/mrf_gross.parquet
  /data0/mrf-pricing-research/mrf/parsed/mrf_negotiated.parquet
  /data0/mrf-pricing-research/crosswalk/facilities_crosswalk.parquet

Output:
  paper/figures/figure1_corpus.png
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np

GROSS = "/data0/mrf-pricing-research/mrf/parsed/mrf_gross.parquet"
NEG   = "/data0/mrf-pricing-research/mrf/parsed/mrf_negotiated.parquet"
XW    = "/data0/mrf-pricing-research/crosswalk/facilities_crosswalk.parquet"

OUT_PNG = Path(__file__).resolve().parents[1] / "paper" / "figures" / "figure1_corpus.png"

UNIVERSE = {"CA": 378, "IN": 150}
VALID_180 = {"CA": 333, "IN": 125}


def schema_buckets(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    df = con.execute(f"""
        WITH per_file AS (
          SELECT ccn, schema_version, COUNT(*) AS n_rows,
                 CASE
                   WHEN schema_version IN ('1','1.0.0')                                            THEN 'v1.x'
                   WHEN schema_version LIKE '2%'                                                    THEN 'v2.x'
                   WHEN schema_version IN ('3','3.0','3.0.0','3.0.1','V3.0.0','CSV Wide V3.0')     THEN 'v3.x'
                   WHEN schema_version LIKE '%kaiser%'                                              THEN 'kaiser_2023'
                   WHEN schema_version LIKE 'misc%'                                                 THEN 'misc_csv'
                   ELSE 'other' END AS bucket
          FROM '{GROSS}'
          GROUP BY ccn, schema_version
        ),
        agg AS (SELECT ccn, bucket, SUM(n_rows) AS n FROM per_file GROUP BY ccn, bucket),
        ranked AS (SELECT ccn, bucket, n,
                          ROW_NUMBER() OVER (PARTITION BY ccn ORDER BY n DESC) AS rk
                   FROM agg)
        SELECT bucket, COUNT(*) AS n_hospitals
        FROM ranked WHERE rk = 1
        GROUP BY bucket ORDER BY n_hospitals DESC
    """).df()
    return dict(zip(df["bucket"], df["n_hospitals"]))


def per_state_counts(con: duckdb.DuckDBPyConnection,
                     pq: str) -> dict[str, int]:
    df = con.execute(f"""
        SELECT xw.state, COUNT(DISTINCT g.ccn) AS n
        FROM '{pq}' g INNER JOIN '{XW}' xw ON xw.ccn = g.ccn
        WHERE xw.state IN ('CA','IN')
        GROUP BY xw.state
    """).df()
    return dict(zip(df["state"], df["n"]))


def main() -> None:
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='12GB'")

    gross_state = per_state_counts(con, GROSS)
    neg_state   = per_state_counts(con, NEG)
    buckets     = schema_buckets(con)
    n_total_gross = sum(buckets.values())

    print(f"[panel a] universe  CA={UNIVERSE['CA']}  IN={UNIVERSE['IN']}")
    print(f"[panel a] valid     CA={VALID_180['CA']}  IN={VALID_180['IN']}")
    print(f"[panel a] gross     CA={gross_state.get('CA')}  IN={gross_state.get('IN')}")
    print(f"[panel a] neg       CA={neg_state.get('CA')}    IN={neg_state.get('IN')}")
    print(f"[panel b] schema buckets (n={n_total_gross}):")
    for k, v in buckets.items():
        print(f"  {k:<14} {v:>4}  ({100*v/n_total_gross:.1f}%)")

    fig, (axa, axb) = plt.subplots(1, 2, figsize=(13, 5.2),
                                    gridspec_kw={"width_ratios": [1.15, 1]})

    # ── Panel (a): grouped bars ────────────────────────────────────────────
    stages = ["Medicare-\ncertified\nuniverse",
              "Valid §180\nMRFs",
              "Parsed\ngross",
              "Parsed\nnegotiated"]
    ca_vals = [UNIVERSE["CA"], VALID_180["CA"], gross_state["CA"], neg_state["CA"]]
    in_vals = [UNIVERSE["IN"], VALID_180["IN"], gross_state["IN"], neg_state["IN"]]
    x = np.arange(len(stages))
    w = 0.38
    axa.bar(x - w/2, ca_vals, w, label="California (n=378)", color="#1f77b4", alpha=0.85)
    axa.bar(x + w/2, in_vals, w, label="Indiana (n=150)",     color="#d62728", alpha=0.85)
    for xi, v in zip(x - w/2, ca_vals):
        axa.text(xi, v + 6, str(v), ha="center", va="bottom", fontsize=9)
    for xi, v in zip(x + w/2, in_vals):
        axa.text(xi, v + 6, str(v), ha="center", va="bottom", fontsize=9)
    axa.set_xticks(x)
    axa.set_xticklabels(stages, fontsize=9)
    axa.set_ylabel("Hospitals")
    axa.set_title("(a) Pipeline coverage by state", fontsize=11)
    axa.legend(loc="upper right", fontsize=9)
    axa.spines["top"].set_visible(False)
    axa.spines["right"].set_visible(False)

    # ── Panel (b): schema-version horizontal bar chart ─────────────────────
    # A horizontal bar chart reads cleanly for highly skewed distributions
    # (v3.x at 74% vs other at 0.6%) where pie labels would otherwise collide.
    order = ["v3.x", "v2.x", "kaiser_2023", "misc_csv", "v1.x", "other"]
    sizes  = [buckets.get(b, 0) for b in order]
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#7f7f7f"]
    # Sort descending for readability
    triples = sorted(zip(sizes, order, colors), reverse=True)
    sizes_s, order_s, colors_s = zip(*triples)
    y_pos = np.arange(len(order_s))
    axb.barh(y_pos, sizes_s, color=colors_s, alpha=0.85,
             edgecolor="white", linewidth=0.8)
    axb.set_yticks(y_pos)
    axb.set_yticklabels(order_s, fontsize=10)
    axb.invert_yaxis()  # largest at top
    for yi, n in zip(y_pos, sizes_s):
        pct = 100 * n / n_total_gross
        axb.text(n + max(sizes_s) * 0.01, yi,
                 f"{n}  ({pct:.1f}%)",
                 va="center", ha="left", fontsize=9.5)
    axb.set_xlabel("Hospitals")
    axb.set_xlim(0, max(sizes_s) * 1.18)
    axb.set_title(f"(b) MRF schema version distribution\n"
                  f"(n={n_total_gross} hospitals with parsed gross prices)",
                  fontsize=11)
    axb.spines["top"].set_visible(False)
    axb.spines["right"].set_visible(False)

    fig.tight_layout()
    OUT_PNG.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT_PNG, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"[out] {OUT_PNG}")


if __name__ == "__main__":
    main()
