#!/usr/bin/env python3
"""
analysis/wang_schema_stratified.py
==================================
Disambiguates the within-hospital gross↔cash log-correlation between two
candidate mechanisms:

  (1) Genuine flat-fraction pricing — hospitals set cash as a single
      fractional offset of chargemaster across all shoppable services.
  (2) Template encoding artifact — the post-2024 CMS §180 v3 template
      permits hospitals to encode discounted cash as a derived field
      (e.g., estimated_amount via flat-percentage formula), which would
      mechanically force r(gross, cash) = 1.0 within hospital.

Hypothesis test: stratify the discounter sample by source MRF schema
version. If r ≈ 1.0 emerges only in v3.x (where derived-cash is
template-permitted), that is consistent with mechanism (2). If r ≈ 1.0
holds in v2.x, v1.x, kaiser_chargemaster_2023 (Kaiser legacy CDM
predating §180), and misc_csv (custom non-§180 formats), that is
consistent with mechanism (1).

Inputs:
  /data0/mrf-pricing-research/mrf/parsed/mrf_gross.parquet   (schema_version)
  /data0/mrf-pricing-research/analysis/wang_per_hospital.parquet
  /data0/mrf-pricing-research/crosswalk/facilities_crosswalk.parquet

Outputs:
  /data0/mrf-pricing-research/analysis/wang_schema_stratified.parquet
  paper/figures/wang_schema_stratified.png
  paper/figures/cash_discount_histogram.png
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

GR_PQ   = "/data0/mrf-pricing-research/mrf/parsed/mrf_gross.parquet"
WANG_PQ = "/data0/mrf-pricing-research/analysis/wang_per_hospital.parquet"
XW_PQ   = "/data0/mrf-pricing-research/crosswalk/facilities_crosswalk.parquet"

OUT_DIR = Path("/data0/mrf-pricing-research/analysis")
OUT_PQ  = OUT_DIR / "wang_schema_stratified.parquet"

FIG_DIR = Path(__file__).resolve().parent.parent / "paper" / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)
FIG_SCHEMA   = FIG_DIR / "wang_schema_stratified.png"
FIG_DISCOUNT = FIG_DIR / "cash_discount_histogram.png"


def normalize_schema_bucket(con: duckdb.DuckDBPyConnection):
    """Coarse-bucket the heterogeneous schema_version strings."""
    con.execute("""
        CREATE TEMP TABLE schema_per_ccn AS
        WITH per_file AS (
            SELECT
                ccn, schema_version, COUNT(*) AS n_rows,
                CASE
                    WHEN schema_version IN ('1', '1.0.0')                                              THEN 'v1.x'
                    WHEN schema_version LIKE '2%'                                                      THEN 'v2.x'
                    WHEN schema_version IN ('3', '3.0', '3.0.0', '3.0.1', 'V3.0.0', 'CSV Wide V3.0')   THEN 'v3.x'
                    WHEN schema_version LIKE '%kaiser%'                                                THEN 'kaiser_2023'
                    WHEN schema_version LIKE 'misc%'                                                   THEN 'misc_csv'
                    ELSE 'other'
                END AS bucket
            FROM '""" + GR_PQ + """'
            GROUP BY ccn, schema_version
        ),
        agg AS (SELECT ccn, bucket, SUM(n_rows) AS n FROM per_file GROUP BY ccn, bucket),
        ranked AS (SELECT ccn, bucket, n,
                          ROW_NUMBER() OVER (PARTITION BY ccn ORDER BY n DESC) AS rk
                   FROM agg)
        SELECT ccn, bucket AS schema_bucket FROM ranked WHERE rk = 1
    """)


def main():
    con = duckdb.connect()
    print("[load] mrf_gross schema versions, wang per-hospital, crosswalk …")
    normalize_schema_bucket(con)

    panel = con.execute(f"""
        SELECT w.ccn, w.state, w.n_codes, w.median_cash_discount, w.discounter,
               w.r_gross_cash, w.n_gross_cash,
               w.r_gross_negmin, w.n_gross_negmin,
               w.r_cash_negmin, w.n_cash_negmin,
               COALESCE(s.schema_bucket, 'unknown') AS schema_bucket
        FROM '{WANG_PQ}' w
        LEFT JOIN schema_per_ccn s USING (ccn)
    """).df()
    print(f"[panel] {len(panel):,} hospitals")
    panel.to_parquet(OUT_PQ, index=False)
    print(f"[out] {OUT_PQ}")

    # ── (A) Schema-stratified gross-cash r distribution ────────────────────
    disc = panel[(panel["discounter"] == True) & panel["r_gross_cash"].notna()].copy()
    print(f"\n[A] Discounter sample: {len(disc):,} hospitals with valid r_gross_cash")

    # Bucket order: pre-§180, then §180 versions, then v3.x last (the suspect)
    bucket_order = ["kaiser_2023", "misc_csv", "v1.x", "v2.x", "v3.x"]
    bucket_label = {
        "kaiser_2023": "kaiser legacy\n(pre-§180)",
        "misc_csv":    "misc / custom\n(non-§180)",
        "v1.x":        "§180 v1.x",
        "v2.x":        "§180 v2.x",
        "v3.x":        "§180 v3.x\n(derived-cash permitted)",
    }
    disc = disc[disc["schema_bucket"].isin(bucket_order)]

    summary = (disc.groupby(["state", "schema_bucket"])
                   .agg(n=("ccn", "count"),
                        median_r=("r_gross_cash", "median"),
                        p25_r   =("r_gross_cash", lambda s: s.quantile(0.25)),
                        p75_r   =("r_gross_cash", lambda s: s.quantile(0.75)),
                        mean_r  =("r_gross_cash", "mean"),
                        median_disc=("median_cash_discount", "median"))
                   .reset_index())
    print("\n[A] Discounters by (state × schema_bucket):")
    print(summary.to_string(index=False))

    # Figure: violin + jitter, faceted by state
    fig, axes = plt.subplots(1, 2, figsize=(11, 5), sharey=True)
    for ax, st in zip(axes, ("CA", "IN")):
        sub = disc[disc["state"] == st]
        positions, labels, data, ns = [], [], [], []
        for i, b in enumerate(bucket_order):
            vals = sub[sub["schema_bucket"] == b]["r_gross_cash"].values
            if len(vals) >= 3:
                positions.append(i)
                labels.append(f"{bucket_label[b]}\nn={len(vals)}")
                data.append(vals)
                ns.append(len(vals))
            else:
                positions.append(i)
                labels.append(f"{bucket_label[b]}\nn={len(vals)}")
                data.append(np.array([]))
                ns.append(len(vals))
        ax.set_title(f"{st}  (n={sub.shape[0]} discounter hospitals)")
        for i, vals in enumerate(data):
            if len(vals) == 0:
                continue
            jitter = np.random.uniform(-0.18, 0.18, size=len(vals))
            ax.scatter(np.full_like(vals, i, dtype=float) + jitter, vals,
                       alpha=0.5, s=14, color="#2b6cb0")
            if len(vals) >= 5:
                vp = ax.violinplot([vals], positions=[i], widths=0.7,
                                   showmedians=True, showextrema=False)
                for body in vp["bodies"]:
                    body.set_facecolor("#cbd5e0"); body.set_alpha(0.4); body.set_edgecolor("#4a5568")
                vp["cmedians"].set_color("#2d3748"); vp["cmedians"].set_linewidth(1.5)
        ax.axhline(1.0, color="#a0aec0", linestyle="--", linewidth=0.8, zorder=0)
        ax.set_xticks(range(len(bucket_order)))
        ax.set_xticklabels(labels, fontsize=8)
        ax.set_ylim(-0.05, 1.08)
        ax.set_ylabel("Pearson r(log gross, log cash)" if st == "CA" else "")
        ax.spines["top"].set_visible(False); ax.spines["right"].set_visible(False)
    fig.suptitle("Within-hospital gross↔cash log-correlation by source schema version (discounters only)",
                 fontsize=11)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig(FIG_SCHEMA, dpi=160, bbox_inches="tight")
    print(f"[fig] {FIG_SCHEMA}")
    plt.close(fig)

    # ── (B) Cash-discount histogram (discounters) ──────────────────────────
    disc_pct = disc["median_cash_discount"].dropna() * 100
    print(f"\n[B] Cash-discount distribution among {len(disc_pct):,} discounters:")
    print(f"    p25={disc_pct.quantile(0.25):.1f}%  p50={disc_pct.median():.1f}%  p75={disc_pct.quantile(0.75):.1f}%  mean={disc_pct.mean():.1f}%")
    # Counts at round values (within ±0.5 percentage points)
    round_targets = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90]
    print("\n  Hospitals within ±0.5pp of common round discounts:")
    for t in round_targets:
        n = ((disc_pct >= t - 0.5) & (disc_pct <= t + 0.5)).sum()
        if n > 0:
            print(f"    {t:>3}% : {n:>3} hospitals")

    # 3-panel: CA all / CA non-Kaiser / IN. Splits the dominant Kaiser spike
    # away from the underlying non-Kaiser fractional-pricing distribution so
    # both signals are legible.
    panels = [
        ("CA — all discounters",
         disc[disc["state"] == "CA"]["median_cash_discount"].dropna() * 100,
         "CA"),
        ("CA — non-Kaiser only",
         disc[(disc["state"] == "CA") &
              (disc["schema_bucket"] != "kaiser_2023")]["median_cash_discount"].dropna() * 100,
         "CA"),
        ("IN — all discounters",
         disc[disc["state"] == "IN"]["median_cash_discount"].dropna() * 100,
         "IN"),
    ]
    fig, axes = plt.subplots(1, 3, figsize=(14, 4.4), sharey=False)
    for ax, (title, vals, _) in zip(axes, panels):
        ax.hist(vals, bins=np.arange(5, 100, 2.5), color="#2b6cb0",
                edgecolor="white", linewidth=0.6, alpha=0.85)
        for t in (25, 50, 75):
            ax.axvline(t, color="#e53e3e", linestyle=":", linewidth=0.9, alpha=0.7)
            ax.text(t, ax.get_ylim()[1] * 0.97, f"{t}%",
                    color="#e53e3e", fontsize=8, ha="center", va="top")
        ax.set_title(f"{title}  (n={len(vals)})", fontsize=10)
        ax.set_xlabel("Within-hospital median cash discount\n(gross − cash) / gross  [%]")
        ax.set_xlim(0, 100)
        ax.spines["top"].set_visible(False); ax.spines["right"].set_visible(False)
    axes[0].set_ylabel("Hospitals")
    fig.suptitle("Distribution of within-hospital median cash discounts (discounter hospitals)",
                 fontsize=11)
    fig.tight_layout(rect=[0, 0, 1, 0.94])
    fig.savefig(FIG_DISCOUNT, dpi=160, bbox_inches="tight")
    print(f"[fig] {FIG_DISCOUNT}")
    plt.close(fig)

    con.close()


if __name__ == "__main__":
    main()
