#!/usr/bin/env python3
"""
analysis/chang_psek_within_between.py
=====================================
Decomposes the pooled cash-ratio income coefficient into within-state and
between-state components on the primary 165-ZIP analytic panel. Backs the
``Within- vs.\\ between-state decomposition'' paragraph in Results §3.4 and
the corresponding Simpson's-paradox caveat in Discussion §4.2.

Reports four specifications on the same panel:
  1. Pooled, no state FE        (the headline -0.96 result)
  2. Pooled with state FE       (within-state coefficient, ~0)
  3. CA only
  4. IN only

Plus the state-level cluster centroids that drive the between-state slope.

Input:
  /data0/mrf-pricing-research/analysis/chang_psek_zip_panel.parquet

Output:
  Stdout summary suitable for inclusion in supplementary materials.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf

PANEL_PQ = "/data0/mrf-pricing-research/analysis/chang_psek_zip_panel.parquet"


def main() -> None:
    panel = pd.read_parquet(PANEL_PQ)
    panel = panel[panel["cash_ratio"] > 0].copy()
    panel["log_r"]   = np.log(panel["cash_ratio"])
    panel["log_inc"] = np.log(panel["median_income"])
    panel["pov_pct"] = panel["poverty_rate"] * 100

    print("State-level cluster centroids (analytic cash-ratio panel, n=165)")
    print("=" * 68)
    summ = (panel.groupby("state")
                  .agg(n_zips=("zip","count"),
                       median_income=("median_income","median"),
                       mean_log_income=("log_inc","mean"),
                       median_cash_ratio=("cash_ratio","median"),
                       mean_log_cash_ratio=("log_r","mean"))
                  .round(3))
    print(summ.to_string())
    print()
    delta_inc = summ.loc["IN","mean_log_income"] - summ.loc["CA","mean_log_income"]
    delta_r   = summ.loc["IN","mean_log_cash_ratio"] - summ.loc["CA","mean_log_cash_ratio"]
    print(f"Between-state difference (IN − CA):")
    print(f"  Δ mean log(income)      : {delta_inc:+.3f}")
    print(f"  Δ mean log(cash ratio)  : {delta_r:+.3f}")
    print(f"  Implied between-cluster slope (Δlog_r / Δlog_inc): "
          f"{delta_r/delta_inc:+.3f}")
    print()

    print("Regression specifications")
    print("=" * 68)

    # 1. Pooled, no FE (primary)
    m1 = smf.ols("log_r ~ pov_pct + log_inc",
                 data=panel).fit(cov_type="HC3")
    # 2. Pooled with state FE (within-state)
    m2 = smf.ols("log_r ~ pov_pct + log_inc + C(state)",
                 data=panel).fit(cov_type="HC3")
    # 3, 4. State-specific
    ms = {}
    for st in ("CA","IN"):
        sub = panel[panel.state == st]
        ms[st] = smf.ols("log_r ~ pov_pct + log_inc",
                         data=sub).fit(cov_type="HC3")

    rows = [
        ("Pooled, no state FE (headline)",
         len(panel),
         m1.params["log_inc"], m1.pvalues["log_inc"], m1.rsquared),
        ("Pooled with state FE (within-state)",
         len(panel),
         m2.params["log_inc"], m2.pvalues["log_inc"], m2.rsquared),
        ("CA only",
         (panel.state=="CA").sum(),
         ms["CA"].params["log_inc"], ms["CA"].pvalues["log_inc"], ms["CA"].rsquared),
        ("IN only",
         (panel.state=="IN").sum(),
         ms["IN"].params["log_inc"], ms["IN"].pvalues["log_inc"], ms["IN"].rsquared),
    ]
    df = pd.DataFrame(rows, columns=["specification","n","beta_log_inc","p","r2"])
    print(df.to_string(index=False,
        formatters={"beta_log_inc":"{:+.4f}".format,
                    "p":"{:.4f}".format,
                    "r2":"{:.4f}".format}))
    print()

    state_fe = m2.params.get("C(state)[T.IN]")
    state_fe_p = m2.pvalues.get("C(state)[T.IN]")
    print(f"State fixed-effect coefficient (IN vs. CA): {state_fe:+.4f}  "
          f"p = {state_fe_p:.4f}")
    print()
    print(f"Diagnostic: pooled β = {m1.params['log_inc']:+.3f} → "
          f"with-FE β = {m2.params['log_inc']:+.3f}. The "
          f"{abs(m1.params['log_inc'] - m2.params['log_inc']):.2f}-unit "
          f"change is the between-state component; the within-state component "
          f"is essentially zero. Treating the headline coefficient as a "
          f"within-state community-risk gradient would be a Simpson's-paradox "
          f"misreading.")


if __name__ == "__main__":
    main()
