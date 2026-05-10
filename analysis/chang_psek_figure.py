#!/usr/bin/env python3
"""
analysis/chang_psek_figure.py
=============================
Renders Figure 4 (Chang & Psek ZIP-level community-risk gradient) from the
panel produced by chang_psek_regression.py. Annotations are computed live
from the same pooled and per-state OLS+HC3 fits, so caption numbers and
in-figure numbers cannot drift.

Inputs:
  /data0/mrf-pricing-research/analysis/chang_psek_zip_panel.parquet

Outputs:
  paper/figures/figure4_chang_psek.png
"""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf

PANEL_PQ = "/data0/mrf-pricing-research/analysis/chang_psek_zip_panel.parquet"
OUT_PNG  = Path(__file__).resolve().parents[1] / "paper" / "figures" / "figure4_chang_psek.png"


def fit(df: pd.DataFrame):
    m = smf.ols("log_r ~ poverty_rate_pct + log_median_income",
                data=df).fit(cov_type="HC3")
    return {
        "n":     len(df),
        "beta":  float(m.params["log_median_income"]),
        "p":     float(m.pvalues["log_median_income"]),
        "r2":    float(m.rsquared),
        "model": m,
    }


def predict_line(model, df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    xs = np.linspace(df["log_median_income"].min(),
                     df["log_median_income"].max(), 50)
    grid = pd.DataFrame({
        "log_median_income": xs,
        "poverty_rate_pct":  np.full_like(xs, df["poverty_rate_pct"].mean()),
    })
    return xs, model.predict(grid).values


def main():
    panel = pd.read_parquet(PANEL_PQ)
    panel = panel[panel["cash_ratio"] > 0].copy()
    panel["log_r"] = np.log(panel["cash_ratio"])

    pooled = fit(panel)
    ca     = fit(panel[panel.state == "CA"])
    ind    = fit(panel[panel.state == "IN"])

    fig, ax = plt.subplots(figsize=(8.5, 5.6))

    sub_ca = panel[panel.state == "CA"]
    sub_in = panel[panel.state == "IN"]

    ax.scatter(sub_ca["log_median_income"], sub_ca["log_r"],
               s=20 + 12 * sub_ca["n_hospitals"], alpha=0.55,
               color="#1f77b4", edgecolor="white", linewidth=0.4,
               label=f"CA (n={ca['n']})")
    ax.scatter(sub_in["log_median_income"], sub_in["log_r"],
               s=20 + 12 * sub_in["n_hospitals"], alpha=0.6,
               color="#d62728", edgecolor="white", linewidth=0.4,
               label=f"IN (n={ind['n']})")

    xs, ys = predict_line(pooled["model"], panel)
    ax.plot(xs, ys, color="black", lw=2.0,
            label=(f"Pooled fit (β={pooled['beta']:+.2f}, "
                   f"p={pooled['p']:.3f}, R²={pooled['r2']:.3f})"))

    xs, ys = predict_line(ca["model"], sub_ca)
    ax.plot(xs, ys, color="#1f77b4", lw=1.5, linestyle="--",
            label=(f"CA fit (β={ca['beta']:+.2f}, p={ca['p']:.3f})"))

    xs, ys = predict_line(ind["model"], sub_in)
    ax.plot(xs, ys, color="#d62728", lw=1.5, linestyle="--",
            label=(f"IN fit (β={ind['beta']:+.2f}, p={ind['p']:.3f})"))

    ax.set_xlabel("log(ZIP median household income, ACS 2024 5-yr)")
    ax.set_ylabel("log(cash-to-Medicare ratio, ZIP median)")
    ax.set_title(f"ZIP-level community-risk gradient — cash ratio (n={pooled['n']} ZIPs)")
    ax.legend(loc="lower left", fontsize=8.5, framealpha=0.92)
    ax.grid(alpha=0.25)
    fig.tight_layout()

    OUT_PNG.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT_PNG, dpi=200)
    plt.close(fig)
    print(f"[out] {OUT_PNG}")
    print(f"      pooled n={pooled['n']}  β={pooled['beta']:+.4f}  "
          f"p={pooled['p']:.4f}  R²={pooled['r2']:.4f}")
    print(f"      CA     n={ca['n']}  β={ca['beta']:+.4f}  p={ca['p']:.4f}")
    print(f"      IN     n={ind['n']}  β={ind['beta']:+.4f}  p={ind['p']:.4f}")


if __name__ == "__main__":
    main()
