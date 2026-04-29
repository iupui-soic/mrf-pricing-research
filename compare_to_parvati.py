#!/usr/bin/env python3
"""
compare_to_parvati.py
=====================
Reproduce Parvati's 2024 analysis on the new corpus so we can verify that
the reproducible ingest yields the same scientific findings as the legacy
`matched_rows_with_zip.csv` pipeline.

Stages:
  1. Data-side summary (per-CPT medians, ZIP/hospital counts).
  2. Census ACS 5-year pull for all CA ZIPs (if CENSUS_API_KEY is set).
  3. CA death records 2024 load + ZIP aggregate.
  4. HVI + Poverty-Adjusted Price Burden recomputation.
  5. OLS regressions mirroring Parvati's Block 7:
        M1  log(price) ~ vulnerability components
        M2  log(price) ~ HVI composite
        M3r age-adjusted mortality proxy ~ price + vulnerability
  6. Side-by-side vs the effect sizes Parvati reported.

Inputs:
  - /data0/mrf-pricing-research/hcai-chargemasters/ingest/matched_rows_with_zip_2024.csv
  - /data0/mrf-pricing-research/hcai-chargemasters/ingest/ca_deaths_zip_2019-2024.csv
  - CENSUS_API_KEY from .env (loaded automatically)

Outputs printed to stdout.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd


INGEST   = Path("/data0/mrf-pricing-research/hcai-chargemasters/ingest")
DEATHS_CSV = INGEST / "ca_deaths_zip_2019-2024.csv"
CENSUS_CACHE = INGEST / "cache_census_zip_2024.csv"


# ── .env loader (no extra dependency) ───────────────────────────────────
def load_dotenv(path: Path = Path(".env")):
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, _, v = line.partition("=")
        k, v = k.strip(), v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


# ── Census ACS 5-year pull ──────────────────────────────────────────────
def pull_census(year: int = 2024) -> pd.DataFrame:
    """Pull ACS 5-year socioeconomic variables for all CA ZCTAs.

    Defaults to the 2024 5-year release (covers 2020–2024) to match
    Parvati's notebook. Delete CENSUS_CACHE to force a refresh.
    """
    if CENSUS_CACHE.exists():
        df = pd.read_csv(CENSUS_CACHE, dtype={"zip": str})
        print(f"[census] cached: {CENSUS_CACHE}  ({len(df):,} ZIPs)")
        return df

    import requests
    key = os.environ.get("CENSUS_API_KEY")
    if not key:
        sys.exit("CENSUS_API_KEY not set (check .env)")

    base = f"https://api.census.gov/data/{year}/acs"

    def pull(endpoint: str, variables: list[str]) -> pd.DataFrame:
        var_str = ",".join(["NAME"] + variables)
        url = (f"{base}/{endpoint}"
               f"?get={var_str}"
               f"&for=zip%20code%20tabulation%20area:*"
               f"&key={key}")
        r = requests.get(url, timeout=90)
        r.raise_for_status()
        data = r.json()
        df = pd.DataFrame(data[1:], columns=data[0])
        df = df.rename(columns={"zip code tabulation area": "zip"})
        return df

    detail_vars = [
        "B19013_001E",   # median household income
        "B01003_001E",   # total population
        "B17001_002E",   # count below poverty
        "B17001_001E",   # poverty universe total
    ]
    subject_vars = [
        "S2701_C05_001E",  # % uninsured
        "S1810_C03_001E",  # % with disability
        "S0101_C02_030E",  # % age 65+
    ]

    print(f"[census] pulling detail tables …")
    df_d = pull("acs5", detail_vars)
    print(f"[census] pulling subject tables …")
    df_s = pull("acs5/subject", subject_vars)

    df = df_d.merge(df_s[["zip"] + subject_vars], on="zip", how="left")
    for c in detail_vars + subject_vars:
        df[c] = pd.to_numeric(df[c], errors="coerce")
        df[c] = df[c].replace(-666666666, np.nan)

    df["median_income"]   = df["B19013_001E"]
    df["total_pop"]       = df["B01003_001E"]
    df["poverty_rate"]    = (df["B17001_002E"] / df["B17001_001E"]).clip(0, 1)
    df["pct_uninsured"]   = df["S2701_C05_001E"] / 100
    df["pct_disability"]  = df["S1810_C03_001E"] / 100
    df["pct_elderly"]     = df["S0101_C02_030E"] / 100
    df["zip"] = df["zip"].astype(str).str.zfill(5)

    # CA ZCTAs are roughly 90001–96162 (excludes 00xxx and DC ranges).
    df = df[df["zip"].between("90001", "96162")].copy()
    keep = ["zip", "median_income", "total_pop", "poverty_rate",
            "pct_uninsured", "pct_disability", "pct_elderly"]
    df = df[keep]
    df = df[df["total_pop"] > 0].copy()

    df.to_csv(CENSUS_CACHE, index=False)
    print(f"[census] cached → {CENSUS_CACHE}  ({len(df):,} CA ZIPs)")
    return df


# ── Mortality load ──────────────────────────────────────────────────────
def load_mortality(year: int = 2024) -> pd.DataFrame:
    """Filter CA ZIP-level deaths to one year / all causes / total population."""
    if not DEATHS_CSV.exists():
        sys.exit(f"missing {DEATHS_CSV}\n"
                 "Download from https://data.chhs.ca.gov/dataset/death-profiles-by-zip-code")
    print(f"[mortality] reading {DEATHS_CSV}")
    df = pd.read_csv(DEATHS_CSV, dtype=str)
    df.columns = df.columns.str.strip().str.lower()
    df["year"] = df["year"].astype(str).str.strip()
    filt = df[
        (df["year"] == str(year)) &
        (df["strata"] == "Total Population") &
        (df["cause"] == "ALL") &
        (df["geography_type"] == "Residence")
    ].copy()
    filt["zip"] = filt["zip_code"].astype(str).str.extract(r"(\d{5})", expand=False)
    filt["count"] = pd.to_numeric(filt["count"], errors="coerce")
    filt = filt.dropna(subset=["zip", "count"])
    agg = filt.groupby("zip", as_index=False)["count"].sum().rename(columns={"count": "deaths"})
    print(f"[mortality] {len(agg):,} ZIPs with {year} all-cause deaths")
    return agg


# ── Indexes + models ────────────────────────────────────────────────────
def winsorize_z(series: pd.Series, lo=0.01, hi=0.99) -> pd.Series:
    lo_v, hi_v = series.quantile(lo), series.quantile(hi)
    clipped = series.clip(lo_v, hi_v)
    return (clipped - clipped.mean()) / clipped.std()


def scale_0_100(s: pd.Series) -> pd.Series:
    return ((s - s.min()) / (s.max() - s.min()) * 100).round(2)


def build_zip_dataset(charge_csv: Path, census: pd.DataFrame,
                      mort: pd.DataFrame) -> pd.DataFrame:
    df = pd.read_csv(charge_csv, dtype={"zip": str, "procedure_code": str})
    # Drop implausibly low prices (Parvati's Block 5.1 rule): avg < $200
    zip_agg = (
        df[df["charge_numeric"] > 0]
          .groupby("zip", as_index=False)
          .agg(avg_price_all=("charge_numeric", "mean"),
               median_price_all=("charge_numeric", "median"),
               n_cpt_codes=("procedure_code", "nunique"),
               n_hospitals_zip=("oshpd_id", "nunique"))
    )
    zip_emerg = (
        df[(df["cpt_group"] == "emergency") & (df["charge_numeric"] > 0)]
          .groupby("zip", as_index=False)
          .agg(avg_price_emerg=("charge_numeric", "mean"))
    )
    zip_deliv = (
        df[(df["cpt_group"] == "delivery") & (df["charge_numeric"] > 0)]
          .groupby("zip", as_index=False)
          .agg(avg_price_deliv=("charge_numeric", "mean"))
    )
    z = (zip_agg
         .merge(zip_emerg, on="zip", how="left")
         .merge(zip_deliv, on="zip", how="left")
         .merge(census, on="zip", how="left")
         .merge(mort, on="zip", how="left"))

    # Parvati's cleaning rules
    before = len(z)
    z = z[z["median_income"].notna()]
    z = z[z["n_cpt_codes"] >= 3]
    z = z[z["avg_price_all"] >= 200]
    z = z[z["total_pop"] >= 100]
    print(f"[zip] before cleaning: {before}, after: {len(z)}")

    z["deaths_per_100k"] = (z["deaths"] / z["total_pop"] * 100_000).replace(
        [np.inf, -np.inf], np.nan)
    # Age-adjusted proxy (Parvati's Block 6 formula — known methodological
    # weakness, retained here for direct comparability with her reported
    # effect sizes; a proper direct-standardization rewrite is future work)
    z["mortality_age_adj_proxy"] = (z["deaths_per_100k"] /
                                    (z["pct_elderly"] * 100)).replace(
                                        [np.inf, -np.inf], np.nan)
    z["log_avg_price_all"] = np.log1p(z["avg_price_all"])
    z["log_median_income"] = np.log1p(z["median_income"])
    return z


def compute_indexes(z: pd.DataFrame) -> pd.DataFrame:
    z = z.copy()
    z["burden_raw"] = (z["avg_price_all"] / z["median_income"]) * z["poverty_rate"]
    z["idx_poverty_burden"] = scale_0_100(
        z["burden_raw"].clip(
            z["burden_raw"].quantile(0.01),
            z["burden_raw"].quantile(0.99))
    )
    for zc, sc in [("z_poverty","poverty_rate"),
                   ("z_uninsured","pct_uninsured"),
                   ("z_elderly","pct_elderly"),
                   ("z_disability","pct_disability")]:
        z[zc] = winsorize_z(z[sc])
    z["hvi_raw"] = z[["z_poverty","z_uninsured","z_elderly","z_disability"]].sum(axis=1)
    z["idx_hvi"] = scale_0_100(z["hvi_raw"])
    return z


def run_regressions(z: pd.DataFrame):
    import statsmodels.formula.api as smf
    # Convert rates to percentages for interpretable coefficients
    reg = z.copy()
    for c in ["poverty_rate","pct_uninsured","pct_elderly","pct_disability"]:
        reg[c+"_pct"] = reg[c] * 100
    reg = reg.dropna(subset=["log_avg_price_all","poverty_rate","pct_uninsured",
                              "pct_elderly","pct_disability","median_income"])
    print(f"[reg] n = {len(reg)}")

    m1 = smf.ols(
        "log_avg_price_all ~ poverty_rate_pct + pct_uninsured_pct "
        "+ pct_elderly_pct + pct_disability_pct + log_median_income",
        data=reg
    ).fit(cov_type="HC3")

    m2 = smf.ols(
        "log_avg_price_all ~ idx_hvi + n_hospitals_zip",
        data=reg
    ).fit(cov_type="HC3")

    m3r = None
    reg_m = reg.dropna(subset=["mortality_age_adj_proxy"])
    if len(reg_m) > 20:
        m3r = smf.ols(
            "mortality_age_adj_proxy ~ avg_price_all + poverty_rate_pct "
            "+ pct_uninsured_pct + pct_disability_pct + log_median_income",
            data=reg_m
        ).fit(cov_type="HC3")

    return {"M1": m1, "M2": m2, "M3r": m3r, "n_m12": len(reg), "n_m3r": len(reg_m)}


def fmt_coef(model, name):
    try:
        b = model.params[name]
        p = model.pvalues[name]
        return f"{b:>+8.4f} (p={p:.4f})"
    except KeyError:
        return "n/a"


def main():
    load_dotenv()

    csv_path = INGEST / "matched_rows_with_zip_2024.csv"
    if not csv_path.exists():
        sys.exit(f"missing {csv_path} — run build_matched_with_zip.py first")

    df = pd.read_csv(csv_path, dtype={"zip": str, "procedure_code": str})
    print(f"=== Loaded {csv_path.name} ===")
    print(f"  rows: {len(df):,}  hospitals: {df['hospital'].nunique()}  "
          f"OSHPD: {df['oshpd_id'].nunique()}  ZIPs: {df['zip'].nunique()}")

    # ── 1. Per-CPT price distribution ───────────────────────────────────
    print("\n=== Per-CPT price distribution (2024) ===")
    print(df.groupby(["procedure_code","cpt_group"])["charge_numeric"].agg(
        count="count", mean="mean", median="median", std="std"
    ).round(2).sort_values("median").to_string())

    # ── 2. Census pull ──────────────────────────────────────────────────
    census = pull_census()

    # ── 3. Mortality ────────────────────────────────────────────────────
    mort = load_mortality(year=2024)

    # ── 4. Build ZIP dataset + indexes ──────────────────────────────────
    z = build_zip_dataset(csv_path, census, mort)
    z = compute_indexes(z)

    print(f"\n=== ZIP-level dataset ready: {len(z):,} ZIPs ===")
    print(z[["avg_price_all","median_income","poverty_rate","pct_uninsured",
             "deaths_per_100k","idx_hvi","idx_poverty_burden"]].describe().round(2).to_string())

    # ── 5. Regressions ──────────────────────────────────────────────────
    print(f"\n=== Regression results (HC3 robust SE) ===")
    res = run_regressions(z)
    m1, m2, m3r = res["M1"], res["M2"], res["M3r"]

    print(f"\nM1  log(price) ~ vulnerability components   |  n={res['n_m12']}  R²={m1.rsquared:.3f}")
    for c in ["Intercept","poverty_rate_pct","pct_uninsured_pct","pct_elderly_pct",
              "pct_disability_pct","log_median_income"]:
        print(f"  {c:25s}: {fmt_coef(m1, c)}")

    print(f"\nM2  log(price) ~ idx_hvi + n_hospitals_zip  |  n={res['n_m12']}  R²={m2.rsquared:.3f}")
    for c in ["Intercept","idx_hvi","n_hospitals_zip"]:
        print(f"  {c:25s}: {fmt_coef(m2, c)}")

    if m3r is not None:
        print(f"\nM3r age-adj mortality ~ price + vuln      |  n={res['n_m3r']}  R²={m3r.rsquared:.3f}")
        for c in ["Intercept","avg_price_all","poverty_rate_pct","pct_uninsured_pct",
                  "pct_disability_pct","log_median_income"]:
            print(f"  {c:25s}: {fmt_coef(m3r, c)}")

    # ── 6. Side-by-side with Parvati's reported effect sizes ────────────
    print("\n=== Side-by-side vs Parvati's 2024 reported effects ===")
    print(f"                                      new corpus            Parvati")
    def coef(m, n):
        try: return f"{m.params[n]:+.4f}"
        except Exception: return "n/a"
    def pval(m, n):
        try: return f"p={m.pvalues[n]:.4f}"
        except Exception: return ""
    print(f"  M1  log_median_income coef      :  {coef(m1,'log_median_income'):>8}  {pval(m1,'log_median_income'):>12}     +0.54 (p≈0.001)")
    print(f"  M1  poverty_rate_pct coef       :  {coef(m1,'poverty_rate_pct'):>8}  {pval(m1,'poverty_rate_pct'):>12}     small, mixed sign")
    print(f"  M2  idx_hvi coef                :  {coef(m2,'idx_hvi'):>8}  {pval(m2,'idx_hvi'):>12}     significant weak")
    print(f"  M2  n_hospitals_zip coef        :  {coef(m2,'n_hospitals_zip'):>8}  {pval(m2,'n_hospitals_zip'):>12}     positive (p≈0.034) [more competition → higher price]")
    if m3r is not None:
        print(f"  M3r avg_price_all coef          :  {coef(m3r,'avg_price_all'):>8}  {pval(m3r,'avg_price_all'):>12}     +0.0017 (p<0.001) [per dollar]")
        print(f"  M3r pct_uninsured_pct coef      :  {coef(m3r,'pct_uninsured_pct'):>8}  {pval(m3r,'pct_uninsured_pct'):>12}     +0.74")

    print("\nNotes:")
    print("  - Parvati's reported effects taken from output_regression_summary.csv")
    print("    annotations in her notebook Block 7.4.")
    print("  - Differences are expected: her input CSV had ~308 ZIPs after her")
    print("    string-join; our OSHPD-join gives 210 ZIPs. Direction and significance")
    print("    should match; magnitudes may shift with the cleaner denominator.")


if __name__ == "__main__":
    main()
