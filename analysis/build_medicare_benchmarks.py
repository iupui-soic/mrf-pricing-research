#!/usr/bin/env python3
"""
analysis/build_medicare_benchmarks.py
=====================================
Builds a unified Medicare allowable table keyed on HCPCS / CPT.

Sources:
  - OPPS Addendum B (Jan 2026)  → HCPCS code, APC, Payment Rate
  - MPFS PPRRVU 2026 Jan         → CPT/HCPCS, Total Non-Facility RVU * CF

Output:
  /data0/mrf-pricing-research/medicare/medicare_cpt_2026.parquet
    columns: code, opps_payment, mpfs_national, source

Methodology note:
  - OPPS Payment Rate is the national unadjusted payment per code.
  - MPFS national = (Work + Non-Facility PE + MP) * CF_2026.
    The 2026 conversion factor (non-MIPS) is taken from the CMS Final
    Rule and set as MPFS_CF below. For the headline ratio analysis we
    prefer OPPS where present (reflects how outpatient hospital services
    are actually paid) and fall back to MPFS.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

OUT = Path("/data0/mrf-pricing-research/medicare/medicare_cpt_2026.parquet")

OPPS_XLSX = Path(
    "/data0/mrf-pricing-research/medicare/extracted/addendum_b_2026_jan/"
    "2026 January Web Addendum B.12.29.25.xlsx"
)
MPFS_CSV  = Path(
    "/data0/mrf-pricing-research/medicare/extracted/rvu26a/PPRRVU2026_Jan_nonQPP.csv"
)

# MPFS conversion factor used in the deposited corpus (Zenodo record
# 19941038). This value is $33.2875 — the CY 2024 final-rule CF for dates
# of service Mar 9–Dec 31 2024 — used as a placeholder during corpus
# construction (data collection window Q4 2025–Q1 2026), which preceded
# the CY 2026 Final Rule release on 2025-10-31. The CY 2026 Final Rule
# (CMS-1832-F, effective 2026-01-01) subsequently set the non-QP CF to
# $33.40 ($33.57 for QP participants). The maximum impact of correcting
# the CF on headline state x price-type medians is 0.14% (on CA min-neg);
# every reported median in the manuscript rounds identically under both
# CFs at the precision shown. We retain $33.2875 here so the script
# reproduces the Zenodo-deposited parquets byte-identically; the next
# corpus release will use MPFS_CF_2026 = 33.40 and refresh the deposit.
MPFS_CF_2026 = 33.2875


def load_opps() -> pd.DataFrame:
    df = pd.read_excel(OPPS_XLSX, skiprows=5, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    df = df[["HCPCS Code", "APC", "Payment Rate"]].copy()
    df = df.rename(columns={
        "HCPCS Code": "code",
        "APC": "apc",
        "Payment Rate": "opps_payment",
    })
    df["code"] = df["code"].astype(str).str.strip().str.upper()
    df["opps_payment"] = pd.to_numeric(df["opps_payment"], errors="coerce")
    df = df.dropna(subset=["code"])
    df = df[df["code"].str.fullmatch(r"[0-9A-Z]{5}")].copy()
    df = df.dropna(subset=["opps_payment"])
    df = df.sort_values("opps_payment").drop_duplicates("code", keep="last")
    print(f"[opps] {len(df):,} HCPCS codes with payment rate")
    return df[["code", "apc", "opps_payment"]]


def load_mpfs() -> pd.DataFrame:
    """Read PPRRVU. CMS publishes with 9 leading metadata rows; the actual
    column row is the 10th. Codes are 5-char HCPCS/CPT; the file has
    duplicate rows for facility vs non-facility — we keep non-facility."""
    raw = pd.read_csv(MPFS_CSV, dtype=str, skiprows=9, low_memory=False)
    raw.columns = [str(c).strip() for c in raw.columns]

    # Numeric coercion helpers
    def num(s):
        return pd.to_numeric(s.astype(str).str.replace(",", ""), errors="coerce")

    # The PPRRVU layout has split RVU columns; the canonical "TOTAL"
    # non-facility RVU is one of the TOTAL columns. We compute defensively
    # from work + non-facility PE + malpractice and cross-check.
    # Column names per CMS layout (positions, not labels, are the contract).
    cols = list(raw.columns)
    # Standard CMS PPRRVU layout: HCPCS, MOD, DESCRIPTION, STATUS, NOT_USED,
    # WORK_RVU, NON_FAC_PE_RVU, NON_FAC_PE_IND, FAC_PE_RVU, FAC_PE_IND,
    # MP_RVU, NON_FAC_TOTAL, FAC_TOTAL, ...
    # Column 0 = HCPCS, 5 = WORK, 6 = NON_FAC_PE, 10 = MP_RVU, 11 = NON_FAC_TOTAL.
    df = pd.DataFrame({
        "code":     raw.iloc[:, 0].astype(str).str.strip().str.upper(),
        "work":     num(raw.iloc[:, 5]),
        "nf_pe":    num(raw.iloc[:, 6]),
        "mp":       num(raw.iloc[:, 10]),
        "nf_total": num(raw.iloc[:, 11]),
    })
    df = df[df["code"].str.fullmatch(r"[0-9A-Z]{5}")].copy()

    # Prefer the published TOTAL non-facility RVU; if missing, sum components.
    rvu = df["nf_total"].where(df["nf_total"].notna(),
                               df["work"].fillna(0) + df["nf_pe"].fillna(0)
                               + df["mp"].fillna(0))
    df["mpfs_national"] = (rvu * MPFS_CF_2026).round(2)
    df = df.dropna(subset=["mpfs_national"])
    df = df[df["mpfs_national"] > 0]
    df = df.sort_values("mpfs_national").drop_duplicates("code", keep="last")
    print(f"[mpfs] {len(df):,} HCPCS/CPT codes with non-facility allowable "
          f"(CF={MPFS_CF_2026})")
    return df[["code", "mpfs_national"]]


def main():
    opps = load_opps()
    mpfs = load_mpfs()
    merged = opps.merge(mpfs, on="code", how="outer")
    merged["source"] = (
        merged["opps_payment"].notna().map({True: "opps", False: ""})
        + merged["mpfs_national"].notna().map({True: "mpfs", False: ""}).where(
            merged["opps_payment"].isna(), "")
    )
    merged.loc[merged["opps_payment"].notna() & merged["mpfs_national"].notna(),
               "source"] = "opps+mpfs"
    merged.loc[merged["opps_payment"].notna() & merged["mpfs_national"].isna(),
               "source"] = "opps"
    merged.loc[merged["opps_payment"].isna() & merged["mpfs_national"].notna(),
               "source"] = "mpfs"

    # Pick a single benchmark per code: OPPS preferred, MPFS fallback
    merged["medicare_allowable"] = merged["opps_payment"].fillna(merged["mpfs_national"])

    out_cols = ["code", "apc", "opps_payment", "mpfs_national",
                "medicare_allowable", "source"]
    merged[out_cols].to_parquet(OUT, index=False)
    print(f"[out] {OUT}  ({len(merged):,} codes)")
    print("\n  Source breakdown:")
    print(merged["source"].value_counts().to_string())
    print(f"\n  Median allowable: ${merged['medicare_allowable'].median():,.2f}")
    print(f"  Mean   allowable: ${merged['medicare_allowable'].mean():,.2f}")


if __name__ == "__main__":
    main()
