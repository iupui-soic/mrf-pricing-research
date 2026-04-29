#!/usr/bin/env python3
"""
build_matched_with_zip.py
=========================
Reproducible replacement for the legacy `matched_rows_with_zip.csv`
used by Parvati's and Sravani's analysis notebooks.

Takes the ingest output at /data0/mrf-pricing-research/hcai-chargemasters/ingest/cdm_all.parquet,
joins OSHPD_ID -> ZIP from the HCAI Licensed Facility Listing (downloaded
automatically if not cached), filters to target CPT codes, and writes a
flat CSV in the schema Parvati's notebook expects:

    year, hospital, zip, procedure_code, description, charge_numeric

The CSV is written in two slices:
    matched_rows_with_zip_2024.csv   (single year — drop-in for Parvati)
    matched_rows_with_zip_all.csv    (2014–2025 — for longitudinal reuse)

Run:
    .venv/bin/python build_matched_with_zip.py
"""

from __future__ import annotations

import argparse
import os
import sys
import urllib.request
from pathlib import Path

import pandas as pd

INGEST_DIR   = Path("/data0/mrf-pricing-research/hcai-chargemasters/ingest")
FACILITY_URL = (
    "https://data.chhs.ca.gov/dataset/59d9abe7-2664-407a-a5aa-f89a866f3381"
    "/resource/641c5557-7d65-4379-8fea-6b7dedbda40b/download/"
    "current-healthcare-facility-listing.csv"
)
FACILITIES_RAW   = INGEST_DIR / "facilities_raw.csv"
FACILITIES_CLEAN = INGEST_DIR / "facilities.csv"

# Target CPT codes = Parvati's lists verbatim (excluding the "99281-99285"
# range-notation pseudo-code that normalize_code can't reconstruct).
EMERGENCY_CPTS = [
    "99281", "99282", "99283", "99284", "99285",  # ED E/M levels 1-5
    "99291", "99292",                              # Critical care
]
DELIVERY_CPTS = [
    "59400", "59409", "59410",                     # Vaginal delivery
    "59510", "59514", "59515",                     # C-section
    "59610", "59618", "59620", "59622",            # VBAC / trial of labor
    "59430",                                       # Postpartum care
]
ALL_TARGETS = set(EMERGENCY_CPTS + DELIVERY_CPTS)


def download_facilities():
    if FACILITIES_RAW.exists():
        print(f"[facilities] cached: {FACILITIES_RAW}")
        return
    INGEST_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[facilities] downloading from HCAI...")
    urllib.request.urlretrieve(FACILITY_URL, FACILITIES_RAW)
    print(f"[facilities] saved: {FACILITIES_RAW}")


def build_facilities_table() -> pd.DataFrame:
    """Return a clean OSHPD_ID -> (ZIP, name, city, county) lookup."""
    if FACILITIES_CLEAN.exists():
        df = pd.read_csv(FACILITIES_CLEAN, dtype=str)
        return df

    raw = pd.read_csv(FACILITIES_RAW, dtype=str)
    # Keep hospital-license rows. Chargemaster disclosures come from
    # licensed hospitals, not home health / clinic / LTC facilities.
    df = raw[raw["LICENSE_TYPE_DESC"] == "Hospital"].copy()
    df = df.rename(columns={
        "OSHPD_ID":           "oshpd_id",
        "FACILITY_NAME":      "facility_name",
        "DBA_ADDRESS1":       "address",
        "DBA_CITY":           "city",
        "DBA_ZIP_CODE":       "zip",
        "COUNTY_NAME":        "county",
        "LATITUDE":           "lat",
        "LONGITUDE":          "lon",
        "FACILITY_STATUS_DESC": "status",
        "LICENSE_CATEGORY_DESC": "license_category",
    })
    df["zip"] = df["zip"].astype(str).str.zfill(5).str[:5]
    keep = ["oshpd_id", "facility_name", "address", "city", "zip",
            "county", "lat", "lon", "status", "license_category"]
    df = df[keep].copy()
    df.to_csv(FACILITIES_CLEAN, index=False)
    print(f"[facilities] cleaned → {FACILITIES_CLEAN}  ({len(df):,} hospitals)")
    return df


def build_matched(facilities: pd.DataFrame, year: int | None) -> pd.DataFrame:
    """Join the ingest corpus to facility ZIPs and return a flat table.

    One row per (year, oshpd_id, CPT, charge_column). Columns renamed to
    match the schema Parvati's notebook expects (`charge_numeric`,
    `hospital`, `zip`).
    """
    parquet = INGEST_DIR / (f"cdm_{year}.parquet" if year else "cdm_all.parquet")
    if not parquet.exists():
        sys.exit(f"missing parquet: {parquet}")

    print(f"[read] {parquet}")
    df = pd.read_parquet(parquet, columns=[
        "year", "oshpd_id", "hospital_folder", "procedure_code",
        "code_type", "description", "charge", "charge_column", "setting",
    ])
    print(f"[read] {len(df):,} rows")

    # Filter to CPT target codes with positive charges
    df = df[(df["code_type"] == "CPT") & df["procedure_code"].isin(ALL_TARGETS)]
    df = df[df["charge"].fillna(0) > 0]
    print(f"[filter] target CPTs with positive charge: {len(df):,} rows")

    # Within-CPT p99 cap. Real ED/OB facility charges have per-code
    # distributions that max out in the low tens of thousands; the tail
    # above that is corrupt data (pharmacy quantities mis-keyed as
    # charges, tuition-dollar rows, etc.). Capping pooled across CPTs
    # would be too lax because delivery codes are systematically higher
    # than ED E/M. Parvati caps pooled, which is less principled but
    # close enough; this is tighter.
    n_before = len(df)
    df["_p99"] = df.groupby("procedure_code")["charge"].transform(
        lambda s: s.quantile(0.99))
    df = df[df["charge"] <= df["_p99"]].drop(columns="_p99").copy()
    print(f"[filter] within-CPT p99 cap: {len(df):,} rows (dropped {n_before-len(df):,})")

    # Join to ZIP — strict OSHPD match first, then a last-6-digit fallback
    # that catches common filename typos (e.g., 160190949 → 106190949 for
    # Henry Mayo Newhall Hospital).
    fac = facilities[["oshpd_id", "facility_name", "zip", "county", "city"]].copy()
    fac["oshpd_tail6"] = fac["oshpd_id"].str[-6:]

    df = df.merge(fac.drop(columns="oshpd_tail6"), on="oshpd_id", how="left")
    before = df["zip"].notna().sum()

    # Fallback on last 6 digits for rows that didn't match strict
    unmatched = df["zip"].isna() & df["oshpd_id"].notna()
    if unmatched.any():
        df["_tail"] = df["oshpd_id"].str[-6:]
        fallback = fac.set_index("oshpd_tail6")[["facility_name","zip","county","city"]]
        fb = df.loc[unmatched, "_tail"].map(fallback["zip"])
        df.loc[unmatched, "zip"]           = df.loc[unmatched, "zip"].fillna(fb)
        df.loc[unmatched, "facility_name"] = df.loc[unmatched, "facility_name"].fillna(
            df.loc[unmatched, "_tail"].map(fallback["facility_name"]))
        df.loc[unmatched, "county"]        = df.loc[unmatched, "county"].fillna(
            df.loc[unmatched, "_tail"].map(fallback["county"]))
        df.loc[unmatched, "city"]          = df.loc[unmatched, "city"].fillna(
            df.loc[unmatched, "_tail"].map(fallback["city"]))
        df = df.drop(columns="_tail")
    recovered = df["zip"].notna().sum() - before
    if recovered:
        print(f"[join] fallback (last-6-digit match) recovered {recovered} rows")

    zip_rate = df["zip"].notna().mean()
    print(f"[join] facility match rate: {zip_rate:.1%}  "
          f"({df['zip'].notna().sum():,}/{len(df):,})")

    # Fall back to hospital_folder when OSHPD is missing
    # (only a few percent typically; log for transparency)
    missing = df[df["zip"].isna()]
    if len(missing) > 0:
        print(f"[warn] {len(missing):,} rows lack OSHPD facility ID; "
              "these will be dropped for ZIP-level analysis")

    df = df.dropna(subset=["zip"]).copy()

    # Shape to Parvati's schema
    out = df.rename(columns={
        "facility_name": "hospital",
        "charge":        "charge_numeric",
    })
    out["cpt_group"] = out["procedure_code"].map(
        lambda c: "emergency" if c in EMERGENCY_CPTS else "delivery"
    )
    cols = ["year", "hospital", "oshpd_id", "zip", "county", "city",
            "procedure_code", "cpt_group", "description",
            "charge_column", "setting", "charge_numeric"]
    return out[cols]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, default=None,
                    help="limit to a single year (default: all)")
    ap.add_argument("--out-dir", type=Path, default=INGEST_DIR,
                    help="where to write the CSV")
    args = ap.parse_args()

    download_facilities()
    fac = build_facilities_table()

    # Build both single-year and full-corpus slices so downstream scripts
    # can pick whichever they need without reading all of cdm_all.
    targets = ([args.year] if args.year else [2024, "all"])

    for t in targets:
        if t == "all":
            df = build_matched(fac, None)
            out_path = args.out_dir / "matched_rows_with_zip_all.csv"
        else:
            df = build_matched(fac, t)
            out_path = args.out_dir / f"matched_rows_with_zip_{t}.csv"
        df.to_csv(out_path, index=False)
        print(f"[out] {out_path}  ({len(df):,} rows)")

        # Quick summary
        print("\n  Summary:")
        print(f"    unique hospitals: {df['hospital'].nunique()}")
        print(f"    unique OSHPD IDs: {df['oshpd_id'].nunique()}")
        print(f"    unique ZIPs:      {df['zip'].nunique()}")
        print(f"    unique counties:  {df['county'].nunique()}")
        print(f"    rows per CPT:")
        for code, n in df.groupby("procedure_code").size().sort_values(ascending=False).items():
            print(f"      {code}: {n:>6,}")
        print()


if __name__ == "__main__":
    main()
