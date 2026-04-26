#!/usr/bin/env python3
"""
build_hospital_list.py
======================
Build the master hospital list for MRF crawling.

Pulls CMS Hospital General Information, filters to CA + IN, dedup on CCN,
and writes `hospitals.csv` — the input to `discover_mrf_urls.py`.

Columns retained:
    ccn            CMS Certification Number (6-char, the `Facility ID`)
    name           facility name
    address        street address
    city, state    city and state abbreviation
    zip            5-digit ZIP
    hospital_type  Acute Care / Critical Access / Children's / Psychiatric
    ownership      Government / Proprietary / Voluntary nonprofit / etc.
    has_ed         "Yes" if emergency services present

Only keeps LICENSE_TYPE == "Hospital"-equivalent: "Acute Care Hospitals",
"Critical Access Hospitals", and "Children's Hospitals" (these are the
inpatient facilities required by CMS-1717-F2 to publish MRFs).

Usage:
    .venv/bin/python mrf/build_hospital_list.py
"""

from __future__ import annotations

import sys
import urllib.request
from pathlib import Path

import pandas as pd

CMS_HOSPITAL_GEN_INFO_URL = (
    "https://data.cms.gov/provider-data/sites/default/files/resources/"
    "893c372430d9d71a1c52737d01239d47_1770163599/"
    "Hospital_General_Information.csv"
)

OUT_DIR = Path("/data0/mrf")
RAW_CSV = OUT_DIR / "hospital_general_info_raw.csv"
OUT_CSV = OUT_DIR / "hospitals.csv"

INCLUDE_TYPES = {
    "Acute Care Hospitals",
    "Acute Care - Department of Defense",
    "Acute Care - Veterans Administration",
    "Critical Access Hospitals",
    "Childrens",  # CMS spells it without apostrophe
    "Psychiatric",  # included; CMS compliance applies to many psych hospitals
    "Rural Emergency Hospital",
}

STATES = {"CA", "IN"}


def download_if_missing(url: str, dst: Path):
    if dst.exists():
        print(f"[cache] {dst}")
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    print(f"[fetch] {url}")
    urllib.request.urlretrieve(url, dst)
    print(f"[saved] {dst}  ({dst.stat().st_size/1024/1024:.1f} MB)")


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    download_if_missing(CMS_HOSPITAL_GEN_INFO_URL, RAW_CSV)

    df = pd.read_csv(RAW_CSV, dtype=str, low_memory=False)
    df.columns = [c.strip() for c in df.columns]

    # Rename for our schema
    df = df.rename(columns={
        "Facility ID":         "ccn",
        "Facility Name":       "name",
        "Address":             "address",
        "City/Town":           "city",
        "State":               "state",
        "ZIP Code":            "zip",
        "County/Parish":       "county",
        "Hospital Type":       "hospital_type",
        "Hospital Ownership":  "ownership",
        "Emergency Services":  "has_ed",
    })

    print(f"[raw] {len(df):,} rows")
    print(f"  hospital types: {df['hospital_type'].value_counts().to_dict()}")
    print(f"  states present: {df['state'].nunique()} unique")

    # Filter to in-scope states + in-scope hospital types
    df = df[df["state"].isin(STATES)].copy()
    print(f"[filter] CA+IN: {len(df):,} rows")

    df = df[df["hospital_type"].isin(INCLUDE_TYPES)].copy()
    print(f"[filter] acute/CAH/children's/psych: {len(df):,} rows")

    # Normalize ZIP to 5-digit string
    df["zip"] = df["zip"].astype(str).str.zfill(5).str[:5]

    # De-dupe on CCN (some rows are multi-row per facility)
    df = df.drop_duplicates(subset="ccn").copy()
    print(f"[dedup] unique CCNs: {len(df):,}")

    # Keep only the useful columns
    keep = ["ccn", "name", "address", "city", "state", "zip",
            "county", "hospital_type", "ownership", "has_ed"]
    df = df[keep].sort_values(["state", "ccn"]).reset_index(drop=True)

    df.to_csv(OUT_CSV, index=False)
    print(f"[out] {OUT_CSV}  ({len(df):,} hospitals)")

    print("\nBy state:")
    print(df["state"].value_counts().to_string())
    print("\nBy hospital type:")
    print(df["hospital_type"].value_counts().to_string())


if __name__ == "__main__":
    main()
