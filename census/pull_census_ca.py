#!/usr/bin/env python3
"""
census/pull_census_ca.py
========================
Pulls ACS 5-year demographics for California ZIP codes and writes the
cache file consumed by the CA chargemaster + analysis pipelines.

Output:
  /data0/mrf-pricing-research/hcai-chargemasters/ingest/cache_census_zip_2024.csv
    columns: zip, median_income, total_pop, poverty_rate,
             pct_uninsured, pct_disability, pct_elderly

This is the CA-side counterpart of `census/pull_census_in.py`. The
ACS vintage (2024 5-year) and the four numeric columns shared with the
IN-side file are kept aligned so the CA-vs-IN ZIP panel in
`analysis/chang_psek_regression.py` is comparable across states.

Source:
  api.census.gov ACS 2024 5-year. Two endpoints are pulled:
    - detailed tables (B*) for income / total population / poverty
    - subject tables (S*) for percent uninsured / disability / 65+
  All US ZIP-code tabulation areas (ZCTA) are returned; we keep CA only
  by joining against /data0/mrf-pricing-research/mrf/hospitals.csv to
  get the set of CA hospital ZIPs, then keep all CA ZIPs (the cache
  used to populate CA chargemaster pipeline downstream is a full-CA
  cache, not just hospital-ZIPs).

Run:
  .venv/bin/python census/pull_census_ca.py
"""

from __future__ import annotations

import json
import urllib.request
import urllib.parse
from pathlib import Path

import pandas as pd

OUT_CSV    = Path(
    "/data0/mrf-pricing-research/hcai-chargemasters/ingest/cache_census_zip_2024.csv"
)
CACHE_DIR  = Path("/data0/mrf-pricing-research/census")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
ACS_DETAIL_CACHE  = CACHE_DIR / "ca_acs_detail_raw.json"
ACS_SUBJECT_CACHE = CACHE_DIR / "ca_acs_subject_raw.json"

ACS_YEAR = 2024

# Detailed tables: median income, poverty count, total population
DETAIL_VARS = [
    "B19013_001E",  # median household income
    "B17001_002E",  # population below poverty level
    "B01003_001E",  # total population
]

# Subject tables: percentages — easier than summing detailed cells.
SUBJECT_VARS = [
    "S2701_C05_001E",  # Percent uninsured (civilian noninst. population)
    "S1810_C03_001E",  # Percent with a disability (civilian noninst. population)
    "S0101_C02_030E",  # Percent 65 years and over (total population)
]

# CA state FIPS = 06 — used to filter ZCTAs to those overlapping CA.
CA_STATE_FIPS = "06"


def _fetch(url: str, cache: Path) -> list:
    if cache.exists():
        print(f"[cache] {cache}")
        return json.loads(cache.read_text())
    print(f"[fetch] {url}")
    with urllib.request.urlopen(url) as r:
        data = json.load(r)
    cache.write_text(json.dumps(data))
    return data


def pull_acs_detail() -> pd.DataFrame:
    url = (
        f"https://api.census.gov/data/{ACS_YEAR}/acs/acs5"
        f"?get=NAME,{','.join(DETAIL_VARS)}"
        f"&for=zip%20code%20tabulation%20area:*"
    )
    data = _fetch(url, ACS_DETAIL_CACHE)
    df = pd.DataFrame(data[1:], columns=data[0])
    df = df.rename(columns={
        "zip code tabulation area": "zip",
        "B19013_001E": "median_income",
        "B17001_002E": "pop_below_poverty",
        "B01003_001E": "total_pop",
    })
    for col in ("median_income", "pop_below_poverty", "total_pop"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[col] = df[col].where(df[col] >= 0)  # -666666666 = suppressed
    df["zip"] = df["zip"].astype(str).str.zfill(5)
    df["poverty_rate"] = df["pop_below_poverty"] / df["total_pop"]
    return df[["zip", "median_income", "total_pop", "poverty_rate"]]


def pull_acs_subject() -> pd.DataFrame:
    url = (
        f"https://api.census.gov/data/{ACS_YEAR}/acs/acs5/subject"
        f"?get=NAME,{','.join(SUBJECT_VARS)}"
        f"&for=zip%20code%20tabulation%20area:*"
    )
    data = _fetch(url, ACS_SUBJECT_CACHE)
    df = pd.DataFrame(data[1:], columns=data[0])
    df = df.rename(columns={
        "zip code tabulation area": "zip",
        "S2701_C05_001E": "pct_uninsured",
        "S1810_C03_001E": "pct_disability",
        "S0101_C02_030E": "pct_elderly",
    })
    for col in ("pct_uninsured", "pct_disability", "pct_elderly"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
        # Subject-table percentages return as 0-100; downstream uses 0-1.
        df[col] = (df[col].where(df[col] >= 0) / 100).round(3)
    df["zip"] = df["zip"].astype(str).str.zfill(5)
    return df[["zip", "pct_uninsured", "pct_disability", "pct_elderly"]]


def ca_zips() -> set[str]:
    """CA ZCTAs: any ZCTA whose centroid falls in CA. We approximate by
    picking ZCTAs starting with 9 (CA prefix range) — strict-correct
    method needs the ZCTA-to-state crosswalk file. The legacy cache used
    a similar prefix-based filter."""
    # 90000-96199 = CA range per USPS.
    return {f"{z:05d}" for z in range(90000, 96200)}


def main():
    detail  = pull_acs_detail()
    subject = pull_acs_subject()
    df = detail.merge(subject, on="zip", how="left")

    ca = df[df["zip"].isin(ca_zips())].copy()
    print(f"[filter] {len(ca):,} CA ZCTAs (of {len(df):,} national ZCTAs)")

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    ca.to_csv(OUT_CSV, index=False)
    print(f"[out] {OUT_CSV}  ({len(ca):,} rows)")
    print("\n  Summary:")
    print(f"    Median income range: ${ca.median_income.min():,.0f} – "
          f"${ca.median_income.max():,.0f}")
    print(f"    Mean poverty rate:   {ca.poverty_rate.mean():.1%}")
    print(f"    Mean uninsured:      {ca.pct_uninsured.mean():.1%}")


if __name__ == "__main__":
    main()
