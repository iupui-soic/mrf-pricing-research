#!/usr/bin/env python3
"""
census/pull_census_in.py
========================
Pulls ACS 5-year Census demographics + CDC PLACES health outcomes
for Indiana ZIP codes, mirroring the CA pipeline structure.

Reads IN hospital ZIPs from /data0/mrf-pricing-research/mrf/hospitals.csv (already built
by mrf/build_hospital_list.py), pulls ACS 5-year and CDC PLACES data
for those ZIPs, and writes:

    /data0/mrf-pricing-research/census/in_zip_demographics.parquet
    /data0/mrf-pricing-research/census/in_zip_health_outcomes.parquet

The PLACES output holds modeled prevalence rates from BRFSS, not death
counts; the CA-side death-records join lives in compare_to_parvati.py
and remains state-specific.

Run:
    .venv/bin/python census/pull_census_in.py
"""

from __future__ import annotations

import json
import urllib.request
import urllib.parse
from pathlib import Path

import pandas as pd

# ── paths ──────────────────────────────────────────────────────────────────
HOSPITALS_CSV = Path("/data0/mrf-pricing-research/mrf/hospitals.csv")
OUT_DIR       = Path("/data0/mrf-pricing-research/census")
OUT_DIR.mkdir(parents=True, exist_ok=True)

ACS_CACHE     = OUT_DIR / "in_acs_raw.json"
PLACES_CACHE  = OUT_DIR / "in_places_raw.csv"

OUT_DEMO   = OUT_DIR / "in_zip_demographics.parquet"
OUT_HEALTH = OUT_DIR / "in_zip_health_outcomes.parquet"

# ACS 5-year vintage. 2024 release covers 2020-2024 and matches the
# CA-side pull in compare_to_parvati.py — keep these aligned so the
# CA-vs-IN comparison isn't confounded by ACS period.
ACS_YEAR = 2024

# ── ACS variables ───────────────────────────────────────────────────────────
ACS_VARS = [
    "B19013_001E",  # median household income
    "B17001_002E",  # population below poverty level
    "B01003_001E",  # total population
    "B02001_002E",  # white alone
    "B02001_003E",  # Black or African American alone
]

ACS_URL = (
    f"https://api.census.gov/data/{ACS_YEAR}/acs/acs5"
    f"?get=NAME,{','.join(ACS_VARS)}"
    f"&for=zip%20code%20tabulation%20area:*"
)

# CDC PLACES ZCTA open-data endpoint — filter by specific ZIPs via WHERE clause
CDC_BASE = "https://data.cdc.gov/resource/qnzd-25i4.csv"


def load_in_zips() -> set[str]:
    """Get the set of ZIPs for IN hospitals from hospitals.csv."""
    if not HOSPITALS_CSV.exists():
        raise FileNotFoundError(
            f"Missing {HOSPITALS_CSV} — run mrf/build_hospital_list.py first"
        )
    hosp = pd.read_csv(HOSPITALS_CSV, dtype=str)
    in_zips = hosp[hosp["state"] == "IN"]["zip"].dropna().unique()
    in_zips = {z.zfill(5)[:5] for z in in_zips}
    print(f"[zips] {len(in_zips)} unique IN hospital ZIPs from hospitals.csv")
    return in_zips


def pull_acs(in_zips: set[str]) -> pd.DataFrame:
    """Pull ACS 5-year demographics and filter to IN ZIPs."""
    if ACS_CACHE.exists():
        print(f"[acs] cached: {ACS_CACHE}")
        with open(ACS_CACHE) as f:
            data = json.load(f)
    else:
        print(f"[acs] downloading from Census API (all US ZIPs)...")
        with urllib.request.urlopen(ACS_URL) as r:
            data = json.load(r)
        with open(ACS_CACHE, "w") as f:
            json.dump(data, f)
        print(f"[acs] saved: {ACS_CACHE}")

    headers = data[0]
    rows    = data[1:]
    df = pd.DataFrame(rows, columns=headers)

    df = df.rename(columns={
        "zip code tabulation area": "zip",
        "B19013_001E": "median_household_income",
        "B17001_002E": "pop_below_poverty",
        "B01003_001E": "total_population",
        "B02001_002E": "pop_white",
        "B02001_003E": "pop_black",
    })

    num_cols = [
        "median_household_income", "pop_below_poverty",
        "total_population", "pop_white", "pop_black",
    ]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Census uses -666666666 as sentinel for missing/suppressed values
    for col in num_cols:
        df[col] = df[col].where(df[col] >= 0, other=pd.NA)

    df["zip"] = df["zip"].astype(str).str.zfill(5)
    df = df[df["zip"].isin(in_zips)].copy()

    df["pct_poverty"] = df["pop_below_poverty"] / df["total_population"]
    df["pct_white"]   = df["pop_white"]          / df["total_population"]
    df["pct_black"]   = df["pop_black"]          / df["total_population"]
    df["state"]       = "IN"

    keep = [
        "zip", "state", "total_population",
        "median_household_income", "pct_poverty",
        "pct_white", "pct_black",
    ]
    df = df[keep].copy()
    print(f"[acs] {len(df):,} IN hospital ZIPs matched to Census data")
    return df


def pull_health_outcomes(in_zips: set[str]) -> pd.DataFrame:
    """Pull CDC PLACES ZCTA health outcomes filtered to IN ZIPs via API.

    PLACES values are BRFSS-modeled prevalence rates, not death counts —
    that is why this is named health_outcomes, not mortality.
    """
    if PLACES_CACHE.exists():
        print(f"[places] cached: {PLACES_CACHE}")
        df = pd.read_csv(PLACES_CACHE, dtype=str)
        df["zip"]   = df["zip"].astype(str).str.zfill(5)
        df["health_outcome_rate"] = pd.to_numeric(
            df["health_outcome_rate"], errors="coerce"
        )
        return df

    # Socrata $where: locationname in('46001','46002',...)
    zip_list = ",".join(f"'{z}'" for z in sorted(in_zips))
    where    = f"locationname in({zip_list})"
    params   = urllib.parse.urlencode({
        "$where": where,
        "$limit": "50000",
    })
    url = f"{CDC_BASE}?{params}"

    print(f"[places] downloading from CDC PLACES (filtered to IN ZIPs)...")
    urllib.request.urlretrieve(url, PLACES_CACHE)
    df = pd.read_csv(PLACES_CACHE, dtype=str)
    print(f"[places] saved: {PLACES_CACHE}  ({len(df):,} raw rows)")

    df = df.rename(columns={
        "locationname": "zip",
        "data_value":   "health_outcome_rate",
        "measure":      "measure",
    })

    df["zip"]   = df["zip"].astype(str).str.zfill(5)
    df["state"] = "IN"

    keep = ["zip", "state", "measure", "health_outcome_rate"]
    df   = df[[c for c in keep if c in df.columns]].copy()
    df["health_outcome_rate"] = pd.to_numeric(
        df["health_outcome_rate"], errors="coerce"
    )

    print(f"[places] {len(df):,} IN ZIP health records from CDC PLACES")
    return df


def main():
    in_zips = load_in_zips()

    acs = pull_acs(in_zips)
    acs.to_parquet(OUT_DEMO, index=False)
    print(f"[out] {OUT_DEMO}  ({len(acs):,} rows)")

    health = pull_health_outcomes(in_zips)
    health.to_parquet(OUT_HEALTH, index=False)
    print(f"[out] {OUT_HEALTH}  ({len(health):,} rows)")

    print("\n  Summary:")
    print(f"    IN hospital ZIPs:                   {len(in_zips)}")
    print(f"    IN ZIPs with Census data:           {len(acs)}")
    print(f"    IN ZIPs with health-outcome data:   {health['zip'].nunique()}")
    if len(acs) > 0:
        print(f"    Median income range:  "
              f"${acs['median_household_income'].min():,.0f} – "
              f"${acs['median_household_income'].max():,.0f}")
        print(f"    Avg poverty rate:     "
              f"{acs['pct_poverty'].mean():.1%}")
    if len(health) > 0:
        print(f"    Sample ZIPs in PLACES:    "
              f"{health['zip'].head(5).tolist()}")
        print(f"    Unique measures:          {health['measure'].nunique()}")


if __name__ == "__main__":
    main()