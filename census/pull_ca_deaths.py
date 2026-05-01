#!/usr/bin/env python3
"""
census/pull_ca_deaths.py
========================
Pulls the CDPH "Death Profiles by ZIP Code" 2019-2024 file from the CHHS
CKAN endpoint into the chargemaster ingest cache.

Output:
  /data0/mrf-pricing-research/hcai-chargemasters/ingest/ca_deaths_zip_2019-2024.csv
    columns: Year, ZIP_Code, Geography_Type, Strata, Strata_Name, Cause,
             Cause_Desc, ICD_Revision, Count, Annotation_Code,
             Annotation_Desc, Data_Revision_Date

The Indiana side has no equivalent ZIP-level mortality file — Indiana's
public mortality release is at the county level, not ZIP. The CA-vs-IN
analysis in `analysis/chang_psek_regression.py` therefore does not use
this file; it is retained for CA-only mortality analyses.

Source:
  CHHS Open Data  package: death-profiles-by-zip-code
                  resource: 2019-2024 Final Deaths by Year by ZIP Code
  https://data.chhs.ca.gov/dataset/death-profiles-by-zip-code

Implementation notes:
  - CHHS resource URLs include the publication date in the filename, so
    they change with each annual update. We resolve the latest URL via
    the CKAN package_search endpoint each run and pick the resource
    whose name starts with the requested year range, falling back to a
    pinned URL if the API is unreachable.

Run:
  .venv/bin/python census/pull_ca_deaths.py
"""

from __future__ import annotations

import json
import urllib.request
import urllib.parse
from pathlib import Path

OUT_CSV = Path(
    "/data0/mrf-pricing-research/hcai-chargemasters/ingest/"
    "ca_deaths_zip_2019-2024.csv"
)

CKAN_SEARCH = (
    "https://data.chhs.ca.gov/api/3/action/package_search?q=death+zip"
)
RESOURCE_NAME_PREFIX = "2019-2024 Final Deaths"

# Pinned URL — known good as of 2026-04-30. Used only if the CKAN search
# fails. CHHS rotates the date prefix in the filename annually.
PINNED_URL = (
    "https://data.chhs.ca.gov/dataset/590aeff1-b022-4240-9a46-3fe90bf3ad91/"
    "resource/d4711b8e-6eb4-417c-91f5-ee075558adbe/download/"
    "20260319_deaths_final_2019-2024_zip_year_sup.csv"
)


def discover_url() -> str:
    """Resolve the latest CSV URL via CKAN. Returns PINNED_URL on failure."""
    try:
        with urllib.request.urlopen(CKAN_SEARCH, timeout=30) as r:
            data = json.load(r)
    except Exception as e:
        print(f"[ckan] search failed ({e}); using pinned URL")
        return PINNED_URL

    if not data.get("success"):
        print(f"[ckan] non-success response; using pinned URL")
        return PINNED_URL

    for pkg in data["result"]["results"]:
        if pkg.get("name") != "death-profiles-by-zip-code":
            continue
        for res in pkg.get("resources", []):
            if (res.get("format", "").upper() == "CSV"
                and res.get("name", "").startswith(RESOURCE_NAME_PREFIX)):
                print(f"[ckan] resolved resource: {res['name']!r}")
                return res["url"]

    print("[ckan] expected resource not found; using pinned URL")
    return PINNED_URL


def main():
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    url = discover_url()
    print(f"[fetch] {url}")
    print(f"[dest]  {OUT_CSV}")
    urllib.request.urlretrieve(url, OUT_CSV)
    n_lines = sum(1 for _ in open(OUT_CSV))
    print(f"[done]  {n_lines:,} lines written")


if __name__ == "__main__":
    main()
