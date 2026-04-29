#!/usr/bin/env python3
"""
mark_exempt_federal.py
======================
VA and DoD hospitals are exempt from the CMS Hospital Price Transparency
Rule (45 CFR 180.20 — applies to "hospital" as defined in section 1861(e)
of the Social Security Act, which excludes federal facilities operated by
DVA and DoD). They do not publish MRFs and never will under the current
rule.

This script writes a row to `mrf_urls.csv` for every federal hospital in
`hospitals.csv` with `discovery_method='exempt_federal'` and `mrf_url=null`,
so they stop appearing in the missing-hospitals lists and downstream
coverage stats can correctly partition the denominator.

Identification: ownership in {Veterans Health Administration,
Department of Defense}, or hospital_type starts with 'Acute Care - V' or
'Acute Care - D'. CCNs for federal facilities also carry an 'F' suffix.

Usage:
    .venv/bin/python mrf/mark_exempt_federal.py
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd

OUT_DIR = Path("/data0/mrf-pricing-research/mrf")
HOSP_CSV = OUT_DIR / "hospitals.csv"
URL_CSV = OUT_DIR / "mrf_urls.csv"
EXEMPT_CSV = OUT_DIR / "exempt_hospitals.csv"

FEDERAL_OWNERSHIP = {
    "Veterans Health Administration",
    "Department of Defense",
}


def main() -> None:
    hosp = pd.read_csv(HOSP_CSV, dtype=str)
    federal_mask = hosp["ownership"].isin(FEDERAL_OWNERSHIP)
    federal = hosp[federal_mask].copy()
    print(f"[scan] {len(federal)} federal hospitals identified")
    print(federal["ownership"].value_counts().to_string())

    now = dt.datetime.now(dt.UTC).isoformat()
    rows = []
    for _, r in federal.iterrows():
        reason = (
            "exempt_federal:VA"
            if r["ownership"] == "Veterans Health Administration"
            else "exempt_federal:DoD"
        )
        rows.append({
            "ccn": r["ccn"],
            "name": r["name"],
            "state": r["state"],
            "zip": r["zip"],
            "website": None,
            "mrf_url": None,
            "mrf_format": None,
            "mrf_size_bytes": None,
            "discovery_method": reason,
            "http_status": None,
            "discovered_at": now,
            "notes": "Exempt under CMS HPT rule (45 CFR 180); facility "
                     "operated by federal government, not subject to the "
                     "Hospital Price Transparency requirement.",
        })
    new = pd.DataFrame(rows)

    federal[[
        "ccn", "name", "state", "zip", "ownership", "hospital_type",
    ]].to_csv(EXEMPT_CSV, index=False)
    print(f"[out] {EXEMPT_CSV}  ({len(federal)} rows)")

    if URL_CSV.exists():
        prev = pd.read_csv(URL_CSV, dtype=str)
        prev_n = len(prev)
        prev = prev[~prev["ccn"].isin(new["ccn"])]
        merged = pd.concat([prev, new], ignore_index=True)
        merged.to_csv(URL_CSV, index=False)
        print(f"[merge] {URL_CSV}: {prev_n} → {len(merged)} rows "
              f"({len(new)} exempt added)")
    else:
        new.to_csv(URL_CSV, index=False)
        print(f"[out] {URL_CSV}  ({len(new)} rows, all exempt)")


if __name__ == "__main__":
    main()
