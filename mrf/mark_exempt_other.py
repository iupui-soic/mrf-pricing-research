#!/usr/bin/env python3
"""
mark_exempt_other.py
====================
Apply exemptions from `/data0/mrf-pricing-research/mrf/exempt_other.csv` to `mrf_urls.csv`.

`exempt_other.csv` collects all non-federal exemption findings from the
discovery pipeline:

  - `closed`              — facility closed (Medicare contract terminated)
  - `ca_phf_exempt`       — CA Psychiatric Health Facility, not Medicare-
                            certified hospital under §1861(e)
  - `small_rural_likely`  — qualifies for CA chargemaster small-rural
                            exemption
  - `gated_portal`        — file exists but only via PARA / Vitalware /
                            PatientSimple JS portals (no static URL)
  - `non_compliant`       — only an obsolete chargemaster posted, not the
                            CMS standardcharges format
  - `payer_only`          — facility directs to payer-side TIC portal
                            instead of publishing its own MRF
  - `no_mrf_published`    — confirmed by manual research that no MRF is
                            posted on the facility's site

Each row is rewritten in `mrf_urls.csv` with `discovery_method=exempt:<reason>`
so it is preserved across re-runs of `seed_known_urls.py` (which honors any
discovery_method starting with `exempt`).

Usage:
    .venv/bin/python mrf/mark_exempt_other.py
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd

OUT_DIR = Path("/data0/mrf-pricing-research/mrf")
HOSP_CSV = OUT_DIR / "hospitals.csv"
URL_CSV = OUT_DIR / "mrf_urls.csv"
EXEMPT_CSV = OUT_DIR / "exempt_other.csv"


def main() -> None:
    hosp = pd.read_csv(HOSP_CSV, dtype=str).set_index("ccn")
    exempt = pd.read_csv(EXEMPT_CSV, dtype=str)
    print(f"[load] {len(exempt)} exemption rows from {EXEMPT_CSV.name}")

    now = dt.datetime.now(dt.UTC).isoformat()
    rows = []
    for _, r in exempt.iterrows():
        ccn = r["ccn"]
        if ccn not in hosp.index:
            print(f"  [warn] ccn {ccn} not in hospitals.csv — skipping")
            continue
        h = hosp.loc[ccn]
        rows.append({
            "ccn": ccn,
            "name": h["name"],
            "state": h["state"],
            "zip": h["zip"],
            "website": None,
            "mrf_url": None,
            "mrf_format": None,
            "mrf_size_bytes": None,
            "discovery_method": f"exempt:{r['exemption_reason']}",
            "http_status": None,
            "discovered_at": now,
            "notes": r["evidence"],
        })
    new = pd.DataFrame(rows)

    if URL_CSV.exists():
        prev = pd.read_csv(URL_CSV, dtype=str)
        prev_n = len(prev)
        prev = prev[~prev["ccn"].isin(new["ccn"])]
        merged = pd.concat([prev, new], ignore_index=True)
        merged.to_csv(URL_CSV, index=False)
        print(f"[merge] {URL_CSV}: {prev_n} → {len(merged)} rows "
              f"({len(new)} exempt added/replaced)")
    else:
        new.to_csv(URL_CSV, index=False)
        print(f"[out] {URL_CSV}  ({len(new)} rows)")


if __name__ == "__main__":
    main()
