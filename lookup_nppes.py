#!/usr/bin/env python3
"""
lookup_nppes.py
===============
Fills NPI gaps for hospitals where MRF metadata didn't carry one (i.e.,
v2.0 / v1.x files where `type_2_npi` doesn't exist) by querying the
public NPPES NPI Registry API.

API: https://npiregistry.cms.hhs.gov/api/?version=2.1&...
We use NPI-2 (organization) lookups by `organization_name + state +
postal_code`. Matching strategy:
  1. Tight: exact 5-digit ZIP + name fuzzy similarity ≥ 0.55
  2. Loose: state-only + name fuzzy ≥ 0.85
A 0.55–0.85 match without ZIP support is logged as ambiguous, not used.

Output: /data0/crosswalk/ccn_to_npi_nppes.csv (ccn, npi, match_method,
match_score, nppes_name, nppes_zip).

The crosswalk builder prefers MRF-derived NPIs (which are
hospital-self-reported in the MRF metadata) and falls back to NPPES
matches.
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from difflib import SequenceMatcher
from pathlib import Path

import requests

API = "https://npiregistry.cms.hhs.gov/api/"
HOSPITALS = Path("/data0/mrf/hospitals.csv")
EXISTING_NPI = Path("/data0/crosswalk/ccn_to_npi.csv")
OUT = Path("/data0/crosswalk/ccn_to_npi_nppes.csv")

UA = {"User-Agent": "PRICEPORTAL-research/0.1 (sunbiz@gmail.com)"}


def normalize(s: str) -> str:
    s = (s or "").upper()
    drop = {"THE", "INC", "INCORPORATED", "LLC", "LP",
            "OF", "AND", "A", "AN"}
    toks = [t.strip(",.()") for t in s.split() if t.strip(",.()")]
    return " ".join(t for t in toks if t not in drop)


def name_sim(a: str, b: str) -> float:
    return SequenceMatcher(None, normalize(a), normalize(b)).ratio()


def query_nppes(name: str, state: str, postal: str | None = None,
                limit: int = 20) -> list[dict]:
    params = {
        "version": "2.1",
        "organization_name": name,
        "state": state,
        "enumeration_type": "NPI-2",
        "limit": limit,
    }
    if postal:
        params["postal_code"] = postal[:5]
    try:
        r = requests.get(API, params=params, headers=UA, timeout=20)
        if r.status_code != 200:
            return []
        return r.json().get("results") or []
    except requests.RequestException:
        return []


def best_match(name: str, results: list[dict],
               want_zip: str | None = None) -> tuple[dict, float] | None:
    if not results:
        return None
    scored = []
    for r in results:
        nm = r.get("basic", {}).get("organization_name", "")
        score = name_sim(name, nm)
        # Bonus if any practice address ZIP matches
        if want_zip:
            for a in r.get("addresses", []):
                if (a.get("postal_code") or "")[:5] == want_zip[:5]:
                    score += 0.05
                    break
        scored.append((r, score))
    scored.sort(key=lambda x: -x[1])
    return scored[0]


def lookup_one(ccn: str, name: str, state: str, postal: str
               ) -> tuple[str, str, float, str, str] | None:
    # Pass 1: ZIP-narrowed search
    results = query_nppes(name, state, postal=postal)
    cand = best_match(name, results, want_zip=postal)
    if cand and cand[1] >= 0.55:
        r, s = cand
        return (r["number"],
                "zip+name",
                s,
                r["basic"].get("organization_name", ""),
                next((a.get("postal_code","")[:5]
                      for a in r.get("addresses", [])), ""))

    # Pass 2: state-only with stricter name threshold
    results = query_nppes(name, state, postal=None, limit=50)
    cand = best_match(name, results, want_zip=postal)
    if cand and cand[1] >= 0.85:
        r, s = cand
        return (r["number"],
                "state+name",
                s,
                r["basic"].get("organization_name", ""),
                next((a.get("postal_code","")[:5]
                      for a in r.get("addresses", [])), ""))

    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true",
                    help="re-query NPPES for all 528 hospitals (default: "
                         "only those without an MRF-derived NPI)")
    ap.add_argument("--limit", type=int, default=0,
                    help="cap on number of hospitals to look up (0=no cap)")
    args = ap.parse_args()

    # Load hospitals + which CCNs already have an MRF-derived NPI
    have_mrf_npi: set[str] = set()
    if EXISTING_NPI.exists():
        with EXISTING_NPI.open() as f:
            for r in csv.DictReader(f):
                if r.get("npi"):
                    have_mrf_npi.add(r["ccn"])

    targets = []
    with HOSPITALS.open() as f:
        for r in csv.DictReader(f):
            if not args.all and r["ccn"] in have_mrf_npi:
                continue
            targets.append(r)
    if args.limit:
        targets = targets[: args.limit]
    print(f"[plan] querying NPPES for {len(targets)} CCNs "
          f"(skipping {len(have_mrf_npi)} that already have MRF-derived NPI)")

    rows: list[dict] = []
    n_hit = 0
    for i, h in enumerate(targets, 1):
        out = lookup_one(h["ccn"], h["name"], h["state"], h["zip"])
        if out:
            npi, method, score, nm, zp = out
            rows.append({
                "ccn": h["ccn"],
                "npi": npi,
                "match_method": method,
                "match_score": f"{score:.3f}",
                "nppes_name": nm,
                "nppes_zip": zp,
            })
            n_hit += 1
        else:
            rows.append({
                "ccn": h["ccn"],
                "npi": "",
                "match_method": "no_match",
                "match_score": "",
                "nppes_name": "",
                "nppes_zip": "",
            })
        if i % 25 == 0:
            print(f"  [{i}/{len(targets)}] hits so far: {n_hit}")
        time.sleep(0.1)  # polite throttling

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"\n[out] {OUT}: {n_hit}/{len(targets)} matches "
          f"({100*n_hit/max(len(targets),1):.1f}%)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
