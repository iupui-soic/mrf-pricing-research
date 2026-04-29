#!/usr/bin/env python3
"""
lookup_nppes.py
===============
Fills NPI gaps for hospitals where MRF metadata didn't carry one (i.e.,
v2.0 / v1.x files where `type_2_npi` doesn't exist) by querying the
public NPPES NPI Registry API.

API: https://npiregistry.cms.hhs.gov/api/?version=2.1&...
     https://npiregistry.cms.hhs.gov/api-page

Search strategy — three passes, most-specific first:

  1. **Taxonomy + state + postal_code (no name)**: NPPES restricts to
     organization NPIs whose self-declared taxonomy matches the CMS POS
     `hospital_type` (e.g., "General Acute Care Hospital"). When this
     returns 1 result, we accept unconditionally (taxonomy + ZIP is a
     very tight key). When it returns >1, we tie-break by name fuzzy.
  2. **org_name + state + postal_code**: classic name-fuzzy search,
     accept at name_sim ≥ 0.55. Catches cases where taxonomy is
     misclassified in NPPES.
  3. **org_name + state (no ZIP)**: catches cases where the hospital's
     NPPES address ZIP differs from the CMS POS ZIP (system parent
     listing, mailing vs practice location). Accept at name_sim ≥ 0.85.

The taxonomy-based first pass is what closes most of the gap left by
the v1 name-only matcher: hospitals where NPPES uses a different legal
name (e.g., "Grossmont Hospital" → "GROSSMONT HOSPITAL CORPORATION",
Sharp Grossmont, etc.) but the taxonomy + ZIP fingerprint is unique.

Output: /data0/mrf-pricing-research/crosswalk/ccn_to_npi_nppes.csv (ccn, npi, match_method,
match_score, nppes_name, nppes_zip).
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
HOSPITALS = Path("/data0/mrf-pricing-research/mrf/hospitals.csv")
EXISTING_NPI = Path("/data0/mrf-pricing-research/crosswalk/ccn_to_npi.csv")
OUT = Path("/data0/mrf-pricing-research/crosswalk/ccn_to_npi_nppes.csv")

UA = {"User-Agent": "PRICEPORTAL-research/0.1 (sunbiz@gmail.com)"}

# Map CMS POS `Hospital Type` → NPPES taxonomy_description.
# NUCC taxonomy text strings as used by the NPPES search.
HOSPITAL_TYPE_TAXONOMIES: dict[str, list[str]] = {
    "Acute Care Hospitals":                    ["General Acute Care Hospital"],
    "Acute Care - Department of Defense":      ["Military Hospital",
                                                "General Acute Care Hospital"],
    "Acute Care - Veterans Administration":    ["Military Hospital",
                                                "General Acute Care Hospital"],
    "Critical Access Hospitals":               ["Critical Access Hospital",
                                                "General Acute Care Hospital"],
    "Childrens":                               ["Children",  # matches "Children's"
                                                "General Acute Care Hospital"],
    "Psychiatric":                             ["Psychiatric Hospital",
                                                "Psychiatric"],
    "Rural Emergency Hospital":                ["Rural Emergency Hospital",
                                                "General Acute Care Hospital"],
}


def normalize(s: str) -> str:
    s = (s or "").upper()
    drop = {"THE", "INC", "INCORPORATED", "LLC", "LP",
            "OF", "AND", "A", "AN", "CORPORATION", "CORP"}
    toks = [t.strip(",.()") for t in s.split() if t.strip(",.()")]
    return " ".join(t for t in toks if t not in drop)


def name_sim(a: str, b: str) -> float:
    return SequenceMatcher(None, normalize(a), normalize(b)).ratio()


# Names whose presence indicates a corporate / non-operating entity
# (management companies, holdings, investment trusts). These are NPI-2
# entities that share an address with hospitals but aren't the hospital.
CORPORATE_BLOCKLIST = (
    "MANAGEMENT", "HOLDINGS", "HOLDING", "INVESTMENTS", "INVESTMENT",
    "PROPERTIES", "PARTNERS", "ENTERPRISES", "REAL ESTATE",
    "INSURANCE", "CONSULTING",
)


def is_corporate_entity(name: str) -> bool:
    up = (name or "").upper()
    return any(kw in up for kw in CORPORATE_BLOCKLIST)


def addr_for(result: dict) -> tuple[str, str, str]:
    """Return (state, zip5, city) of first address (LOCATION preferred)."""
    addrs = result.get("addresses", [])
    loc = next((a for a in addrs
                if (a.get("address_purpose") or "").upper() == "LOCATION"),
               addrs[0] if addrs else {})
    return (
        (loc.get("state") or "").upper(),
        (loc.get("postal_code") or "")[:5],
        (loc.get("city") or "").upper(),
    )


def query_nppes(*, organization_name: str | None = None,
                state: str | None = None,
                postal_code: str | None = None,
                taxonomy_description: str | None = None,
                limit: int = 20) -> list[dict]:
    """Single NPPES query. Returns the `results` array (possibly empty)."""
    params: dict[str, str | int] = {
        "version": "2.1",
        "enumeration_type": "NPI-2",
        "limit": limit,
    }
    if organization_name:
        params["organization_name"] = organization_name
    if state:
        params["state"] = state
    if postal_code:
        params["postal_code"] = postal_code[:5]
    if taxonomy_description:
        params["taxonomy_description"] = taxonomy_description
    try:
        r = requests.get(API, params=params, headers=UA, timeout=20)
        if r.status_code != 200:
            return []
        return r.json().get("results") or []
    except requests.RequestException:
        return []


def best_by_name(name: str, results: list[dict],
                 require_state: str | None = None,
                 require_zip3: str | None = None,
                 ) -> tuple[dict, float] | None:
    """Score results by name similarity, with hard filters.

    - Reject corporate/management entities outright.
    - Require state match if `require_state` given.
    - Require first-3-digit ZIP match if `require_zip3` given (catches
      cases like CCN 150177 (Indiana 47150) → Grand Forks ND 58201).
    """
    if not results:
        return None
    scored = []
    for r in results:
        nm = r.get("basic", {}).get("organization_name", "")
        if is_corporate_entity(nm):
            continue
        st, zp5, _ = addr_for(r)
        if require_state and st != require_state.upper():
            continue
        if require_zip3 and zp5[:3] != require_zip3[:3]:
            continue
        scored.append((r, name_sim(name, nm)))
    if not scored:
        return None
    scored.sort(key=lambda x: -x[1])
    return scored[0]


def lookup_one(ccn: str, name: str, state: str, postal: str,
               hosp_type: str
               ) -> tuple[str, str, float, str, str] | None:
    """Three-pass lookup; returns (npi, method, score, nppes_name, nppes_zip)."""

    zip3 = postal[:3]

    # Pass 1: taxonomy + state + ZIP — narrows to hospitals only.
    # Corporate-entity blocklist + state/zip3 hard filter prevent the
    # "American Hospital Management" / "Doctors Hospital-Grand Forks"
    # class of false positive.
    for taxonomy in HOSPITAL_TYPE_TAXONOMIES.get(hosp_type, []):
        results = query_nppes(state=state, postal_code=postal,
                              taxonomy_description=taxonomy, limit=10)
        if not results:
            continue
        # Filter corporate entities and wrong-state results before counting
        results = [r for r in results
                   if not is_corporate_entity(
                       r.get("basic", {}).get("organization_name", ""))
                   and addr_for(r)[0] == state.upper()
                   and addr_for(r)[1][:3] == zip3]
        if not results:
            continue
        if len(results) == 1:
            r = results[0]
            return (r["number"], f"taxonomy:{taxonomy}", 1.0,
                    r["basic"].get("organization_name", ""),
                    addr_for(r)[1])
        # Multiple — name tie-break with raised threshold
        cand = best_by_name(name, results,
                            require_state=state, require_zip3=zip3)
        if cand and cand[1] >= 0.55:
            r, s = cand
            return (r["number"], f"taxonomy+name:{taxonomy}", s,
                    r["basic"].get("organization_name", ""),
                    addr_for(r)[1])

    # Pass 2: org_name + state + ZIP (classic, name-fuzzy)
    results = query_nppes(organization_name=name, state=state,
                          postal_code=postal, limit=20)
    cand = best_by_name(name, results,
                        require_state=state, require_zip3=zip3)
    if cand and cand[1] >= 0.55:
        r, s = cand
        return (r["number"], "zip+name", s,
                r["basic"].get("organization_name", ""),
                addr_for(r)[1])

    # Pass 3: org_name + state, drop ZIP, stricter name threshold
    results = query_nppes(organization_name=name, state=state, limit=50)
    cand = best_by_name(name, results, require_state=state)
    if cand and cand[1] >= 0.85:
        r, s = cand
        return (r["number"], "state+name", s,
                r["basic"].get("organization_name", ""),
                addr_for(r)[1])

    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true",
                    help="re-query NPPES for all 528 hospitals (default: "
                         "only those without an MRF-derived NPI)")
    ap.add_argument("--limit", type=int, default=0,
                    help="cap on number of hospitals to look up (0=no cap)")
    args = ap.parse_args()

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
    method_counts: dict[str, int] = {}
    for i, h in enumerate(targets, 1):
        out = lookup_one(h["ccn"], h["name"], h["state"], h["zip"],
                         h.get("hospital_type", ""))
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
            base_method = method.split(":")[0]
            method_counts[base_method] = method_counts.get(base_method, 0) + 1
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
            print(f"  [{i}/{len(targets)}] hits so far: {n_hit} ({method_counts})")
        time.sleep(0.1)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"\n[out] {OUT}: {n_hit}/{len(targets)} matches "
          f"({100*n_hit/max(len(targets),1):.1f}%)")
    print(f"[methods] {method_counts}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
