#!/usr/bin/env python3
"""
lookup_propublica_eins.py
=========================
Fills EIN gaps for nonprofit hospitals where the MRF URL/filename didn't
expose an EIN (aggregator portals like PARA, Box, hospital-price-index,
ecommunity, etc.). Uses ProPublica's Nonprofit Explorer API which mirrors
the IRS Form 990 dataset — covers ~all 501(c)(3) and (c)(4) hospitals.

API: https://projects.propublica.org/nonprofits/api/v2/search.json
  ?q=<name>&state[id]=<state>

For-profit hospitals have no equivalent free public EIN source (CMS does
not publish TIN; SEC EDGAR only covers publicly traded). Government-
owned hospital districts vary — some file Form 990 (those will be found
here), most do not. This script logs the unfilled residue.

Output: /data0/crosswalk/ccn_to_ein_propublica.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from difflib import SequenceMatcher
from pathlib import Path

import requests

API = "https://projects.propublica.org/nonprofits/api/v2/search.json"
HOSPITALS = Path("/data0/mrf/hospitals.csv")
XWALK = Path("/data0/crosswalk/facilities_crosswalk.csv")
DOWNLOADS = Path("/data0/mrf/downloads.csv")
OUT = Path("/data0/crosswalk/ccn_to_ein_propublica.csv")

UA = {"User-Agent": "PRICEPORTAL-research/0.1 (sunbiz@gmail.com)"}

NONPROFIT_OWNERSHIPS = {
    "Voluntary non-profit - Private",
    "Voluntary non-profit - Other",
    "Voluntary non-profit - Church",
    # Some hospital districts file Form 990; include them and let the
    # search miss naturally if they don't.
    "Government - Hospital District or Authority",
}


def normalize(s: str) -> str:
    s = (s or "").upper()
    drop = {"THE", "INC", "INCORPORATED", "LLC", "LP",
            "OF", "AND", "A", "AN"}
    toks = [t.strip(",.()") for t in s.split() if t.strip(",.()")]
    return " ".join(t for t in toks if t not in drop)


def name_sim(a: str, b: str) -> float:
    return SequenceMatcher(None, normalize(a), normalize(b)).ratio()


def search(name: str, state: str) -> list[dict]:
    try:
        r = requests.get(API,
                         params={"q": name, "state[id]": state},
                         headers=UA, timeout=20)
        if r.status_code != 200:
            return []
        return r.json().get("organizations") or []
    except requests.RequestException:
        return []


def best_match(name: str, city: str, results: list[dict]
               ) -> tuple[dict, float] | None:
    if not results:
        return None
    scored = []
    for r in results:
        score = name_sim(name, r.get("name", ""))
        # Bonus for city match
        if city and (r.get("city") or "").upper() == city.upper():
            score += 0.05
        # NTEE code beginning with 'E' = Health (E20-E24 = hospitals)
        ntee = (r.get("ntee_code") or "")
        if ntee.startswith("E"):
            score += 0.03
        scored.append((r, score))
    scored.sort(key=lambda x: -x[1])
    return scored[0]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true",
                    help="re-query for all hospitals without EIN, regardless "
                         "of ownership (default: only likely-nonprofits)")
    args = ap.parse_args()

    # Universe: ok-downloaded hospitals without an EIN, optionally
    # restricted to nonprofit ownership classes
    have_ein: set[str] = set()
    hosp_meta: dict[str, dict] = {}
    with XWALK.open() as f:
        for r in csv.DictReader(f):
            hosp_meta[r["ccn"]] = r
            if r.get("ein"):
                have_ein.add(r["ccn"])

    ok: set[str] = set()
    with DOWNLOADS.open() as f:
        for r in csv.DictReader(f):
            if r.get("status") == "ok":
                ok.add(r["ccn"])

    targets = []
    for ccn in ok - have_ein:
        h = hosp_meta.get(ccn)
        if not h:
            continue
        if not args.all and h["ownership"] not in NONPROFIT_OWNERSHIPS:
            continue
        targets.append(h)

    print(f"[plan] querying ProPublica for {len(targets)} hospitals "
          f"({'all ownerships' if args.all else 'nonprofit + districts only'})")

    rows: list[dict] = []
    n_hit = 0
    for i, h in enumerate(targets, 1):
        results = search(h["name"], h["state"])
        cand = best_match(h["name"], h["city"], results)
        if cand and cand[1] >= 0.55:
            r, s = cand
            ein = str(r.get("ein") or "")
            if len(ein) == 9:
                ein_fmt = f"{ein[:2]}-{ein[2:]}"
            else:
                ein_fmt = ein
            rows.append({
                "ccn": h["ccn"],
                "ein": ein_fmt,
                "match_score": f"{s:.3f}",
                "propublica_name": r.get("name", ""),
                "propublica_city": r.get("city", ""),
                "ntee_code": r.get("ntee_code") or "",
            })
            n_hit += 1
        else:
            rows.append({
                "ccn": h["ccn"],
                "ein": "",
                "match_score": f"{cand[1]:.3f}" if cand else "",
                "propublica_name": "",
                "propublica_city": "",
                "ntee_code": "",
            })
        if i % 10 == 0:
            print(f"  [{i}/{len(targets)}] hits so far: {n_hit}")
        time.sleep(0.2)  # ProPublica asks for ≤5 req/s

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
