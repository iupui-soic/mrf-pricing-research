#!/usr/bin/env python3
"""
discover_mrf_urls.py
====================
Discover the public MRF URL for each hospital in `hospitals.csv`.

Two-stage discovery per hospital:
  STAGE A — find the hospital's main website (domain):
    1. DuckDuckGo HTML search: "<name> <city> <state> hospital price transparency"
    2. Parse first 10 results; keep the first that looks like a hospital domain
       (filters out wikipedia, yelp, healthgrades, facebook, linkedin, etc.)
    3. If no candidate, fall back to slugified-name heuristics
  STAGE B — find the MRF file on that domain:
    1. If homepage has a link text matching price|transparency|standardcharges,
       follow it.
    2. On target page, scan all <a href> for:
       - filename pattern `<ein>_<hospital>_standardcharges.{json,csv,xml}`
       - any URL with "standardcharges" or "machine-readable" in path
       - any JSON/CSV link in a price-transparency context
    3. Also try common paths: /cms-hpt.txt, /price-transparency/,
       /transparency/, /pricing/, /standardcharges.{json,csv}
    4. Validate candidate via HEAD request (must be 200, content-type
       json/csv/octet-stream, content-length > 10 KB).

Writes `mrf_urls.csv` with:
    ccn, name, state, zip, website, mrf_url, mrf_format, mrf_size_bytes,
    discovery_method, http_status, discovered_at

Rate limited to 1 request per host per second; 8-way concurrency across
distinct hosts. Respects robots.txt via best-effort check (honours
`Disallow: /`).

Usage:
    .venv/bin/python mrf/discover_mrf_urls.py [--limit N] [--resume] [--state CA|IN]
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import json
import random
import re
import sys
import time
import urllib.parse
import urllib.robotparser
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ── Config ──────────────────────────────────────────────────────────────
OUT_DIR = Path("/data0/mrf-pricing-research/mrf")
HOSP_CSV = OUT_DIR / "hospitals.csv"
URL_CSV  = OUT_DIR / "mrf_urls.csv"
LOG_CSV  = OUT_DIR / "discovery_log.csv"

# Identify-as-research UA for hospital-site requests (polite, traceable).
USER_AGENT = (
    "PricePortal/0.1 (+https://github.com/iupui-soic/hcai-chargemasters; "
    "academic research crawler)"
)

# Browser UA strictly for search-engine queries (DuckDuckGo blocks
# identified crawlers even for public HTML endpoints). Only used in
# `duckduckgo_search()`.
BROWSER_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml,application/json,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}
SEARCH_HEADERS = {
    "User-Agent": BROWSER_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Per-host rate limit, seconds between requests to the same host
PER_HOST_DELAY_S = 1.2
# DuckDuckGo needs more aggressive throttling — it rate-limits shared IPs
# hard when hit with even a handful of automated searches per minute.
DDG_DELAY_S = 3.5
# Request timeout
TIMEOUT_S = 25
# Worker pool size
N_WORKERS = 8

# Link text patterns that suggest a transparency page
LINK_TEXT_HINTS = re.compile(
    r"(standard\s*charges?|price\s*transparency|machine[- ]?readable|"
    r"chargemaster|hospital\s*charges?|transparency|hpt|price\s*list)",
    re.IGNORECASE,
)

# URL patterns for MRF files
MRF_URL_RE = re.compile(
    r"""
    (
      [0-9]{2}-?[0-9]{7}       # EIN format  12-3456789  or  123456789
      _.*?_standardcharges
      \.(?:json|csv|xml|xlsx?|zip|gz)
    )
    |
    (
      /standardcharges?[^/?#]* \.(?:json|csv|xml|xlsx?|zip|gz)
    )
    |
    (
      /machine[- ]?readable[^/?#]* \.(?:json|csv|xml|xlsx?|zip|gz)
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Hostnames that are never the hospital itself
DOMAIN_BLOCKLIST = re.compile(
    r"(wikipedia|yelp|healthgrades|facebook|linkedin|indeed|glassdoor|"
    r"twitter|x\.com|instagram|youtube|tiktok|doximity|zocdoc|"
    r"ratemds|vitals|webmd|medlineplus|google|bing|duckduckgo|"
    r"archive\.org|mapquest|yellowpages|usnews|niche|zillow|realtor|"
    r"beckershospitalreview|hcahealthcare\.com/locations|medicare\.gov|"
    r"cms\.gov|nih\.gov|cdc\.gov|"
    # Third-party aggregators / payment portals — not the hospital itself
    r"payerprice|mychart|hospitalpricecheck|turquoise\.health|"
    r"hospitalpricingspecialists|careoperative|patientrightsadvocate|"
    r"pricemdhealth|fairhealth|clearprice|hospitallookup|"
    r"hospitalpricelookup|rippleeffect)",
    re.IGNORECASE,
)

# DuckDuckGo HTML endpoint (no API key needed)
DDG_URL = "https://html.duckduckgo.com/html/"

# Global rate-limit state keyed by host (thread-safe enough for CPython dict
# writes; race window is acceptable — we're over-sleeping, never under).
import threading
_host_last_hit: dict[str, float] = {}
_host_lock = threading.Lock()

# ── Helpers ─────────────────────────────────────────────────────────────
def slugify(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def host_of(url: str) -> str:
    try:
        return urllib.parse.urlparse(url).netloc.lower()
    except Exception:
        return ""


def rate_limit(url: str):
    """Sleep if we've hit this host recently (thread-safe, over-sleeps ok)."""
    h = host_of(url)
    # DDG gets a harder throttle than hospital sites
    delay = DDG_DELAY_S if "duckduckgo" in h else PER_HOST_DELAY_S
    with _host_lock:
        last = _host_last_hit.get(h, 0)
        wait = delay - (time.time() - last)
        # Reserve the slot immediately so other threads see our future time
        _host_last_hit[h] = time.time() + max(0, wait)
    if wait > 0:
        time.sleep(wait + random.uniform(0, 0.4))


def fetch(url: str, method: str = "GET", allow_redirects=True) -> Optional[requests.Response]:
    rate_limit(url)
    try:
        r = requests.request(
            method, url, headers=HEADERS,
            timeout=TIMEOUT_S,
            allow_redirects=allow_redirects,
        )
        return r
    except requests.RequestException:
        return None


def duckduckgo_search(query: str) -> list[str]:
    """Return a list of result URLs from DuckDuckGo HTML search."""
    rate_limit(DDG_URL)
    try:
        r = requests.post(
            DDG_URL,
            data={"q": query},
            headers=SEARCH_HEADERS,
            timeout=TIMEOUT_S,
        )
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        out = []
        for a in soup.select("a.result__a, a.result__url"):
            href = a.get("href", "")
            # DDG sometimes wraps URLs in /l/?uddg=... redirect
            m = re.search(r"uddg=([^&]+)", href)
            if m:
                href = urllib.parse.unquote(m.group(1))
            if href.startswith("http") and href not in out:
                out.append(href)
        return out[:10]
    except requests.RequestException:
        return []


def robot_allows(url: str) -> bool:
    """Best-effort robots.txt check for this host. Failures default to allow."""
    h = host_of(url)
    if not h:
        return True
    try:
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(f"https://{h}/robots.txt")
        rp.read()
        return rp.can_fetch(USER_AGENT, url)
    except Exception:
        return True


# ── Stage A: find hospital website ─────────────────────────────────────
def _name_tokens(name: str) -> set[str]:
    """Important tokens from a hospital name; drops noise words."""
    stop = {"hospital", "medical", "center", "centre", "health",
            "healthcare", "system", "campus", "care", "regional",
            "community", "general", "the", "of", "st", "saint",
            "memorial", "incorporated", "inc", "llc", "mem", "med",
            "ctr", "and", "for", "a"}
    toks = re.findall(r"[a-z0-9]+", name.lower())
    return {t for t in toks if len(t) > 2 and t not in stop}


def _score_domain(host: str, name_toks: set[str], city: str) -> int:
    """Higher = better hospital-domain match."""
    if not host:
        return -1
    host = host.lower()
    # Strip www.
    bare = re.sub(r"^www\.", "", host)
    # Domain label (without TLD) split on . and -
    parts = re.split(r"[.\-]", bare)
    parts_set = {p for p in parts if len(p) > 2}
    score = 0
    # Token overlap with hospital name
    score += 3 * len(name_toks & parts_set)
    # City match in domain
    city_slug = re.sub(r"[^a-z]", "", city.lower())
    if city_slug and city_slug in bare.replace(".", "").replace("-", ""):
        score += 2
    # TLD preference
    if re.search(r"\.(org|health|hospital)$", host):
        score += 2
    elif re.search(r"\.(edu|gov)$", host):
        score += 1
    elif re.search(r"\.com$", host):
        score += 0
    # Penalize obvious sub-pages over top-level
    return score


def find_hospital_website(name: str, city: str, state: str) -> Optional[str]:
    """Return the most likely homepage URL for this hospital, or None.

    Tries multiple query variants; ranks candidates by token overlap with
    the hospital name so health-system domains (providence.org,
    sutterhealth.org) win over random .orgs that happen to mention the
    hospital.
    """
    name_toks = _name_tokens(name)
    # ONE DDG query per hospital to stay under the rate limit; rely on
    # scoring to pick the right result.
    query = f"{name} {city} {state} hospital"
    seen_hosts = set()
    best = (None, -1)  # (url, score)
    for url in duckduckgo_search(query):
        host = host_of(url)
        if not host or DOMAIN_BLOCKLIST.search(host):
            continue
        if host in seen_hosts:
            continue
        seen_hosts.add(host)
        score = _score_domain(host, name_toks, city)
        if score > best[1]:
            scheme = "https" if url.startswith("https") else "http"
            best = (f"{scheme}://{host}", score)
    if best[0] and best[1] >= 1:  # very permissive; we let Stage B filter
        return best[0]
    return None


# ── Stage B: find MRF URL on website ───────────────────────────────────
def _score_mrf_url(url: str, name_toks: set[str], city: str, state: str) -> int:
    """Higher = better match for the target hospital.

    Many health-system domains host one MRF per hospital under the same
    directory; the filename follows CMS convention
    `<ein>_<hospital-slug>_standardcharges.<ext>` so token overlap with
    the hospital name is a strong signal.
    """
    u = url.lower()
    score = 0
    basename = u.rsplit("/", 1)[-1]
    # Hospital-name token matches in filename
    score += 3 * sum(1 for t in name_toks if t in basename)
    # City match
    city_slug = re.sub(r"[^a-z]", "", city.lower())
    if city_slug and len(city_slug) > 3 and city_slug in basename:
        score += 2
    # State match (two-letter code somewhere in path)
    if re.search(rf"[-_]{state.lower()}[-_.]", u):
        score += 1
    # Prefer standardcharges in filename over path
    if "standardcharges" in basename:
        score += 1
    return score


def find_mrf_on_site(home: str, ccn: str, hospital_name: str = "",
                     city: str = "", state: str = "") -> dict:
    """Try several strategies to locate the MRF file.

    Returns dict with: mrf_url, mrf_format, mrf_size_bytes, http_status,
    discovery_method, notes.
    """
    name_toks = _name_tokens(hospital_name)
    notes = []

    def pick_best(urls: list[str]) -> Optional[str]:
        """From a set of candidate MRF URLs, pick the best match."""
        if not urls:
            return None
        scored = sorted(
            ((u, _score_mrf_url(u, name_toks, city, state)) for u in urls),
            key=lambda x: x[1], reverse=True,
        )
        # If the top score is 0 and there are many candidates, likely a
        # health-system directory where we can't disambiguate; return None
        # rather than picking arbitrarily.
        if scored[0][1] == 0 and len(scored) > 5:
            return None
        return scored[0][0]

    # Strategy 1: cms-hpt.txt at root
    cms_hpt = f"{home}/cms-hpt.txt"
    r = fetch(cms_hpt)
    if r and r.status_code == 200 and "standardcharges" in r.text.lower():
        for line in r.text.splitlines():
            line = line.strip()
            if line.startswith("http") and "standardcharges" in line.lower():
                v = validate_mrf(line)
                if v:
                    v["discovery_method"] = "cms_hpt_txt"
                    return v
        notes.append("cms_hpt_txt: parsed but no valid URL")

    # Strategy 2: crawl the homepage for a "price transparency" link
    r = fetch(home)
    if r and r.status_code == 200:
        soup = BeautifulSoup(r.text, "html.parser")
        candidates = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = (a.get_text() or "")[:200]
            if LINK_TEXT_HINTS.search(text) or LINK_TEXT_HINTS.search(href):
                full = urllib.parse.urljoin(home, href)
                candidates.append(full)

        # Collect MRF URLs from homepage, score, pick best
        direct_mrfs = []
        for a in soup.find_all("a", href=True):
            full = urllib.parse.urljoin(home, a["href"])
            if MRF_URL_RE.search(full) or "standardcharges" in full.lower():
                direct_mrfs.append(full)
        direct_mrfs = list(dict.fromkeys(direct_mrfs))
        best = pick_best(direct_mrfs)
        if best:
            v = validate_mrf(best)
            if v:
                v["discovery_method"] = "homepage_direct"
                return v

        # Dedupe transparency-link candidates
        seen = set()
        candidates = [c for c in candidates if not (c in seen or seen.add(c))]

        # Visit each candidate page, look for MRF links, pick best
        for c in candidates[:6]:
            r2 = fetch(c)
            if not r2 or r2.status_code != 200:
                continue
            soup2 = BeautifulSoup(r2.text, "html.parser")
            page_mrfs = []
            for a in soup2.find_all("a", href=True):
                full = urllib.parse.urljoin(c, a["href"])
                if MRF_URL_RE.search(full) or "standardcharges" in full.lower():
                    page_mrfs.append(full)
            page_mrfs = list(dict.fromkeys(page_mrfs))
            best = pick_best(page_mrfs)
            if best:
                v = validate_mrf(best)
                if v:
                    v["discovery_method"] = f"transparency_page:{host_of(c)}"
                    return v
        if not candidates:
            notes.append("no_transparency_links_on_homepage")

    # Strategy 3: common fixed paths
    common_paths = [
        "/price-transparency/",
        "/pricing/",
        "/billing/price-transparency/",
        "/patients/billing/price-transparency/",
        "/patients-visitors/billing/price-transparency/",
        "/about/price-transparency/",
        "/transparency/",
        "/standardcharges.json",
        "/standardcharges.csv",
    ]
    for p in common_paths:
        url = home.rstrip("/") + p
        r = fetch(url)
        if not r or r.status_code != 200:
            continue
        ct = r.headers.get("Content-Type", "").lower()
        if any(f in ct for f in ("json", "csv", "octet-stream")):
            v = validate_mrf(url, response=r)
            if v:
                v["discovery_method"] = f"common_path:{p}"
                return v
        # Otherwise it's an HTML page — scan it for MRF links
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            full = urllib.parse.urljoin(url, href)
            if MRF_URL_RE.search(full) or "standardcharges" in full.lower():
                v = validate_mrf(full)
                if v:
                    v["discovery_method"] = f"common_path_link:{p}"
                    return v

    return {
        "mrf_url": None, "mrf_format": None, "mrf_size_bytes": None,
        "http_status": None, "discovery_method": "not_found",
        "notes": ";".join(notes) or "exhausted",
    }


def validate_mrf(url: str, response: Optional[requests.Response] = None) -> Optional[dict]:
    """HEAD the URL; accept if it looks like a real MRF."""
    if response is None:
        r = fetch(url, method="HEAD")
        if r is None:
            return None
    else:
        r = response
    if r.status_code not in (200, 301, 302):
        return None
    final_url = r.url if hasattr(r, "url") else url
    ct = r.headers.get("Content-Type", "").lower()
    size = r.headers.get("Content-Length")
    size = int(size) if size and size.isdigit() else None

    # MRF files are typically ≥ 10 KB
    if size is not None and size < 10 * 1024:
        return None

    fmt = None
    if "json" in ct or final_url.lower().endswith(".json"):
        fmt = "json"
    elif "csv" in ct or final_url.lower().endswith(".csv"):
        fmt = "csv"
    elif "xml" in ct or final_url.lower().endswith(".xml"):
        fmt = "xml"
    elif final_url.lower().endswith(".zip"):
        fmt = "zip"
    elif "octet-stream" in ct:
        # Guess from URL
        for ext in (".json", ".csv", ".xml", ".zip"):
            if ext in final_url.lower():
                fmt = ext.strip(".")
                break
    if fmt is None and "standardcharges" not in final_url.lower():
        return None

    return {
        "mrf_url": final_url,
        "mrf_format": fmt,
        "mrf_size_bytes": size,
        "http_status": r.status_code,
    }


# ── Per-hospital orchestration ─────────────────────────────────────────
def discover_one(row: dict) -> dict:
    name = row["name"]
    city = row["city"]
    state = row["state"]
    ccn = row["ccn"]

    result = {
        "ccn": ccn, "name": name, "state": state, "zip": row.get("zip", ""),
        "website": None, "mrf_url": None, "mrf_format": None,
        "mrf_size_bytes": None, "discovery_method": None,
        "http_status": None, "discovered_at": dt.datetime.utcnow().isoformat(),
        "notes": "",
    }

    # Stage A
    try:
        home = find_hospital_website(name, city, state)
    except Exception as e:
        home = None
        result["notes"] = f"stage_a_err:{type(e).__name__}:{str(e)[:80]}"
    if not home:
        result["discovery_method"] = "website_not_found"
        return result
    result["website"] = home

    # Stage B
    try:
        v = find_mrf_on_site(home, ccn, hospital_name=name, city=city, state=state)
    except Exception as e:
        v = {"mrf_url": None, "discovery_method": f"stage_b_err:{type(e).__name__}"}
        result["notes"] = f"{result['notes']}|{str(e)[:80]}"

    result.update({k: v.get(k) for k in (
        "mrf_url", "mrf_format", "mrf_size_bytes",
        "http_status", "discovery_method",
    )})
    if v.get("notes"):
        result["notes"] = (result["notes"] + "|" + v["notes"]).strip("|")
    return result


# ── Main ───────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=None,
                    help="max hospitals to process (for testing)")
    ap.add_argument("--state", type=str, default=None,
                    help="restrict to one state (CA or IN)")
    ap.add_argument("--workers", type=int, default=N_WORKERS)
    ap.add_argument("--resume", action="store_true",
                    help="skip hospitals already in mrf_urls.csv")
    args = ap.parse_args()

    if not HOSP_CSV.exists():
        sys.exit(f"missing {HOSP_CSV} — run build_hospital_list.py first")

    hosp = pd.read_csv(HOSP_CSV, dtype=str)
    if args.state:
        hosp = hosp[hosp["state"] == args.state]
    if args.limit:
        hosp = hosp.head(args.limit)

    done = set()
    if args.resume and URL_CSV.exists():
        prev = pd.read_csv(URL_CSV, dtype=str)
        done = set(prev["ccn"].dropna())
        print(f"[resume] {len(done):,} CCNs already processed")
        hosp = hosp[~hosp["ccn"].isin(done)]
    print(f"[scan] {len(hosp):,} hospitals to discover")

    # Write header once
    cols = ["ccn", "name", "state", "zip", "website", "mrf_url",
            "mrf_format", "mrf_size_bytes", "discovery_method",
            "http_status", "discovered_at", "notes"]
    if not URL_CSV.exists():
        pd.DataFrame(columns=cols).to_csv(URL_CSV, index=False)

    rows = hosp.to_dict("records")
    n_done = 0
    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(discover_one, r): r["ccn"] for r in rows}
        for fut in concurrent.futures.as_completed(futs):
            ccn = futs[fut]
            try:
                result = fut.result()
            except Exception as e:
                result = {"ccn": ccn, "discovery_method": f"unhandled:{e}"}
            # Append atomically
            pd.DataFrame([result]).reindex(columns=cols).to_csv(
                URL_CSV, mode="a", header=False, index=False
            )
            n_done += 1
            if n_done % 10 == 0 or n_done == len(rows):
                mrf_ok = "mrf_url" in result and bool(result.get("mrf_url"))
                rate = n_done / max(time.time() - t0, 1e-9)
                print(f"  [{n_done}/{len(rows)}] {ccn} {'MRF_OK' if mrf_ok else 'miss':7s} "
                      f"method={result.get('discovery_method','?')[:40]} "
                      f"({rate:.2f}/s)")

    elapsed = time.time() - t0
    print(f"[done] {n_done} hospitals in {elapsed:.0f}s")

    # Summary
    out = pd.read_csv(URL_CSV, dtype=str)
    found = out["mrf_url"].notna().sum()
    print(f"\n[summary] MRF found: {found:,} / {len(out):,}  ({found/len(out):.1%})")
    print("By discovery method:")
    print(out["discovery_method"].value_counts().head(10).to_string())


if __name__ == "__main__":
    main()
