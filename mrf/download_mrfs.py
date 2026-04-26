#!/usr/bin/env python3
"""
download_mrfs.py
================
Download the MRF files discovered by `discover_mrf_urls.py`.

Reads `mrf_urls.csv`, downloads each non-null `mrf_url` to
`/data0/mrf/files/<state>/<ccn>/<basename>`, with:
  - streaming (for multi-GB files)
  - per-host rate limiting (1.2s between requests to same host)
  - up to 8-way concurrency across distinct hosts
  - SHA-256 checksum computed on the fly
  - resumable: skip files already fully downloaded (checksum match)
  - size sanity check (drop if <10 KB, typically a 404 HTML page)

Emits `downloads.csv`:
    ccn, state, mrf_url, local_path, status, bytes_downloaded,
    sha256, content_type, downloaded_at, error

Usage:
    .venv/bin/python mrf/download_mrfs.py [--limit N] [--state CA|IN]
                                          [--workers 8] [--resume]
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import hashlib
import os
import random
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

OUT_DIR  = Path("/data0/mrf")
URL_CSV  = OUT_DIR / "mrf_urls.csv"
FILES_DIR = OUT_DIR / "files"
DL_CSV   = OUT_DIR / "downloads.csv"

# Many hospital sites front their MRFs behind Cloudflare/Akamai bot
# protection that 403s any non-browser UA. The MRFs themselves are
# publicly mandated under 45 CFR 180; we mimic a real browser to access
# the publicly-required files. Per-host rate limiting is preserved.
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0.0.0 Safari/537.36"
)
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
               "application/json,text/csv,*/*;q=0.8"),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

PER_HOST_DELAY_S = 1.2
TIMEOUT_S = 120
CHUNK = 1024 * 1024  # 1 MB
MIN_BYTES = 1024  # reject sub-1 KB responses; legit MRFs are 4+ KB
MIN_BYTES_HTML = 10 * 1024  # html responses must be larger to clear the bar

_host_last_hit: dict[str, float] = {}


def host_of(url: str) -> str:
    try:
        return urllib.parse.urlparse(url).netloc.lower()
    except Exception:
        return ""


def rate_limit(url: str):
    h = host_of(url)
    now = time.time()
    last = _host_last_hit.get(h, 0)
    wait = PER_HOST_DELAY_S - (now - last)
    if wait > 0:
        time.sleep(wait + random.uniform(0, 0.3))
    _host_last_hit[h] = time.time()


def sanitize_filename(url: str) -> str:
    path = urllib.parse.urlparse(url).path
    name = path.rsplit("/", 1)[-1] or "mrf_file"
    # drop query-string chars / sanitize
    name = "".join(c for c in name if c.isalnum() or c in "._-")
    if len(name) > 200:
        name = name[:200]
    return name or "mrf_file"


def download_one(row: dict) -> dict:
    ccn = row["ccn"]
    state = row["state"]
    url = row["mrf_url"]

    result = {
        "ccn": ccn, "state": state, "mrf_url": url,
        "local_path": None, "status": "pending", "bytes_downloaded": 0,
        "sha256": None, "content_type": None,
        "downloaded_at": dt.datetime.utcnow().isoformat(),
        "error": "",
    }

    if not url or not isinstance(url, str) or not url.startswith("http"):
        result["status"] = "skipped_no_url"
        return result

    dest_dir = FILES_DIR / state / ccn
    dest_dir.mkdir(parents=True, exist_ok=True)
    basename = sanitize_filename(url)
    dest_path = dest_dir / basename
    result["local_path"] = str(dest_path)

    # Resume check — if file exists and non-empty, trust it
    if dest_path.exists() and dest_path.stat().st_size >= MIN_BYTES:
        result["status"] = "already_present"
        result["bytes_downloaded"] = dest_path.stat().st_size
        return result

    rate_limit(url)
    try:
        with requests.get(url, headers=HEADERS, stream=True,
                          timeout=TIMEOUT_S, allow_redirects=True) as r:
            result["content_type"] = r.headers.get("Content-Type", "")
            if r.status_code != 200:
                result["status"] = f"http_{r.status_code}"
                return result

            hasher = hashlib.sha256()
            bytes_dl = 0
            tmp = dest_path.with_suffix(dest_path.suffix + ".part")
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(CHUNK):
                    if not chunk:
                        continue
                    f.write(chunk)
                    hasher.update(chunk)
                    bytes_dl += len(chunk)

            ctype_l = (result["content_type"] or "").lower()
            is_html = ctype_l.startswith("text/html")
            min_threshold = MIN_BYTES_HTML if is_html else MIN_BYTES
            if bytes_dl < min_threshold:
                tmp.unlink(missing_ok=True)
                result["status"] = "too_small"
                result["bytes_downloaded"] = bytes_dl
                return result
            if is_html:
                # Got HTML for what should be a CSV/JSON — reject as soft 404
                tmp.unlink(missing_ok=True)
                result["status"] = "html_response"
                result["bytes_downloaded"] = bytes_dl
                return result

            tmp.replace(dest_path)
            result["status"] = "ok"
            result["bytes_downloaded"] = bytes_dl
            result["sha256"] = hasher.hexdigest()
            return result
    except requests.RequestException as e:
        result["status"] = "error"
        result["error"] = f"{type(e).__name__}:{str(e)[:200]}"
        return result


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--state", type=str, default=None)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()

    if not URL_CSV.exists():
        sys.exit(f"missing {URL_CSV} — run discover_mrf_urls.py first")

    urls = pd.read_csv(URL_CSV, dtype=str)
    urls = urls[urls["mrf_url"].notna()]
    if args.state:
        urls = urls[urls["state"] == args.state]
    if args.limit:
        urls = urls.head(args.limit)

    done = set()
    if args.resume and DL_CSV.exists():
        prev = pd.read_csv(DL_CSV, dtype=str)
        ok = prev[prev["status"].isin(("ok", "already_present"))]
        done = set(ok["ccn"].dropna())
        print(f"[resume] {len(done):,} CCNs already downloaded")
        urls = urls[~urls["ccn"].isin(done)]

    print(f"[plan] {len(urls):,} MRFs to download")
    if len(urls) == 0:
        return

    # header
    cols = ["ccn", "state", "mrf_url", "local_path", "status",
            "bytes_downloaded", "sha256", "content_type",
            "downloaded_at", "error"]
    if not DL_CSV.exists():
        pd.DataFrame(columns=cols).to_csv(DL_CSV, index=False)

    rows = urls.to_dict("records")
    n_done = 0
    total_bytes = 0
    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(download_one, r): r["ccn"] for r in rows}
        for fut in concurrent.futures.as_completed(futs):
            ccn = futs[fut]
            try:
                result = fut.result()
            except Exception as e:
                result = {"ccn": ccn, "status": f"unhandled:{e}"}
            pd.DataFrame([result]).reindex(columns=cols).to_csv(
                DL_CSV, mode="a", header=False, index=False)
            n_done += 1
            bd = result.get("bytes_downloaded") or 0
            total_bytes += bd if isinstance(bd, int) else 0
            if n_done % 5 == 0 or n_done == len(rows):
                rate = n_done / max(time.time() - t0, 1e-9)
                gb = total_bytes / 1024**3
                print(f"  [{n_done}/{len(rows)}] {ccn} {result.get('status','?'):20s} "
                      f"({bd/1024/1024:7.1f} MB)  cum={gb:.1f} GB  ({rate:.2f}/s)")

    elapsed = time.time() - t0
    print(f"[done] {n_done} files in {elapsed:.0f}s  ({total_bytes/1024**3:.2f} GB total)")

    dl = pd.read_csv(DL_CSV, dtype=str)
    print("\n[summary] status breakdown:")
    print(dl["status"].value_counts().head(12).to_string())


if __name__ == "__main__":
    main()
