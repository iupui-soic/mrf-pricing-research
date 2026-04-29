#!/usr/bin/env python3
"""
fetch_para_hcfs.py
==================
Recovers MRFs for hospitals exempted as `exempt:portal_landing` whose
`mrf_url` points at PARA HCFS's hosted portal
(`apps.para-hcfs.com/PTT/FinalLinks/<Hospital>.aspx`).

Each hospital page is an ExtJS SPA that loads a global JS function
`DownloadReport(db, type)` whose body has a hardcoded `dbName` (e.g.
`dbCIMCAVALONCA`). Calling `DownloadReport('hospital', 'CDMWithoutLabel')`
triggers an in-app download of the §180-compliant standardcharges CSV
hosted at `Reports.aspx?dbName=<db>&type=CDMWithoutLabel`.

Output:
- File saved to `/data0/mrf-pricing-research/mrf/files/<state>/<ccn>/<original_filename>`
- `mrf_urls.csv` updated: `discovery_method` flipped from
  `exempt:portal_landing` to `para_hcfs_playwright`
- `downloads.csv` appended with the new local_path + size + content-type
"""

from __future__ import annotations

import csv
import hashlib
import sys
import time
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright

DATA_DIR = Path("/data0/mrf-pricing-research/mrf")
FILES_DIR = DATA_DIR / "files"
URLS_CSV = DATA_DIR / "mrf_urls.csv"
DOWNLOADS_CSV = DATA_DIR / "downloads.csv"
HOSPITALS_CSV = DATA_DIR / "hospitals.csv"
PARA_HOST = "apps.para-hcfs.com"


def discover_para_targets() -> list[dict]:
    """Pull rows whose mrf_url is a PARA HCFS portal."""
    hosp = pd.read_csv(HOSPITALS_CSV, dtype={"ccn": str})
    hosp["ccn"] = hosp.ccn.str.zfill(6)
    ccn2state = dict(zip(hosp.ccn, hosp.state))

    mu = pd.read_csv(URLS_CSV, dtype={"ccn": str})
    mu["ccn"] = mu.ccn.str.zfill(6)
    targets = mu[
        mu.discovery_method.fillna("").str.startswith("exempt")
        & mu.mrf_url.fillna("").str.contains(PARA_HOST, case=False)
    ].copy()
    out = []
    for _, r in targets.iterrows():
        out.append({
            "ccn": r["ccn"],
            "state": ccn2state.get(r["ccn"], "?"),
            "url": r["mrf_url"],
            "name": r.get("notes", "") or r.get("name", ""),
        })
    return out


def fetch_one(page, url: str) -> tuple[bytes, str] | None:
    """Navigate and trigger the download. Returns (bytes, suggested_filename)."""
    page.goto(url, wait_until="networkidle", timeout=60_000)
    # The page must fully render so DownloadReport is defined globally.
    # Verify and call it.
    has_fn = page.evaluate(
        'typeof DownloadReport === "function"'
    )
    if not has_fn:
        return None
    with page.expect_download(timeout=300_000) as dl_info:
        page.evaluate('DownloadReport("hospital", "CDMWithoutLabel")')
    dl = dl_info.value
    tmp = Path("/tmp") / f"para_dl_{int(time.time()*1000)}.csv"
    dl.save_as(str(tmp))
    data = tmp.read_bytes()
    tmp.unlink(missing_ok=True)
    return (data, dl.suggested_filename or "standardcharges.csv")


def update_urls_csv(ccn: str, new_method: str) -> None:
    df = pd.read_csv(URLS_CSV, dtype=str)
    df.loc[df.ccn.str.zfill(6) == ccn, "discovery_method"] = new_method
    df.to_csv(URLS_CSV, index=False)


def append_download_row(row: dict) -> None:
    df = pd.read_csv(DOWNLOADS_CSV, dtype=str) if DOWNLOADS_CSV.exists() \
        else pd.DataFrame()
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    df.to_csv(DOWNLOADS_CSV, index=False)


def main() -> None:
    targets = discover_para_targets()
    print(f"[plan] {len(targets)} PARA HCFS portals to scrape", flush=True)

    if not targets:
        return

    summary = {"ok": 0, "fail": 0, "skip_existing": 0}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        for i, t in enumerate(targets, 1):
            ccn, state, url = t["ccn"], t["state"], t["url"]
            out_dir = FILES_DIR / state / ccn
            existing = list(out_dir.glob("*standardcharges*")) \
                if out_dir.exists() else []
            if existing:
                print(f"[{i:2d}/{len(targets)}] skip {ccn} ({state}) "
                      f"— already have {existing[0].name}", flush=True)
                summary["skip_existing"] += 1
                continue

            print(f"[{i:2d}/{len(targets)}] {ccn} ({state}) {url[-50:]}",
                  flush=True)
            try:
                result = fetch_one(page, url)
                if result is None:
                    print(f"          ✗ DownloadReport function not present",
                          flush=True)
                    summary["fail"] += 1
                    continue
                data, fname = result
                size = len(data)
                # Sanity: must be > 5 KB and not look like HTML
                if size < 5_000 or data[:100].lower().lstrip().startswith(b"<"):
                    print(f"          ✗ implausible response ({size} B, "
                          f"head={data[:80]!r})", flush=True)
                    summary["fail"] += 1
                    continue
                out_dir.mkdir(parents=True, exist_ok=True)
                out_path = out_dir / fname
                out_path.write_bytes(data)
                sha = hashlib.sha256(data).hexdigest()[:16]
                print(f"          ✓ {fname} ({size:,} B, sha={sha})",
                      flush=True)

                update_urls_csv(ccn, "para_hcfs_playwright")
                append_download_row({
                    "ccn": ccn,
                    "state": state,
                    "url": url,
                    "local_path": str(out_path),
                    "size": size,
                    "content_type": "text/csv",
                    "sha256": hashlib.sha256(data).hexdigest(),
                    "status": "ok",
                    "fetched_at": pd.Timestamp.utcnow().isoformat(),
                    "method": "para_hcfs_playwright",
                })
                summary["ok"] += 1
            except Exception as e:
                print(f"          ✗ {type(e).__name__}: {e}", flush=True)
                summary["fail"] += 1

        browser.close()

    print()
    print(f"[done] {summary}")
    print()
    print("Next:  .venv/bin/python mrf/parse_mrf.py "
          "&& .venv/bin/python mrf/concat_parts.py")


if __name__ == "__main__":
    main()
