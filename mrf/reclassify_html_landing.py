#!/usr/bin/env python3
"""
reclassify_html_landing.py
==========================
Discovers files marked status=ok in downloads.csv whose content is in
fact an HTML landing/error page (PARA HCFS portal pages, hospital
transparency-page redirects, etc.) and reclassifies them as
`exempt:portal_landing` in mrf_urls.csv + status=html_landing in
downloads.csv. The actual file is kept on disk for audit trail; it just
no longer counts as a valid MRF.

Found 23 such files on first pass; mostly apps.para-hcfs.com aspx
endpoints that require a JS challenge to serve the underlying file.
"""

from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path

OUT_DIR = Path("/data0/mrf-pricing-research/mrf")
URL_CSV = OUT_DIR / "mrf_urls.csv"
DL_CSV = OUT_DIR / "downloads.csv"
FILES_DIR = OUT_DIR / "files"


def is_html(path: Path) -> bool:
    if not path.is_file():
        return False
    with path.open("rb") as f:
        magic = f.read(64).lower().lstrip()
    return magic.startswith((b"<!doctype html", b"<html"))


def main() -> None:
    # Backups
    for p in (URL_CSV, DL_CSV):
        bak = p.with_suffix(p.suffix + ".bak.preHTMLfix")
        bak.write_bytes(p.read_bytes())
        print(f"[backup] {bak}")

    # Find HTML-content rows in downloads.csv that are status=ok
    suspects: list[dict] = []
    with DL_CSV.open() as f:
        cols = csv.DictReader(f).fieldnames
        f.seek(0)
        rows = list(csv.DictReader(f))
    for r in rows:
        if r.get("status") != "ok":
            continue
        if is_html(Path(r["local_path"])):
            suspects.append(r)
    print(f"[scan] {len(suspects)} html-landing files mis-classified as ok")

    suspect_ccns = {r["ccn"] for r in suspects}
    now = dt.datetime.now(dt.timezone.utc).isoformat()

    # Update downloads.csv
    for r in rows:
        if r["ccn"] in suspect_ccns and r.get("status") == "ok":
            r["status"] = "html_landing"
            r["error"] = ("response was an HTML landing/portal page, not the "
                          "actual MRF; reclassified as exempt")
    with DL_CSV.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"[downloads] flipped {len(suspect_ccns)} rows ok -> html_landing")

    # Update mrf_urls.csv: change discovery_method to exempt:portal_landing
    with URL_CSV.open() as f:
        ucols = csv.DictReader(f).fieldnames
        f.seek(0)
        urows = list(csv.DictReader(f))
    flipped = 0
    for r in urows:
        if r["ccn"] in suspect_ccns:
            r["discovery_method"] = "exempt:portal_landing"
            r["mrf_format"] = ""
            r["mrf_size_bytes"] = ""
            r["http_status"] = ""
            r["discovered_at"] = now
            r["notes"] = ("HTML landing page returned instead of MRF "
                          "(PARA HCFS / hospitalpricedisclosure / hospital "
                          "transparency redirect); requires JS-capable "
                          "browser session to reach the underlying file")
            flipped += 1
    with URL_CSV.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=ucols)
        w.writeheader()
        for r in urows:
            w.writerow(r)
    print(f"[mrf_urls] flipped {flipped} rows -> exempt:portal_landing")


if __name__ == "__main__":
    main()
