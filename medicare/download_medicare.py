#!/usr/bin/env python3
"""
download_medicare.py
====================
Fetches CMS Medicare fee-schedule data into /data0/mrf-pricing-research/medicare/:

  mpfs/  — Physician Fee Schedule RVU files (CY2024–2026, quarterly)
  opps/  — Outpatient PPS Addendum B (HCPCS × APC × payment rate)
  ipps/  — Inpatient PPS Table 5 (MS-DRG weights, GMLOS, AMLOS)

After download, each .zip is extracted into /data0/mrf-pricing-research/medicare/extracted/<slot>/
for downstream parsers. A `downloads.csv` ledger is written with
sha256/size/source URL for every artifact (Reproducibility checklist §10
of PROJECT_PLAN.md requires this).

Usage:
    .venv/bin/python medicare/download_medicare.py            # default: full set
    .venv/bin/python medicare/download_medicare.py --resume   # skip files already on disk
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import sys
import zipfile
from pathlib import Path

import requests

OUT_DIR = Path("/data0/mrf-pricing-research/medicare")
LEDGER = OUT_DIR / "downloads.csv"

UA = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120 Safari/537.36"
    )
}

# Verified 2026-04-26 against cms.gov canonical paths.
URLS: dict[str, str] = {
    # MPFS RVU files: each ZIP contains PPRRVU (per-HCPCS RVUs), GPCI
    # (locality cost indices), and ZIP-to-locality crosswalks. The "A"
    # release is January (initial), "D" is October (final-corrections).
    "mpfs/rvu24a.zip": "https://www.cms.gov/files/zip/rvu24a.zip",
    "mpfs/rvu24d.zip": "https://www.cms.gov/files/zip/rvu24d.zip",
    "mpfs/rvu25a.zip": "https://www.cms.gov/files/zip/rvu25a.zip",
    "mpfs/rvu25d.zip": "https://www.cms.gov/files/zip/rvu25d.zip",
    "mpfs/rvu26a.zip": "https://www.cms.gov/files/zip/rvu26a.zip",
    # OPPS Addendum B: HCPCS code × APC × Status Indicator × payment rate
    # (national unadjusted). Quarterly. Most recent for each CY taken.
    "opps/addendum_b_2025_jul.zip":
        "https://www.cms.gov/files/zip/july-2025-opps-addendum-b.zip",
    "opps/addendum_b_2026_jan.zip":
        "https://www.cms.gov/files/zip/january-2026-opps-addendum-b.zip",
    # IPPS Table 5: MS-DRG list with weights (relative weight, GMLOS, AMLOS)
    # by federal fiscal year. FY2025 published Aug 2024, FY2026 published
    # Aug 2025; both currently effective for different discharge windows.
    "ipps/table5_fy2025.zip":
        "https://www.cms.gov/files/zip/fy-2025-ipps-final-rule-table-5.zip",
    "ipps/table5_fy2026.zip":
        "https://www.cms.gov/files/zip/fy2026-ipps-fr-table-5.zip",
}


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def download_one(rel_path: str, url: str, *, resume: bool) -> dict:
    dest = OUT_DIR / rel_path
    dest.parent.mkdir(parents=True, exist_ok=True)
    if resume and dest.exists() and dest.stat().st_size > 1024:
        size = dest.stat().st_size
        print(f"  [skip] {rel_path} already on disk ({size:,} bytes)")
        return _row(rel_path, url, dest, size, sha256_of(dest), "ok",
                    "already_present")

    tmp = dest.with_suffix(dest.suffix + ".part")
    print(f"  [get ] {url}")
    try:
        with requests.get(url, headers=UA, timeout=60, stream=True) as r:
            r.raise_for_status()
            ctype = (r.headers.get("Content-Type") or "").split(";")[0].strip()
            if "zip" not in ctype and "octet-stream" not in ctype:
                print(f"  [warn] {rel_path}: unexpected Content-Type {ctype!r}")
            with tmp.open("wb") as f:
                for chunk in r.iter_content(1 << 20):
                    if chunk:
                        f.write(chunk)
        tmp.replace(dest)
    except Exception as e:
        if tmp.exists():
            tmp.unlink()
        print(f"  [fail] {rel_path}: {e}")
        return _row(rel_path, url, dest, 0, "", "fail", str(e)[:200])

    size = dest.stat().st_size
    sha = sha256_of(dest)
    print(f"  [ok  ] {rel_path}  {size:,} bytes  sha256={sha[:12]}…")
    return _row(rel_path, url, dest, size, sha, "ok", "")


def _row(rel: str, url: str, path: Path, size: int, sha: str,
         status: str, error: str) -> dict:
    return {
        "rel_path": rel,
        "source_url": url,
        "local_path": str(path),
        "bytes": size,
        "sha256": sha,
        "status": status,
        "downloaded_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "error": error,
    }


def write_ledger(rows: list[dict]) -> None:
    cols = list(rows[0].keys())
    with LEDGER.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"\n[ledger] {LEDGER} ({len(rows)} rows)")


def extract_all(rows: list[dict]) -> None:
    extract_root = OUT_DIR / "extracted"
    for r in rows:
        if r["status"] != "ok":
            continue
        slot = Path(r["rel_path"]).with_suffix("").name  # rvu24a, addendum_b_2026_jan, etc.
        target = extract_root / slot
        if target.exists() and any(target.iterdir()):
            continue
        target.mkdir(parents=True, exist_ok=True)
        try:
            with zipfile.ZipFile(r["local_path"]) as z:
                z.extractall(target)
            n = sum(1 for _ in target.rglob("*") if _.is_file())
            print(f"  [unzip] {slot}: {n} files -> {target}")
        except zipfile.BadZipFile as e:
            print(f"  [fail] {slot}: not a valid zip: {e}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--resume", action="store_true",
                    help="skip files already on disk")
    args = ap.parse_args()

    rows = []
    print(f"[start] downloading {len(URLS)} CMS files -> {OUT_DIR}")
    for rel, url in URLS.items():
        rows.append(download_one(rel, url, resume=args.resume))

    write_ledger(rows)
    print("\n[extract] unzipping artifacts")
    extract_all(rows)

    n_ok = sum(1 for r in rows if r["status"] == "ok")
    print(f"\n[done] {n_ok}/{len(rows)} files OK")
    return 0 if n_ok == len(rows) else 1


if __name__ == "__main__":
    sys.exit(main())
