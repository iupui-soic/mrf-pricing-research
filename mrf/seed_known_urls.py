#!/usr/bin/env python3
"""
seed_known_urls.py
==================
Populate `mrf_urls.csv` from a curated or external seed file before the
automated crawler runs. Search-engine-based discovery is fragile at
500+ hospital scale (DDG/Bing rate-limit hard, Google CSE costs quota),
so the crawler is most reliable when it starts from a seed of known
hospital → MRF-URL pairs and only falls back to search for residuals.

Supported seed sources (listed in precedence order when multiple files
are available):

  1. `/data0/mrf/seed_manual.csv`        — hand-curated; authoritative
  2. `/data0/mrf/seed_health_systems.csv` — health-system MRF directories
                                          (Providence, Kaiser, HCA, etc.)
  3. `/data0/mrf/seed_dolthub.csv`       — DoltHub hospital-price-
                                          transparency-v3 snapshot (stale
                                          but covers ~30% US hospitals)
  4. `/data0/mrf/seed_turquoise.csv`     — Turquoise Health free 14-
                                          service public dataset

Each seed file must have columns: `ccn,mrf_url` (optional: `notes`).

Merges seeds in precedence order onto `hospitals.csv`, validates the URL
with a HEAD request, and writes `mrf_urls.csv` with only the rows that
resolved. Any hospital without a seed is left for `discover_mrf_urls.py`
to handle.

Usage:
    .venv/bin/python mrf/seed_known_urls.py
"""

from __future__ import annotations

import datetime as dt
import sys
import time
from pathlib import Path

import pandas as pd
import requests

OUT_DIR = Path("/data0/mrf")
HOSP_CSV = OUT_DIR / "hospitals.csv"
URL_CSV  = OUT_DIR / "mrf_urls.csv"

# Higher-precedence seeds (later, manually verified rounds) come FIRST so
# the first-wins dedup picks them over earlier failed-URL entries for the
# same CCN. Order matters: r3 (manually verified) > r2 (parent-system
# crawls) > original longtail batches.
SEEDS = (
    sorted(OUT_DIR.glob("seed_r3_*.csv"))
    + sorted(OUT_DIR.glob("seed_r2_*.csv"))
    + sorted(OUT_DIR.glob("seed_manual.csv"))
    + sorted(OUT_DIR.glob("seed_longtail*.csv"))
    + [p for p in sorted(OUT_DIR.glob("seed_*.csv"))
       if not p.name.startswith(("seed_r3_", "seed_r2_", "seed_manual",
                                  "seed_longtail"))]
)

HEADERS = {
    "User-Agent": (
        "PricePortal/0.1 (+https://github.com/iupui-soic/hcai-chargemasters; "
        "academic research crawler)"
    )
}


def load_seeds() -> pd.DataFrame:
    """Concat all seed CSVs with source tag, deduped on ccn (first wins)."""
    frames = []
    for p in SEEDS:
        if not p.exists():
            continue
        df = pd.read_csv(p, dtype=str, engine="python", on_bad_lines="warn")
        df.columns = [c.strip().lower() for c in df.columns]
        if "ccn" not in df.columns or "mrf_url" not in df.columns:
            print(f"[warn] {p}: missing ccn or mrf_url — skipping")
            continue
        df["seed_source"] = p.stem
        frames.append(df[["ccn", "mrf_url", "seed_source"] +
                         (["notes"] if "notes" in df.columns else [])])
    if not frames:
        print("[info] no seed files found")
        return pd.DataFrame(columns=["ccn", "mrf_url", "seed_source"])
    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(subset="ccn", keep="first")
    print(f"[seed] {len(out):,} unique (ccn → URL) pairs from {len(frames)} file(s)")
    return out


_FILE_CTYPES = (
    "text/csv", "text/plain", "application/csv",
    "application/json", "application/xml", "text/xml",
    "application/octet-stream",
)


def head_validate(url: str) -> tuple[bool, int | None, str | None]:
    """Validate via HEAD; accept on 200 with a file-like Content-Type.

    Some MRFs are tiny (4–8 KB for state-psych files), so size alone is a
    bad filter. Reject only when the response is unmistakably an HTML
    error/redirect page or returns non-200.
    """
    try:
        r = requests.head(url, headers=HEADERS, timeout=20, allow_redirects=True)
        size = r.headers.get("Content-Length")
        size = int(size) if size and size.isdigit() else None
        ctype = (r.headers.get("Content-Type") or "").lower().split(";")[0].strip()
        if r.status_code != 200:
            return False, r.status_code, size
        if ctype.startswith("text/html"):
            return False, r.status_code, size
        if ctype and not any(ctype.startswith(c) for c in _FILE_CTYPES):
            # Unknown content-type — accept if URL has a known file extension
            ext = url.lower().split("?")[0].rsplit(".", 1)[-1]
            if ext not in {"json", "csv", "xml", "jsonl", "gz", "zip"}:
                return False, r.status_code, size
        return True, r.status_code, size
    except requests.RequestException:
        return False, None, None


def main():
    if not HOSP_CSV.exists():
        sys.exit(f"missing {HOSP_CSV}")
    hosp = pd.read_csv(HOSP_CSV, dtype=str)
    seeds = load_seeds()

    if seeds.empty:
        print("[info] nothing to seed; exiting")
        return

    df = hosp.merge(seeds, on="ccn", how="inner")
    print(f"[merge] {len(df):,} hospitals have a seed URL")

    # Validate each URL
    rows = []
    ok_count = 0
    for i, r in df.iterrows():
        url = r["mrf_url"]
        ok, status, size = head_validate(url)
        if ok:
            ok_count += 1
        rows.append({
            "ccn": r["ccn"],
            "name": r["name"],
            "state": r["state"],
            "zip": r["zip"],
            "website": None,
            "mrf_url": url if ok else None,
            "mrf_format": url.split(".")[-1].lower().split("?")[0][:5] if ok else None,
            "mrf_size_bytes": size,
            "discovery_method": f"seed:{r['seed_source']}" if ok else "seed_invalid",
            "http_status": status,
            "discovered_at": dt.datetime.now(dt.UTC).isoformat(),
            "notes": "",
        })
        # Be polite: stagger HEAD requests
        time.sleep(0.2)
        if (i + 1) % 20 == 0:
            print(f"  [{i+1}/{len(df)}] {ok_count} OK so far")

    out = pd.DataFrame(rows)
    # If mrf_urls.csv already exists, merge with two preservation rules:
    # 1. Permanent exemption rows (discovery_method starts "exempt") always
    #    win — regulatory truth, not URL discovery state.
    # 2. If a CCN has a successful download on disk (downloads.csv shows
    #    status=ok for it), preserve the prior mrf_urls row instead of
    #    overwriting with a fresh `seed_invalid` — the URL clearly worked
    #    at some point, and HEAD-revalidation can fail transiently due to
    #    bot detection (Cloudflare 403s, CDN edge errors, host UA blocks).
    if URL_CSV.exists():
        prev = pd.read_csv(URL_CSV, dtype=str)
        exempt_mask = prev["discovery_method"].fillna("").str.startswith("exempt")
        exempt_ccns = set(prev.loc[exempt_mask, "ccn"])

        downloaded_ccns: set[str] = set()
        dl_csv = OUT_DIR / "downloads.csv"
        if dl_csv.exists():
            dl = pd.read_csv(dl_csv, dtype=str)
            downloaded_ccns = set(
                dl.loc[dl["status"].isin(("ok", "already_present")), "ccn"]
                  .dropna())

        # For CCNs whose new validation says seed_invalid but we already
        # downloaded the file, keep the previous (working) URL row instead.
        seed_invalid_mask = out["mrf_url"].isna()
        rescue_ccns = set(out.loc[seed_invalid_mask, "ccn"]) & downloaded_ccns
        if rescue_ccns:
            print(f"[rescue] {len(rescue_ccns)} CCNs failed HEAD this run "
                  f"but have ok downloads — keeping prior URL")
            out = out[~out["ccn"].isin(rescue_ccns)]

        protected_ccns = exempt_ccns | rescue_ccns
        out = out[~out["ccn"].isin(exempt_ccns)]
        prev_keep = prev[prev["ccn"].isin(protected_ccns)
                         | ~prev["ccn"].isin(out["ccn"])]
        out = pd.concat([out, prev_keep], ignore_index=True)
        out = out.drop_duplicates(subset="ccn", keep="first")

    out.to_csv(URL_CSV, index=False)
    print(f"\n[out] {URL_CSV}  ({len(out):,} rows; {ok_count:,} seed-validated)")


if __name__ == "__main__":
    main()
