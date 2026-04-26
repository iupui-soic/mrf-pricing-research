#!/usr/bin/env python3
"""
retry_get.py
============
Some hospital MRF servers return 403/redirect/empty for HEAD requests but
respond correctly to GET. `seed_known_urls.py` validates with HEAD and
discards those URLs as `seed_invalid`, so the downloader never tries them.
This script reads `/tmp/retry_unresolved.csv` (built by an ad-hoc query
against the seed CSVs) and runs `download_one()` from `download_mrfs.py`
on each row. Successful downloads update `downloads.csv` and the file
lands on disk; the next `seed_known_urls.py` run will then accept those
URLs since the file exists.

Inputs:  /tmp/retry_unresolved.csv  (cols: ccn,state,mrf_url,name,source)
Outputs: appended rows in /data0/mrf/downloads.csv;
         /data0/mrf/files/<state>/<ccn>/<basename> for each success.

Usage:
    .venv/bin/python mrf/retry_get.py
"""

from __future__ import annotations

import concurrent.futures
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from download_mrfs import download_one, DL_CSV  # noqa: E402

RETRY_CSV = Path("/tmp/retry_unresolved.csv")


def main() -> None:
    df = pd.read_csv(RETRY_CSV, dtype=str)
    print(f"[retry] {len(df)} URLs to GET-retry")

    cols = ["ccn", "state", "mrf_url", "local_path", "status",
            "bytes_downloaded", "sha256", "content_type",
            "downloaded_at", "error"]

    rows = df.to_dict("records")
    n_done = 0
    n_ok = 0
    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
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
            if result.get("status") == "ok":
                n_ok += 1
            if n_done % 5 == 0 or n_done == len(rows):
                rate = n_done / max(time.time() - t0, 1e-9)
                print(f"  [{n_done}/{len(rows)}] {ccn} {result.get('status','?'):20s} "
                      f"({(result.get('bytes_downloaded') or 0)/1024/1024:7.1f} MB)  "
                      f"new_ok={n_ok}  ({rate:.2f}/s)")

    print(f"\n[done] {n_done} attempts, {n_ok} new ok, {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
