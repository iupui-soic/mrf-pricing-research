#!/usr/bin/env python3
"""
concat_parts.py
===============
Merges per-hospital parquet parts written by parse_mrf.py into the two
unified output files:

  /data0/mrf-pricing-research/mrf/parsed/mrf_gross.parquet
  /data0/mrf-pricing-research/mrf/parsed/mrf_negotiated.parquet

Reads files from /data0/mrf-pricing-research/mrf/parsed/parts/{gross,neg}_<state>_<ccn>.parquet
and streams them through a single ParquetWriter (preserves schema,
re-uses snappy compression). Memory-bounded: each part is read and
written one at a time.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq

PARTS = Path("/data0/mrf-pricing-research/mrf/parsed/parts")
OUT_DIR = Path("/data0/mrf-pricing-research/mrf/parsed")
GROSS_OUT = OUT_DIR / "mrf_gross.parquet"
NEG_OUT = OUT_DIR / "mrf_negotiated.parquet"


def concat(prefix: str, out: Path) -> int:
    parts = sorted(PARTS.glob(f"{prefix}_*.parquet"))
    if not parts:
        print(f"[skip] no parts for {prefix}_*")
        return 0
    schema = pq.read_schema(parts[0])
    out_tmp = out.with_suffix(".parquet.tmp")
    if out_tmp.exists():
        out_tmp.unlink()
    writer = pq.ParquetWriter(out_tmp, schema, compression="snappy")
    n_rows = 0
    n_files = 0
    try:
        for p in parts:
            t = pq.read_table(p)
            if t.num_rows == 0:
                continue
            writer.write_table(t)
            n_rows += t.num_rows
            n_files += 1
            if n_files % 25 == 0:
                print(f"  {prefix}: {n_files}/{len(parts)} files, "
                      f"{n_rows:,} rows so far", flush=True)
    finally:
        writer.close()
    out_tmp.replace(out)
    size_mb = out.stat().st_size / 1e6
    print(f"[out] {out}: {n_rows:,} rows from {n_files} parts ({size_mb:.1f} MB)")
    return n_rows


def main() -> None:
    print(f"[plan] concat parts under {PARTS}")
    g = concat("gross", GROSS_OUT)
    n = concat("neg", NEG_OUT)
    print(f"\n[done] gross={g:,} negotiated={n:,}")


if __name__ == "__main__":
    main()
