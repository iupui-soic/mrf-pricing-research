#!/usr/bin/env python3
"""
parse_kaiser.py
===============
Recovers gross-charge data from Kaiser Foundation Hospitals' chargemaster
ZIPs that the main parser flagged as `skip:kaiser_legacy`.

Kaiser does not publish a CMS §180-compliant MRF — instead each ZIP at
`uhsfilecdn` / `kp.org` contains 5 inner CSVs (CDM, DRG, INTRODUCTION,
Medications, Supply). The CDM is the only file with codeable line items
and dollar amounts in a parseable layout. Negotiated cells in the CDM
are placeholder strings ("Note A", "Not applicable", "None") because
Kaiser is an integrated system, so we emit GROSS-only output.

Reads the source ZIPs by walking `/data0/mrf-pricing-research/mrf/parsed/mrf_log.csv` for rows
with `status == "skip:kaiser_legacy"` whose filename starts with the
Kaiser EIN `941105628-`. Writes one parquet part per CCN to
`/data0/mrf-pricing-research/mrf/parsed/parts/gross_<state>_<ccn>.parquet` so the existing
`concat_parts.py` can fold them into `mrf_gross.parquet` unchanged.

After this runs:
    .venv/bin/python mrf/concat_parts.py
"""

from __future__ import annotations

import csv
import io
import re
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq

# Reuse the gross schema from parse_mrf so concat picks it up cleanly.
sys.path.insert(0, str(Path(__file__).parent))
from parse_mrf import GROSS_SCHEMA, PARTS_DIR  # type: ignore

LOG_CSV = Path("/data0/mrf-pricing-research/mrf/parsed/mrf_log.csv")
DONE_DIR = Path("/data0/mrf-pricing-research/mrf/parsed/done")
KAISER_EIN = "941105628"
SCHEMA_TAG = "kaiser_chargemaster_2023"
FILE_FORMAT = "kaiser_cdm"

# Allow csv.reader to handle the multi-line header cell ("Charge #\n(Px Code)").
csv.field_size_limit(10_000_000)


_DOLLAR_RE = re.compile(r"[\d.]+")


def _to_float(cell: str | None) -> float | None:
    if not cell:
        return None
    s = str(cell).strip()
    if not s or s.lower() in {"note a", "not applicable", "none", "n/a", "na", "-"}:
        return None
    # " $6,140 " → 6140.0
    cleaned = s.replace(",", "").replace("$", "").strip()
    m = _DOLLAR_RE.search(cleaned)
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def _norm(s: str | None) -> str:
    return (s or "").strip()


@dataclass
class KaiserTask:
    ccn: str
    state: str
    zip_path: Path


def discover_tasks() -> list[KaiserTask]:
    tasks: list[KaiserTask] = []
    seen: set[str] = set()
    with LOG_CSV.open() as f:
        for row in csv.DictReader(f):
            if row.get("status") != "skip:kaiser_legacy":
                continue
            src = row.get("source_file", "")
            fn = Path(src).name
            if not fn.startswith(KAISER_EIN + "-"):
                continue
            ccn = row["ccn"]
            if ccn in seen:
                continue
            seen.add(ccn)
            tasks.append(KaiserTask(ccn=ccn, state=row["state"],
                                    zip_path=Path(src)))
    return tasks


def _find_cdm_inner(z: zipfile.ZipFile) -> str | None:
    for n in z.namelist():
        low = n.lower()
        if low.endswith(".csv") and "chargedescriptionmaster" in low:
            return n
    return None


def _index_of(header: list[str], *needles: str) -> int | None:
    """Find the first column whose normalized name contains all needles."""
    for i, h in enumerate(header):
        hn = h.lower().replace("\n", " ").replace("  ", " ").strip()
        if all(nd in hn for nd in needles):
            return i
    return None


def _classify_code(code: str) -> str:
    """Best-effort code_type tag for the Kaiser 'Procedure Code (CPT / HCPCS)' col."""
    if not code:
        return ""
    c = code.strip()
    if not c:
        return ""
    if c[0].isalpha() and len(c) <= 7:
        return "HCPCS"
    if c.isdigit() and len(c) == 5:
        return "CPT"
    return ""


def parse_cdm(task: KaiserTask) -> tuple[int, str]:
    """Parse one Kaiser CDM, write parquet part. Returns (n_rows, status)."""
    gross_part = PARTS_DIR / f"gross_{task.state}_{task.ccn}.parquet"
    gtmp = gross_part.with_suffix(".parquet.tmp")
    if gtmp.exists():
        gtmp.unlink()

    with zipfile.ZipFile(task.zip_path) as z:
        inner = _find_cdm_inner(z)
        if not inner:
            return (0, "no_cdm_inner")
        with z.open(inner) as fp:
            data = fp.read().decode("utf-8", errors="replace")

    reader = csv.reader(io.StringIO(data))
    rows = list(reader)
    if len(rows) < 6:
        return (0, "too_short")

    # Find the header row — first row with >= 6 cells where one cell contains
    # "Procedure Name" or "Procedure Code" (some hospitals shift it by ±1 row).
    header_idx = None
    for i in range(min(10, len(rows))):
        joined = "|".join((c or "") for c in rows[i]).lower()
        if "gross charge" in joined and ("procedure name" in joined
                                         or "description" in joined):
            header_idx = i
            break
    if header_idx is None:
        return (0, "header_not_found")

    header = [c.strip() for c in rows[header_idx]]
    data_rows = rows[header_idx + 1:]

    # Map columns. Some hospitals say "Billing Code", others "Rev code".
    i_charge_id  = _index_of(header, "charge", "px") or _index_of(header, "charge")
    i_proc_code  = _index_of(header, "procedure", "code")
    i_modifier   = _index_of(header, "default", "modifier")
    i_rev        = _index_of(header, "billing", "code") or _index_of(header, "rev")
    i_proc_name  = _index_of(header, "procedure", "name")
    i_gross      = _index_of(header, "gross", "charge")
    i_cash       = _index_of(header, "discounted", "cash")
    i_setting    = _index_of(header, "inpatient", "outpatient")
    i_min        = _index_of(header, "minimum", "negotiated")
    i_max        = _index_of(header, "maximum", "negotiated")

    if i_proc_name is None or i_gross is None:
        return (0, "missing_required_cols")

    builder = {col: [] for col in GROSS_SCHEMA.names}
    n = 0
    src_str = str(task.zip_path)

    for r in data_rows:
        if not r or all((not c or not c.strip()) for c in r):
            continue
        if len(r) < len(header) and len(r) <= 2:
            continue  # trailing footer line
        def g(i):
            return r[i].strip() if i is not None and i < len(r) and r[i] else ""

        proc_name  = g(i_proc_name)
        if not proc_name:
            continue
        gross = _to_float(g(i_gross))
        cash  = _to_float(g(i_cash))
        if gross is None and cash is None:
            continue  # no usable price

        proc_code  = g(i_proc_code)
        charge_id  = g(i_charge_id)
        rev_code   = g(i_rev)
        modifier   = g(i_modifier)
        setting    = g(i_setting).upper()
        if "BOTH" in setting:
            billing = "both"; sset = "both"
        elif "INPATIENT" in setting and "OUTPATIENT" in setting:
            billing = "both"; sset = "both"
        elif "INPATIENT" in setting:
            billing = "inpatient"; sset = "inpatient"
        elif "OUTPATIENT" in setting:
            billing = "outpatient"; sset = "outpatient"
        else:
            billing = ""; sset = ""

        builder["ccn"].append(task.ccn)
        builder["description"].append(proc_name[:500])
        builder["code"].append(proc_code[:32])
        builder["code_type"].append(_classify_code(proc_code))
        builder["code2"].append((charge_id or rev_code)[:32])
        builder["code2_type"].append("kaiser_charge_id" if charge_id
                                     else ("rev_code" if rev_code else ""))
        builder["billing_class"].append(billing)
        builder["setting"].append(sset)
        builder["drug_unit"].append("")
        builder["drug_type"].append("")
        builder["modifiers"].append(modifier)
        builder["gross_charge"].append(gross)
        builder["discounted_cash"].append(cash)
        builder["standard_charge_min"].append(_to_float(g(i_min)))
        builder["standard_charge_max"].append(_to_float(g(i_max)))
        builder["schema_version"].append(SCHEMA_TAG)
        builder["file_format"].append(FILE_FORMAT)
        builder["source_file"].append(src_str)
        n += 1

    if n == 0:
        return (0, "no_priceable_rows")

    table = pa.table(builder, schema=GROSS_SCHEMA)
    pq.write_table(table, gtmp, compression="snappy")
    gtmp.replace(gross_part)
    DONE_DIR.mkdir(parents=True, exist_ok=True)
    (DONE_DIR / f"{task.state}_{task.ccn}.flag").touch()
    return (n, "ok")


def main() -> None:
    tasks = discover_tasks()
    print(f"[plan] {len(tasks)} Kaiser ZIPs to parse", flush=True)
    if not tasks:
        return
    PARTS_DIR.mkdir(parents=True, exist_ok=True)
    summary: dict[str, int] = {}
    total_rows = 0
    for i, t in enumerate(tasks, 1):
        try:
            n, status = parse_cdm(t)
        except Exception as e:
            n, status = 0, f"exception:{type(e).__name__}"
            print(f"  [err] ccn={t.ccn} {e!r}", flush=True)
        summary[status] = summary.get(status, 0) + 1
        total_rows += n
        print(f"[{i:2d}/{len(tasks)}] {status:22s} ccn={t.ccn} ({t.state}) "
              f"rows={n:>6d}", flush=True)

    print()
    print(f"[done] {len(tasks)} hospitals, {total_rows:,} rows total")
    for k, v in sorted(summary.items()):
        print(f"  {k:22s} {v}")
    print()
    print("Run `python mrf/concat_parts.py` to fold these into the unified parquets.")


if __name__ == "__main__":
    main()
