#!/usr/bin/env python3
"""
parse_misc_wide.py
==================
Recovers the 16 misc CSV chargemasters that `parse_mrf.py` flagged as
`skip:kaiser_legacy` but which AREN'T actually Kaiser. These fall into a
few sub-formats (IHH/Prime "Age: No" wide, PARA "Run Date" wide, Adams
Memorial tall, OrthoIndy chargemaster, El Camino, Tri-City, etc.).

Strategy: scan the first ~20 rows for a header row containing a "gross"
charge column. Fuzzy-map the remaining columns. Then emit one of three
output shapes:

  - **chargemaster**  (gross-only, no payer): one gross row per item
  - **tall**          (has PAYER_NAME column): one gross row per unique
                      item + one neg row per item × payer
  - **wide**          (multiple named payer columns): one gross row per
                      item + one neg row per (item, payer) where the
                      payer cell has a real dollar value

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

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))
from parse_mrf import GROSS_SCHEMA, NEG_SCHEMA, PARTS_DIR, DONE_DIR  # type: ignore

LOG_CSV = Path("/data0/mrf-pricing-research/mrf/parsed/mrf_log.csv")
URLS_CSV = Path("/data0/mrf-pricing-research/mrf/mrf_urls.csv")
HOSPITALS_CSV = Path("/data0/mrf-pricing-research/mrf/hospitals.csv")
KAISER_EIN = "941105628"
SCHEMA_TAG_PREFIX = "misc_csv"

# Soft-exempt categories whose hospitals may have a downloaded file we can
# salvage gross-charge data from (the file is not §180 compliant but is a
# usable chargemaster).
SOFT_EXEMPT_METHODS = {
    "exempt:non_compliant",
    "exempt:non_standard_filename",
    "exempt:bot_blocked_needs_manual",
    "exempt:file_rotated_needs_manual",
}

csv.field_size_limit(50_000_000)


# ---------- Cell helpers ----------

_DOLLAR_NUM = re.compile(r"-?\d[\d,]*\.?\d*")
_PLACEHOLDERS = {
    "", "n/a", "na", "note a", "not applicable", "none",
    "-", "--", ".", "0", "0.00", "$0.00", "$ -",
}


def _to_float(cell) -> float | None:
    if cell is None:
        return None
    s = str(cell).strip()
    if s.lower().strip("$ ").strip() in _PLACEHOLDERS:
        return None
    s2 = s.replace(",", "").replace("$", "").strip()
    m = _DOLLAR_NUM.search(s2)
    if not m:
        return None
    try:
        v = float(m.group(0).replace(",", ""))
        return v if v != 0 else None
    except ValueError:
        return None


def _norm(s) -> str:
    return ("" if s is None else str(s)).strip()


def _norm_header(s) -> str:
    return _norm(s).lower().replace("\n", " ").replace("\r", " ").strip()


# ---------- Column mapping (fuzzy) ----------

GROSS_PATTERNS  = ["gross_charge", "gross charge", "grosscharge", "lastprice",
                   "list_price", "list price", "standard_charge",
                   "cdmcharge", "cdm_charge"]
# Fallback: a header cell whose normalized value is exactly one of these means
# "the price column" when no specific GROSS_PATTERNS match.
GROSS_BARE_PATTERNS = {"charge", "current charge", "price", "current price"}
CASH_PATTERNS   = ["discounted_cash", "discounted cash", "cash_price",
                   "cash price", "self_pay", "self pay"]
MIN_PATTERNS    = ["min_negotiated", "min negotiated", "minimum negotiated",
                   "de-identified minimum", "de_identified_minimum",
                   "deidentified minimum"]
MAX_PATTERNS    = ["max_negotiated", "max negotiated", "maximum negotiated",
                   "de-identified maximum", "de_identified_maximum",
                   "deidentified maximum"]
DESC_PATTERNS   = ["procedure_name", "procedure name", "charge_description",
                   "charge description", "description", "primary_code_description",
                   "primary code description", "billdesc", "desc"]
CODE_PATTERNS   = ["primary_code", "cpt_hcpcs", "cpt/hcpcs", "cpt hcpcs",
                   "hcpcs_code", "cpt_code", "hcpcs", "procedure_code",
                   "procedure code", "cpt"]
REV_PATTERNS    = ["rev_code", "rev code", "revenue_code", "revenue code",
                   "billing_code", "billing code", "chgcat"]
PAYER_NAME_PATTERNS = ["payer_name", "payer name", "insurance plan",
                       "insurance_plan"]
SETTING_PATTERNS = ["billing_class", "billing class", "patient_type",
                    "patient type", "pt_summary", "bill_type", "setting",
                    "inpatient/outpatient", "inpatient / outpatient"]
PAYER_NEGOTIATED_PATTERNS = ["payer_negotiated_rate", "payer negotiated",
                             "negotiated_rate", "negotiated rate",
                             "standard_charge_negotiated"]


def find_col(header_low: list[str], patterns: list[str]) -> int | None:
    for i, h in enumerate(header_low):
        for pat in patterns:
            if pat in h:
                return i
    return None


def find_gross_col(header_low: list[str]) -> int | None:
    """Find the gross-charge column. Tries specific patterns, falls back to
    a bare 'Charge' / 'Price' header cell."""
    i = find_col(header_low, GROSS_PATTERNS)
    if i is not None:
        return i
    for i, h in enumerate(header_low):
        if h in GROSS_BARE_PATTERNS:
            return i
    return None


def find_header_row(rows: list[list[str]], max_scan: int = 25) -> int | None:
    """Return the index of the first row that looks like a data header.
    Accepts: a 'gross_charge'/'lastprice'/'cdmcharge' column, OR a bare
    'Charge'/'Price' column header (used by some chargemaster CSVs)."""
    for i, r in enumerate(rows[:max_scan]):
        cells = [_norm_header(c) for c in r]
        joined = "|".join(cells)
        has_price = any(p in joined for p in GROSS_PATTERNS) \
            or any(c in GROSS_BARE_PATTERNS for c in cells)
        if not has_price:
            continue
        has_desc = any(d in joined for d in
                       ["description", "procedure", "billdesc",
                        "cdmdesc", "deptname"]) \
            or any(c == "desc" or c == "name" for c in cells)
        if has_desc:
            return i
    return None


def _classify_code(code: str) -> str:
    c = (code or "").strip()
    if not c: return ""
    if c[0].isalpha() and len(c) <= 7: return "HCPCS"
    if c.isdigit() and len(c) == 5:    return "CPT"
    return ""


# ---------- Discovery ----------

@dataclass
class MiscTask:
    ccn: str
    state: str
    path: Path


def discover_tasks() -> list[MiscTask]:
    tasks: list[MiscTask] = []
    seen: set[str] = set()

    # Source 1: parser log, status=skip:kaiser_legacy (the misc bucket).
    with LOG_CSV.open() as f:
        for row in csv.DictReader(f):
            if row.get("status") != "skip:kaiser_legacy":
                continue
            src = row.get("source_file", "")
            fn = Path(src).name
            if fn.startswith(KAISER_EIN + "-"):
                continue  # actual Kaiser; handled by parse_kaiser.py
            ccn = row["ccn"]
            if ccn in seen:
                continue
            seen.add(ccn)
            tasks.append(MiscTask(ccn=ccn, state=row["state"], path=Path(src)))

    # Source 2: soft-exempt CCNs in mrf_urls.csv that DO have a downloaded
    # file on disk — usually non-§180 chargemasters that were exempted
    # before being routed through the main parser.
    if URLS_CSV.exists() and HOSPITALS_CSV.exists():
        hosp = {}
        with HOSPITALS_CSV.open() as f:
            for row in csv.DictReader(f):
                hosp[row["ccn"].zfill(6)] = row.get("state", "")
        with URLS_CSV.open() as f:
            for row in csv.DictReader(f):
                if row.get("discovery_method") not in SOFT_EXEMPT_METHODS:
                    continue
                ccn = row["ccn"].zfill(6)
                if ccn in seen:
                    continue
                state = hosp.get(ccn)
                if state not in {"CA", "IN"}:
                    continue
                files_dir = Path(f"/data0/mrf-pricing-research/mrf/files/{state}/{ccn}")
                if not files_dir.exists():
                    continue
                # Pick the largest CSV/JSON/ZIP file in the directory.
                candidates = [p for p in files_dir.iterdir()
                              if p.is_file() and p.suffix.lower() in
                              {".csv", ".json", ".zip", ""}]
                if not candidates:
                    continue
                candidates.sort(key=lambda p: p.stat().st_size, reverse=True)
                seen.add(ccn)
                tasks.append(MiscTask(ccn=ccn, state=state, path=candidates[0]))

    return tasks


# ---------- Reader ----------

def _load_csv_rows(path: Path) -> list[list[str]]:
    """Load rows from a CSV (or first CSV inside a ZIP), tolerating BOM."""
    data: bytes
    if path.suffix.lower() == ".zip" or zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as z:
            inner = next((n for n in z.namelist() if n.lower().endswith(".csv")),
                         None)
            if not inner:
                return []
            with z.open(inner) as f:
                data = f.read()
    else:
        data = path.read_bytes()
    text = data.decode("utf-8-sig", errors="replace")
    return list(csv.reader(io.StringIO(text)))


# ---------- Parse one ----------

def parse_one(task: MiscTask) -> tuple[int, int, str]:
    """Returns (n_gross_rows, n_neg_rows, status)."""
    rows = _load_csv_rows(task.path)
    if len(rows) < 3:
        return (0, 0, "too_short")

    h_idx = find_header_row(rows)
    if h_idx is None:
        return (0, 0, "no_header_found")

    header     = [_norm(c) for c in rows[h_idx]]
    header_low = [_norm_header(c) for c in header]
    data_rows  = rows[h_idx + 1:]

    i_gross   = find_gross_col(header_low)
    if i_gross is None:
        return (0, 0, "no_gross_col")
    i_cash    = find_col(header_low, CASH_PATTERNS)
    i_min     = find_col(header_low, MIN_PATTERNS)
    i_max     = find_col(header_low, MAX_PATTERNS)
    i_desc    = find_col(header_low, DESC_PATTERNS)
    i_code    = find_col(header_low, CODE_PATTERNS)
    i_rev     = find_col(header_low, REV_PATTERNS)
    i_setting = find_col(header_low, SETTING_PATTERNS)
    i_payer   = find_col(header_low, PAYER_NAME_PATTERNS)
    i_payer_rate = find_col(header_low, PAYER_NEGOTIATED_PATTERNS)

    if i_desc is None:
        return (0, 0, "no_desc_col")

    # Identify wide payer columns: any column with a $ in >50% of rows that
    # ISN'T already classified above.
    classified = {x for x in [i_gross, i_cash, i_min, i_max, i_desc, i_code,
                              i_rev, i_setting, i_payer, i_payer_rate]
                  if x is not None}
    payer_col_indices: list[int] = []
    sample = data_rows[:200]
    for j in range(len(header_low)):
        if j in classified:
            continue
        h = header_low[j]
        if not h or len(h) < 4:
            continue
        # Skip likely metadata cols
        if any(skip in h for skip in [
                "as of date", "line type", "line id", "charge code", "drg",
                "modifier", "ndc", "rev_code", "rev code", "active", "dept",
                "code type", "price tier", "rx_unit", "rx unit", "deptname",
                "barproc", "cpt4", "hcpcs", "primary_code", "primary code",
                "patient type", "patient_type", "pt_summary", "billdesc",
                "desc", "name", "location", "bill_type", "bill type",
                "modifiers"]):
            continue
        # Count cells with usable dollar values, AND require either decimals
        # or $-signs in a meaningful share — otherwise a column of integer
        # CDM codes / rev codes / NDCs is mistaken for a payer rate column.
        n_dollars = 0
        n_seen = 0
        n_decimal_or_dollar = 0
        for r in sample:
            if j < len(r):
                cell = r[j]
                v = _to_float(cell)
                if v is not None:
                    n_dollars += 1
                    cs = str(cell)
                    if "." in cs or "$" in cs:
                        n_decimal_or_dollar += 1
                n_seen += 1
        if not n_seen:
            continue
        # Two acceptance paths:
        #  (a) header is explicitly payer-ish (price/rate/$/known payer name)
        header_explicit = any(tok in h for tok in [
            "price", "rate", "negotiated", " $", "$ ", "payer",
            "aetna", "anthem", "blue ", "cigna", "humana", "united",
            "kaiser", "medicare", "medicaid", "tricare", "molina",
            "centene", "wellcare", "champva"])
        share_dollars = n_dollars / n_seen
        share_decimal = n_decimal_or_dollar / n_seen
        if header_explicit and share_dollars >= 0.20:
            payer_col_indices.append(j)
        elif share_dollars >= 0.20 and share_decimal >= 0.30:
            # Fallback: lots of dollar-shaped values, mostly with decimals.
            payer_col_indices.append(j)

    # Decide format
    if i_payer is not None or i_payer_rate is not None:
        fmt = "tall"
    elif payer_col_indices:
        fmt = "wide"
    else:
        fmt = "chargemaster"

    schema_tag = f"{SCHEMA_TAG_PREFIX}_{fmt}"
    file_format = f"misc_{fmt}"
    src_str = str(task.path)

    g = {col: [] for col in GROSS_SCHEMA.names}
    n = {col: [] for col in NEG_SCHEMA.names}

    seen_keys: set[tuple] = set()  # for tall: dedupe gross rows per (code,desc)

    for r in data_rows:
        if not r or all(not _norm(c) for c in r):
            continue
        def col(i):
            return r[i] if i is not None and i < len(r) else ""

        desc = _norm(col(i_desc))
        if not desc:
            continue
        gross_v = _to_float(col(i_gross))
        cash_v  = _to_float(col(i_cash))
        min_v   = _to_float(col(i_min))
        max_v   = _to_float(col(i_max))
        if gross_v is None and cash_v is None and min_v is None and max_v is None:
            continue

        code = _norm(col(i_code))[:32]
        rev  = _norm(col(i_rev))[:32]
        setting_raw = _norm(col(i_setting)).lower()
        if "inpatient" in setting_raw and "outpatient" in setting_raw:
            billing, sset = "both", "both"
        elif "inpatient" in setting_raw or setting_raw.startswith("ip"):
            billing, sset = "inpatient", "inpatient"
        elif "outpatient" in setting_raw or setting_raw.startswith("op"):
            billing, sset = "outpatient", "outpatient"
        elif "emergency" in setting_raw:
            billing, sset = "outpatient", "emergency"
        elif "professional" in setting_raw:
            billing, sset = "professional", "professional"
        else:
            billing, sset = "", ""

        # GROSS row — for tall, dedupe by (code, desc); for wide/cm, emit each line
        gkey = (code, desc) if fmt == "tall" else id(r)
        if gkey not in seen_keys:
            seen_keys.add(gkey)
            g["ccn"].append(task.ccn)
            g["description"].append(desc[:500])
            g["code"].append(code)
            g["code_type"].append(_classify_code(code))
            g["code2"].append(rev)
            g["code2_type"].append("rev_code" if rev else "")
            g["billing_class"].append(billing)
            g["setting"].append(sset)
            g["drug_unit"].append("")
            g["drug_type"].append("")
            g["modifiers"].append("")
            g["gross_charge"].append(gross_v)
            g["discounted_cash"].append(cash_v)
            g["standard_charge_min"].append(min_v)
            g["standard_charge_max"].append(max_v)
            g["schema_version"].append(schema_tag)
            g["file_format"].append(file_format)
            g["source_file"].append(src_str)

        # NEG rows
        if fmt == "tall":
            payer = _norm(col(i_payer))[:200]
            rate  = _to_float(col(i_payer_rate)) if i_payer_rate is not None else None
            if payer and rate is not None:
                n["ccn"].append(task.ccn)
                n["description"].append(desc[:500])
                n["code"].append(code)
                n["code_type"].append(_classify_code(code))
                n["billing_class"].append(billing)
                n["setting"].append(sset)
                n["payer_name"].append(payer)
                n["plan_name"].append("")
                n["negotiated_dollar"].append(rate)
                n["negotiated_percentage"].append(None)
                n["negotiated_algorithm"].append("")
                n["estimated_amount"].append(None)
                n["methodology"].append("")
                n["modifiers"].append("")
                n["schema_version"].append(schema_tag)
                n["file_format"].append(file_format)
                n["source_file"].append(src_str)
        elif fmt == "wide":
            for j in payer_col_indices:
                v = _to_float(col(j))
                if v is None:
                    continue
                payer = header[j][:200]
                n["ccn"].append(task.ccn)
                n["description"].append(desc[:500])
                n["code"].append(code)
                n["code_type"].append(_classify_code(code))
                n["billing_class"].append(billing)
                n["setting"].append(sset)
                n["payer_name"].append(payer)
                n["plan_name"].append("")
                n["negotiated_dollar"].append(v)
                n["negotiated_percentage"].append(None)
                n["negotiated_algorithm"].append("")
                n["estimated_amount"].append(None)
                n["methodology"].append("")
                n["modifiers"].append("")
                n["schema_version"].append(schema_tag)
                n["file_format"].append(file_format)
                n["source_file"].append(src_str)

    n_gross = len(g["ccn"])
    n_neg   = len(n["ccn"])
    if n_gross == 0:
        return (0, 0, "no_priceable_rows")

    # Write parquet parts
    gross_part = PARTS_DIR / f"gross_{task.state}_{task.ccn}.parquet"
    gtmp = gross_part.with_suffix(".parquet.tmp")
    if gtmp.exists():
        gtmp.unlink()
    gtable = pa.table(g, schema=GROSS_SCHEMA)
    pq.write_table(gtable, gtmp, compression="snappy")
    gtmp.replace(gross_part)

    if n_neg > 0:
        neg_part = PARTS_DIR / f"neg_{task.state}_{task.ccn}.parquet"
        ntmp = neg_part.with_suffix(".parquet.tmp")
        if ntmp.exists():
            ntmp.unlink()
        ntable = pa.table(n, schema=NEG_SCHEMA)
        pq.write_table(ntable, ntmp, compression="snappy")
        ntmp.replace(neg_part)

    DONE_DIR.mkdir(parents=True, exist_ok=True)
    (DONE_DIR / f"{task.state}_{task.ccn}.flag").touch()
    return (n_gross, n_neg, f"ok_{fmt}")


def main() -> None:
    tasks = discover_tasks()
    print(f"[plan] {len(tasks)} misc CSV chargemasters", flush=True)
    if not tasks:
        return
    PARTS_DIR.mkdir(parents=True, exist_ok=True)
    summary: dict[str, int] = {}
    total_g = total_n = 0
    for i, t in enumerate(tasks, 1):
        try:
            ng, nn, status = parse_one(t)
        except Exception as e:
            ng, nn, status = 0, 0, f"exception:{type(e).__name__}"
            print(f"  [err] ccn={t.ccn} {e!r}", flush=True)
        summary[status] = summary.get(status, 0) + 1
        total_g += ng
        total_n += nn
        print(f"[{i:2d}/{len(tasks)}] {status:20s} ccn={t.ccn} ({t.state}) "
              f"gross={ng:>6d} neg={nn:>8d}  {t.path.name[:60]}",
              flush=True)

    print()
    print(f"[done] {len(tasks)} hospitals: gross={total_g:,} neg={total_n:,}")
    for k, v in sorted(summary.items()):
        print(f"  {k:25s} {v}")
    print()
    print("Run `python mrf/concat_parts.py` to fold these into the unified parquets.")


if __name__ == "__main__":
    main()
