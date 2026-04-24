#!/usr/bin/env python3
"""
ingest_hcai.py
==============
Reproducible ingestion pipeline for California HCAI hospital chargemaster
(CDM) disclosures across release years 2014-2025.

Walks a directory tree of year/hospital/file, selects the CDM files and
the CDM-like sheets within them, auto-detects the header row and column
roles via fuzzy matching, validates CPT/HCPCS codes, preserves each
setting-specific charge column as its own row, and writes typed parquet
per year plus a master parquet, an error log, a per-file log, and a
coverage report.

Usage:
    python ingest_hcai.py \\
        --base /data0/hcai-chargemasters \\
        --out  /data0/hcai-chargemasters/ingest \\
        [--year 2024]        # limit to one year
        [--workers 8]        # default: os.cpu_count() // 2
        [--resume]           # skip files already in ingest_log.csv
        [--serial]           # force single-process (for debugging)

Outputs under --out:
    cdm_<year>.parquet   per-year typed table
    cdm_all.parquet      concatenated master
    ingest_errors.csv    one row per failed file or sheet
    ingest_log.csv       one row per (file, sheet) with diagnostics
    coverage_report.md   human-readable summary
"""

from __future__ import annotations

import argparse
import multiprocessing as mp
import os
import re
import sys
import time
import traceback
import warnings
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable

import pandas as pd

warnings.filterwarnings("ignore")

INGEST_VERSION = "2026-04-23-v1"

# ── File-level filtering ────────────────────────────────────────────────
# Filename must contain CDM (case-insensitive) but none of these other
# tokens. Common25 / Top50 / PCT_CHG / Comments files are legitimately
# part of the HCAI packet but have different schemas and aren't gross
# chargemasters.
SUPPORTED_EXTS = {".xlsx", ".xls", ".xlsm", ".csv"}
FILE_KEEP_RE   = re.compile(r"cdm", re.IGNORECASE)
FILE_DROP_RE   = re.compile(
    r"pct[_\s-]*chg|common\s*\d+|25\s*most|top\s*\d+|comments?",
    re.IGNORECASE,
)

# ── Sheet-level filtering ───────────────────────────────────────────────
# Drop only true non-data sheets. "Top 25 DRG", "25 Most Common Procedures",
# and "Top 50 List" sheets are legitimate subsets that carry DRG or CPT
# rows we want — score_sheet() will reject them if they fail quality checks.
SHEET_DROP_RE = re.compile(
    r"instructions|summary|pcgr|contents|index|cover|"
    r"revenue\s*summary|legend|readme|\btotals?\b",
    re.IGNORECASE,
)

# ── OSHPD ID extraction ─────────────────────────────────────────────────
# Strategy chain: strict prefix -> explicit OSHPD_ tag -> 9-digit anywhere
OSHPD_PREFIX_RE = re.compile(r"^(\d{9})[_\s]")
OSHPD_TAG_RE    = re.compile(r"OSHPD[_\s-]*(\d{9})", re.IGNORECASE)
OSHPD_ANY_RE    = re.compile(r"(?<!\d)(\d{9})(?!\d)")

def extract_oshpd_id(filename: str) -> str | None:
    for r in (OSHPD_PREFIX_RE, OSHPD_TAG_RE, OSHPD_ANY_RE):
        m = r.search(filename)
        if m:
            return m.group(1)
    return None

# ── Column role patterns (applied to column names) ──────────────────────
# CPT/HCPCS columns — physician procedure and ancillary codes.
CPT_HCPCS_COL_RE = re.compile(
    r"\b("
    r"cpt|hcpcs|cpt[\s/\-]*hcpcs|hcpcs[\s/\-]*cpt|"
    r"proc(?:edure)?\s*code|billing\s*code|service\s*code|cpt\s*code|"
    r"hcpcs\s*code"
    r")\b",
    re.IGNORECASE,
)

# Revenue code columns — UB-04 NUBC 3-4 digit categories.
# Require explicit "revenue" or "rev code" to avoid matching "Revision",
# "Review", "Revenue Adjustment", etc.
REVCODE_COL_RE = re.compile(
    r"\b(revenue\s*code|rev\s*code|rev[-_]?cd|ub[-\s]*04|nubc)\b",
    re.IGNORECASE,
)

# MS-DRG / DRG columns — CMS 3-digit inpatient bundles.
# Must NOT be followed by description/name/title words, so "MS-DRG Description"
# is correctly treated as a description column, not a code column.
DRG_COL_RE = re.compile(
    r"\b(ms[-\s]*drg|drg)\b(?!\s*(?:desc|description|name|title|narrative|category))",
    re.IGNORECASE,
)

# NDC columns — FDA 10-11 digit drug codes.
NDC_COL_RE = re.compile(
    r"\b(ndc|national\s*drug)\b",
    re.IGNORECASE,
)

# ICD-10-PCS columns — CMS/CDC 7-char alphanumeric inpatient procedure codes.
# Careful: "procedure code" matches CPT_HCPCS_COL_RE, so match only explicit PCS/ICD hints.
ICD10PCS_COL_RE = re.compile(
    r"(?:\bicd[-\s]*10[-\s]*pcs\b|\bicd[-\s]*pcs\b|\bpcs\s*code\b|\bicd\d*\s*proc)",
    re.IGNORECASE,
)

# Generic "Code" column — ambiguous; we'll value-sniff with strict thresholds.
GENERIC_CODE_COL_RE = re.compile(r"^\s*code\s*$|\bcode\b", re.IGNORECASE)

# Compatibility shim: any code column name matches this OR one of the specific patterns.
CODE_COL_RE = re.compile(
    CPT_HCPCS_COL_RE.pattern + "|" +
    REVCODE_COL_RE.pattern + "|" +
    DRG_COL_RE.pattern + "|" +
    NDC_COL_RE.pattern + "|" +
    ICD10PCS_COL_RE.pattern + "|" +
    r"\bcode\b",
    re.IGNORECASE,
)

DESC_COL_RE = re.compile(
    r"\b(description|desc|procedure\s*name|service\s*name|item\s*name|"
    r"narrative|procedure\s*description)\b",
    re.IGNORECASE,
)

# CHARGE pattern is inclusive (we keep ALL matching columns as separate
# output rows); excludes internal "charge code" / "charge desc" columns.
# Plurals allowed throughout ("Charges", "Prices", "Amounts").
CHARGE_COL_RE = re.compile(
    r"(?i)"
    r"(?!.*\b(charge\s*code|charge\s*desc|charge\s*num|code\s*number)\b)"
    r"("
    r"gross\s*charges?|standard\s*charges?|average\s*charges?|avg\.?\s*charges?|"
    r"list\s*prices?|cdm\s*prices?|billed\s*charges?|total\s*charges?|"
    r"\bi[\s/\-]*p\b|\bo[\s/\-]*p\b|\be[\s/\-]*r\b|\bs[\s/\-]*b\b|\bo[\s/\-]*r\b|"
    r"inpatient|outpatient|emergency|standby|operating\s*room|"
    r"\bcharges?\b|\bprices?\b|\bamounts?\b|\brates?\b|"
    r"\d{4}\s*prices?|\d{4}\s*charges?|"
    r"standard\s*amounts?|cdm\s*amounts?"
    r")",
)
CHARGE_CODE_NEG = re.compile(r"charge\s*code|charge\s*desc|code\s*number", re.IGNORECASE)

UNIT_COL_RE = re.compile(r"\b(unit|uom|per)\b", re.IGNORECASE)

# Map a charge column name to a setting tag
SETTING_PATTERNS = [
    ("IP",  re.compile(r"inpatient|\bi[\s/\-]*p\b", re.IGNORECASE)),
    ("OP",  re.compile(r"outpatient|\bo[\s/\-]*p\b", re.IGNORECASE)),
    ("ER",  re.compile(r"emergency|\be[\s/\-]*r\b|\bed\b", re.IGNORECASE)),
    ("SB",  re.compile(r"standby|\bs[\s/\-]*b\b", re.IGNORECASE)),
    ("OR",  re.compile(r"operating.*room|\bo[\s/\-]*r\b", re.IGNORECASE)),
]

def setting_from_column(colname: str) -> str:
    for tag, r in SETTING_PATTERNS:
        if r.search(colname):
            return tag
    return "ALL"

# ── Code-value validation ───────────────────────────────────────────────
# Value regexes are applied to *normalized* values (uppercase, alnum-only).
# Each code family has its own validator; the column's type hint decides
# which validator to use at extraction time.
CPT_RE      = re.compile(r"^\d{5}$")
HCPCS_RE    = re.compile(r"^[A-Z]\d{4}$")
# UB-04 rev codes: 4-digit canonical (0100-0999) or 3-digit dropped-leading-0 (100-999).
# The real namespace is ~360 defined prefixes within 0001-0999; we enforce the
# 0-prefixed or 3-digit shape here. A true prefix whitelist lives below.
REVCODE_RE  = re.compile(r"^(?:0\d{3}|\d{3})$")
DRG_RE      = re.compile(r"^\d{3}$")              # MS-DRG 001-999 space
NDC_RE      = re.compile(r"^\d{10,11}$")          # normalized; dashes stripped
ICD10PCS_RE = re.compile(r"^[A-Z0-9]{7}$")        # 7-char alphanumeric

# NUBC UB-04 valid revenue-code prefixes (first 3 digits after the leading 0).
# Reference: https://www.nubc.org — 25 series × ~16 ranges ≈ 360 valid codes.
# A value outside these prefixes is almost certainly an internal code that
# happened to be 3-4 digits. Prefixes are conservative and low-risk.
REVCODE_VALID_PREFIXES = {
    # 010-021 room & board
    "010","011","012","013","014","015","016","017","018","019","020","021",
    "022",                       # 022 ICU incremental nursing charge
    "023","024",                 # 023-024 ICU-like incremental nursing
    "025","026","027","028","029",  # pharmacy, IV therapy, med-surg supplies, DME, anesthesia
    "030","031","032","033","034","035","036","037","038","039",
    "040","041","042","043","044","045","046","047","048","049",
    "050","051","052","053","054","055","056","057","058","059",
    "060","061","062","063","064","065","066","067","068","069",
    "070","071","072","073","074","075","076","077","078","079",
    "080","081","082","083","084","085","086","087","088","089",
    "090","091","092","093","094","095","096","097","098","099",
    "100","101","102","103","104","105","106","107","108","109",  # self-admin, PT, leave, etc.
    "110","111","112","113","114","115","116","117",
    "120","121","122","123","124","125","126","127","128","129",
}

def _revcode_prefix(code: str) -> str:
    # Input is already normalized (digits only). Canonical prefix is first 3
    # digits of the 4-digit form — so a 4-digit "0450" → "045", "0910" → "091".
    # For a 3-digit "450" we treat as dropped-leading-0 → "045".
    if len(code) == 4:
        return code[:3]
    if len(code) == 3:
        return "0" + code[:2]
    return ""

def is_valid_revcode(code: str) -> bool:
    if not REVCODE_RE.match(code):
        return False
    return _revcode_prefix(code) in REVCODE_VALID_PREFIXES

def normalize_code(v) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(v).strip().upper())

def detect_code_type(code: str) -> str | None:
    """Ambiguous detection — used only when column type is unknown.

    Tries CPT and HCPCS only; rev/DRG/NDC/PCS must come from a named column
    to avoid false positives (a 3-digit number could be a rev code, a DRG,
    or an internal counter).
    """
    if CPT_RE.match(code):
        return "CPT"
    if HCPCS_RE.match(code):
        return "HCPCS"
    return None

def validate_code_for_type(code: str, expected: str) -> str | None:
    """Return the confirmed code_type if `code` matches the expected family.

    `expected` is one of:
        CPT_HCPCS  — column named as CPT/HCPCS/procedure code
        REVCODE    — column named as revenue code
        DRG        — column named as DRG / MS-DRG
        NDC        — column named as NDC / National Drug Code
        ICD10PCS   — column named as ICD-10-PCS
        AMBIG      — generic "Code" column; fall back to CPT/HCPCS only
    """
    if expected == "CPT_HCPCS":
        return detect_code_type(code)  # CPT or HCPCS
    if expected == "REVCODE":
        return "REVCODE" if is_valid_revcode(code) else None
    if expected == "DRG":
        return "DRG" if DRG_RE.match(code) else None
    if expected == "NDC":
        return "NDC" if NDC_RE.match(code) else None
    if expected == "ICD10PCS":
        return "ICD10PCS" if ICD10PCS_RE.match(code) else None
    if expected == "AMBIG":
        return detect_code_type(code)
    return None

def classify_code_column(colname: str) -> str | None:
    """Given a column name, return the expected code family or None.

    Order matters — most-specific patterns first so a 'Revenue Code'
    column isn't misclassified as a generic CPT/HCPCS 'code' column.
    """
    if not isinstance(colname, str):
        return None
    if ICD10PCS_COL_RE.search(colname):
        return "ICD10PCS"
    if NDC_COL_RE.search(colname):
        return "NDC"
    if DRG_COL_RE.search(colname):
        return "DRG"
    if REVCODE_COL_RE.search(colname):
        return "REVCODE"
    if CPT_HCPCS_COL_RE.search(colname):
        return "CPT_HCPCS"
    if GENERIC_CODE_COL_RE.search(colname):
        return "AMBIG"
    return None

# ── Charge value cleaning ───────────────────────────────────────────────
PAREN_NEG_RE = re.compile(r"^\((.+)\)$")

def clean_charge(raw) -> float | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s.lower() in ("n/a", "na", "nan", "none", "-", ".", "tbd"):
        return None
    neg = False
    m = PAREN_NEG_RE.match(s)
    if m:
        neg = True
        s = m.group(1)
    s = s.replace("$", "").replace(",", "").strip()
    try:
        v = float(s)
        return -v if neg else v
    except ValueError:
        return None

# ── Header-row detection ────────────────────────────────────────────────
HEADER_KEYWORD_RE = re.compile(
    r"(cpt|hcpcs|code|charge|price|amount|description|desc|procedure|"
    r"service|rate|cost|revenue|standard)",
    re.IGNORECASE,
)

def find_header_row(raw: pd.DataFrame, max_scan: int = 30) -> int:
    """Scored search for the most-likely header row.

    Keyword hits are weighted 5x over plain-text breadth so a
    multi-column real header outscores an 'Effective Date:' title.
    """
    best_row, best_score = 0, -1
    for i in range(min(max_scan, len(raw))):
        row = raw.iloc[i]
        keyword_hits = sum(
            1 for v in row
            if isinstance(v, str) and HEADER_KEYWORD_RE.search(v)
        )
        breadth = sum(
            1 for v in row
            if isinstance(v, str) and len(v.strip()) > 1
        )
        score = keyword_hits * 5 + breadth
        if score > best_score:
            best_score, best_row = score, i
    return best_row

# ── Sheet scoring ───────────────────────────────────────────────────────
def sheet_is_droppable(name: str) -> bool:
    return bool(SHEET_DROP_RE.search(str(name)))

def score_sheet(df_head: pd.DataFrame) -> int:
    """Higher = more likely a real CDM sheet."""
    if df_head is None or df_head.empty:
        return 0
    n_cols = df_head.shape[1]
    score = 0
    if n_cols >= 3:
        score += 1
    if n_cols >= 5:
        score += 1
    # Does any column header look like a standardized-code column?
    col_strs = [str(c) for c in df_head.columns]
    named_types = [classify_code_column(c) for c in col_strs]
    if any(t is not None for t in named_types):
        score += 2
    # Does any cell (first 50 rows) look like a valid code of any kind?
    # This lets DRG-only sheets ("Top 25 DRG") pass even without CPTs.
    for i, col in enumerate(df_head.columns):
        expected = named_types[i] if i < len(named_types) else None
        sample = df_head[col].dropna().astype(str).head(50)
        hits = 0
        for v in sample:
            nv = normalize_code(v)
            if expected:
                if validate_code_for_type(nv, expected) is not None:
                    hits += 1
            else:
                if detect_code_type(nv) is not None:
                    hits += 1
        if hits >= 1:
            score += 2
            break
    return score

# ── Column role resolution ──────────────────────────────────────────────
VALUE_SNIFF_MIN_SAMPLE = 50
VALUE_SNIFF_MIN_HIT_RATE = 0.30  # ≥30% of sampled non-null values must be valid CPT/HCPCS
AMBIG_MIN_HIT_RATE = 0.50        # a generic "Code" column needs ≥50% CPT/HCPCS-valid values

def _hit_rate(series: pd.Series) -> float:
    s = series.dropna().astype(str).head(500)
    if len(s) == 0:
        return 0.0
    hits = sum(1 for v in s if detect_code_type(normalize_code(v)) is not None)
    return hits / len(s)

def find_code_columns(df: pd.DataFrame) -> list[tuple[str, str]]:
    """Identify columns that hold standardized procedure/drug codes.

    Returns a list of (column_name, expected_type) tuples. expected_type is
    one of: CPT_HCPCS | REVCODE | DRG | NDC | ICD10PCS | AMBIG.

    Rules:
      1. Column-name classification runs first; every recognized name kept.
      2. If any specific-type column was found (CPT_HCPCS / REVCODE / DRG /
         NDC / ICD10PCS), AMBIG columns are dropped — a sheet with both a
         properly-named "CPT Code" column and a generic "Code" column
         should extract only from the named one, to avoid pulling internal
         5-digit hospital codes into the CPT bucket.
      3. Value-sniff fallback only runs when no named column at all was
         found. It scans for CPT/HCPCS patterns and requires a ≥30% hit
         rate so columns of internal codes (which may have a stray CPT
         match) don't get adopted.
    """
    typed: list[tuple[str, str]] = []
    seen = set()
    for c in df.columns:
        if not isinstance(c, str):
            continue
        cls = classify_code_column(c)
        if cls is not None:
            typed.append((c, cls))
            seen.add(c)

    specific_types = {"CPT_HCPCS", "REVCODE", "DRG", "NDC", "ICD10PCS"}
    has_specific = any(t in specific_types for _, t in typed)
    if has_specific:
        # Drop AMBIG rows — specific columns win
        typed = [(c, t) for c, t in typed if t != "AMBIG"]
        return typed

    if typed:
        # Only AMBIG columns from names. Require each to pass a hit-rate
        # check against CPT/HCPCS so columns of internal 5-digit hospital
        # codes don't leak into the CPT corpus.
        keep = []
        for c, t in typed:
            if t == "AMBIG":
                if _hit_rate(df[c]) >= AMBIG_MIN_HIT_RATE:
                    keep.append((c, t))
            else:
                keep.append((c, t))
        return keep

    # Value-sniff fallback — no named column matched at all
    for c in df.columns:
        if c in seen:
            continue
        series = df[c].dropna().astype(str)
        if len(series) < VALUE_SNIFF_MIN_SAMPLE:
            continue
        sample = series.head(500)
        hits = sum(
            1 for v in sample
            if detect_code_type(normalize_code(v)) is not None
        )
        if hits / len(sample) >= VALUE_SNIFF_MIN_HIT_RATE:
            typed.append((c, "AMBIG"))
    return typed

def find_desc_column(df: pd.DataFrame) -> str | None:
    for c in df.columns:
        if isinstance(c, str) and DESC_COL_RE.search(c):
            return c
    return None

def find_charge_columns(df: pd.DataFrame) -> list[str]:
    """Return ALL columns that look like price columns."""
    out = []
    for c in df.columns:
        if not isinstance(c, str):
            continue
        if CHARGE_CODE_NEG.search(c):
            continue
        if CHARGE_COL_RE.search(c):
            out.append(c)
    return out

def find_unit_column(df: pd.DataFrame) -> str | None:
    for c in df.columns:
        if isinstance(c, str) and UNIT_COL_RE.search(c):
            return c
    return None

# ── Data classes for structured returns ─────────────────────────────────
@dataclass
class FileResult:
    rows: list[dict]       # extracted CDM rows
    logs: list[dict]       # per-sheet diagnostics
    errors: list[dict]     # per-file or per-sheet errors

# ── Core per-file processing ────────────────────────────────────────────
def process_one_file(base_dir: Path, filepath: Path, year: str,
                     hospital_folder: str) -> FileResult:
    """Open a single CDM file, extract rows, return structured result.

    Never raises; all exceptions recorded in errors.
    """
    result = FileResult(rows=[], logs=[], errors=[])
    rel_path = str(filepath.relative_to(base_dir))
    oshpd_id = extract_oshpd_id(filepath.name)
    ext = filepath.suffix.lower()

    def err(stage: str, msg: str, sheet: str = ""):
        result.errors.append({
            "year": year,
            "file_path": rel_path,
            "sheet_name": sheet,
            "stage": stage,
            "error": msg[:500],
        })

    try:
        if ext == ".csv":
            sheets_to_process: list[tuple[str, pd.DataFrame]] = []
            try:
                raw = _read_csv_raw(filepath)
            except Exception as e:
                err("open_csv", repr(e))
                return result
            sheets_to_process.append(("(csv)", raw))
        elif ext in (".xlsx", ".xls", ".xlsm"):
            engine = "xlrd" if ext == ".xls" else "openpyxl"
            try:
                xl = pd.ExcelFile(filepath, engine=engine)
            except Exception as e:
                err("open_xlsx", repr(e))
                return result
            sheets_to_process = []
            for sh in xl.sheet_names:
                if sheet_is_droppable(sh):
                    continue
                try:
                    raw = pd.read_excel(
                        filepath, sheet_name=sh, header=None,
                        dtype=str, engine=engine,
                    )
                except Exception as e:
                    err("read_sheet", repr(e), sheet=sh)
                    continue
                sheets_to_process.append((sh, raw))
            if not sheets_to_process:
                err("no_cdm_sheet", "all sheets dropped by name filter")
                return result
        else:
            err("unsupported_ext", ext)
            return result

        any_rows = False
        for sheet_name, raw in sheets_to_process:
            if raw is None or raw.empty:
                result.logs.append(_log_row(year, rel_path, sheet_name, oshpd_id,
                                            hospital_folder, -1, [], "", [],
                                            0, 0, "empty_sheet"))
                continue

            t0 = time.time()
            header_row = find_header_row(raw)
            body = raw.iloc[header_row + 1:].copy()
            body.columns = [
                str(v).strip() if pd.notna(v) else f"_col{i}"
                for i, v in enumerate(raw.iloc[header_row])
            ]
            body = body.reset_index(drop=True)

            # Score to confirm this sheet is actually CDM-like
            if score_sheet(body.head(50)) < 2:
                result.logs.append(_log_row(year, rel_path, sheet_name, oshpd_id,
                                            hospital_folder, header_row, [], "", [],
                                            len(body), 0, "low_score"))
                continue

            code_cols   = find_code_columns(body)
            desc_col    = find_desc_column(body)
            charge_cols = find_charge_columns(body)
            unit_col    = find_unit_column(body)

            if not code_cols:
                result.logs.append(_log_row(year, rel_path, sheet_name, oshpd_id,
                                            hospital_folder, header_row, [], desc_col, charge_cols,
                                            len(body), 0, "no_code_col"))
                continue
            if not charge_cols:
                result.logs.append(_log_row(year, rel_path, sheet_name, oshpd_id,
                                            hospital_folder, header_row, code_cols, desc_col, [],
                                            len(body), 0, "no_charge_col"))
                continue

            n_matches = 0
            for ccol, expected_type in code_cols:
                if ccol not in body.columns:
                    continue
                code_series = body[ccol].dropna()
                for idx, raw_val in code_series.items():
                    norm = normalize_code(raw_val)
                    ctype = validate_code_for_type(norm, expected_type)
                    if ctype is None:
                        continue
                    desc = ""
                    if desc_col and desc_col in body.columns:
                        v = body.at[idx, desc_col]
                        if pd.notna(v):
                            desc = str(v).strip()
                    unit = ""
                    if unit_col and unit_col in body.columns:
                        v = body.at[idx, unit_col]
                        if pd.notna(v):
                            unit = str(v).strip()
                    for chcol in charge_cols:
                        raw_charge = body.at[idx, chcol] if chcol in body.columns else None
                        if pd.isna(raw_charge):
                            continue
                        cleaned = clean_charge(raw_charge)
                        setting = setting_from_column(chcol)
                        result.rows.append({
                            "year":           int(year),
                            "oshpd_id":       oshpd_id,
                            "hospital_folder": hospital_folder,
                            "file_source":    rel_path,
                            "sheet_name":     sheet_name,
                            "header_row":     header_row,
                            "code_type":      ctype,
                            "procedure_code": norm,
                            "description":    desc[:500],
                            "charge_column":  re.sub(r"\s+", " ", str(chcol)).strip()[:80],
                            "charge_raw":     str(raw_charge)[:100],
                            "charge":         cleaned,
                            "setting":        setting,
                            "unit":           unit[:60],
                            "ingest_version": INGEST_VERSION,
                        })
                        n_matches += 1
            any_rows = any_rows or (n_matches > 0)
            result.logs.append(_log_row(
                year, rel_path, sheet_name, oshpd_id, hospital_folder,
                header_row, code_cols, desc_col, charge_cols,
                len(body), n_matches,
                "ok" if n_matches > 0 else "no_matches",
                parse_seconds=time.time() - t0,
            ))

        if not any_rows and not result.errors:
            err("no_rows_extracted",
                f"file parsed but zero CPT/HCPCS rows across {len(sheets_to_process)} sheet(s)")
    except Exception:
        err("unhandled", traceback.format_exc())
    return result


def _read_csv_raw(filepath: Path) -> pd.DataFrame:
    """CSV reader with encoding + header-row detection wrapper.

    Returns a header=None frame so the downstream header detector can work
    uniformly on xlsx and csv inputs.
    """
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            raw = pd.read_csv(filepath, header=None, dtype=str, encoding=enc,
                              low_memory=False, on_bad_lines="skip")
            return raw
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Could not decode CSV: {filepath}")


def _log_row(year, rel, sheet, oshpd, hosp, header_row,
             code_cols, desc_col, charge_cols,
             n_rows, n_matches, status, parse_seconds=0.0):
    # code_cols is list[tuple[str, str]] like [('CPT Code', 'CPT_HCPCS'), ...]
    def _fmt_cols(cols):
        if not cols:
            return ""
        out = []
        for item in cols:
            if isinstance(item, tuple):
                out.append(f"{item[0]}<{item[1]}>")
            else:
                out.append(str(item))
        return "|".join(out)

    return {
        "year": year,
        "file_path": rel,
        "sheet_name": sheet,
        "oshpd_id": oshpd or "",
        "hospital_folder": hosp,
        "header_row": header_row,
        "code_cols": _fmt_cols(code_cols),
        "desc_col": desc_col or "",
        "charge_cols": "|".join(map(str, charge_cols)) if charge_cols else "",
        "n_rows_in_sheet": n_rows,
        "n_matches": n_matches,
        "status": status,
        "parse_seconds": round(parse_seconds, 3),
    }


# ── Directory walk ──────────────────────────────────────────────────────
_WALK_JUNK_FILES = {"thumbs.db", ".ds_store", "desktop.ini", ".gitkeep"}

def _dir_has_real_files(d: Path) -> bool:
    """True if `d` contains at least one non-hidden, non-junk file.

    Defends `find_hospital_root` against stray Thumbs.db / .DS_Store files
    that sit at the wrapper level and falsely signal that we've reached
    the hospital layer. A lone Thumbs.db in /2018/2018/ was enough to
    strand the entire 2018 cohort in an earlier run.
    """
    try:
        for p in d.iterdir():
            if not p.is_file():
                continue
            name = p.name
            if name.startswith(".") or name.startswith("~$"):
                continue
            if name.lower() in _WALK_JUNK_FILES:
                continue
            return True
    except OSError:
        return False
    return False

def find_hospital_root(year_dir: Path, max_depth: int = 4) -> Path:
    """Find the directory whose immediate children are hospital folders.

    HCAI packets use inconsistent wrappers (year/year/hospitals,
    year/ChargemasterCDM-YYYY/hospitals, or year/hospitals directly).
    """
    cur = year_dir
    for _ in range(max_depth):
        subdirs = [s for s in cur.iterdir() if s.is_dir()]
        if not subdirs:
            return cur
        n_with_files = sum(1 for s in subdirs if _dir_has_real_files(s))
        if n_with_files >= max(1, len(subdirs) // 2):
            return cur
        # Drill into the largest subfolder (by count of descendants)
        subdirs.sort(key=lambda s: sum(1 for _ in s.rglob("*")), reverse=True)
        cur = subdirs[0]
    return cur


def iter_candidate_files(base: Path, years: Iterable[str] | None):
    """Yield (year, hospital_folder, filepath) for every CDM candidate."""
    for year_dir in sorted(base.iterdir()):
        if not year_dir.is_dir() or not re.fullmatch(r"20\d{2}", year_dir.name):
            continue
        year = year_dir.name
        if years and year not in years:
            continue
        hosp_root = find_hospital_root(year_dir)
        if not hosp_root.exists():
            continue
        for hospital_entry in sorted(hosp_root.iterdir()):
            if not hospital_entry.is_dir():
                continue
            for f in hospital_entry.iterdir():
                if not f.is_file():
                    continue
                name = f.name
                if name.startswith("~$"):
                    continue
                if f.suffix.lower() not in SUPPORTED_EXTS:
                    continue
                if not FILE_KEEP_RE.search(name):
                    continue
                if FILE_DROP_RE.search(name):
                    continue
                try:
                    if f.stat().st_size < 1024:
                        continue
                except OSError:
                    continue
                yield year, hospital_entry.name, f


# ── Worker for multiprocessing ──────────────────────────────────────────
def _worker(args):
    base, rel, year, hospital_folder = args
    return process_one_file(Path(base), Path(base) / rel, year, hospital_folder)


# ── Main orchestration ──────────────────────────────────────────────────
def run(base: Path, out: Path, years: set[str] | None,
        workers: int, resume: bool, serial: bool):
    out.mkdir(parents=True, exist_ok=True)
    log_path = out / "ingest_log.csv"
    err_path = out / "ingest_errors.csv"

    done = set()
    if resume and log_path.exists():
        try:
            prev = pd.read_csv(log_path, dtype=str)
            done = set(prev["file_path"].dropna().unique())
            print(f"[resume] {len(done):,} files already logged; skipping")
        except Exception as e:
            print(f"[resume] could not read {log_path}: {e}")

    # Build task list
    tasks = []
    for year, hospital_folder, fpath in iter_candidate_files(base, years):
        rel = str(fpath.relative_to(base))
        if rel in done:
            continue
        tasks.append((str(base), rel, year, hospital_folder))
    print(f"[scan] {len(tasks):,} candidate CDM files to parse")

    if not tasks:
        print("[scan] nothing to do")
        return

    # Try to get a progress bar; fall back to plain loop if tqdm missing
    try:
        from tqdm import tqdm as _tqdm
    except ImportError:
        def _tqdm(it, **_):
            return it

    all_rows: list[dict] = []
    all_logs: list[dict] = []
    all_errs: list[dict] = []

    def _consume(fr: FileResult):
        all_rows.extend(fr.rows)
        all_logs.extend(fr.logs)
        all_errs.extend(fr.errors)

    t_start = time.time()
    if serial or workers <= 1:
        for t in _tqdm(tasks, desc="parsing", unit="file"):
            _consume(_worker(t))
    else:
        ctx = mp.get_context("spawn")
        with ctx.Pool(workers) as pool:
            for fr in _tqdm(
                pool.imap_unordered(_worker, tasks, chunksize=4),
                total=len(tasks), desc="parsing", unit="file",
            ):
                _consume(fr)
    elapsed = time.time() - t_start
    print(f"[done] parsed {len(tasks):,} files in {elapsed:.1f}s "
          f"({len(tasks)/max(elapsed,1e-9):.1f} files/s)")

    # Append to error + log CSVs (or create)
    if resume and log_path.exists() and all_logs:
        pd.DataFrame(all_logs).to_csv(log_path, mode="a", header=False, index=False)
    else:
        pd.DataFrame(all_logs).to_csv(log_path, index=False)
    if resume and err_path.exists() and all_errs:
        pd.DataFrame(all_errs).to_csv(err_path, mode="a", header=False, index=False)
    else:
        pd.DataFrame(all_errs).to_csv(err_path, index=False)
    print(f"[out] {log_path}  ({len(all_logs):,} sheet rows)")
    print(f"[out] {err_path}  ({len(all_errs):,} errors)")

    # Write parquet per year (append behaviour not supported by parquet;
    # recompute the involved year files from what we just emitted PLUS
    # whatever already existed for years not in scope).
    if all_rows:
        df = pd.DataFrame(all_rows)
        df = df.sort_values(["year", "oshpd_id", "procedure_code", "setting"], na_position="last")
        for year, part in df.groupby("year", sort=True):
            py = out / f"cdm_{year}.parquet"
            if resume and py.exists():
                old = pd.read_parquet(py)
                part = pd.concat([old, part], ignore_index=True)
            part.to_parquet(py, compression="snappy", index=False)
            print(f"[out] {py}  ({len(part):,} rows)")
    else:
        print("[out] no CDM rows extracted — skipping parquet write")

    _write_coverage(out, years)


def _write_coverage(out: Path, years: set[str] | None):
    log_path = out / "ingest_log.csv"
    err_path = out / "ingest_errors.csv"
    report_path = out / "coverage_report.md"

    lines = ["# HCAI chargemaster ingestion — coverage report", ""]
    lines.append(f"- ingest_version: `{INGEST_VERSION}`")
    lines.append(f"- scope: {'all years' if not years else sorted(years)}")
    lines.append("")

    # Per-year, per-code-type breakdown
    lines.append("## Per-year rows by code type")
    lines.append("")
    lines.append("| year | total | CPT | HCPCS | REVCODE | DRG | NDC | ICD10PCS | unique_oshpd |")
    lines.append("|-----:|------:|----:|------:|--------:|----:|----:|---------:|-------------:|")
    total_rows = 0
    for py in sorted(out.glob("cdm_20*.parquet")):
        y = py.stem.replace("cdm_", "")
        try:
            dfp = pd.read_parquet(py, columns=["oshpd_id", "code_type"])
        except Exception:
            continue
        n = len(dfp)
        total_rows += n
        ct = dfp["code_type"].value_counts()
        lines.append(
            f"| {y} | {n:,} | "
            f"{ct.get('CPT', 0):,} | {ct.get('HCPCS', 0):,} | "
            f"{ct.get('REVCODE', 0):,} | {ct.get('DRG', 0):,} | "
            f"{ct.get('NDC', 0):,} | {ct.get('ICD10PCS', 0):,} | "
            f"{dfp['oshpd_id'].nunique():,} |"
        )
    lines.append(f"| **total** | **{total_rows:,}** | | | | | | | |")
    lines.append("")

    # Status distribution from log
    if log_path.exists():
        log = pd.read_csv(log_path, dtype=str)
        lines.append("## Sheet-level status distribution")
        lines.append("")
        lines.append("| year | ok | no_code_col | no_charge_col | no_matches | low_score | empty | other |")
        lines.append("|-----:|---:|---:|---:|---:|---:|---:|---:|")
        status_buckets = ["ok", "no_code_col", "no_charge_col",
                          "no_matches", "low_score", "empty_sheet"]
        for y in sorted(log["year"].dropna().unique()):
            sub = log[log["year"] == y]
            counts = {s: (sub["status"] == s).sum() for s in status_buckets}
            other = len(sub) - sum(counts.values())
            lines.append(f"| {y} | {counts['ok']} | {counts['no_code_col']} | "
                         f"{counts['no_charge_col']} | {counts['no_matches']} | "
                         f"{counts['low_score']} | {counts['empty_sheet']} | {other} |")
        lines.append("")

    # Errors
    if err_path.exists():
        err = pd.read_csv(err_path, dtype=str)
        lines.append(f"## Errors ({len(err):,} rows)")
        lines.append("")
        if len(err):
            lines.append("Top stages:")
            for stage, n in err["stage"].value_counts().head(10).items():
                lines.append(f"- `{stage}`: {n}")
        lines.append("")

    report_path.write_text("\n".join(lines))
    print(f"[out] {report_path}")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base", type=Path,
                    default=Path("/data0/hcai-chargemasters"))
    ap.add_argument("--out", type=Path,
                    default=Path("/data0/hcai-chargemasters/ingest"))
    ap.add_argument("--year", type=str, action="append",
                    help="limit to one or more years; repeat flag for multiple")
    ap.add_argument("--workers", type=int,
                    default=max(1, (os.cpu_count() or 2) // 2))
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--serial", action="store_true")
    args = ap.parse_args()

    years = set(args.year) if args.year else None
    if years:
        for y in years:
            if not re.fullmatch(r"20\d{2}", y):
                sys.exit(f"bad --year: {y}")

    run(args.base, args.out, years, args.workers, args.resume, args.serial)

    # Final: concatenate all per-year parquets into a master
    master = args.out / "cdm_all.parquet"
    parts = sorted(args.out.glob("cdm_20*.parquet"))
    if parts:
        master_df = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)
        master_df.to_parquet(master, compression="snappy", index=False)
        print(f"[out] {master}  ({len(master_df):,} rows)")


if __name__ == "__main__":
    main()
