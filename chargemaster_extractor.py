"""
chargemaster_extractor.py
--------------------------
Recursively traverses a folder structure organized by year → hospital → files,
extracts ALL rows containing valid CPT or HCPCS codes, and writes three output CSVs.
No target list required — every valid code found is captured.

Expected layout:
    base_dir/
        2013/
            HospitalA/
                chargemaster.xlsx
            HospitalB/
                cdm.csv
        2014/
            ...

Outputs (written next to this script):
    matched_rows.csv        – all matched rows with metadata
    extraction_log.csv      – per-sheet processing summary
    missing_files_or_errors.csv – files that could not be read
"""

import os
import re
import pandas as pd

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

# Root directory containing year folders
BASE_DIR = r"C:\Users\deeksha\OneDrive - Indiana University\californis\data_unzipped"

# Where to write the three output files
OUTPUT_DIR = r"C:\Users\deeksha\OneDrive - Indiana University\californis\output"

# ─────────────────────────────────────────────
# TARGET CPT / HCPCS CODES
# ─────────────────────────────────────────────
# Emergency Medicine CPT codes (99281-99285 = ER visits by severity)
# Gynecology CPT codes (common OB/GYN procedures)
# Add or remove codes here as needed for your research.
TARGET_CODES = {
    # ── Emergency Medicine ──────────────────────────────────────────
    "99281", "99282", "99283", "99284", "99285",  # ER visit levels 1-5
    "99291", "99292",                              # Critical care

    # ── Gynecology ──────────────────────────────────────────────────
    "59400", "59409", "59410",                     # Vaginal delivery
    "59510", "59514", "59515",                     # Cesarean delivery
}

# ─────────────────────────────────────────────
# REGEX PATTERNS
# ─────────────────────────────────────────────

# Column-name patterns that likely contain procedure codes
CODE_COL_PATTERNS = re.compile(
    r"(cpt|hcpcs|procedure\s*code|proc\s*code|cpt[\s/\-]*hcpcs|service\s*code|"
    r"\bcode\b|billing\s*code|revenue\s*code)",
    re.IGNORECASE,
)

# Column-name patterns for description and charge
DESC_COL_PATTERNS = re.compile(
    r"(description|desc|procedure\s*name|service\s*name|item\s*name|narrative)",
    re.IGNORECASE,
)
CHARGE_COL_PATTERNS = re.compile(
    # Match price/amount columns but EXCLUDE "Charge Code" (internal hospital ID)
    # Also catches "June 2024 Prices", "Average Charge", "Gross Charge" etc.
    r"(?i)^(?!charge\s*code)(?=.*(price|amount|rate|average\s*charge|avg\s*charge|"
    r"gross\s*charge|standard\s*charge|billed\s*charge|cdm\s*price|list\s*price|"
    r"\d{4}\s*price))",
    re.IGNORECASE,
)

# CPT codes: 5 digits (optionally followed by a 2-char modifier)
CPT_PATTERN = re.compile(r"^\d{5}([A-Z0-9]{2})?$")

# HCPCS Level II codes: letter + 4 digits
HCPCS_PATTERN = re.compile(r"^[A-Z]\d{4}$")


# ─────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────


def normalize_code(value) -> str:
    """Strip whitespace, uppercase, remove non-alphanumeric characters."""
    return re.sub(r"[^A-Z0-9]", "", str(value).strip().upper())


def detect_code_type(code: str) -> str:
    """Return 'CPT', 'HCPCS', or 'UNKNOWN' based on code format."""
    if CPT_PATTERN.match(code):
        return "CPT"
    if HCPCS_PATTERN.match(code):
        return "HCPCS"
    return "UNKNOWN"


def find_columns(df: pd.DataFrame, pattern: re.Pattern) -> list:
    """Return column names matching a regex pattern."""
    return [c for c in df.columns if pattern.search(str(c))]


# Keywords that strongly indicate a real header row
HEADER_KEYWORDS = re.compile(
    r"(cpt|hcpcs|code|charge|price|amount|description|desc|procedure|service|rate|cost|revenue)",
    re.IGNORECASE,
)

def find_header_row(df_raw: pd.DataFrame, max_scan: int = 15) -> int:
    """
    Find the real header row by scoring each row on two criteria:
    1. KEYWORD score  — how many cells contain header-like words (cpt, charge, description etc.)
    2. BREADTH score  — how many non-empty string cells (width of the header)
    Keyword hits are weighted heavily so title rows like "Common OP Procedures"
    don't outscore a real multi-column header.
    """
    best_row, best_score = 0, -1
    for i in range(min(max_scan, len(df_raw))):
        row = df_raw.iloc[i]
        keyword_hits = sum(
            1 for v in row
            if isinstance(v, str) and HEADER_KEYWORDS.search(v)
        )
        breadth = sum(
            1 for v in row
            if isinstance(v, str) and len(v.strip()) > 1
        )
        # Keyword hits worth 5x more than plain text breadth
        score = keyword_hits * 5 + breadth
        if score > best_score:
            best_score, best_row = score, i
    return best_row


def get_engine(filepath: str) -> str:
    """Return the correct pandas engine based on file extension."""
    ext = os.path.splitext(filepath)[1].lower()
    if ext == ".xls":
        return "xlrd"
    return "openpyxl"


def read_sheet(filepath: str, sheet_name, engine: str) -> pd.DataFrame:
    """
    Read a single Excel sheet in one pass, auto-detecting the header row.
    Uses xlrd for .xls and openpyxl for .xlsx/.xlsm.
    """
    # Single raw read to find header row and return data
    raw = pd.read_excel(filepath, sheet_name=sheet_name, header=None,
                        dtype=str, engine=engine)
    header_row = find_header_row(raw)
    # Slice: rows above header become the header, rows below are data
    df = raw.iloc[header_row + 1:].copy()
    df.columns = [str(v).strip() for v in raw.iloc[header_row]]
    df.reset_index(drop=True, inplace=True)
    return df


def read_csv_file(filepath: str) -> pd.DataFrame:
    """Read a CSV, trying common encodings."""
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            df = pd.read_csv(filepath, dtype=str, encoding=enc)
            df.columns = [str(c).strip() for c in df.columns]
            return df
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Could not decode CSV: {filepath}")


# ─────────────────────────────────────────────
# CORE EXTRACTION LOGIC
# ─────────────────────────────────────────────

def is_valid_code(code: str) -> bool:
    """Return True if code is a valid CPT/HCPCS format AND in our target list."""
    is_valid_format = bool(CPT_PATTERN.match(code) or HCPCS_PATTERN.match(code))
    return is_valid_format and code in TARGET_CODES


def extract_matches(
    df: pd.DataFrame,
    year: str,
    hospital: str,
    filename: str,
    sheet_name: str,
) -> tuple[list[dict], dict]:
    """
    Scan `df` and extract ALL rows that contain a valid CPT or HCPCS code.

    Returns:
        matched_rows  – list of dicts (one per matched row)
        log_entry     – dict summarising what was detected in this sheet
    """
    # Identify candidate code columns by name
    code_cols = find_columns(df, CODE_COL_PATTERNS)
    desc_cols = find_columns(df, DESC_COL_PATTERNS)
    charge_cols = find_columns(df, CHARGE_COL_PATTERNS)

    # If no named code column found, scan ALL columns for target code values
    if not code_cols:
        for col in df.columns:
            sample = df[col].dropna().head(100)
            hits = sample.apply(lambda v: is_valid_code(normalize_code(v))).sum()
            if hits >= 1:  # even 1 target code hit is enough
                code_cols.append(col)

    matched_rows = []

    for code_col in code_cols:
        for _, row in df.iterrows():
            raw_val = row.get(code_col, "")
            norm = normalize_code(raw_val)
            if not is_valid_code(norm):
                continue  # skip blanks, internal codes, non-CPT/HCPCS values

            description = next(
                (row[c] for c in desc_cols if pd.notna(row.get(c))), ""
            )
            charge = next(
                (row[c] for c in charge_cols if pd.notna(row.get(c))), ""
            )

            matched_rows.append({
                "year":          year,
                "hospital":      hospital,
                "file_name":     filename,
                "sheet_name":    sheet_name,
                "source_column": code_col,
                "procedure_code": norm,
                "code_type":     detect_code_type(norm),
                "description":   str(description).strip(),
                "charge":        str(charge).strip(),
            })

    log_entry = {
        "file_name":         filename,
        "sheet_name":        sheet_name,
        "code_cols_found":   ", ".join(code_cols) if code_cols else "NONE",
        "desc_cols_found":   ", ".join(desc_cols) if desc_cols else "NONE",
        "charge_cols_found": ", ".join(charge_cols) if charge_cols else "NONE",
        "num_matches":       len(matched_rows),
    }
    return matched_rows, log_entry


# ─────────────────────────────────────────────
# FILE PROCESSING
# ─────────────────────────────────────────────

def process_file(
    filepath: str,
    year: str,
    hospital: str,
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Process one file (Excel or CSV).

    Returns:
        all_matches   – matched rows from this file
        log_entries   – one log entry per sheet/file
        error_entries – populated only on failure
    """
    filename = os.path.basename(filepath)
    all_matches, log_entries, error_entries = [], [], []

    try:
        ext = os.path.splitext(filepath)[1].lower()

        if ext in (".xlsx", ".xls", ".xlsm"):
            engine = get_engine(filepath)
            xl = pd.ExcelFile(filepath, engine=engine)
            sheets = xl.sheet_names
            for sheet in sheets:
                try:
                    df = read_sheet(filepath, sheet, engine)
                    matches, log = extract_matches(
                        df, year, hospital, filename, sheet
                    )
                    all_matches.extend(matches)
                    log_entries.append(log)
                    print(
                        f"  [sheet] {sheet:40s} → {log['num_matches']} match(es)"
                    )
                except Exception as sheet_err:
                    print(f"  [WARN] Could not read sheet '{sheet}': {sheet_err}")
                    log_entries.append({
                        "file_name": filename,
                        "sheet_name": sheet,
                        "code_cols_found": "ERROR",
                        "desc_cols_found": "",
                        "charge_cols_found": "",
                        "num_matches": 0,
                    })

        elif ext == ".csv":
            df = read_csv_file(filepath)
            matches, log = extract_matches(
                df, year, hospital, filename, "N/A"
            )
            all_matches.extend(matches)
            log_entries.append(log)
            print(f"  [csv ] {filename:40s} → {log['num_matches']} match(es)")

        else:
            print(f"  [SKIP] Unsupported file type: {filename}")

    except Exception as e:
        print(f"  [ERROR] Failed to process {filename}: {e}")
        error_entries.append({
            "file_path": filepath,
            "year":      year,
            "hospital":  hospital,
            "error":     str(e),
        })

    return all_matches, log_entries, error_entries


# ─────────────────────────────────────────────
# DIRECTORY TRAVERSAL
# ─────────────────────────────────────────────

def traverse_and_extract(base_dir: str):
    """
    Walk base_dir/year/hospital/files and collect all results.
    Folder depth is: base_dir → year → hospital → (any depth of files).
    """
    all_matches, all_logs, all_errors = [], [], []

    if not os.path.isdir(base_dir):
        print(f"[ERROR] Base directory not found: {base_dir}")
        return all_matches, all_logs, all_errors

    # ── DEMO MODE SETTINGS ───────────────────────────────────────────────
    # Hospitals to sample per year for demo (set to None for all hospitals)
    DEMO_HOSPITALS_PER_YEAR = None  # None = ALL hospitals, set a number e.g. 3 to limit
    # Only process 2018 and after
    # None = ALL years | customize as needed:
    #   TEST_YEARS = {"2018"}               → only 2018
    #   TEST_YEARS = {"2018", "2019"}        → custom years
    #   TEST_YEARS = None                    → all years
    TEST_YEARS = {"2024"}  # only 2024 for testing
    # ─────────────────────────────────────────────────────────────────────

    for year_entry in sorted(os.scandir(base_dir), key=lambda e: e.name):
        if not year_entry.is_dir():
            continue
        # Extract 4-digit year from folder name regardless of naming convention
        # Handles: '2014-hospital-chargemasters', 'chargemaster-cdm-2020r', etc.
        year_match = re.search(r'(20\d{2})', year_entry.name)
        if not year_match:
            continue
        year = year_match.group(1)

        # Skip years outside test range
        if TEST_YEARS and year not in TEST_YEARS:
            print(f"[SKIP] {year_entry.name}")
            continue

        # Drill down through subfolders until we find hospital folders
        # Handles: year/hospitals, year/year/hospitals, year/folder/hospitals
        def find_hospital_dir(path, depth=0):
            if depth > 3:
                return path
            subdirs = [s for s in os.scandir(path) if s.is_dir()]
            if not subdirs:
                return path
            # If subdirs contain files -> these are hospital folders
            has_files = any(
                any(os.path.isfile(os.path.join(s.path, f))
                    for f in os.listdir(s.path))
                for s in subdirs
            )
            if has_files:
                return path
            # Otherwise go deeper
            return find_hospital_dir(subdirs[0].path, depth + 1)

        hospital_search_dir = find_hospital_dir(year_entry.path)
        print(f"[{year}] Hospital dir: {os.path.relpath(hospital_search_dir, year_entry.path)}")


        all_hospitals = sorted(
            [e for e in os.scandir(hospital_search_dir) if e.is_dir()],
            key=lambda e: e.name
        )
        # In demo mode, only take first N hospitals per year
        if DEMO_HOSPITALS_PER_YEAR:
            all_hospitals = all_hospitals[:DEMO_HOSPITALS_PER_YEAR]
            print(f"  [DEMO] Sampling {len(all_hospitals)} hospitals for {year}")

        for hospital_entry in all_hospitals:
            hospital = hospital_entry.name
            print(f"\n[{year}] {hospital}")

            # Only look at files directly inside the hospital folder (no deeper)
            # os.walk was recursing into sibling folders causing hospital mismatch
            for fname in os.listdir(hospital_entry.path):
                ext = os.path.splitext(fname)[1].lower()
                if ext not in (".xlsx", ".xls", ".xlsm", ".csv"):
                    continue
                fpath = os.path.join(hospital_entry.path, fname)
                if not os.path.isfile(fpath):
                    continue
                print(f"  Processing: {fname}")
                matches, logs, errors = process_file(
                    fpath, year, hospital
                )
                all_matches.extend(matches)
                all_logs.extend(logs)
                all_errors.extend(errors)

    return all_matches, all_logs, all_errors


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. Traverse directory and extract all CPT/HCPCS codes
    print(f"\n[INFO] Scanning: {BASE_DIR}\n{'─'*60}")
    all_matches, all_logs, all_errors = traverse_and_extract(BASE_DIR)

    # 3. Save matched_rows.csv
    matched_path = os.path.join(OUTPUT_DIR, "matched_rows.csv")
    matched_cols = [
        "year", "hospital", "file_name", "sheet_name",
        "source_column", "procedure_code", "code_type",
        "description", "charge",
    ]
    if all_matches:
        pd.DataFrame(all_matches, columns=matched_cols).to_csv(
            matched_path, index=False
        )
    else:
        pd.DataFrame(columns=matched_cols).to_csv(matched_path, index=False)
    print(f"\n[OUT] matched_rows.csv          → {len(all_matches)} row(s)")

    # 4. Save extraction_log.csv
    log_path = os.path.join(OUTPUT_DIR, "extraction_log.csv")
    pd.DataFrame(all_logs).to_csv(log_path, index=False)
    print(f"[OUT] extraction_log.csv        → {len(all_logs)} sheet(s) processed")

    # 5. Save missing_files_or_errors.csv
    err_path = os.path.join(OUTPUT_DIR, "missing_files_or_errors.csv")
    pd.DataFrame(all_errors).to_csv(err_path, index=False)
    print(f"[OUT] missing_files_or_errors.csv → {len(all_errors)} error(s)")

    print("\n[DONE] Extraction complete.")


if __name__ == "__main__":
    main()