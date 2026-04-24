"""
validate_extraction.py
-----------------------
Validates the chargemaster extraction results by comparing:
1. How many year folders exist in the data
2. How many hospital folders exist per year
3. How many hospitals we actually got data for
4. What CPT/HCPCS codes were found and how many times
"""

import os
import re
import pandas as pd

# ─────────────────────────────────────────────
# PATHS — update if needed
# ─────────────────────────────────────────────
BASE_DIR   = r"C:\Users\deeksha\OneDrive - Indiana University\californis\data_unzipped"
OUTPUT_DIR = r"C:\Users\deeksha\OneDrive - Indiana University\californis\output"


# ─────────────────────────────────────────────
# STEP 1 — What exists in the folder structure?
# ─────────────────────────────────────────────
def scan_folder_structure(base_dir):
    print("\n" + "="*60)
    print("STEP 1: FOLDER STRUCTURE SUMMARY")
    print("="*60)

    year_summary = {}

    for entry in sorted(os.scandir(base_dir), key=lambda e: e.name):
        if not entry.is_dir():
            continue
        year_match = re.search(r'(20\d{2})', entry.name)
        if not year_match:
            continue
        year = year_match.group(1)

        # Drill down to find hospital folders
        hospital_dir = entry.path
        for _ in range(3):  # max 3 levels deep
            subdirs = [s for s in os.scandir(hospital_dir) if s.is_dir()]
            if not subdirs:
                break
            has_files = any(
                any(os.path.isfile(os.path.join(s.path, f)) for f in os.listdir(s.path))
                for s in subdirs
            )
            if has_files:
                break
            hospital_dir = subdirs[0].path

        hospitals = [s.name for s in os.scandir(hospital_dir) if s.is_dir()]
        year_summary[year] = {
            "folder": entry.name,
            "hospital_count": len(hospitals),
            "hospitals": sorted(hospitals)
        }

        print(f"\n  [{year}] Folder: {entry.name}")
        print(f"         Hospitals found: {len(hospitals)}")

    print(f"\n  TOTAL YEARS IN FOLDER : {len(year_summary)}")
    print(f"  YEARS                 : {sorted(year_summary.keys())}")
    total_hospitals = sum(v['hospital_count'] for v in year_summary.values())
    print(f"  TOTAL HOSPITAL FOLDERS: {total_hospitals}")

    return year_summary


# ─────────────────────────────────────────────
# STEP 2 — What did we actually extract?
# ─────────────────────────────────────────────
def validate_extraction(output_dir, year_summary):
    print("\n" + "="*60)
    print("STEP 2: EXTRACTION RESULTS SUMMARY")
    print("="*60)

    matched_path = os.path.join(output_dir, "matched_rows.csv")
    log_path     = os.path.join(output_dir, "extraction_log.csv")
    error_path   = os.path.join(output_dir, "missing_files_or_errors.csv")

    if not os.path.exists(matched_path):
        print("\n  [ERROR] matched_rows.csv not found. Run the extractor first.")
        return

    df      = pd.read_csv(matched_path, dtype=str)
    def safe_read(path):
        try:
            return pd.read_csv(path, dtype=str)
        except Exception:
            return pd.DataFrame()

    log_df  = safe_read(log_path)
    err_df  = safe_read(error_path)

    print(f"\n  Total matched rows     : {len(df)}")
    print(f"  Total sheets processed : {len(log_df)}")
    print(f"  Total files with errors: {len(err_df)}")

    # Years extracted
    years_extracted = sorted(df["year"].dropna().unique())
    print(f"\n  Years with data        : {len(years_extracted)}")
    print(f"  Years                  : {years_extracted}")

    # Hospitals extracted per year
    print(f"\n  {'YEAR':<8} {'HOSPITALS IN FOLDER':>22} {'HOSPITALS WITH DATA':>22} {'TOTAL ROWS':>12}")
    print(f"  {'-'*68}")
    for year in sorted(df["year"].dropna().unique()):
        year_df     = df[df["year"] == year]
        got_data    = year_df["hospital"].nunique()
        in_folder   = year_summary.get(year, {}).get("hospital_count", "N/A")
        rows        = len(year_df)
        print(f"  {year:<8} {str(in_folder):>22} {got_data:>22} {rows:>12}")

    # Missing hospitals (in folder but not in output)
    print(f"\n" + "="*60)
    print("STEP 3: HOSPITALS IN FOLDER BUT NO DATA EXTRACTED")
    print("="*60)
    for year, info in sorted(year_summary.items()):
        if year not in years_extracted:
            print(f"\n  [{year}] — entire year was skipped (not in TEST_YEARS)")
            continue
        year_df         = df[df["year"] == year]
        extracted_hosp  = set(year_df["hospital"].dropna().str.strip().str.upper())
        folder_hosp     = set(h.upper() for h in info["hospitals"])
        missing         = folder_hosp - extracted_hosp
        if missing:
            print(f"\n  [{year}] {len(missing)} hospitals had no matches:")
            for h in sorted(missing)[:10]:  # show max 10
                print(f"    - {h}")
            if len(missing) > 10:
                print(f"    ... and {len(missing)-10} more")
        else:
            print(f"\n  [{year}] All hospitals had at least one match ✓")


# ─────────────────────────────────────────────
# STEP 3 — CPT / HCPCS code summary
# ─────────────────────────────────────────────
def validate_codes(output_dir):
    print(f"\n" + "="*60)
    print("STEP 4: CPT / HCPCS CODE SUMMARY")
    print("="*60)

    matched_path = os.path.join(output_dir, "matched_rows.csv")
    df = pd.read_csv(matched_path, dtype=str)

    # Code type split
    print(f"\n  Code type breakdown:")
    print(df["code_type"].value_counts().to_string())

    # All unique codes found
    code_counts = df.groupby(["procedure_code", "code_type"]).size().reset_index(name="occurrences")
    code_counts = code_counts.sort_values("occurrences", ascending=False)

    print(f"\n  {'CODE':<12} {'TYPE':<8} {'OCCURRENCES':>12}")
    print(f"  {'-'*35}")
    for _, row in code_counts.iterrows():
        print(f"  {row['procedure_code']:<12} {row['code_type']:<8} {row['occurrences']:>12}")

    print(f"\n  TOTAL UNIQUE CODES FOUND: {len(code_counts)}")

    # Codes in target list but NOT found anywhere
    TARGET_CODES = {
        "99281", "99282", "99283", "99284", "99285",  # ER visit levels 1-5
    "99291", "99292",                              # Critical care

    # ── Gynecology ──────────────────────────────────────────────────
    "59400", "59409", "59410",                     # Vaginal delivery
    "59510", "59514", "59515", 
    }
    found_codes  = set(df["procedure_code"].dropna().unique())
    missing_codes = TARGET_CODES - found_codes
    print(f"\n  Target codes NOT found in any file ({len(missing_codes)}):")
    for c in sorted(missing_codes):
        print(f"    - {c}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    print("\n" + "="*60)
    print("   CHARGEMASTER EXTRACTION VALIDATOR")
    print("="*60)

    year_summary = scan_folder_structure(BASE_DIR)
    validate_extraction(OUTPUT_DIR, year_summary)
    validate_codes(OUTPUT_DIR)

    print("\n" + "="*60)
    print("   VALIDATION COMPLETE")
    print("="*60)

if __name__ == "__main__":
    main()