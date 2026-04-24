# California Chargemasters — Ingest & Analysis Pipeline

Reproducible pipeline that extracts standardized procedure/drug codes and
gross charges from the California HCAI (OSHPD) hospital chargemaster
disclosures 2014–2025, joins them to facility ZIP codes via the HCAI
Licensed Facility Listing, and prepares a corpus suitable for
price-transparency and health-equity analyses.

## Layout

| File | Purpose |
|---|---|
| `ingest_hcai.py` | Walks `/data0/hcai-chargemasters/<year>/` tree, parses heterogeneous CDM xlsx/xls/csv files, emits typed parquet per year + master. |
| `build_matched_with_zip.py` | Joins corpus to HCAI facility ZIP listing; produces `matched_rows_with_zip_<year>.csv` in the schema Parvati's notebook expects. |
| `compare_to_parvati.py` | Reproduces Parvati's Block 2 / 5 summary stats on the new corpus for side-by-side verification. |
| `requirements.txt` | Pinned dependencies (pandas 2.2+, openpyxl, xlrd 2.0.2, pyarrow, tqdm, …). |
| `CORPUS_README.md` | Corpus schema, size, known limitations. |
| `coverage_report.md` | Per-year row / hospital / code-type counts. |
| `comparison_2024.txt` | Snapshot output of `compare_to_parvati.py` for the 2024 slice. |

## Reproducibility

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# Step 1 — ingest (15 min on 16 cores, 4,372 CDM files)
.venv/bin/python ingest_hcai.py --workers 16

# Step 2 — facility ZIP join (30 s)
.venv/bin/python build_matched_with_zip.py

# Step 3 — compare with Parvati's reported 2024 figures
.venv/bin/python compare_to_parvati.py
```

## Why this replaces the legacy pipeline

The lab's prior `matched_rows_with_zip.csv` was produced by a Windows-only
workflow (`PLHI_ANALYSIS.ipynb` + `chargemaster_extractor.py` + manual
Excel-based ZIP join) that:

- Ran in DEMO mode against 3 hospitals in 2024 only, not the full corpus.
- Kept no record of which hospital-name → ZIP mappings were used.
- Filtered to a small CPT list at ingest time, discarding HCPCS / revenue
  / DRG / NDC data.
- Accepted Common-25 and PCT_CHG files indistinguishably from real CDMs,
  introducing fee-schedule and percent-change values into the "charge"
  column that had to be cleaned downstream.
- Required manual intervention between each run.

The new pipeline captures the full 2014–2025 corpus (56.5 M rows across
all six standardized code families), uses OSHPD facility IDs for
deterministic cross-year joins, emits parquet for fast downstream
reads, and is idempotent/reproducible from a single CLI.

## Corpus size

| Code family | Rows (2014–25) | Unique codes | Hospitals |
|---|---:|---:|---:|
| CPT | 6.5 M | ~55 K* | 439 |
| HCPCS Level II | 24.0 M | ~4 K | ~150 |
| Revenue Code (UB-04) | 23.6 M | ~490 | ~80 |
| MS-DRG | ~570 | ~80 | ~10 |
| NDC | 3.4 M | ~60 K | ~60 |
| ICD-10-PCS | 0 | — | — |

*Real AMA CPT space is ~10 K; the inflation is internal 5-digit hospital
codes that still pass `^\d{5}$`. Apply an AMA reference-list filter
downstream if you need a clean count.

## Next steps not yet implemented

1. **AMA CPT + CMS HCPCS canonical reference filter** to collapse the
   ~55 K "unique CPTs" down to the real ~10 K namespace.
2. **Hospital-name canonicalization** for the ~5% of rows where the
   filename carries no OSHPD ID (mostly 2024–2025 Kaiser packets).
3. **Census ACS 5-year pull + mortality join** to recompute Parvati's
   HVI and regression models on the new corpus. Needs a Census API key.
