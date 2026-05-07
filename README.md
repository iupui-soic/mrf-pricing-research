# PricePortal — Open hospital price transparency pipeline (CA + IN)

Reproducible pipeline that builds an open, multi-source hospital price
transparency corpus for California and Indiana, spanning four price
types (chargemaster / cash / negotiated / Medicare-allowable) at
hospital × code × ZIP resolution.

**Public portal**: https://pricingapp.streamlit.app/ — Streamlit app over
the analytic outputs. Source: https://github.com/pnaliyatthaliyazchayil/PricingPortal

**Archived corpus (Zenodo, DOI-citable)**: [10.5281/zenodo.19941038](https://doi.org/10.5281/zenodo.19941038) — 16 analysis-grade parquet files (6.42 GB) covering federal MRFs (305M gross + 417M negotiated rows across 528 hospitals), Medicare CPT/HCPCS benchmarks, the hospital × code price-to-Medicare ratio panel (1,528,609 rows), Wang 2023 / Chang & Psek 2024 replication outputs, and ZIP-level community-risk variables. CC-BY-4.0. See `DATA_DICTIONARY.md` in the deposit for per-file schema.

## Pipeline components

```
ingest_hcai.py ───────────────► CA chargemaster corpus 2014–25 (56.5 M rows)
                                /data0/mrf-pricing-research/hcai-chargemasters/ingest/cdm_*.parquet

mrf/  ────────────────────────► Federal HPT MRFs (CA + IN, 528 hospitals)
                                /data0/mrf-pricing-research/mrf/files/<state>/<ccn>/
                                /data0/mrf-pricing-research/mrf/parsed/mrf_{gross,negotiated}.parquet

medicare/ ────────────────────► CMS fee schedules (MPFS, OPPS, IPPS)
                                /data0/mrf-pricing-research/medicare/extracted/<slot>/

census/ ──────────────────────► CA + IN ACS 2024 5-year, CDC PLACES (IN), CDPH deaths (CA)
                                /data0/mrf-pricing-research/census/in_zip_{demographics,health_outcomes}.parquet
                                /data0/mrf-pricing-research/hcai-chargemasters/ingest/cache_census_zip_2024.csv
                                /data0/mrf-pricing-research/hcai-chargemasters/ingest/ca_deaths_zip_2019-2024.csv

build_crosswalk.py ───────────► Hospital identity crosswalk
                                /data0/mrf-pricing-research/crosswalk/facilities_crosswalk.parquet

analysis/ ────────────────────► Medicare benchmarks + ratio panels
                                /data0/mrf-pricing-research/analysis/{ratios_*,wang_*,chang_psek_*,state_compliance}.parquet
```

## Layout

### CA chargemaster ingest (`/`)

| File | Purpose |
|---|---|
| `ingest_hcai.py` | Walks `/data0/mrf-pricing-research/hcai-chargemasters/<year>/`, parses heterogeneous CDM xlsx/xls/csv files, emits typed parquet per year + master. |
| `build_matched_with_zip.py` | Joins corpus to HCAI facility ZIP listing; produces `matched_rows_with_zip_<year>.csv`. |
| `compare_to_parvati.py` | Cross-pipeline reconciliation against the legacy notebook's summary stats. |
| `coverage_report.md` | Per-year row / hospital / code-type counts. |
| `comparison_2024.txt` | Snapshot output of `compare_to_parvati.py` for the 2024 slice. |
| `CORPUS_README.md` | Per-corpus schema, size, known limitations. |

### Federal MRF discovery + download (`mrf/`)

| File | Purpose |
|---|---|
| `mrf/build_hospital_list.py` | Pulls CMS Hospital General Information, filters to CA + IN, emits `hospitals.csv` (528 Medicare-certified facilities). |
| `mrf/discover_mrf_urls.py` | URL discovery via per-host crawl + filename validation. |
| `mrf/seed_known_urls.py` | Merges curated seed CSVs (`seed_*.csv`) into `mrf_urls.csv` with HEAD validation. |
| `mrf/download_mrfs.py` | Per-host rate-limited downloader; resumable; writes `downloads.csv` with sha256 + content-type. |
| `mrf/sync_manual_uploads.py` | Reads `pending_hospitals.csv` URLs + an in-script `FILE_MAP` of manually-downloaded files; moves each into `<state>/<ccn>/` and updates ledgers. Used for the long tail. |
| `mrf/reclassify_html_landing.py` | Catches files served as HTML landing pages (PARA HCFS, hospitalpricedisclosure.com, etc.) and reclassifies them `exempt:portal_landing`. |
| `mrf/mark_exempt_*.py` | Marks federal (VA / DoD) and other exempt categories. |
| `mrf/pending_hospitals.csv` | Long-tail seed list — manually-researched URLs that bypass automated discovery. |

### Medicare benchmark fetch (`medicare/`)

| File | Purpose |
|---|---|
| `medicare/download_medicare.py` | Fetches MPFS RVU files (CY2024–2026), OPPS Addendum B (Jul-2025 + Jan-2026), IPPS Table 5 (FY2025 + FY2026). 9 ZIPs auto-extracted. Ledger at `/data0/mrf-pricing-research/medicare/downloads.csv`. |

### Hospital identity crosswalk (`/`)

| File | Purpose |
|---|---|
| `build_crosswalk.py` | Joins CMS POS + HCAI facilities + extracted EINs + extracted/looked-up NPIs into `/data0/mrf-pricing-research/crosswalk/facilities_crosswalk.parquet`. |
| `extract_npi_from_mrfs.py` | Reads `type_2_npi` from CMS v3.0 MRF metadata (CSV/JSON/XLSX/ZIP-aware, content-sniffing). |
| `lookup_nppes.py` | NPPES NPI Registry API fallback for v2.0/v1.x MRFs (where `type_2_npi` doesn't exist). Three-pass: taxonomy+state+ZIP exact, then name+ZIP fuzzy, then state-only. Corporate-entity blocklist + state/ZIP3 hard filter prevent false positives. |

### Census + health outcomes (`census/`)

| File | Purpose |
|---|---|
| `census/pull_census_ca.py` | Pulls ACS 2024 5-year ZIP demographics for all CA ZCTAs into `cache_census_zip_2024.csv` (zip, median_income, total_pop, poverty_rate, pct_uninsured, pct_disability, pct_elderly). Two API calls: detailed tables for income/poverty/population, subject tables for the three percentages. |
| `census/pull_census_in.py` | IN-side counterpart: ACS 2024 5-year for the 114 IN hospital ZIPs (zip, total_pop, median_household_income, pct_poverty, pct_white, pct_black) plus CDC PLACES modeled health-outcome prevalence (40 measures × 114 ZIPs). Same ACS vintage as the CA-side pull. |
| `census/pull_ca_deaths.py` | Pulls the CDPH "Death Profiles by ZIP Code" 2019–2024 file from CHHS Open Data via CKAN; falls back to a pinned URL if the API is unreachable. CA-only — Indiana publishes mortality at the county level, not ZIP. |

### Analysis layer (`analysis/`)

| File | Purpose |
|---|---|
| `analysis/build_medicare_benchmarks.py` | Joins MPFS + OPPS Addendum B into `medicare_cpt_2026.parquet` (9,709 codes; OPPS Payment Rate preferred, MPFS national fallback at CF=33.2875). Source provenance retained per code. |
| `analysis/build_ratios.py` | DuckDB-driven join of MRF parquets × Medicare benchmark → `ratios_hospital_code.parquet` (1.55 M rows) + per-state and per-payer summaries. |
| `analysis/wang_correlations.py` | Reproduces Wang, Bai & Anderson (Health Affairs 2023): per-hospital Pearson r on log prices for (gross, cash, neg_min) pairs, plus cash-discount segmentation; emits `wang_per_hospital.parquet` + `wang_state_summary.parquet`. |
| `analysis/chang_psek_regression.py` | Extends Chang & Psek (BMC HSR 2024) from HSA to ZIP resolution: per-ZIP median ratios joined with ACS 5-year demographics, plus OLS for `{gross,cash,neg_min}_ratio ~ poverty_rate_pct + log_median_income` (pooled / CA / IN). Emits `chang_psek_zip_panel.parquet` + `chang_psek_models.txt`. |

### Audit framework (`audit/`)

| File | Purpose |
|---|---|
| `audit/build_audit_sample.py` | Generates `audit_sample_200.csv` — stratified random sample from `cdm_all.parquet` for human-auditor labeling. Deterministic (seed=20260426). |
| `audit/score_audit.py` | Reads the labeled sample, computes per-field + per-code_type precision, emits `audit_results.md`. |
| `audit/README.md` | Labeling rubric (what counts as correct for each of the four `audit_correct_*` fields). |

## Bootstrap on a new machine

`download_data.sh` rsyncs the namespaced `/data0/mrf-pricing-research/`
corpora from a remote server over SSH so you don't have to re-run the
multi-day download/parse pipeline on every fresh clone. Pulls
`crosswalk + medicare + census + analysis + mrf` by default
(~80 GB; opt-in for `hcai-chargemasters` via `EXPLICIT_CHARGEMASTER=1`).
No credentials in the script — auth is whatever your local ssh config
provides for `$SRC`.

```bash
# Default: pull every corpus except the multi-TB CA chargemaster source.
SRC=user@otherhost bash download_data.sh

# Selected corpora, with bandwidth cap and dry-run preview:
SRC=user@otherhost BWLIMIT=20000 DRY_RUN=1 \
    bash download_data.sh medicare census analysis
```

## Reproducibility

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# Step 1 — CA chargemaster ingest (15 min, 16 cores, 4,372 CDM files)
.venv/bin/python ingest_hcai.py --workers 16
.venv/bin/python build_matched_with_zip.py
.venv/bin/python compare_to_parvati.py     # sanity check

# Step 2 — federal MRF discovery + download (CA + IN)
.venv/bin/python mrf/build_hospital_list.py     # 528 hospitals
.venv/bin/python mrf/seed_known_urls.py         # validate seed URLs
.venv/bin/python mrf/download_mrfs.py --resume  # ~72 GB total

# Step 3 — Medicare fee schedules (24 MB)
.venv/bin/python medicare/download_medicare.py

# Step 4 — hospital identity crosswalk
.venv/bin/python extract_npi_from_mrfs.py       # NPI from MRF metadata
.venv/bin/python lookup_nppes.py                # NPPES fallback for residuals
.venv/bin/python build_crosswalk.py             # join → facilities_crosswalk.parquet

# Step 5 — audit framework
.venv/bin/python audit/build_audit_sample.py    # 200-row sample for human labeling
# (manual labeling step — see audit/README.md for rubric)
.venv/bin/python audit/score_audit.py           # → audit_results.md

# Step 6 — Census + mortality
.venv/bin/python census/pull_census_ca.py    # CA ACS → cache_census_zip_2024.csv
.venv/bin/python census/pull_census_in.py    # IN ACS + CDC PLACES
.venv/bin/python census/pull_ca_deaths.py    # CDPH ZIP-level deaths 2019-2024

# Step 7 — MRF parsing → unified parquet
.venv/bin/python mrf/parse_mrf.py               # parts under mrf/parsed/_parts/
.venv/bin/python mrf/concat_parts.py            # → mrf_gross.parquet + mrf_negotiated.parquet
.venv/bin/python mrf/coverage_report.py

# Step 8 — analytic layer (Medicare benchmark + price-to-Medicare ratio panel)
.venv/bin/python analysis/build_medicare_benchmarks.py
.venv/bin/python analysis/build_ratios.py
.venv/bin/python analysis/wang_correlations.py
.venv/bin/python analysis/chang_psek_regression.py
```

Every download produces a ledger CSV with sha256, byte size, source URL,
and timestamp.

## Universe and denominator

528 Medicare-certified hospitals across CA (378) + IN (150), sourced
from CMS Hospital General Information (Provider of Services), filtered
to all CMS hospital types (Acute Care, Critical Access, Children's,
Psychiatric, Rural Emergency, plus VA/DoD acute), de-duplicated on CCN.

The CMS HPT rule (45 CFR 180) binds Medicare-certified hospitals; broader
state-licensure lists (HCAI ≈ 544 in CA; IN SDH ≈ 170) include facility
classes not bound by the rule (Chemical Dependency Recovery Hospitals,
many CA Psychiatric Health Facilities, etc.). 528 is the correct
denominator for HPT compliance research.

## Why this replaces the lab's prior single-state pipeline

The previous `matched_rows_with_zip.csv` came from a Windows-only
workflow (`PLHI_ANALYSIS.ipynb` + `chargemaster_extractor.py` + manual
Excel-based ZIP join) that ran in DEMO mode against 3 hospitals in 2024
only, kept no record of hospital-name → ZIP mappings, filtered to a
small CPT list at ingest (discarding HCPCS / revenue / DRG / NDC data),
and required manual intervention between each run. It also covered only
CA chargemaster — no federal HPT MRFs, no Medicare benchmarks, no
cross-state comparison.

The current pipeline captures the full 2014–2025 CA chargemaster corpus
(56.5 M rows), adds the federal MRF corpus for CA + IN (458 valid MRFs +
70 exempt = 528 universe; 484 hospitals contributing to parsed parquets),
the Medicare benchmark fee schedules (MPFS / OPPS / IPPS), a hospital
identity crosswalk linking everything via CCN, an analytic ratio panel
(1.55 M hospital × code rows), and a public Streamlit portal at
https://pricingapp.streamlit.app/ over the analytic outputs. All
artifacts are sha256-checksummed and reproducible from a single CLI
sequence.
