#!/usr/bin/env python3
"""
build_crosswalk.py
==================
Builds `/data0/crosswalk/facilities_crosswalk.parquet` keyed on CCN, with:

  ccn               — CMS Certification Number (6-digit)
  oshpd_id          — CA HCAI facility ID (9-digit; null for non-CA + unmatched)
  ein               — Employer Identification Number (extracted from MRF URL/filename)
  npi               — National Provider Identifier (null in v1)
  in_facility_id    — Indiana SDH licensure ID (null in v1; needs IN data pull)
  name              — Hospital name (from CMS POS)
  address, city, state, zip, county — from CMS POS
  hospital_type, ownership, has_ed   — from CMS POS
  oshpd_match_score — 0.0–1.0 fuzzy similarity used to lock the CA OSHPD join
  oshpd_match_method — 'exact_zip+name' | 'zip_only_fuzzy' | 'name_only_fuzzy' | 'unmatched'

Inputs (already on disk):
  /data0/mrf/hospitals.csv            — 528-hospital CA+IN universe (from CMS POS)
  /data0/hcai-chargemasters/ingest/facilities.csv — 545 CA HCAI facilities
  /data0/mrf/mrf_urls.csv             — for EIN extraction from URL/filename
  /data0/mrf/downloads.csv            — fallback for EIN extraction from local_path

Indiana SDH licensure crosswalk and CMS NPPES NPI lookups deferred to v2.
"""

from __future__ import annotations

import csv
import re
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd

OUT_DIR = Path("/data0/crosswalk")
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PARQUET = OUT_DIR / "facilities_crosswalk.parquet"
OUT_CSV = OUT_DIR / "facilities_crosswalk.csv"  # for quick inspection
COVERAGE_MD = OUT_DIR / "crosswalk_coverage.md"

HOSPITALS = Path("/data0/mrf/hospitals.csv")
HCAI_FACILITIES = Path("/data0/hcai-chargemasters/ingest/facilities.csv")
MRF_URLS = Path("/data0/mrf/mrf_urls.csv")
DOWNLOADS = Path("/data0/mrf/downloads.csv")
NPI_CSV = OUT_DIR / "ccn_to_npi.csv"        # from extract_npi_from_mrfs.py
NPI_NPPES = OUT_DIR / "ccn_to_npi_nppes.csv"  # from lookup_nppes.py
EIN_PROPUBLICA = OUT_DIR / "ccn_to_ein_propublica.csv"  # from lookup_propublica_eins.py

# Hand-mapped CCN -> OSHPD overrides for hospitals the fuzzy join couldn't
# place automatically (mostly renames between CMS POS and HCAI). Verified
# 2026-04-26 by ZIP-co-located lookups in HCAI facilities.csv.
MANUAL_OSHPD_OVERRIDES: dict[str, str | None] = {
    "050115": "106374382",  # Palomar Health Downtown Campus -> Palomar Medical Center Escondido
    "050191": "106190053",  # St Mary Medical Center, Long Beach
    "050239": "106190323",  # Glendale Adventist -> Adventist Health Glendale (rename)
    "050295": "106150761",  # Mercy Hospital, Bakersfield
    "050342": "106130760",  # Pioneers Memorial Healthcare District -> Pioneers Memorial Hospital
    "050444": "106240942",  # Mercy Medical Center, Merced
    "050557": "106500939",  # Memorial Medical Center, Modesto
    "050747": "106301258",  # Coastal Communities Hospital -> South Coast Global Medical Center (rename)
    "051336": "106270777",  # Southern Monterey County Memorial -> George L. Mee Memorial (rename)
    "054053": "106190232",  # Del Amo Hospital -> Del Amo Behavioral Health System
    "054152": "106444029",  # Santa Cruz County PHF -> Telecare Santa Cruz PHF (operator change)
    # No HCAI counterpart (closed or never licensed under HCAI):
    "050102": None,         # Parkview Community Hospital MC, Riverside - not in HCAI listing
    "050726": None,         # Stanislaus Surgical Hospital - closed 2024-09, already exempt
}

# An EIN in CMS MRF filenames takes one of two forms:
#   "330751869"     (9 contiguous digits)
#   "33-0751869"    (XX-XXXXXXX with a literal hyphen)
# Both appear at the START of the filename. The trailing separator is
# usually "_" but Kaiser/HCA-style filenames use "-" instead. Bound on
# any non-digit, non-letter delimiter, then require a name token.
EIN_PATTERN = re.compile(
    r"(?:^|/)(\d{2}-?\d{7})[-_]",
    re.IGNORECASE,
)

NAME_NOISE = re.compile(r"[^A-Z0-9 ]+")
NAME_STOPWORDS = {
    "THE", "A", "AN", "OF", "AND", "INC", "INCORPORATED", "LLC", "LP",
    "MEDICAL", "CENTER", "HOSPITAL", "HOSPITALS", "HOSP", "HEALTH",
    "HEALTHCARE", "REGIONAL", "MEM", "MEMORIAL",
}


def normalize_name(s: str) -> str:
    s = NAME_NOISE.sub(" ", (s or "").upper())
    toks = [t for t in s.split() if t and t not in NAME_STOPWORDS]
    return " ".join(toks)


def name_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, normalize_name(a), normalize_name(b)).ratio()


def extract_ein(*candidates: str) -> str | None:
    for c in candidates:
        if not c:
            continue
        m = EIN_PATTERN.search(c)
        if m:
            ein = m.group(1).replace("-", "")
            if len(ein) == 9:
                return f"{ein[:2]}-{ein[2:]}"
    return None


def load_eins() -> dict[str, str]:
    """ccn -> EIN, scraped from MRF URLs and local filenames."""
    eins: dict[str, str] = {}

    if MRF_URLS.exists():
        with MRF_URLS.open() as f:
            for r in csv.DictReader(f):
                ccn = r.get("ccn")
                if not ccn or ccn in eins:
                    continue
                ein = extract_ein(r.get("mrf_url", ""))
                if ein:
                    eins[ccn] = ein

    if DOWNLOADS.exists():
        with DOWNLOADS.open() as f:
            for r in csv.DictReader(f):
                ccn = r.get("ccn")
                if not ccn or ccn in eins:
                    continue
                ein = extract_ein(r.get("local_path", ""), r.get("mrf_url", ""))
                if ein:
                    eins[ccn] = ein

    return eins


def join_oshpd(hosp_ca: pd.DataFrame, hcai: pd.DataFrame) -> pd.DataFrame:
    """Match CA hospitals (CMS CCN) to HCAI OSHPD IDs by ZIP + fuzzy name."""
    hcai = hcai.copy()
    hcai["zip5"] = hcai["zip"].astype(str).str.zfill(5).str[:5]
    hcai["norm_name"] = hcai["facility_name"].fillna("").map(normalize_name)

    out_cols = ["oshpd_id", "oshpd_match_score", "oshpd_match_method"]
    rows = []

    for _, h in hosp_ca.iterrows():
        zip5 = str(h["zip"]).zfill(5)[:5]
        target = h["name"]

        # Pass 1: same ZIP, fuzzy on name -> best score
        same_zip = hcai[hcai["zip5"] == zip5]
        if not same_zip.empty:
            scores = same_zip["facility_name"].apply(
                lambda n: name_similarity(n, target))
            best_idx = scores.idxmax()
            best_score = scores.loc[best_idx]
            if best_score >= 0.65:
                method = "exact_zip+name" if best_score >= 0.9 else "zip_only_fuzzy"
                rows.append((same_zip.loc[best_idx, "oshpd_id"], best_score, method))
                continue

        # Pass 2: no good ZIP match; fuzzy on name across all HCAI facilities
        scores = hcai["facility_name"].apply(
            lambda n: name_similarity(n, target))
        best_idx = scores.idxmax()
        best_score = scores.loc[best_idx]
        if best_score >= 0.85:
            rows.append((hcai.loc[best_idx, "oshpd_id"], best_score, "name_only_fuzzy"))
        else:
            rows.append((None, best_score, "unmatched"))

    return pd.DataFrame(rows, columns=out_cols, index=hosp_ca.index)


def main() -> None:
    print("[load] inputs")
    hosp = pd.read_csv(HOSPITALS, dtype=str)
    hosp.columns = [c.lower() for c in hosp.columns]
    print(f"  hospitals.csv: {len(hosp)} rows ({hosp['state'].value_counts().to_dict()})")

    hcai = pd.read_csv(HCAI_FACILITIES, dtype=str)
    hcai.columns = [c.lower() for c in hcai.columns]
    print(f"  HCAI facilities.csv: {len(hcai)} rows")

    eins = load_eins()
    print(f"  EINs extracted from MRF URLs/filenames: {len(eins)}")

    # Crosswalk OSHPD for CA hospitals only.
    hosp_ca = hosp[hosp["state"] == "CA"].copy()
    print(f"\n[join] OSHPD ID for {len(hosp_ca)} CA hospitals")
    oshpd = join_oshpd(hosp_ca, hcai)
    hosp_ca = pd.concat([hosp_ca, oshpd], axis=1)

    # CCNs ending in 'F' are federal (VA/DoD); they are not state-licensed
    # and have no OSHPD ID by design — recategorize them away from "unmatched".
    federal_mask = hosp_ca["ccn"].str.endswith("F")
    hosp_ca.loc[federal_mask, "oshpd_match_method"] = "n/a (federal)"
    hosp_ca.loc[federal_mask, "oshpd_match_score"] = None
    hosp_ca.loc[federal_mask, "oshpd_id"] = None

    # Apply hand-mapped overrides for hospitals the fuzzy join couldn't place.
    for ccn, oshpd_id in MANUAL_OSHPD_OVERRIDES.items():
        mask = hosp_ca["ccn"] == ccn
        if not mask.any():
            continue
        if oshpd_id is None:
            hosp_ca.loc[mask, "oshpd_id"] = None
            hosp_ca.loc[mask, "oshpd_match_method"] = "n/a (no HCAI counterpart)"
            hosp_ca.loc[mask, "oshpd_match_score"] = None
        else:
            hosp_ca.loc[mask, "oshpd_id"] = oshpd_id
            hosp_ca.loc[mask, "oshpd_match_method"] = "manual_override"
            hosp_ca.loc[mask, "oshpd_match_score"] = 1.0

    method_counts = hosp_ca["oshpd_match_method"].value_counts().to_dict()
    print(f"  matches: {method_counts}")

    # Indiana hospitals: no OSHPD; fill with nulls.
    hosp_in = hosp[hosp["state"] == "IN"].copy()
    hosp_in["oshpd_id"] = None
    hosp_in["oshpd_match_score"] = None
    hosp_in["oshpd_match_method"] = "n/a (non-CA)"

    out = pd.concat([hosp_ca, hosp_in], ignore_index=True)
    # EIN: prefer URL-extracted, fall back to ProPublica Nonprofit Explorer
    ein_combined: dict[str, tuple[str, str]] = {ccn: (e, "url_filename")
                                                 for ccn, e in eins.items()}
    if EIN_PROPUBLICA.exists():
        with EIN_PROPUBLICA.open() as f:
            for r in csv.DictReader(f):
                if r.get("ein") and r["ccn"] not in ein_combined:
                    ein_combined[r["ccn"]] = (r["ein"], "propublica_990")
    out["ein"] = out["ccn"].map(lambda c: ein_combined.get(c, (None,))[0])
    out["ein_source"] = out["ccn"].map(
        lambda c: ein_combined.get(c, (None, None))[1])

    # NPI: prefer MRF metadata (hospital-self-reported), fall back to
    # NPPES API matches. Each source recorded in `npi_source`.
    npis: dict[str, tuple[str, str]] = {}  # ccn -> (npi, source)
    if NPI_CSV.exists():
        with NPI_CSV.open() as f:
            for r in csv.DictReader(f):
                if r.get("npi"):
                    npis[r["ccn"]] = (r["npi"], "mrf_metadata")
    if NPI_NPPES.exists():
        with NPI_NPPES.open() as f:
            for r in csv.DictReader(f):
                if r.get("npi") and r["ccn"] not in npis:
                    npis[r["ccn"]] = (r["npi"], f"nppes:{r.get('match_method','?')}")
    out["npi"] = out["ccn"].map(lambda c: npis.get(c, (None,))[0])
    out["npi_source"] = out["ccn"].map(lambda c: npis.get(c, (None, None))[1])
    n_mrf = sum(1 for v in npis.values() if v[1] == "mrf_metadata")
    n_nppes = sum(1 for v in npis.values() if v[1].startswith("nppes"))
    print(f"  NPIs: {len(npis)} (mrf_metadata={n_mrf}, nppes={n_nppes})")

    out["in_facility_id"] = None

    cols = [
        "ccn", "oshpd_id", "ein", "ein_source",
        "npi", "npi_source", "in_facility_id",
        "name", "address", "city", "state", "zip", "county",
        "hospital_type", "ownership", "has_ed",
        "oshpd_match_score", "oshpd_match_method",
    ]
    out = out[cols]
    out.to_parquet(OUT_PARQUET, index=False)
    out.to_csv(OUT_CSV, index=False)
    print(f"\n[out] {OUT_PARQUET}")
    print(f"      {OUT_CSV}")

    # Coverage report
    n = len(out)
    n_ca = (out["state"] == "CA").sum()
    n_in = (out["state"] == "IN").sum()
    n_ein = out["ein"].notna().sum()
    n_oshpd = out["oshpd_id"].notna().sum()
    n_oshpd_strong = ((out["oshpd_match_method"] == "exact_zip+name")).sum()
    n_oshpd_manual = ((out["oshpd_match_method"] == "manual_override")).sum()
    n_federal = ((out["state"] == "CA") &
                 (out["oshpd_match_method"] == "n/a (federal)")).sum()
    n_no_hcai = ((out["state"] == "CA") &
                 (out["oshpd_match_method"] == "n/a (no HCAI counterpart)")).sum()
    n_unmatched_ca = ((out["state"] == "CA") &
                      (out["oshpd_match_method"] == "unmatched")).sum()
    n_ca_eligible = n_ca - n_federal - n_no_hcai
    n_npi = out["npi"].notna().sum()
    n_mrf_npi_universe = (out["npi_source"] == "mrf_metadata").sum()
    n_nppes_npi_universe = out["npi_source"].fillna("").str.startswith("nppes").sum()

    # Effective MRF universe: hospitals with a real (non-html, non-exempt)
    # MRF on disk. NPI/EIN denominators should be against this set.
    n_real_mrf = 0
    real_mrf_ccns: set[str] = set()
    if (OUT_DIR / "../mrf/downloads.csv").exists() or Path("/data0/mrf/downloads.csv").exists():
        import csv as _csv
        with open("/data0/mrf/downloads.csv") as f:
            for r in _csv.DictReader(f):
                if r.get("status") == "ok":
                    real_mrf_ccns.add(r["ccn"])
        n_real_mrf = len(real_mrf_ccns)
    n_ein_real = sum(1 for ccn in real_mrf_ccns if ein_combined.get(ccn, (None,))[0])
    n_ein_url = (out["ein_source"] == "url_filename").sum()
    n_ein_pp = (out["ein_source"] == "propublica_990").sum()
    n_npi_real = sum(1 for ccn in real_mrf_ccns if ccn in npis)

    md = [
        "# Facilities Crosswalk — Coverage",
        "",
        f"- Total hospitals in universe: **{n}** (CA={n_ca}, IN={n_in})",
        f"- Hospitals with a real MRF on disk (status=ok): **{n_real_mrf}**",
        f"- CCN: 100% (every row keyed on CCN)",
        f"- EIN: **{n_ein}/{n}** of universe ({100*n_ein/n:.1f}%); "
        f"**{n_ein_real}/{n_real_mrf}** of hospitals with a real MRF "
        f"({100*n_ein_real/max(n_real_mrf,1):.1f}%). Sources, in priority "
        f"order:",
        f"  1. **CMS-mandated MRF filename** "
        f"`<EIN>_<hospital>_standardcharges.{{csv,json}}` "
        f"({n_ein_url}/{n} = {100*n_ein_url/n:.1f}%) — when hospitals "
        f"host their own file rather than serving via an aggregator.",
        f"  2. **ProPublica Nonprofit Explorer (IRS Form 990)** fallback "
        f"({n_ein_pp}/{n} = {100*n_ein_pp/n:.1f}%) — covers nonprofit "
        f"and many hospital-district hospitals (see "
        f"`lookup_propublica_eins.py`).",
        f"- For-profit hospitals served via aggregator portals (PARA, "
        f"hospital-price-index, Box, Craneware) without an EIN-prefixed "
        f"filename have **no public free EIN source**; CMS does not "
        f"publish CCN→EIN, and IRS Form 990 only covers tax-exempt "
        f"entities. Those gaps are residual.",
        f"- OSHPD ID matched (CA state-licensed only): **{n_oshpd}/"
        f"{n_ca_eligible}** ({100*n_oshpd/n_ca_eligible:.1f}%)",
        f"  - exact ZIP + ≥0.9 name similarity: {n_oshpd_strong}",
        f"  - ZIP match + 0.65–0.9 name similarity: "
        f"{(out['oshpd_match_method']=='zip_only_fuzzy').sum()}",
        f"  - cross-ZIP name match (≥0.85): "
        f"{(out['oshpd_match_method']=='name_only_fuzzy').sum()}",
        f"  - manual override (rename / hand-mapped): {n_oshpd_manual}",
        f"  - federal CA (VA/DoD, exempt from HCAI): {n_federal}",
        f"  - no HCAI counterpart (closed/never licensed): {n_no_hcai}",
        f"  - unmatched: **{n_unmatched_ca}**",
        f"- IN facility ID: 0/{n_in} (deferred — Indiana SDH publishes a PDF "
        f"licensure listing, no public CSV; not blocking since the analysis "
        f"keys on CCN for IN)",
        f"- NPI: **{n_npi}/{n}** of universe ({100*n_npi/n:.1f}%); "
        f"**{n_npi_real}/{n_real_mrf}** of hospitals with a real MRF "
        f"({100*n_npi_real/max(n_real_mrf,1):.1f}%). "
        f"Sources, in priority order:",
        f"  1. **MRF metadata `type_2_npi` field** "
        f"({n_mrf_npi_universe}/{n} = {100*n_mrf_npi_universe/n:.1f}%) — "
        f"hospital-self-reported in CMS v3.0 MRFs. v2.0 files don't "
        f"have this field at all (added July 2024).",
        f"  2. **NPPES NPI Registry API** fallback "
        f"({n_nppes_npi_universe}/{n} = "
        f"{100*n_nppes_npi_universe/n:.1f}%) — public CMS endpoint at "
        f"`npiregistry.cms.hhs.gov/api/`, queried by name+state+ZIP "
        f"with fuzzy match (see `lookup_nppes.py`).",
        f"- The {n - n_npi} remaining gaps are split: ~25 are exempt "
        f"hospitals with no MRF and no NPPES NPI-2 record under the "
        f"CMS POS name; the rest are hospitals where NPPES name "
        f"divergence (DBA vs legal name) prevented a confident match. "
        f"A bulk NPPES dissemination download (~10 GB) could close most "
        f"of these.",
        "",
        "## Unmatched CA hospitals (need manual OSHPD lookup)",
        "",
        "| CCN | Name | City | ZIP | best_score |",
        "|---|---|---|---|---|",
    ]
    for _, r in out[(out["state"] == "CA") &
                    (out["oshpd_match_method"] == "unmatched")].iterrows():
        md.append(f"| {r['ccn']} | {r['name']} | {r['city']} | {r['zip']} | "
                  f"{float(r['oshpd_match_score'] or 0):.2f} |")

    COVERAGE_MD.write_text("\n".join(md) + "\n")
    print(f"      {COVERAGE_MD}")


if __name__ == "__main__":
    main()
