#!/usr/bin/env python3
"""
sync_manual_uploads.py
======================
Move manually-uploaded MRF files from /data0/mrf/files/ root into the
canonical /data0/mrf/files/<STATE>/<CCN>/<filename> layout, and update
mrf_urls.csv + downloads.csv accordingly.

Source of truth for (ccn -> URL) is mrf/pending_hospitals.csv (column
mrf_url). Source of truth for (ccn -> uploaded filename) is the
explicit FILE_MAP below — built by inspecting the files dropped in
/data0/mrf/files/ root after the user manually downloaded them.

Three pending hospitals have no file (handled as exemptions):
  054089 Jewish Home & Rehab Center      -> exempt:gated_portal (site down)
  054133 DSH-Metropolitan State Hospital -> exempt:state_psych_hpt
  150046 Terre Haute Regional Hospital   -> exempt:closed (acquired by Union Health)
"""

from __future__ import annotations

import csv
import datetime as dt
import hashlib
import shutil
import sys
from pathlib import Path

OUT_DIR = Path("/data0/mrf")
FILES_DIR = OUT_DIR / "files"
URL_CSV = OUT_DIR / "mrf_urls.csv"
DL_CSV = OUT_DIR / "downloads.csv"
PENDING_CSV = Path(__file__).resolve().parent / "pending_hospitals.csv"

# CCN -> filename in /data0/mrf/files/ (root) for each manual upload.
FILE_MAP: dict[str, str] = {
    "050022": "33-0751869_RIVERSIDE-COMMUNITY-HOSPITAL_standardcharges.json",
    "050125": "946000533_regional-medical-center_standardcharges.csv",
    "050131": "940562680-1104059153_novato-community-hospital_standardcharges.csv",
    "050211": "943302014_alameda-hospital_standardcharges.csv",
    "050245": "956002748_san-bernardino-county_standardcharges.csv",
    "050320": "943302014_highland-hospital_standardcharges.csv",
    "050393": "CDM April_2026_PHDH.csv",
    "050471": "PHGSH_CDM_Charge_March2026.csv",
    "050528": "941156621-1366896300_memorial-hospital-los-banos_standardcharges.csv",
    "050549": "95-2321136_LOS-ROBLES-HOSPITAL-AND-MEDICAL-CENTER_standardcharges.json",
    "054055": "953421289_college-hospital-cerritos_standcharges_03_2025.csv",
    "150115": "350985964_little-company-of-mary-hospital-of-indiana-inc_standardcharges.csv",
    "150149": "352062016_deaconess-health-system-the-womens-hospital_standardcharges.csv",
    "150172": "205071967_PMC-Regional-Hospital_standardcharges.csv",
    "151312": "273532963_indiana-university-health-white-memorial-hospital-inc._standardcharges.zip",
    "151319": "350877575_deaconess-health-system-gibson-general-hospital_standardcharges.csv",
    "154020": "351375696_strawhun_standardcharges.csv",
    "154052": "351330771_porter-starke-services,-inc._standardcharges.csv",
    "154058": "452471121_doctors-behavioral-hospital,-llc_standardcharges.csv",
    "154061": "471265300_rivercrest-specialty-hospital,-llc_standardcharges.csv",
    "154063": "473943366_neuropsychiatric-hospital-of-indianapolis,-llc_standardcharges.csv",
    "154065": "822569371_neurobehavioral-hospital,-llc_standardcharges.csv",
    "154066": "831779940_brightwellbehavioralhealth_machine+readable+file.csv",
}

EXEMPTIONS: dict[str, tuple[str, str]] = {
    "054089": (
        "exempt:gated_portal",
        "Jewish Home & Rehab Center — only via panaceainc Hospital Price Index portal; site currently down",
    ),
    "054133": (
        "exempt:state_psych_hpt",
        "DSH-Metropolitan State Hospital — CA Dept of State Hospitals; state psych facility, CMS HPT rule does not apply",
    ),
    "150046": (
        "exempt:closed",
        "Terre Haute Regional Hospital — acquired by Union Health; no current standalone MRF",
    ),
}

# Files in /data0/mrf/files/ root that are duplicates of files already
# present under STATE/CCN — just delete the root-level copy.
DUPLICATES_TO_REMOVE: list[str] = [
    "842021041_medical-behavioral-hospital-of-indianapolis_standardcharges.csv",  # CCN 154068, already OK
]

EXT_TO_CTYPE = {
    ".csv": "text/csv",
    ".json": "application/json",
    ".xml": "application/xml",
    ".zip": "application/zip",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def hospitals_index() -> dict[str, dict]:
    out = {}
    with (OUT_DIR / "hospitals.csv").open() as f:
        for row in csv.DictReader(f):
            out[row["ccn"]] = row
    return out


def pending_index() -> dict[str, dict]:
    out = {}
    with PENDING_CSV.open() as f:
        for row in csv.DictReader(f):
            out[row["ccn"]] = row
    return out


def read_csv_rows(path: Path) -> tuple[list[str], list[dict]]:
    with path.open() as f:
        rdr = csv.DictReader(f)
        cols = rdr.fieldnames or []
        rows = list(rdr)
    return cols, rows


def write_csv_rows(path: Path, cols: list[str], rows: list[dict]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    tmp.replace(path)


def main() -> None:
    hosp = hospitals_index()
    pending = pending_index()
    now = dt.datetime.now(dt.timezone.utc).isoformat()

    # 1) Move uploaded files into <STATE>/<CCN>/.
    moves: list[tuple[str, Path, int, str, str]] = []  # (ccn, dest, size, sha, ctype)
    skipped: list[str] = []
    for ccn, fname in FILE_MAP.items():
        if ccn not in hosp:
            print(f"[skip] {ccn}: not in hospitals.csv")
            skipped.append(ccn)
            continue
        src = FILES_DIR / fname
        if not src.exists():
            print(f"[skip] {ccn}: source file missing -> {src}")
            skipped.append(ccn)
            continue
        state = hosp[ccn]["state"]
        dest_dir = FILES_DIR / state / ccn
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / fname
        if dest.exists() and dest.stat().st_size == src.stat().st_size:
            # Same file already at destination — just remove the source dup
            print(f"[dup ] {ccn}: dest already exists ({dest.name}); removing root copy")
            src.unlink()
        else:
            shutil.move(str(src), str(dest))
            print(f"[move] {ccn}: {fname} -> {dest}")
        size = dest.stat().st_size
        sha = sha256_of(dest)
        ctype = EXT_TO_CTYPE.get(dest.suffix.lower(), "application/octet-stream")
        moves.append((ccn, dest, size, sha, ctype))

    # 2) Remove flat duplicates that match an existing OK entry.
    for fname in DUPLICATES_TO_REMOVE:
        p = FILES_DIR / fname
        if p.exists():
            p.unlink()
            print(f"[del ] removed duplicate root copy: {fname}")

    # 3) Update mrf_urls.csv: replace seed_invalid rows for these CCNs.
    url_cols, url_rows = read_csv_rows(URL_CSV)
    by_ccn = {r["ccn"]: r for r in url_rows}

    for ccn, dest, size, sha, ctype in moves:
        url = (pending.get(ccn) or {}).get("mrf_url", "").strip()
        notes = (pending.get(ccn) or {}).get("notes", "").strip()
        ext = dest.suffix.lower().lstrip(".")
        new_row = {
            "ccn": ccn,
            "name": hosp[ccn]["name"],
            "state": hosp[ccn]["state"],
            "zip": hosp[ccn]["zip"],
            "website": "",
            "mrf_url": url,
            "mrf_format": ext,
            "mrf_size_bytes": size,
            "discovery_method": "seed:seed_manual_r4",
            "http_status": "200",
            "discovered_at": now,
            "notes": notes,
        }
        if ccn in by_ccn:
            by_ccn[ccn].update(new_row)
        else:
            url_rows.append(new_row)
            by_ccn[ccn] = new_row

    # Apply exemptions (override any existing seed_invalid row).
    for ccn, (method, note) in EXEMPTIONS.items():
        url = (pending.get(ccn) or {}).get("mrf_url", "").strip()
        new_row = {
            "ccn": ccn,
            "name": hosp.get(ccn, {}).get("name", ""),
            "state": hosp.get(ccn, {}).get("state", ""),
            "zip": hosp.get(ccn, {}).get("zip", ""),
            "website": "",
            "mrf_url": url,
            "mrf_format": "",
            "mrf_size_bytes": "",
            "discovery_method": method,
            "http_status": "",
            "discovered_at": now,
            "notes": note,
        }
        if ccn in by_ccn:
            by_ccn[ccn].update(new_row)
        else:
            url_rows.append(new_row)
            by_ccn[ccn] = new_row

    write_csv_rows(URL_CSV, url_cols, url_rows)
    print(f"[urls] wrote {URL_CSV} ({len(url_rows)} rows)")

    # 4) Append to downloads.csv (one row per moved file).
    dl_cols, dl_rows = read_csv_rows(DL_CSV)
    existing_ok = {r["ccn"] for r in dl_rows if r.get("status") == "ok"}
    appended = 0
    for ccn, dest, size, sha, ctype in moves:
        if ccn in existing_ok:
            # Replace the prior non-ok entry instead of duplicating.
            for r in dl_rows:
                if r["ccn"] == ccn and r.get("status") == "ok":
                    r.update({
                        "mrf_url": (pending.get(ccn) or {}).get("mrf_url", ""),
                        "local_path": str(dest),
                        "bytes_downloaded": str(size),
                        "sha256": sha,
                        "content_type": ctype,
                        "downloaded_at": now,
                        "error": "",
                    })
                    break
            continue
        dl_rows.append({
            "ccn": ccn,
            "state": hosp[ccn]["state"],
            "mrf_url": (pending.get(ccn) or {}).get("mrf_url", ""),
            "local_path": str(dest),
            "status": "ok",
            "bytes_downloaded": str(size),
            "sha256": sha,
            "content_type": ctype,
            "downloaded_at": now,
            "error": "",
        })
        appended += 1
    write_csv_rows(DL_CSV, dl_cols, dl_rows)
    print(f"[dl  ] wrote {DL_CSV} (+{appended} new ok rows)")

    print()
    print(f"summary: moved {len(moves)} files, exempted {len(EXEMPTIONS)} hospitals, "
          f"skipped {len(skipped)}")
    if skipped:
        print(f"  skipped CCNs: {skipped}")


if __name__ == "__main__":
    main()
