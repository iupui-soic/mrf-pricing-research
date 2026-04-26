#!/usr/bin/env python3
"""
coverage_report.py
==================
Reads `mrf_log.csv` + the two output parquets and writes a
human-readable Markdown coverage report at
`/data0/mrf/parsed/coverage_report.md`. Mirrors the shape of the CA
chargemaster ingest's `coverage_report.md` so the two corpora can be
compared at a glance in the paper §Methods.

Sections:
  1. Top-line counts (files in, parsed ok, skipped, failed; row totals)
  2. Schema-version distribution (v3.x / v2.x / v1.x / unknown)
  3. File-format distribution (json / csv_tall / csv_wide / xlsx / kaiser_legacy)
  4. Per-state hospital coverage (CA vs IN: hospitals × format × schema)
  5. Coverage of the 70 CMS shoppable services per hospital (gross side)
  6. Negotiated-rate denseness (hospitals × payers × plans counts)
  7. Failure inventory (status != ok, grouped by reason)
"""

from __future__ import annotations

from pathlib import Path
from collections import Counter

import pandas as pd

PARSED = Path("/data0/mrf/parsed")
LOG_CSV = PARSED / "mrf_log.csv"
GROSS_PARQUET = PARSED / "mrf_gross.parquet"
NEG_PARQUET = PARSED / "mrf_negotiated.parquet"
OUT = PARSED / "coverage_report.md"

# CMS-mandated 70 shoppable services (CPT/HCPCS only, abbreviated).
# Source: 45 CFR 180.40(b) — used to score per-hospital coverage of
# the CMS shoppable list.
CMS_SHOPPABLE = {
    # Anesthesia
    "00104", "00400", "00567", "00740", "00810", "00834", "00840", "00942",
    # Evaluation & Management
    "99202", "99203", "99204", "99205", "99211", "99212", "99213", "99214",
    "99215", "99281", "99282", "99283", "99284", "99285", "99291",
    # Maternity / OB
    "59400", "59409", "59410", "59425", "59426", "59430", "59510", "59514",
    "59515", "59610", "59612", "59614", "59618", "59620", "59622",
    # Imaging — CT/MRI/X-ray
    "70450", "70491", "70551", "71045", "71046", "71250", "72148", "72193",
    "73221", "73502", "73721", "74176", "74177",
    # Mammography / Ultrasound
    "76700", "76705", "76770", "76830", "76856", "77065", "77066", "77067",
    # Lab panels
    "80048", "80050", "80053", "80061", "80076", "81001", "81002", "82947",
    "83036", "84443", "85025", "86592",
    # Surgery / Procedures
    "29826", "29881", "43239", "45378", "45380", "45385", "47562", "49083",
    "55700", "62323", "64483", "66984",
    # Radiation Oncology
    "77067", "77387",
}


def main() -> None:
    if not LOG_CSV.exists():
        raise SystemExit(f"missing {LOG_CSV}")

    log = pd.read_csv(LOG_CSV, dtype=str).fillna("")
    log["n_gross"] = pd.to_numeric(log["n_gross"], errors="coerce").fillna(0).astype(int)
    log["n_negotiated"] = pd.to_numeric(log["n_negotiated"], errors="coerce").fillna(0).astype(int)

    n_files = len(log)
    n_ok = (log["status"] == "ok").sum()
    n_skip = log["status"].str.startswith("skip:").sum()
    n_fail = n_files - n_ok - n_skip
    g_total = log["n_gross"].sum()
    n_total = log["n_negotiated"].sum()

    lines = ["# MRF Parsing — Coverage Report", ""]
    lines += [
        "## 1. Top-line",
        "",
        f"- Files processed: **{n_files}**",
        f"  - parsed ok: **{n_ok}**",
        f"  - skipped (kaiser_legacy / unknown): {n_skip}",
        f"  - failed: {n_fail}",
        f"- **Gross / cash / min / max rows**: {g_total:,}",
        f"- **Negotiated (payer × plan) rows**: {n_total:,}",
        "",
    ]

    # 2. Schema-version distribution
    sv = log[log["status"] == "ok"]["schema_version"].value_counts()
    lines += ["## 2. Schema-version distribution (parsed-ok files)", ""]
    lines += ["| version | files |", "|---|---:|"]
    for v, c in sv.items():
        lines.append(f"| `{v}` | {c} |")
    lines += [""]

    # 3. File-format distribution
    fmt = log[log["status"] == "ok"]["file_format"].value_counts()
    lines += ["## 3. File-format distribution (parsed-ok files)", ""]
    lines += ["| format | files |", "|---|---:|"]
    for f, c in fmt.items():
        lines.append(f"| `{f}` | {c} |")
    lines += [""]

    # 4. By state
    by_state = log.groupby(["state", "status"]).size().unstack(fill_value=0)
    lines += ["## 4. By state", ""]
    lines += ["| state | total | ok | skip | fail |", "|---|---:|---:|---:|---:|"]
    for state, sub in log.groupby("state"):
        ok = (sub["status"] == "ok").sum()
        sk = sub["status"].str.startswith("skip:").sum()
        fa = len(sub) - ok - sk
        lines.append(f"| **{state}** | {len(sub)} | {ok} | {sk} | {fa} |")
    lines += [""]

    # 5. CMS shoppable coverage — only computable if gross parquet exists
    if GROSS_PARQUET.exists():
        try:
            g = pd.read_parquet(GROSS_PARQUET, columns=["ccn", "code", "code_type"])
            cpt_codes = g[g["code_type"].str.upper().isin(["CPT", "HCPCS"])]
            shop = cpt_codes[cpt_codes["code"].isin(CMS_SHOPPABLE)]
            per_hosp = (shop.groupby("ccn")["code"].nunique()
                            .reindex(log[log["status"] == "ok"]["ccn"], fill_value=0))
            lines += ["## 5. CMS shoppable-service coverage", ""]
            lines += [
                f"- CMS-mandated 70 shoppable services covered per hospital "
                f"(CPT/HCPCS in `mrf_gross.parquet`):",
                "",
                "| services covered | hospitals |",
                "|---|---:|",
            ]
            buckets = pd.cut(per_hosp, [-1, 0, 9, 19, 39, 70],
                             labels=["0", "1–9", "10–19", "20–39", "40+"])
            for lab, cnt in buckets.value_counts().sort_index().items():
                lines.append(f"| {lab} | {cnt} |")
            lines.append(f"| (median per hospital) | **{int(per_hosp.median())}** |")
            lines += [""]
        except Exception as e:
            lines += [f"## 5. CMS shoppable-service coverage", "",
                      f"_(skipped — {e})_", ""]

    # 6. Negotiated denseness
    if NEG_PARQUET.exists():
        try:
            n = pd.read_parquet(NEG_PARQUET, columns=["ccn", "payer_name"])
            payers_per_hosp = n.groupby("ccn")["payer_name"].nunique()
            lines += ["## 6. Negotiated-rate denseness", ""]
            lines += [
                f"- Median distinct payers per hospital: "
                f"**{int(payers_per_hosp.median())}**",
                f"- Max: **{payers_per_hosp.max()}**",
                f"- Hospitals with ≥10 distinct payers: "
                f"**{(payers_per_hosp >= 10).sum()}**",
                "",
            ]
        except Exception as e:
            lines += [f"## 6. Negotiated-rate denseness", "",
                      f"_(skipped — {e})_", ""]

    # 7. Failures
    fails = log[~log["status"].isin(["ok"]) & ~log["status"].str.startswith("skip:")]
    lines += ["## 7. Failures", ""]
    if fails.empty:
        lines += ["None.", ""]
    else:
        lines += ["| ccn | format | version | status | error |",
                  "|---|---|---|---|---|"]
        for _, r in fails.iterrows():
            lines.append(f"| {r['ccn']} | `{r['file_format']}` | `{r['schema_version']}` "
                         f"| `{r['status']}` | {r['error'][:120]} |")
        lines += [""]

    # 8. Skipped
    skips = log[log["status"].str.startswith("skip:")]
    lines += ["## 8. Skipped (non-parsable formats)", ""]
    if skips.empty:
        lines += ["None.", ""]
    else:
        lines += ["| status | count |", "|---|---:|"]
        for s, c in skips["status"].value_counts().items():
            lines.append(f"| `{s}` | {c} |")
        lines += [""]

    OUT.write_text("\n".join(lines) + "\n")
    print(f"[out] {OUT}")


if __name__ == "__main__":
    main()
