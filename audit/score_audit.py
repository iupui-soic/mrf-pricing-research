#!/usr/bin/env python3
"""
score_audit.py
==============
Reads the human-labeled `audit_sample_200.csv` and computes precision-by-
field. Writes `audit_results.md` with overall numbers + a per-field +
per-code_type breakdown, plus a residual-error qualitative summary table
suitable for paste-into the paper §Methods.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
SAMPLE = HERE / "audit_sample_200.csv"
OUT = HERE / "audit_results.md"

FIELDS = [
    "audit_correct_code_type",
    "audit_correct_procedure_code",
    "audit_correct_charge",
    "audit_correct_setting",
]


def precision(series: pd.Series) -> tuple[int, int, int, float | None]:
    """Return (n_y, n_n, n_unlabeled_or_unsure, precision)."""
    s = series.fillna("").astype(str).str.strip().str.upper()
    n_y = (s == "Y").sum()
    n_n = (s == "N").sum()
    n_other = len(s) - n_y - n_n  # blank / ? / typo
    p = n_y / (n_y + n_n) if (n_y + n_n) > 0 else None
    return n_y, n_n, n_other, p


def main() -> None:
    if not SAMPLE.exists():
        raise SystemExit(f"missing {SAMPLE}; run build_audit_sample.py first")

    df = pd.read_csv(SAMPLE, dtype=str)
    print(f"[load] {len(df)} rows")

    lines = ["# CA Chargemaster Ingest — 200-row Audit Results", ""]
    lines.append(f"- Sample: {len(df)} rows, seed in `build_audit_sample.py`")
    lines.append("")
    lines.append("## Overall precision by field")
    lines.append("")
    lines.append("| Field | Correct (Y) | Wrong (N) | Unlabeled | Precision |")
    lines.append("|---|---:|---:|---:|---:|")
    overall = {}
    for f in FIELDS:
        y, n, u, p = precision(df[f])
        overall[f] = p
        ps = f"{p:.3f}" if p is not None else "—"
        lines.append(f"| `{f}` | {y} | {n} | {u} | {ps} |")
    lines.append("")

    lines.append("## Precision by code_type")
    lines.append("")
    types = sorted(df["code_type"].dropna().unique())
    header = "| code_type | n | " + " | ".join(f.replace("audit_correct_", "")
                                                for f in FIELDS) + " |"
    sep = "|---|" + "|".join("---:" for _ in range(len(FIELDS) + 1)) + "|"
    lines.append(header)
    lines.append(sep)
    for ct in types:
        sub = df[df["code_type"] == ct]
        cells = [f"**{ct}**", str(len(sub))]
        for f in FIELDS:
            _, _, _, p = precision(sub[f])
            cells.append(f"{p:.2f}" if p is not None else "—")
        lines.append("| " + " | ".join(cells) + " |")
    lines.append("")

    lines.append("## Residual errors (qualitative)")
    lines.append("")
    bad = df[df[FIELDS].apply(
        lambda r: r.fillna("").astype(str).str.upper().isin(["N"]).any(),
        axis=1)]
    if bad.empty:
        lines.append("None — all rows passed.")
    else:
        lines.append(f"{len(bad)} rows with ≥1 'N' label.")
        lines.append("")
        lines.append("| row_id | code_type | procedure_code | charge_raw "
                     "| failing_field(s) | notes |")
        lines.append("|---|---|---|---|---|---|")
        for _, r in bad.iterrows():
            failing = []
            for f in FIELDS:
                if str(r.get(f, "")).strip().upper() == "N":
                    failing.append(f.replace("audit_correct_", ""))
            lines.append(
                f"| {r['row_id']} | {r['code_type']} | "
                f"{r['procedure_code']} | {r['charge_raw']} | "
                f"{', '.join(failing)} | {r.get('audit_notes', '')} |")

    OUT.write_text("\n".join(lines) + "\n")
    print(f"[out] {OUT}")


if __name__ == "__main__":
    main()
