# 200-row precision audit — CA chargemaster ingest

Supports PROJECT_PLAN.md §6 Week 1 deliverable: hand-labeled precision
sample for the paper §Methods.

## Workflow

```bash
# 1. Generate the sample (deterministic; seed=20260426)
.venv/bin/python audit/build_audit_sample.py
# -> audit/audit_sample_200.csv  (200 rows × 19 cols)

# 2. HUMAN STEP: open audit_sample_200.csv in Excel/Numbers/Google Sheets
#    and fill in the four `audit_correct_*` columns plus `audit_notes`.
#    Reference the original CDM file at <file_source> when in doubt.
#    Save back as audit_sample_200.csv (do not rename).

# 3. Score
.venv/bin/python audit/score_audit.py
# -> audit/audit_results.md
```

## Sample composition (stratified random, seed=20260426)

| code_type | n |
|---|---:|
| HCPCS | 75 |
| REVCODE | 80 |
| CPT | 23 |
| NDC | 12 |
| DRG | 10 |
| **Total** | **200** |

## Labeling rubric

For each row the auditor compares the parsed values to the raw CDM file
(linked via `file_source`, `sheet_name`, `header_row`). Mark each field
**Y** (correct), **N** (incorrect), or leave blank if unsure.

| Field | What "correct" means |
|---|---|
| `audit_correct_code_type` | The labeled code_type (CPT/HCPCS/REVCODE/DRG/NDC) is what `procedure_code` actually represents. Common error: a 5-digit number labeled CPT that is actually a HCPCS or revenue code. |
| `audit_correct_procedure_code` | `procedure_code` is the verbatim code from the CDM line, normalized (uppercase, alphanumeric only). Common error: stray punctuation or leading zeros stripped that shouldn't be. |
| `audit_correct_charge` | `charge` is the dollar charge for that line, not a fee schedule, percent change, or unit count. Cross-check by re-opening the CDM file at `header_row` to confirm the column under `charge_column` is a real charge. |
| `audit_correct_setting` | `setting` (IP/OP/ER/OR/SB/ALL) reflects the column the value came from. Common error: "PRICE" columns labeled ALL when the file is in fact OP-only. |

`audit_notes` — free text. Useful for documenting recurring failure
modes (e.g., "Common-25 file misread as a CDM").

## Reporting target for §Methods

`score_audit.py` emits per-field precision and per-code_type precision.
Plan to report:

- Overall precision (per field, weighted): expect ≥0.95 on `procedure_code`
  and `charge`; ≥0.90 on `code_type` and `setting`.
- Per-code_type precision table.
- Qualitative residual-error inventory (what failed, why) — paste into
  paper §Methods.

If precision falls below the targets above, log the specific failure
modes; some are fixable in `ingest_hcai.py` (header-detection edge cases,
charge-column heuristics) and worth a v2 ingest pass before paper
submission.
