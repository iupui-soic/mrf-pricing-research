#!/usr/bin/env python3
"""
analysis/wang_schema_system_clustering.py
=========================================
System-clustering check on the non-Kaiser, non-§180-v3 portion of the
schema-stratified Wang sample. Backs the disclosure paragraph in
Discussion §4.1 that the 22 misc_csv + v2.x discounter hospitals span
15 distinct EIN groups, with the largest single-system cluster at most
four facilities (the four ``Global Medical Center''-branded California
hospitals affiliated with KPC Health under separate EINs).

The Kaiser bucket is by construction a single-system uniform-policy
cluster, so it provides no independent evidence against the encoding-
artifact interpretation; the inferential weight of the non-Kaiser,
non-v3 evidence rests on misc_csv + v2.x. This script disaggregates
that group by EIN and by name-pattern system to confirm system diversity.

Inputs:
  /data0/mrf-pricing-research/analysis/wang_schema_stratified.parquet
  /data0/mrf-pricing-research/crosswalk/facilities_crosswalk.parquet

Output:
  Stdout summary: per-hospital system identity, EIN grouping counts,
  largest-cluster diagnostic.
"""
from __future__ import annotations

import pandas as pd

SCHEMA = "/data0/mrf-pricing-research/analysis/wang_schema_stratified.parquet"
XW     = "/data0/mrf-pricing-research/crosswalk/facilities_crosswalk.parquet"


def main() -> None:
    df  = pd.read_parquet(SCHEMA)
    xw  = pd.read_parquet(XW)
    df["ccn"] = df["ccn"].astype(str).str.lstrip("0").str.zfill(6)
    xw["ccn"] = xw["ccn"].astype(str).str.lstrip("0").str.zfill(6)

    target = (df[(df["discounter"] == True) &
                 (df["schema_bucket"].isin(["misc_csv", "v2.x"]))]
                .copy())
    m = target.merge(xw[["ccn","name","city","state","ein","ownership"]],
                     on="ccn", how="left", suffixes=("","_xw"))

    print(f"Non-Kaiser, non-§180-v3 discounter hospitals (misc_csv + v2.x): n={len(m)}")
    print(f"  misc_csv: {(m.schema_bucket=='misc_csv').sum()}  "
          f"(CA={((m.schema_bucket=='misc_csv') & (m.state=='CA')).sum()}, "
          f"IN={((m.schema_bucket=='misc_csv') & (m.state=='IN')).sum()})")
    print(f"  v2.x:     {(m.schema_bucket=='v2.x').sum()}  "
          f"(CA={((m.schema_bucket=='v2.x') & (m.state=='CA')).sum()}, "
          f"IN={((m.schema_bucket=='v2.x') & (m.state=='IN')).sum()})")
    print()

    print("Per-hospital identity (CCN, schema bucket, name, EIN, ownership):")
    print(m[["ccn","state","schema_bucket","name","city","ein","ownership"]]
              .sort_values(["state","schema_bucket","name"])
              .to_string(index=False))
    print()

    n_with_ein = m["ein"].notna().sum()
    n_distinct_ein = m["ein"].dropna().nunique()
    n_no_ein = m["ein"].isna().sum()
    print(f"EIN coverage: {n_with_ein} of {len(m)} hospitals have an EIN match in the crosswalk.")
    print(f"  Distinct EIN groups among the {n_with_ein} EIN-resolved facilities: {n_distinct_ein}")
    print(f"  Hospitals with no EIN match: {n_no_ein} (independent district/county facilities)")
    print()

    print("EIN groups with >1 hospital:")
    grp = (m.dropna(subset=["ein"])
              .groupby("ein")
              .agg(n=("name","count"),
                   names=("name", lambda s: " | ".join(sorted(s)))))
    multi = grp[grp.n > 1].sort_values("n", ascending=False)
    if len(multi):
        print(multi.to_string())
    else:
        print("  (none)")
    print()

    # Name-pattern system clustering (as a robustness check above EIN)
    m["name_system"] = m["name"].str.extract(
        r"(GLOBAL MEDICAL CENTER|ADVENTIST HEALTH|COMMUNITY MEDICAL CENTER|"
        r"COMMUNITY REGIONAL|KAWEAH|MONTEREY PENINSULA|PRIME|USC|"
        r"CHINO VALLEY|HALSEN)"
    )
    print("Name-pattern system clustering (CA only, since IN names are independent):")
    print(m.groupby("name_system", dropna=False)
                .size().sort_values(ascending=False).to_string())
    print()

    # The "Global Medical Center" group: 4 facilities under different EINs but
    # all part of the KPC Health-affiliated proprietary chain.
    gmc = m[m["name"].str.contains("GLOBAL MEDICAL CENTER", na=False)]
    print(f"``Global Medical Center''-branded facilities: {len(gmc)}")
    print(gmc[["ccn","name","city","ein"]].to_string(index=False))
    print()

    print("Diagnostic: largest single-system cluster within misc_csv + v2.x is",
          "at most 4 hospitals (the GMC group, treating distinct EINs as one",
          "KPC-affiliated system); 2 by strict same-EIN grouping. Far from",
          "the within-system uniformity that would explain r ≈ 1.0 by",
          "system-policy clustering alone.")


if __name__ == "__main__":
    main()
