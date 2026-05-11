"""
Profile NFIP claims and policies datasets.

Loads all CSVs from datasets/claims/ and datasets/policies/, computes
summary statistics, and writes a formatted markdown report to docs/.
"""

import os
import glob
import pandas as pd

# Paths relative to the project root
DATASETS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "datasets")
CLAIMS_DIR = os.path.join(DATASETS_DIR, "claims")
POLICIES_DIR = os.path.join(DATASETS_DIR, "policies")
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "docs", "data_profile_report.md")

# Columns that hold coded values — we show their top 15 most common values
CATEGORICAL_COLUMNS = {
    "claims": ["floodZoneCurrent", "occupancyType", "state", "causeOfDamage", "ratedFloodZone",
               "primaryResidenceIndicator", "elevatedBuildingIndicator", "basementEnclosureCrawlspaceType"],
    "policies": ["floodZoneCurrent", "occupancyType", "propertyState", "ratedFloodZone",
                 "construction", "primaryResidenceIndicator", "crsClassCode",
                 "elevatedBuildingIndicator", "basementEnclosureCrawlspaceType"],
}

# Date columns get a min/max range rather than value counts
DATE_COLUMNS = {
    "claims": ["dateOfLoss"],
    "policies": ["policyEffectiveDate", "policyTerminationDate"],
}

# Claims and policies use different column names for the state field
STATE_COLUMN = {
    "claims": "state",
    "policies": "propertyState",
}


def load_csvs(directory: str) -> pd.DataFrame:
    """Load and concatenate all CSVs from a directory."""
    # glob finds all matching files; sorted ensures consistent ordering
    csv_files = sorted(glob.glob(os.path.join(directory, "*.csv")))
    if not csv_files:
        return pd.DataFrame()

    dfs = []
    for f in csv_files:
        # low_memory=False avoids dtype inference warnings on large files
        df = pd.read_csv(f, low_memory=False)
        dfs.append(df)
        print(f"  Loaded {os.path.basename(f)}: {len(df):,} rows")

    # Stack all state files into one DataFrame with a fresh index
    return pd.concat(dfs, ignore_index=True)


def profile_dataset(df: pd.DataFrame, name: str) -> list[str]:
    """Generate profile report lines for a dataset."""
    lines = []
    lines.append(f"## {name.title()} Dataset")
    lines.append("")

    state_col = STATE_COLUMN.get(name)
    lines.append(f"**Total rows:** {len(df):,}")
    lines.append("")

    # Row counts per state — quick sanity check that all five states loaded
    if state_col and state_col in df.columns:
        lines.append("### Row Count per State")
        lines.append("")
        lines.append("| State | Rows |")
        lines.append("|-------|------|")
        for state, count in df[state_col].value_counts().sort_index().items():
            lines.append(f"| {state} | {count:,} |")
        lines.append("")

    # Column dtypes — helps spot columns that loaded as object instead of numeric
    lines.append("### Column Types")
    lines.append("")
    lines.append("| Column | Dtype |")
    lines.append("|--------|-------|")
    for col in df.columns:
        lines.append(f"| {col} | {df[col].dtype} |")
    lines.append("")

    # Null analysis — high null rates flag incomplete API fields
    lines.append("### Null Analysis")
    lines.append("")
    lines.append("| Column | Null Count | Null % |")
    lines.append("|--------|-----------|--------|")
    for col in df.columns:
        null_count = df[col].isna().sum()
        null_pct = null_count / len(df) * 100 if len(df) > 0 else 0
        lines.append(f"| {col} | {null_count:,} | {null_pct:.1f}% |")
    lines.append("")

    # Basic stats for numeric columns — min/max/mean/median
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    if numeric_cols:
        lines.append("### Numeric Column Statistics")
        lines.append("")
        lines.append("| Column | Min | Max | Mean | Median |")
        lines.append("|--------|-----|-----|------|--------|")
        for col in numeric_cols:
            col_data = df[col].dropna()
            if len(col_data) == 0:
                lines.append(f"| {col} | — | — | — | — |")
            else:
                lines.append(
                    f"| {col} | {col_data.min():,.2f} | {col_data.max():,.2f} "
                    f"| {col_data.mean():,.2f} | {col_data.median():,.2f} |"
                )
        lines.append("")

    # Top 15 values for coded/categorical columns — surfaces unexpected codes
    cat_cols = CATEGORICAL_COLUMNS.get(name, [])
    present_cat_cols = [c for c in cat_cols if c in df.columns]
    if present_cat_cols:
        lines.append("### Categorical Column Value Counts (Top 15)")
        lines.append("")
        for col in present_cat_cols:
            lines.append(f"#### {col}")
            lines.append("")
            lines.append("| Value | Count |")
            lines.append("|-------|-------|")
            for val, count in df[col].value_counts().head(15).items():
                lines.append(f"| {val} | {count:,} |")
            lines.append("")

    # Date ranges — confirms data spans the expected historical period
    date_cols = DATE_COLUMNS.get(name, [])
    present_date_cols = [c for c in date_cols if c in df.columns]
    if present_date_cols:
        lines.append("### Date Ranges")
        lines.append("")
        lines.append("| Column | Min | Max |")
        lines.append("|--------|-----|-----|")
        for col in present_date_cols:
            # errors="coerce" turns unparseable values into NaT rather than crashing
            parsed = pd.to_datetime(df[col], errors="coerce").dropna()
            if len(parsed) > 0:
                lines.append(f"| {col} | {parsed.min().date()} | {parsed.max().date()} |")
            else:
                lines.append(f"| {col} | — | — |")
        lines.append("")

    return lines


def main():
    report_lines = ["# NFIP Data Profile Report", ""]

    print("Loading claims data...")
    claims_df = load_csvs(CLAIMS_DIR)
    print("Loading policies data...")
    policies_df = load_csvs(POLICIES_DIR)

    if claims_df.empty and policies_df.empty:
        print("No data files found. Run the ingestion scripts first.")
        return

    if not claims_df.empty:
        print(f"\nProfiling claims: {len(claims_df):,} rows")
        report_lines.extend(profile_dataset(claims_df, "claims"))

    if not policies_df.empty:
        print(f"\nProfiling policies: {len(policies_df):,} rows")
        report_lines.extend(profile_dataset(policies_df, "policies"))

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        f.write("\n".join(report_lines))

    print(f"\nReport saved to {OUTPUT_PATH}")

    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    if not claims_df.empty:
        print(f"  Claims:   {len(claims_df):>10,} rows")
    if not policies_df.empty:
        print(f"  Policies: {len(policies_df):>10,} rows")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
