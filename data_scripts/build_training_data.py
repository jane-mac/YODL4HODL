"""
Build FOMC Model Training Dataset

Creates a flat CSV where each row is one FOMC meeting (Y variable from
fomc_decisions.csv) with features from the 3 months leading up to each
decision.

Monthly sources (fed_futures.csv, fred_macro_data.csv, market_data.csv)
produce three columns per variable, one per month in the look-back window:
  {variable}_3_months_prior
  {variable}_2_months_prior
  {variable}_1_month_prior

Daily sources (fed_funds_target_daily.csv, market_data_daily.csv,
fed_futures_daily.csv) produce std, min, and max computed over all daily
observations in the 3 calendar months preceding the meeting month:
  {variable}_std
  {variable}_min
  {variable}_max

Column prefixes by source:
  fftd_  → fed_funds_target_daily
  ff_    → fed_futures (monthly)
  macro_ → fred_macro_data
  mkt_   → market_data (monthly)
  mktd_  → market_data_daily
  ffd_   → fed_futures_daily

Output: raw_data/training_data.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "raw_data"

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
print("Loading data...")

fomc = pd.read_csv(RAW / "fomc_decisions.csv", parse_dates=["date"])

# Monthly sources
ff_monthly  = pd.read_csv(RAW / "fed_futures.csv",     parse_dates=["date"])
macro       = pd.read_csv(RAW / "fred_macro_data.csv", parse_dates=["date"])
mkt_monthly = pd.read_csv(RAW / "market_data.csv",     parse_dates=["date"])

# Daily sources
fftd = pd.read_csv(RAW / "fed_funds_target_daily.csv", parse_dates=["date"])
mktd = pd.read_csv(RAW / "market_data_daily.csv",      parse_dates=["date"])
ffd  = pd.read_csv(RAW / "fed_futures_daily.csv",      parse_dates=["date"])

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def prefix_columns(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """Rename all non-index columns with a source prefix."""
    return df.rename(columns={c: f"{prefix}{c}" for c in df.columns if c != "year_month"})


def to_monthly(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """
    Convert a monthly-granularity file (end-of-month dates) to a lookup
    table keyed by pandas Period('M').
    """
    df = df.copy()
    df["year_month"] = df["date"].dt.to_period("M")
    df = df.drop(columns=["date"])
    return prefix_columns(df, prefix).set_index("year_month")


def daily_window_stats(df: pd.DataFrame, prefix: str, window_start: pd.Timestamp, window_end: pd.Timestamp) -> dict:
    """
    Compute std, min, and max for all numeric columns in a daily DataFrame
    over the rows that fall within [window_start, window_end).
    Returns a flat dict with keys {prefix}{col}_std / _min / _max.
    """
    mask = (df["date"] >= window_start) & (df["date"] < window_end)
    window = df.loc[mask, df.select_dtypes(include="number").columns]

    result = {}
    for col in window.columns:
        result[f"{prefix}{col}_std"] = window[col].std()
        result[f"{prefix}{col}_min"] = window[col].min()
        result[f"{prefix}{col}_max"] = window[col].max()
    return result


# ---------------------------------------------------------------------------
# Build monthly lookup tables (for monthly sources)
# ---------------------------------------------------------------------------
print("Building monthly lookup tables...")

monthly_lookups = {
    "ff":   to_monthly(ff_monthly, "ff_"),
    "macro":to_monthly(macro,      "macro_"),
    "mkt":  to_monthly(mkt_monthly,"mkt_"),
}

# Pre-sort daily sources for fast slicing
daily_sources = {
    "fftd": fftd.sort_values("date").reset_index(drop=True),
    "mktd": mktd.sort_values("date").reset_index(drop=True),
    "ffd":  ffd.sort_values("date").reset_index(drop=True),
}

daily_prefixes = {
    "fftd": "fftd_",
    "mktd": "mktd_",
    "ffd":  "ffd_",
}

# ---------------------------------------------------------------------------
# Build training rows
# ---------------------------------------------------------------------------
print("Building training rows...")

records = []

for _, fomc_row in fomc.iterrows():
    meeting_date   = fomc_row["date"]
    meeting_period = meeting_date.to_period("M")

    record = {
        "date":        meeting_date,
        "rate_before": fomc_row["rate_before"],
        "decision":    fomc_row["decision"],
    }

    # Monthly sources: one value per variable per look-back month
    for months_back in [3, 2, 1]:
        prior_period = meeting_period - months_back
        suffix = f"_{months_back}_months_prior"

        for ds_df in monthly_lookups.values():
            if prior_period in ds_df.index:
                for col, val in ds_df.loc[prior_period].items():
                    record[f"{col}{suffix}"] = val
            else:
                for col in ds_df.columns:
                    record[f"{col}{suffix}"] = np.nan

    # Daily sources: std, min, max over the full 3-month window
    window_start = (meeting_period - 3).to_timestamp(how="start")
    window_end   = meeting_period.to_timestamp(how="start")

    for key, df in daily_sources.items():
        prefix = daily_prefixes[key]
        record.update(daily_window_stats(df, prefix, window_start, window_end))

    records.append(record)

# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------
result = pd.DataFrame(records)

# Drop columns that are entirely empty or missing data for >50% of decisions
threshold = len(result) * 0.5
sparse_cols = [col for col in result.columns if result[col].isna().sum() > threshold]
if sparse_cols:
    print(f"\nDropping {len(sparse_cols)} columns with data for <50% of decisions.")
    result = result.drop(columns=sparse_cols)

output_path = RAW / "training_data.csv"
result.to_csv(output_path, index=False)

print(f"\nDone.")
print(f"  Rows    : {len(result)}")
print(f"  Columns : {len(result.columns)}")
print(f"  Output  : {output_path}")
