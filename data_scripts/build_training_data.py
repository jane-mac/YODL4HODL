"""
Build FOMC Model Training Dataset

Creates a flat CSV where each row is one FOMC meeting (Y variable from
fomc_decisions.csv) with features from the 3 months leading up to each
decision.

Monthly data sources (fed_futures.csv, fred_macro_data.csv, market_data.csv)
and daily data sources (fed_funds_target_daily.csv, market_data_daily.csv,
fed_futures_daily.csv) are each represented with three columns per variable:
  {variable}_{n}_months_prior  for n in [3, 2, 1]

Daily sources are aggregated to monthly (mean, median, std per month) before
the look-back columns are constructed. Each daily variable produces three
columns per stat, e.g.:
  fftd_target_rate_mean_1_months_prior
  fftd_target_rate_median_1_months_prior
  fftd_target_rate_std_1_months_prior

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
ff_monthly   = pd.read_csv(RAW / "fed_futures.csv",     parse_dates=["date"])
macro        = pd.read_csv(RAW / "fred_macro_data.csv", parse_dates=["date"])
mkt_monthly  = pd.read_csv(RAW / "market_data.csv",     parse_dates=["date"])

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


def daily_to_monthly(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """
    Aggregate a daily file to monthly using mean, median, and std for each
    numeric column. Returns a lookup table keyed by Period('M') with columns
    named {prefix}{original_col}_mean, ..._median, ..._std.
    """
    df = df.copy()
    df = df.sort_values("date")
    df["year_month"] = df["date"].dt.to_period("M")
    numeric_cols = df.select_dtypes(include="number").columns.tolist()

    grp = df.groupby("year_month")[numeric_cols]
    mean   = grp.mean()  .rename(columns={c: f"{prefix}{c}_mean"   for c in numeric_cols})
    median = grp.median().rename(columns={c: f"{prefix}{c}_median" for c in numeric_cols})
    std    = grp.std()   .rename(columns={c: f"{prefix}{c}_std"    for c in numeric_cols})

    return pd.concat([mean, median, std], axis=1)


# ---------------------------------------------------------------------------
# Build per-source monthly lookup tables
# ---------------------------------------------------------------------------
print("Building monthly lookup tables...")

lookups = {
    "fftd": daily_to_monthly(fftd, "fftd_"),
    "ff":   to_monthly(ff_monthly, "ff_"),
    "macro":to_monthly(macro,      "macro_"),
    "mkt":  to_monthly(mkt_monthly,"mkt_"),
    "mktd": daily_to_monthly(mktd, "mktd_"),
    "ffd":  daily_to_monthly(ffd,  "ffd_"),
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
        "date":           meeting_date,
        "rate_before":    fomc_row["rate_before"],
        "rate_after":     fomc_row["rate_after"],
        "rate_change_bps":fomc_row["rate_change_bps"],
        "decision":       fomc_row["decision"],
    }

    for months_back in [3, 2, 1]:
        prior_period = meeting_period - months_back
        suffix = f"_{months_back}_months_prior"

        for ds_df in lookups.values():
            if prior_period in ds_df.index:
                for col, val in ds_df.loc[prior_period].items():
                    record[f"{col}{suffix}"] = val
            else:
                for col in ds_df.columns:
                    record[f"{col}{suffix}"] = np.nan

    records.append(record)

# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------
result = pd.DataFrame(records)
output_path = RAW / "training_data.csv"
result.to_csv(output_path, index=False)

print(f"\nDone.")
print(f"  Rows    : {len(result)}")
print(f"  Columns : {len(result.columns)}")
print(f"  Output  : {output_path}")
