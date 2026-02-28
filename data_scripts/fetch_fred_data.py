"""
FRED Data Fetcher for Fed Rate Prediction Model

Pulls macroeconomic indicators from the Federal Reserve Economic Data (FRED) API.
Outputs a consolidated dataset with features aligned to FOMC meeting dates.

Requires: pip install fredapi pandas
Get a free API key at: https://fred.stlouisfed.org/docs/api/api_key.html
"""

import os
from datetime import datetime
from fredapi import Fred
import pandas as pd

# ============================================================================
# CONFIGURATION
# ============================================================================

# Set your FRED API key here or as environment variable FRED_API_KEY
FRED_API_KEY = os.environ.get("FRED_API_KEY", "YOUR_API_KEY_HERE")

# Date range for data pull
START_DATE = "1990-01-01"
END_DATE = datetime.now().strftime("%Y-%m-%d")

# Output file
OUTPUT_FILE = "fred_macro_data.csv"

# ============================================================================
# FRED SERIES DEFINITIONS
# ============================================================================

# Key economic indicators relevant to Fed rate decisions
FRED_SERIES = {
    # Target Variable - Fed Funds Rate
    "DFEDTARU": "fed_funds_upper",      # Fed Funds Target Upper (post-2008)
    "DFEDTARL": "fed_funds_lower",      # Fed Funds Target Lower (post-2008)
    "DFEDTAR": "fed_funds_target",      # Fed Funds Target (pre-2008)
    "FEDFUNDS": "fed_funds_effective",  # Effective Fed Funds Rate

    # Inflation Indicators
    "CPIAUCSL": "cpi",                  # Consumer Price Index
    "PCEPILFE": "core_pce",             # Core PCE (Fed's preferred measure)
    "CPILFESL": "core_cpi",             # Core CPI (ex food & energy)
    "T5YIE": "breakeven_5y",            # 5-Year Breakeven Inflation
    "T10YIE": "breakeven_10y",          # 10-Year Breakeven Inflation

    # Employment Indicators
    "UNRATE": "unemployment_rate",       # Unemployment Rate
    "PAYEMS": "nonfarm_payrolls",       # Total Nonfarm Payrolls
    "ICSA": "initial_claims",           # Initial Jobless Claims
    "CIVPART": "labor_force_part",      # Labor Force Participation Rate
    "U6RATE": "u6_unemployment",        # U-6 Unemployment Rate

    # GDP & Output
    "GDPC1": "real_gdp",                # Real GDP
    "INDPRO": "industrial_production",  # Industrial Production Index
    "CAPACITY": "capacity_utilization", # Capacity Utilization

    # Consumer & Retail
    "RSAFS": "retail_sales",            # Advance Retail Sales
    "UMCSENT": "michigan_sentiment",    # Michigan Consumer Sentiment
    "PCE": "personal_consumption",      # Personal Consumption Expenditures

    # Treasury Yields & Spreads
    "DGS1": "treasury_1y",              # 1-Year Treasury
    "DGS2": "treasury_2y",              # 2-Year Treasury
    "DGS5": "treasury_5y",              # 5-Year Treasury
    "DGS10": "treasury_10y",            # 10-Year Treasury
    "DGS30": "treasury_30y",            # 30-Year Treasury
    "T10Y2Y": "yield_spread_10y2y",     # 10Y-2Y Spread (yield curve)
    "T10Y3M": "yield_spread_10y3m",     # 10Y-3M Spread (yield curve)

    # Credit Spreads & Risk
    "BAA10Y": "baa_spread",             # BAA Corporate Bond Spread
    "TEDRATE": "ted_spread",            # TED Spread (LIBOR - T-Bill)

    # Housing
    "HOUST": "housing_starts",          # Housing Starts
    "CSUSHPINSA": "case_shiller_index", # Case-Shiller Home Price Index

    # Money Supply
    "M2SL": "m2_money_supply",          # M2 Money Supply
}

# ============================================================================
# FOMC MEETING DATES (for alignment)
# ============================================================================

# Historical FOMC meeting dates (can be extended or pulled dynamically)
# These are announcement dates - typically the second day of 2-day meetings
FOMC_DATES_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"


def fetch_fred_data(api_key: str, series_dict: dict, start: str, end: str) -> pd.DataFrame:
    """
    Fetch multiple series from FRED and combine into a single DataFrame.
    """
    fred = Fred(api_key=api_key)

    dataframes = []
    failed_series = []

    for series_id, column_name in series_dict.items():
        try:
            print(f"Fetching {series_id} -> {column_name}...")
            data = fred.get_series(series_id, observation_start=start, observation_end=end)
            df = data.to_frame(name=column_name)
            dataframes.append(df)
        except Exception as e:
            print(f"  WARNING: Failed to fetch {series_id}: {e}")
            failed_series.append(series_id)

    if not dataframes:
        raise ValueError("No data was successfully fetched")

    # Combine all series
    combined = pd.concat(dataframes, axis=1)
    combined.index.name = "date"

    print(f"\nFetched {len(dataframes)} series successfully")
    if failed_series:
        print(f"Failed series: {failed_series}")

    return combined


def compute_rate_changes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute Fed rate decision labels: RAISE, LOWER, MAINTAIN
    Uses the effective fed funds rate to detect changes.
    """
    df = df.copy()

    # Compute daily change in effective fed funds rate
    df["fed_funds_change"] = df["fed_funds_effective"].diff()

    # Threshold for significant change (in basis points)
    threshold = 0.10  # 10 basis points

    # Label rate decisions
    def classify_change(change):
        if pd.isna(change):
            return None
        elif change > threshold:
            return "RAISE"
        elif change < -threshold:
            return "LOWER"
        else:
            return "MAINTAIN"

    df["rate_decision"] = df["fed_funds_change"].apply(classify_change)

    return df


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute derived features useful for prediction:
    - Month-over-month changes
    - Year-over-year changes
    - Rolling averages
    """
    df = df.copy()

    # Columns to compute changes for
    pct_change_cols = [
        "cpi", "core_pce", "core_cpi", "nonfarm_payrolls",
        "retail_sales", "industrial_production", "real_gdp",
        "housing_starts", "m2_money_supply"
    ]

    for col in pct_change_cols:
        if col in df.columns:
            # Month-over-month percent change
            df[f"{col}_mom"] = df[col].pct_change(periods=1) * 100
            # Year-over-year percent change
            df[f"{col}_yoy"] = df[col].pct_change(periods=12) * 100

    # Yield curve inversion flag
    if "yield_spread_10y2y" in df.columns:
        df["yield_curve_inverted"] = (df["yield_spread_10y2y"] < 0).astype(int)

    # Unemployment trend (3-month change)
    if "unemployment_rate" in df.columns:
        df["unemployment_3m_change"] = df["unemployment_rate"].diff(periods=3)

    return df


def resample_to_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """
    Resample daily data to monthly (end of month) for consistency.
    Takes the last available value of each month.
    """
    return df.resample("M").last()


def main():
    print("=" * 60)
    print("FRED Data Fetcher for Fed Rate Prediction")
    print("=" * 60)

    if FRED_API_KEY == "YOUR_API_KEY_HERE":
        print("\nERROR: Please set your FRED API key!")
        print("Get a free key at: https://fred.stlouisfed.org/docs/api/api_key.html")
        print("\nSet it via:")
        print("  1. Environment variable: export FRED_API_KEY='your_key'")
        print("  2. Edit this script and replace YOUR_API_KEY_HERE")
        return

    print(f"\nDate range: {START_DATE} to {END_DATE}")
    print(f"Fetching {len(FRED_SERIES)} series...\n")

    # Fetch raw data
    df = fetch_fred_data(FRED_API_KEY, FRED_SERIES, START_DATE, END_DATE)

    # Resample to monthly
    print("\nResampling to monthly frequency...")
    df_monthly = resample_to_monthly(df)

    # Compute rate change labels
    print("Computing rate decision labels...")
    df_monthly = compute_rate_changes(df_monthly)

    # Compute derived features
    print("Computing derived features...")
    df_monthly = compute_features(df_monthly)

    # Save to CSV
    df_monthly.to_csv(OUTPUT_FILE)
    print(f"\nSaved to {OUTPUT_FILE}")
    print(f"Shape: {df_monthly.shape}")
    print(f"Date range: {df_monthly.index.min()} to {df_monthly.index.max()}")

    # Summary statistics
    print("\n" + "=" * 60)
    print("RATE DECISION DISTRIBUTION")
    print("=" * 60)
    if "rate_decision" in df_monthly.columns:
        print(df_monthly["rate_decision"].value_counts(dropna=False))

    print("\n" + "=" * 60)
    print("COLUMN SUMMARY")
    print("=" * 60)
    print(f"Total columns: {len(df_monthly.columns)}")
    print("\nColumns with >50% missing data:")
    missing = (df_monthly.isnull().sum() / len(df_monthly) * 100).sort_values(ascending=False)
    print(missing[missing > 50])


if __name__ == "__main__":
    main()
