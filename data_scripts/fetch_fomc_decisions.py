"""
FOMC Decision Fetcher

Pulls historical FOMC meeting dates and rate decisions from FRED.
Creates the target variable dataset for Fed rate prediction.

The Fed Funds Target Rate changed structure in Dec 2008:
- Pre-2008: Single target rate (DFEDTAR)
- Post-2008: Target range with upper/lower bounds (DFEDTARU/DFEDTARL)
"""

import os
from datetime import datetime
from fredapi import Fred
import pandas as pd
import numpy as np

# ============================================================================
# CONFIGURATION
# ============================================================================

FRED_API_KEY = os.environ.get("FRED_API_KEY", "YOUR_API_KEY_HERE")
START_DATE = "1990-01-01"
END_DATE = datetime.now().strftime("%Y-%m-%d")
OUTPUT_FILE = "fomc_decisions.csv"

# Rate change threshold in percentage points
RATE_CHANGE_THRESHOLD = 0.10  # 10 basis points


def fetch_target_rates(api_key: str, start: str, end: str) -> pd.DataFrame:
    """
    Fetch Fed Funds Target Rate history from FRED.
    Combines pre-2008 (single target) and post-2008 (range) data.
    """
    fred = Fred(api_key=api_key)

    print("Fetching Fed Funds Target Rate history...")

    # Pre-2008: Single target rate
    try:
        dfedtar = fred.get_series("DFEDTAR", observation_start=start, observation_end=end)
        dfedtar = dfedtar.to_frame(name="target_rate")
        print(f"  DFEDTAR (pre-2008): {len(dfedtar)} observations")
    except Exception as e:
        print(f"  Warning: Could not fetch DFEDTAR: {e}")
        dfedtar = pd.DataFrame()

    # Post-2008: Target range (upper bound)
    try:
        dfedtaru = fred.get_series("DFEDTARU", observation_start=start, observation_end=end)
        dfedtaru = dfedtaru.to_frame(name="target_upper")
        print(f"  DFEDTARU (post-2008 upper): {len(dfedtaru)} observations")
    except Exception as e:
        print(f"  Warning: Could not fetch DFEDTARU: {e}")
        dfedtaru = pd.DataFrame()

    # Post-2008: Target range (lower bound)
    try:
        dfedtarl = fred.get_series("DFEDTARL", observation_start=start, observation_end=end)
        dfedtarl = dfedtarl.to_frame(name="target_lower")
        print(f"  DFEDTARL (post-2008 lower): {len(dfedtarl)} observations")
    except Exception as e:
        print(f"  Warning: Could not fetch DFEDTARL: {e}")
        dfedtarl = pd.DataFrame()

    # Combine into single dataframe
    df = pd.concat([dfedtar, dfedtaru, dfedtarl], axis=1)
    df.index.name = "date"

    # Create unified target rate column
    # Use midpoint of range for post-2008 period
    df["target_rate_unified"] = df["target_rate"].fillna(
        (df["target_upper"] + df["target_lower"]) / 2
    )

    return df


def identify_rate_decisions(df: pd.DataFrame, threshold: float = 0.10) -> pd.DataFrame:
    """
    Identify FOMC rate decisions from target rate changes.

    Returns DataFrame with only dates where rate changed or was explicitly held.
    """
    df = df.copy()

    # Calculate change from previous value
    df["rate_change_bps"] = df["target_rate_unified"].diff() * 100  # in basis points

    # Classify the decision
    def classify(change_bps):
        if pd.isna(change_bps):
            return None
        elif change_bps > threshold * 100:
            return "RAISE"
        elif change_bps < -threshold * 100:
            return "LOWER"
        else:
            return "MAINTAIN"

    df["decision"] = df["rate_change_bps"].apply(classify)

    # Filter to only rows where target rate changed (FOMC decision dates)
    # Note: FRED updates the target rate on the day of the FOMC announcement
    rate_changes = df[df["target_rate_unified"].diff().abs() > 0.001].copy()

    # Add MAINTAIN decisions - these are trickier since the rate stays the same
    # For now, we just capture actual changes

    return df, rate_changes


def create_fomc_decision_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a clean dataset of FOMC decisions suitable for ML.
    """
    # Get rows where rate changed
    changes = df[df["target_rate_unified"].diff().abs() > 0.001].copy()

    if len(changes) == 0:
        print("Warning: No rate changes detected")
        return pd.DataFrame()

    # Calculate decision metrics
    changes["rate_change_bps"] = changes["target_rate_unified"].diff() * 100
    changes["prev_rate"] = changes["target_rate_unified"].shift(1)
    changes["new_rate"] = changes["target_rate_unified"]

    # Classify
    changes["decision"] = changes["rate_change_bps"].apply(
        lambda x: "RAISE" if x > 10 else ("LOWER" if x < -10 else "MAINTAIN")
    )

    # Calculate magnitude
    changes["magnitude_bps"] = changes["rate_change_bps"].abs()

    # Select and rename columns
    result = changes[["prev_rate", "new_rate", "rate_change_bps", "magnitude_bps", "decision"]].copy()
    result.index.name = "date"

    return result


def get_fomc_meeting_dates_hardcoded() -> list:
    """
    Hardcoded FOMC meeting dates (announcement dates).
    These are the 8 scheduled meetings per year, plus any emergency meetings.

    For a complete list, scrape from federalreserve.gov or use the existing
    scrapeFOMCtranscripts.py script in this directory.
    """
    # Sample of recent FOMC dates - extend as needed
    fomc_dates = [
        # 2024
        "2024-01-31", "2024-03-20", "2024-05-01", "2024-06-12",
        "2024-07-31", "2024-09-18", "2024-11-07", "2024-12-18",
        # 2023
        "2023-02-01", "2023-03-22", "2023-05-03", "2023-06-14",
        "2023-07-26", "2023-09-20", "2023-11-01", "2023-12-13",
        # 2022
        "2022-01-26", "2022-03-16", "2022-05-04", "2022-06-15",
        "2022-07-27", "2022-09-21", "2022-11-02", "2022-12-14",
        # Add more historical dates as needed...
    ]
    return [pd.Timestamp(d) for d in fomc_dates]


def main():
    print("=" * 60)
    print("FOMC Rate Decision Fetcher")
    print("=" * 60)

    if FRED_API_KEY == "YOUR_API_KEY_HERE":
        print("\nERROR: Please set your FRED API key!")
        print("Get a free key at: https://fred.stlouisfed.org/docs/api/api_key.html")
        return

    print(f"\nDate range: {START_DATE} to {END_DATE}\n")

    # Fetch target rate history
    df = fetch_target_rates(FRED_API_KEY, START_DATE, END_DATE)
    print(f"\nTotal observations: {len(df)}")

    # Identify rate decisions
    full_df, changes = identify_rate_decisions(df, RATE_CHANGE_THRESHOLD)

    # Create ML-ready dataset
    decisions = create_fomc_decision_dataset(full_df)

    # Save full daily data
    full_df.to_csv("fed_funds_target_daily.csv")
    print(f"\nSaved daily target rate data to: fed_funds_target_daily.csv")

    # Save decisions dataset
    if len(decisions) > 0:
        decisions.to_csv(OUTPUT_FILE)
        print(f"Saved FOMC decisions to: {OUTPUT_FILE}")
        print(f"\nDecisions summary:")
        print(f"  Total rate changes: {len(decisions)}")
        print(f"\nDecision distribution:")
        print(decisions["decision"].value_counts())
        print(f"\nRate change statistics (basis points):")
        print(decisions["rate_change_bps"].describe())
        print(f"\nSample decisions:")
        print(decisions.tail(10))
    else:
        print("No decisions to save")


if __name__ == "__main__":
    main()
