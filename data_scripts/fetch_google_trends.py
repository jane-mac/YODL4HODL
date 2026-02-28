"""
Google Trends Fetcher for Fed Rate Prediction

Fetches search interest for economic terms as alternative indicators.
The theory: public search behavior reflects economic anxiety/optimism
and may lead or coincide with Fed decisions.

Requires: pip install pytrends pandas
"""

import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from pytrends.request import TrendReq

# ============================================================================
# CONFIGURATION
# ============================================================================

OUTPUT_FILE = "google_trends.csv"
OUTPUT_DAILY_FILE = "google_trends_daily.csv"

# Lookback window for FOMC alignment (days)
LOOKBACK_DAYS = 21

# Search terms to track
# Grouped by category (max 5 per request due to Google's limits)
SEARCH_TERMS = {
    "fear_indicators": [
        "recession",
        "market crash",
        "layoffs",
        "unemployment",
        "depression",
    ],
    "inflation_indicators": [
        "inflation",
        "prices rising",
        "cost of living",
        "gas prices",
        "grocery prices",
    ],
    "fed_indicators": [
        "federal reserve",
        "interest rates",
        "fed rate hike",
        "fed rate cut",
        "fomc",
    ],
    "job_indicators": [
        "jobs",
        "hiring",
        "job openings",
        "job market",
        "find a job",
    ],
    "housing_indicators": [
        "housing market",
        "mortgage rates",
        "home prices",
        "housing crash",
        "buy a house",
    ],
    "desperation_indicators": [
        "sell my car",
        "pawn shop",
        "payday loan",
        "food stamps",
        "bankruptcy",
    ],
    "optimism_indicators": [
        "invest in stocks",
        "buy stocks",
        "stock market",
        "retirement",
        "401k",
    ],
}

# Fed Chairs and their tenures (for time-specific searches)
FED_CHAIRS = [
    ("alan greenspan", "2004-01-01", "2006-01-31"),   # GT starts 2004, Greenspan ended 2006
    ("ben bernanke", "2006-02-01", "2014-01-31"),
    ("janet yellen", "2014-02-03", "2018-02-03"),
    ("jerome powell", "2018-02-05", "2026-12-31"),
]

# Rate limiting (Google Trends is strict)
REQUEST_DELAY = 5  # seconds between requests


# ============================================================================
# GOOGLE TRENDS FETCHER
# ============================================================================

def fetch_trends_for_terms(
    pytrends: TrendReq,
    terms: list,
    timeframe: str = "2004-01-01 2024-12-31",
    geo: str = "US",
) -> pd.DataFrame:
    """
    Fetch Google Trends data for a list of terms.

    Args:
        pytrends: TrendReq instance
        terms: List of search terms (max 5)
        timeframe: Date range string
        geo: Geographic region

    Returns:
        DataFrame with search interest over time
    """
    try:
        pytrends.build_payload(
            kw_list=terms[:5],  # Max 5 terms per request
            cat=0,  # All categories
            timeframe=timeframe,
            geo=geo,
        )

        df = pytrends.interest_over_time()

        if 'isPartial' in df.columns:
            df = df.drop(columns=['isPartial'])

        return df

    except Exception as e:
        print(f"    Error fetching trends: {e}")
        return pd.DataFrame()


def fetch_fed_chair_trends(pytrends: TrendReq) -> pd.DataFrame:
    """
    Fetch search interest for each Fed Chair during their tenure.
    Combines into a single 'fed_chair_searches' column.
    """
    print("\nFetching Fed Chair search interest (by tenure)...")

    all_chair_data = []

    for chair_name, start_date, end_date in FED_CHAIRS:
        # Adjust end date if in future
        if datetime.strptime(end_date, "%Y-%m-%d") > datetime.now():
            end_date = datetime.now().strftime("%Y-%m-%d")

        # Skip if start is after end (shouldn't happen)
        if start_date >= end_date:
            continue

        timeframe = f"{start_date} {end_date}"
        print(f"  {chair_name}: {timeframe}")

        try:
            pytrends.build_payload(
                kw_list=[chair_name],
                cat=0,
                timeframe=timeframe,
                geo="US",
            )

            df = pytrends.interest_over_time()

            if len(df) > 0 and chair_name in df.columns:
                # Rename to generic column
                chair_df = df[[chair_name]].copy()
                chair_df.columns = ['fed_chair_searches']
                all_chair_data.append(chair_df)
                print(f"    ✓ Got {len(chair_df)} data points")

            time.sleep(REQUEST_DELAY)

        except Exception as e:
            print(f"    ✗ Error: {e}")

    if all_chair_data:
        # Concatenate all periods
        combined = pd.concat(all_chair_data)
        combined = combined.sort_index()
        # Remove any duplicates (overlapping dates)
        combined = combined[~combined.index.duplicated(keep='first')]
        return combined

    return pd.DataFrame()


def fetch_all_trends(start_date: str = "2004-01-01", end_date: str = None) -> pd.DataFrame:
    """
    Fetch Google Trends data for all search term groups.
    """
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    timeframe = f"{start_date} {end_date}"

    print(f"Fetching Google Trends data")
    print(f"Timeframe: {timeframe}")
    print(f"Region: US")

    # Initialize pytrends
    pytrends = TrendReq(hl='en-US', tz=360, timeout=(10, 25))

    all_data = []

    # Fetch standard search terms
    for category, terms in SEARCH_TERMS.items():
        print(f"\nFetching {category}: {terms}")

        df = fetch_trends_for_terms(pytrends, terms, timeframe)

        if len(df) > 0:
            # Rename columns to include category prefix
            df.columns = [f"{category}_{col}" for col in df.columns]
            all_data.append(df)
            print(f"  ✓ Got {len(df)} data points")
        else:
            print(f"  ✗ No data returned")

        # Rate limiting
        time.sleep(REQUEST_DELAY)

    # Fetch Fed Chair searches (time-period specific)
    chair_df = fetch_fed_chair_trends(pytrends)
    if len(chair_df) > 0:
        all_data.append(chair_df)
        print(f"  ✓ Fed Chair data: {len(chair_df)} data points")

    if not all_data:
        print("No data fetched!")
        return pd.DataFrame()

    # Combine all dataframes
    combined = pd.concat(all_data, axis=1)
    combined.index.name = 'date'

    return combined


# ============================================================================
# DERIVED FEATURES
# ============================================================================

def compute_trend_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute derived features from Google Trends data.
    """
    df = df.copy()

    # Aggregate indices by category
    for category in SEARCH_TERMS.keys():
        category_cols = [col for col in df.columns if col.startswith(category)]
        if category_cols:
            df[f"{category}_index"] = df[category_cols].mean(axis=1)

    # Fear vs Optimism ratio
    if 'fear_indicators_index' in df.columns and 'optimism_indicators_index' in df.columns:
        # Avoid division by zero
        optimism = df['optimism_indicators_index'].replace(0, 1)
        df['fear_optimism_ratio'] = df['fear_indicators_index'] / optimism

    # Desperation index (normalized)
    if 'desperation_indicators_index' in df.columns:
        df['desperation_zscore'] = (
            (df['desperation_indicators_index'] - df['desperation_indicators_index'].rolling(52).mean())
            / df['desperation_indicators_index'].rolling(52).std()
        )

    # Fed attention index
    if 'fed_indicators_index' in df.columns:
        df['fed_attention_zscore'] = (
            (df['fed_indicators_index'] - df['fed_indicators_index'].rolling(52).mean())
            / df['fed_indicators_index'].rolling(52).std()
        )

    # Fed Chair attention (searches for current chair's name)
    if 'fed_chair_searches' in df.columns:
        df['fed_chair_zscore'] = (
            (df['fed_chair_searches'] - df['fed_chair_searches'].rolling(52).mean())
            / df['fed_chair_searches'].rolling(52).std()
        )
        df['fed_chair_spike'] = (
            (df['fed_chair_searches'] > df['fed_chair_searches'].rolling(52).mean() +
             2 * df['fed_chair_searches'].rolling(52).std()).astype(int)
        )

    # Week-over-week changes for key terms
    key_cols = [
        'fear_indicators_recession',
        'inflation_indicators_inflation',
        'fed_indicators_interest rates',
        'job_indicators_jobs',
    ]

    for col in key_cols:
        if col in df.columns:
            safe_name = col.replace(' ', '_')
            df[f"{safe_name}_wow_change"] = df[col].diff()
            df[f"{safe_name}_4w_change"] = df[col].diff(4)

    # Spike detection (> 2 std devs above rolling mean)
    if 'fear_indicators_recession' in df.columns:
        rolling_mean = df['fear_indicators_recession'].rolling(52).mean()
        rolling_std = df['fear_indicators_recession'].rolling(52).std()
        df['recession_search_spike'] = (
            (df['fear_indicators_recession'] > rolling_mean + 2 * rolling_std).astype(int)
        )

    return df


# ============================================================================
# FOMC ALIGNMENT
# ============================================================================

def align_to_fomc_decisions(
    trends_df: pd.DataFrame,
    decisions_file: str = "fomc_decisions.csv",
    lookback_days: int = LOOKBACK_DAYS,
) -> pd.DataFrame:
    """
    Aggregate Google Trends data for the window before each FOMC decision.
    """
    # Load FOMC decisions
    decisions_path = Path(decisions_file)
    if not decisions_path.exists():
        print(f"Warning: {decisions_file} not found")
        return pd.DataFrame()

    decisions = pd.read_csv(decisions_file, index_col=0, parse_dates=True)

    results = []

    for decision_date in decisions.index:
        window_start = decision_date - timedelta(days=lookback_days)
        window_end = decision_date - timedelta(days=1)

        # Filter trends to window
        mask = (trends_df.index >= window_start) & (trends_df.index <= window_end)
        window_data = trends_df[mask]

        if len(window_data) == 0:
            continue

        # Aggregate: mean and max for the window
        row = {"decision_date": decision_date.strftime("%Y-%m-%d")}

        for col in trends_df.columns:
            if trends_df[col].dtype in ['float64', 'int64']:
                row[f"{col}_mean"] = window_data[col].mean()
                row[f"{col}_max"] = window_data[col].max()
                # Trend within window (last week vs first week)
                if len(window_data) >= 2:
                    row[f"{col}_trend"] = window_data[col].iloc[-1] - window_data[col].iloc[0]

        results.append(row)

    return pd.DataFrame(results)


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 60)
    print("Google Trends Fetcher")
    print("=" * 60)
    print("\nSearch term categories:")
    for category, terms in SEARCH_TERMS.items():
        print(f"  {category}: {terms}")

    # Fetch trends data
    print("\n" + "-" * 40)
    print("STEP 1: Fetch Google Trends Data")
    print("-" * 40)

    # Google Trends data starts from 2004
    df = fetch_all_trends(start_date="2004-01-01")

    if len(df) == 0:
        print("No data fetched. Google may be rate limiting.")
        print("Try again in a few minutes or reduce the number of terms.")
        return

    print(f"\nRaw data: {len(df)} weeks, {len(df.columns)} columns")
    print(f"Date range: {df.index.min()} to {df.index.max()}")

    # Compute derived features
    print("\n" + "-" * 40)
    print("STEP 2: Compute Derived Features")
    print("-" * 40)

    df = compute_trend_features(df)
    print(f"Total columns after features: {len(df.columns)}")

    # Save weekly data
    df.to_csv(OUTPUT_DAILY_FILE)
    print(f"\nSaved weekly trends to: {OUTPUT_DAILY_FILE}")

    # Align to FOMC decisions
    print("\n" + "-" * 40)
    print("STEP 3: Align to FOMC Decisions")
    print("-" * 40)

    fomc_aligned = align_to_fomc_decisions(df)

    if len(fomc_aligned) > 0:
        fomc_aligned = fomc_aligned.set_index("decision_date")
        fomc_aligned.to_csv(OUTPUT_FILE)
        print(f"Saved FOMC-aligned data to: {OUTPUT_FILE}")
        print(f"Decisions covered: {len(fomc_aligned)}")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Weekly data points: {len(df)}")
    print(f"FOMC decisions covered: {len(fomc_aligned)}")
    print(f"Features per decision: {len(fomc_aligned.columns)}")

    # Show key indices
    print("\n" + "=" * 60)
    print("KEY INDEX STATISTICS (weekly)")
    print("=" * 60)

    index_cols = [col for col in df.columns if col.endswith('_index')]
    if index_cols:
        print(df[index_cols].describe())

    print("\n" + "=" * 60)
    print("SAMPLE FOMC-ALIGNED DATA")
    print("=" * 60)

    # Show a subset of columns
    sample_cols = [col for col in fomc_aligned.columns if 'recession' in col.lower() or 'fear' in col.lower()][:6]
    if sample_cols:
        print(fomc_aligned[sample_cols].tail(10).to_string())


if __name__ == "__main__":
    main()
