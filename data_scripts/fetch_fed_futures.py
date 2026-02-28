"""
Fed Funds Futures Fetcher

Fetches Fed Funds Futures data to capture market expectations of rate changes.
Fed Funds Futures directly encode what the market expects the Fed to do.

Data sources:
1. FRED - Fed Funds futures-implied rates and probabilities (free)
2. Yahoo Finance - Fed Funds Futures contracts (free)

Requires: pip install fredapi yfinance pandas
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

# Try to import fredapi
try:
    from fredapi import Fred
    HAS_FRED = True
except ImportError:
    HAS_FRED = False
    print("Note: fredapi not installed. FRED data will be skipped.")

# ============================================================================
# CONFIGURATION
# ============================================================================

FRED_API_KEY = os.environ.get("FRED_API_KEY", "YOUR_API_KEY_HERE")

START_DATE = "2000-01-01"
END_DATE = datetime.now().strftime("%Y-%m-%d")

OUTPUT_FILE = "fed_futures.csv"

# ============================================================================
# FRED SERIES
# ============================================================================

# Fed Funds Futures-related series from FRED
FRED_FUTURES_SERIES = {
    # Implied rates from futures
    "FEDTARMD": "implied_rate_median",  # Market-implied Fed Funds target (median)
    "FEDTARCTM": "implied_rate_mode",   # Market-implied Fed Funds target (mode)

    # Effective rate for comparison
    "DFF": "effective_ff_rate",          # Daily effective Fed Funds rate

    # Market expectations (Cleveland Fed)
    "EXPINF1YR": "expected_inflation_1y",  # Expected inflation 1 year ahead
    "EXPINF10YR": "expected_inflation_10y", # Expected inflation 10 years ahead
}

# ============================================================================
# YAHOO FINANCE FED FUNDS FUTURES
# ============================================================================

def get_ff_futures_tickers() -> list:
    """
    Generate Fed Funds Futures ticker symbols for Yahoo Finance.

    CME Fed Funds Futures trade as ZQ (30-day Fed Funds)
    Format: ZQ + month code + year
    Month codes: F=Jan, G=Feb, H=Mar, J=Apr, K=May, M=Jun,
                 N=Jul, Q=Aug, U=Sep, V=Oct, X=Nov, Z=Dec
    """
    month_codes = {
        1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
        7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
    }

    tickers = []
    current_year = datetime.now().year
    current_month = datetime.now().month

    # Get futures for next 12 months
    for i in range(12):
        month = (current_month + i - 1) % 12 + 1
        year = current_year + (current_month + i - 1) // 12

        # CME Globex ticker format
        ticker = f"ZQ{month_codes[month]}{str(year)[-2:]}.CBT"
        tickers.append({
            'ticker': ticker,
            'month': month,
            'year': year,
            'expiry': f"{year}-{month:02d}",
        })

    return tickers


def fetch_yahoo_futures() -> pd.DataFrame:
    """
    Fetch Fed Funds Futures from Yahoo Finance.
    Note: Yahoo Finance futures data can be spotty.
    """
    print("\nFetching Fed Funds Futures from Yahoo Finance...")

    # Try different ticker formats
    ticker_formats = [
        "ZQ=F",      # Generic front month
        "ZQH25.CBT", # Specific month (March 2025)
        "ZQM25.CBT", # June 2025
        "ZQU25.CBT", # September 2025
        "ZQZ25.CBT", # December 2025
    ]

    all_data = {}

    for ticker in ticker_formats:
        try:
            print(f"  Trying {ticker}...")
            data = yf.download(ticker, start=START_DATE, end=END_DATE, progress=False)

            if len(data) > 0:
                # Handle MultiIndex columns
                if isinstance(data.columns, pd.MultiIndex):
                    data.columns = data.columns.get_level_values(0)

                if 'Close' in data.columns:
                    # Fed Funds Futures price = 100 - implied rate
                    # So implied rate = 100 - price
                    implied_rate = 100 - data['Close']
                    all_data[ticker.replace('.CBT', '').replace('=F', '')] = implied_rate
                    print(f"    ✓ Got {len(data)} rows")
        except Exception as e:
            print(f"    ✗ Error: {e}")

    if all_data:
        df = pd.DataFrame(all_data)
        df.index.name = 'date'
        return df

    return pd.DataFrame()


# ============================================================================
# FRED DATA
# ============================================================================

def fetch_fred_futures_data() -> pd.DataFrame:
    """Fetch Fed Funds related data from FRED."""
    if not HAS_FRED:
        return pd.DataFrame()

    if FRED_API_KEY == "YOUR_API_KEY_HERE":
        print("Warning: FRED API key not set. Skipping FRED data.")
        return pd.DataFrame()

    print("\nFetching Fed Funds data from FRED...")
    fred = Fred(api_key=FRED_API_KEY)

    all_data = {}

    for series_id, col_name in FRED_FUTURES_SERIES.items():
        try:
            print(f"  Fetching {series_id} -> {col_name}...")
            data = fred.get_series(series_id, observation_start=START_DATE, observation_end=END_DATE)
            all_data[col_name] = data
            print(f"    ✓ Got {len(data)} observations")
        except Exception as e:
            print(f"    ✗ Error: {e}")

    if all_data:
        df = pd.DataFrame(all_data)
        df.index.name = 'date'
        return df

    return pd.DataFrame()


# ============================================================================
# IMPLIED PROBABILITY CALCULATION
# ============================================================================

def calculate_implied_probabilities(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate implied probabilities of rate changes from futures data.

    The Fed typically moves in 25bp increments.
    We can estimate probabilities based on where the implied rate falls
    relative to potential target rates.
    """
    df = df.copy()

    if 'effective_ff_rate' not in df.columns:
        return df

    # Current rate (use effective rate as proxy)
    current_rate = df['effective_ff_rate']

    # Implied rate from futures (if available)
    if 'implied_rate_median' in df.columns:
        implied = df['implied_rate_median']
    elif 'implied_rate_mode' in df.columns:
        implied = df['implied_rate_mode']
    else:
        return df

    # Calculate expected change
    df['expected_rate_change'] = implied - current_rate

    # Estimate probability of hike/cut
    # If expected change > 12.5bp (half of 25bp), market expects hike
    # If expected change < -12.5bp, market expects cut
    df['prob_hike'] = (df['expected_rate_change'] / 0.25).clip(0, 1) * 100
    df['prob_cut'] = (-df['expected_rate_change'] / 0.25).clip(0, 1) * 100
    df['prob_hold'] = 100 - df['prob_hike'] - df['prob_cut']
    df['prob_hold'] = df['prob_hold'].clip(0, 100)

    # Normalize
    total = df['prob_hike'] + df['prob_cut'] + df['prob_hold']
    df['prob_hike'] = df['prob_hike'] / total * 100
    df['prob_cut'] = df['prob_cut'] / total * 100
    df['prob_hold'] = df['prob_hold'] / total * 100

    return df


# ============================================================================
# CME FEDWATCH PROXY
# ============================================================================

def calculate_fedwatch_proxy(df: pd.DataFrame, fomc_dates: list = None) -> pd.DataFrame:
    """
    Calculate a proxy for CME FedWatch probabilities.

    FedWatch uses Fed Funds Futures to calculate probability of rate changes.
    Formula: P(hike) = (implied_rate - current_target) / 0.25

    This is a simplified version - actual FedWatch is more sophisticated.
    """
    df = df.copy()

    if 'effective_ff_rate' not in df.columns:
        return df

    # Calculate rolling metrics
    df['ff_rate_5d_change'] = df['effective_ff_rate'].diff(5)
    df['ff_rate_21d_change'] = df['effective_ff_rate'].diff(21)

    # Volatility of rate expectations
    if 'implied_rate_median' in df.columns:
        df['implied_rate_volatility'] = df['implied_rate_median'].rolling(21).std()

        # Deviation from current rate
        df['rate_deviation'] = df['implied_rate_median'] - df['effective_ff_rate']

        # Momentum in expectations
        df['expectation_momentum'] = df['rate_deviation'].diff(5)

    return df


# ============================================================================
# ALTERNATIVE: TREASURY-BASED EXPECTATIONS
# ============================================================================

def fetch_treasury_based_expectations() -> pd.DataFrame:
    """
    Use Treasury yields as a proxy for rate expectations.
    The 2-year Treasury closely tracks Fed Funds expectations.
    """
    print("\nFetching Treasury-based rate expectations...")

    tickers = {
        '^IRX': 'treasury_3m',   # 3-month T-bill (closest to Fed Funds)
        '^FVX': 'treasury_5y',   # 5-year Treasury
        '^TNX': 'treasury_10y',  # 10-year Treasury
    }

    all_data = {}

    for ticker, name in tickers.items():
        try:
            data = yf.download(ticker, start=START_DATE, end=END_DATE, progress=False)

            if len(data) > 0:
                if isinstance(data.columns, pd.MultiIndex):
                    data.columns = data.columns.get_level_values(0)

                if 'Close' in data.columns:
                    all_data[name] = data['Close']
                    print(f"  ✓ {name}: {len(data)} rows")
        except Exception as e:
            print(f"  ✗ {ticker}: {e}")

    if all_data:
        df = pd.DataFrame(all_data)

        # 3-month T-bill rate is a good proxy for near-term Fed expectations
        if 'treasury_3m' in df.columns:
            df['tbill_ff_spread'] = df.get('treasury_3m', 0)  # Will subtract FF rate later

        # 2s10s spread (if we have the data) indicates rate expectations
        if 'treasury_10y' in df.columns and 'treasury_3m' in df.columns:
            df['curve_10y3m'] = df['treasury_10y'] - df['treasury_3m']
            df['curve_inverted'] = (df['curve_10y3m'] < 0).astype(int)

        df.index.name = 'date'
        return df

    return pd.DataFrame()


# ============================================================================
# MAIN
# ============================================================================

def load_fomc_decisions(filepath: str = "fomc_decisions.csv") -> pd.DataFrame:
    """Load FOMC decisions for alignment."""
    path = Path(filepath)
    if path.exists():
        return pd.read_csv(path, index_col=0, parse_dates=True)
    return pd.DataFrame()


def main():
    print("=" * 60)
    print("Fed Funds Futures Fetcher")
    print("=" * 60)
    print(f"Date range: {START_DATE} to {END_DATE}")

    all_data = []

    # 1. Fetch FRED data (most reliable)
    fred_df = fetch_fred_futures_data()
    if len(fred_df) > 0:
        all_data.append(fred_df)
        print(f"  FRED data: {len(fred_df)} rows, {len(fred_df.columns)} columns")

    # 2. Fetch Yahoo Finance futures
    yahoo_df = fetch_yahoo_futures()
    if len(yahoo_df) > 0:
        all_data.append(yahoo_df)
        print(f"  Yahoo futures: {len(yahoo_df)} rows")

    # 3. Fetch Treasury-based expectations
    treasury_df = fetch_treasury_based_expectations()
    if len(treasury_df) > 0:
        all_data.append(treasury_df)
        print(f"  Treasury data: {len(treasury_df)} rows")

    if not all_data:
        print("\nERROR: No data fetched. Check your API keys and internet connection.")
        return

    # Combine all data
    print("\nCombining datasets...")
    df = pd.concat(all_data, axis=1)
    df = df.sort_index()

    # Remove duplicate columns
    df = df.loc[:, ~df.columns.duplicated()]

    print(f"Combined dataset: {len(df)} rows, {len(df.columns)} columns")

    # Calculate implied probabilities
    print("\nCalculating implied probabilities...")
    df = calculate_implied_probabilities(df)
    df = calculate_fedwatch_proxy(df)

    # Add spread vs effective rate
    if 'effective_ff_rate' in df.columns and 'treasury_3m' in df.columns:
        df['tbill_ff_spread'] = df['treasury_3m'] - df['effective_ff_rate']

    # Resample to daily (forward fill gaps)
    df = df.resample('D').last().ffill()

    # Save daily data
    daily_file = "fed_futures_daily.csv"
    df.to_csv(daily_file)
    print(f"\nSaved daily data to: {daily_file}")

    # Resample to monthly
    df_monthly = df.resample('M').last()
    df_monthly.to_csv(OUTPUT_FILE)
    print(f"Saved monthly data to: {OUTPUT_FILE}")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Date range: {df.index.min()} to {df.index.max()}")
    print(f"\nColumns:")
    for col in df.columns:
        non_null = df[col].notna().sum()
        print(f"  {col}: {non_null} observations")

    print("\n" + "=" * 60)
    print("KEY STATISTICS")
    print("=" * 60)
    key_cols = ['effective_ff_rate', 'expected_rate_change', 'prob_hike', 'prob_cut']
    available_cols = [c for c in key_cols if c in df.columns]
    if available_cols:
        print(df[available_cols].describe())

    print("\n" + "=" * 60)
    print("SAMPLE DATA (recent)")
    print("=" * 60)
    print(df.tail(10).to_string())


if __name__ == "__main__":
    main()
