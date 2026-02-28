"""
Market Data Fetcher

Pulls S&P 500, VIX, and other market indicators using yfinance.
These capture market sentiment and risk appetite relevant to Fed decisions.

Requires: pip install yfinance pandas
"""

import pandas as pd
import yfinance as yf
from datetime import datetime
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

START_DATE = "1990-01-01"
END_DATE = datetime.now().strftime("%Y-%m-%d")
OUTPUT_FILE = "market_data.csv"

# Tickers to fetch
TICKERS = {
    # Core indices
    "^GSPC": "sp500",           # S&P 500
    "^VIX": "vix",              # CBOE Volatility Index
    "^DJI": "dow_jones",        # Dow Jones Industrial Average
    "^IXIC": "nasdaq",          # NASDAQ Composite

    # Other useful indicators
    "^TNX": "treasury_10y_yield",  # 10-Year Treasury Yield
    "^TYX": "treasury_30y_yield",  # 30-Year Treasury Yield
    "^FVX": "treasury_5y_yield",   # 5-Year Treasury Yield
    "^IRX": "treasury_3m_yield",   # 3-Month Treasury Yield

    # Dollar and commodities
    "DX-Y.NYB": "dollar_index",    # US Dollar Index
    "GC=F": "gold",                # Gold Futures
    "CL=F": "crude_oil",           # Crude Oil Futures

    # Credit/corporate
    "HYG": "high_yield_bonds",     # iShares High Yield Corporate Bond ETF
    "LQD": "investment_grade",     # iShares Investment Grade Corporate Bond ETF
}


def fetch_ticker_data(ticker: str, start: str, end: str) -> pd.DataFrame:
    """Fetch OHLCV data for a single ticker."""
    try:
        data = yf.download(ticker, start=start, end=end, progress=False)
        return data
    except Exception as e:
        print(f"  Error fetching {ticker}: {e}")
        return pd.DataFrame()


def fetch_all_market_data(tickers: dict, start: str, end: str) -> pd.DataFrame:
    """
    Fetch adjusted close prices for all tickers.
    Returns a DataFrame with dates as index and tickers as columns.
    """
    print(f"Fetching market data from {start} to {end}")
    print(f"Tickers: {list(tickers.keys())}\n")

    all_series = []

    for ticker, name in tickers.items():
        print(f"Fetching {ticker} -> {name}...")

        try:
            data = yf.download(ticker, start=start, end=end, progress=False)

            if len(data) > 0:
                # Handle MultiIndex columns (newer yfinance versions)
                if isinstance(data.columns, pd.MultiIndex):
                    data.columns = data.columns.get_level_values(0)

                # Use Adjusted Close for price data
                if 'Adj Close' in data.columns:
                    series = data['Adj Close'].rename(name)
                    all_series.append(series)
                elif 'Close' in data.columns:
                    series = data['Close'].rename(name)
                    all_series.append(series)

                # Also get volume for major indices
                if ticker in ['^GSPC', '^DJI', '^IXIC'] and 'Volume' in data.columns:
                    vol_series = data['Volume'].rename(f"{name}_volume")
                    all_series.append(vol_series)

                print(f"  ✓ {len(data)} rows")
            else:
                print(f"  ✗ No data returned")

        except Exception as e:
            print(f"  ✗ Error: {e}")

    # Combine all series into single DataFrame
    if not all_series:
        return pd.DataFrame()

    df = pd.concat(all_series, axis=1)
    df.index.name = 'date'

    return df


def compute_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute derived features useful for Fed rate prediction.
    """
    df = df.copy()

    # Daily returns
    if 'sp500' in df.columns:
        df['sp500_return_1d'] = df['sp500'].pct_change() * 100
        df['sp500_return_5d'] = df['sp500'].pct_change(periods=5) * 100
        df['sp500_return_21d'] = df['sp500'].pct_change(periods=21) * 100  # ~1 month

        # Rolling volatility (realized vol)
        df['sp500_volatility_21d'] = df['sp500_return_1d'].rolling(21).std() * (252 ** 0.5)

        # Distance from 52-week high/low
        df['sp500_52w_high'] = df['sp500'].rolling(252).max()
        df['sp500_52w_low'] = df['sp500'].rolling(252).min()
        df['sp500_pct_from_high'] = (df['sp500'] / df['sp500_52w_high'] - 1) * 100

    # VIX features
    if 'vix' in df.columns:
        df['vix_change_1d'] = df['vix'].diff()
        df['vix_change_5d'] = df['vix'].diff(periods=5)
        df['vix_ma_21d'] = df['vix'].rolling(21).mean()

        # VIX regime (high/low volatility)
        df['vix_high_regime'] = (df['vix'] > 25).astype(int)
        df['vix_extreme'] = (df['vix'] > 35).astype(int)

    # Yield curve features (if treasury data available)
    if 'treasury_10y_yield' in df.columns and 'treasury_3m_yield' in df.columns:
        df['yield_curve_10y3m'] = df['treasury_10y_yield'] - df['treasury_3m_yield']
        df['yield_curve_inverted'] = (df['yield_curve_10y3m'] < 0).astype(int)

    if 'treasury_10y_yield' in df.columns and 'treasury_5y_yield' in df.columns:
        df['yield_curve_10y5y'] = df['treasury_10y_yield'] - df['treasury_5y_yield']

    # Credit spread proxy (if bond ETF data available)
    if 'high_yield_bonds' in df.columns and 'investment_grade' in df.columns:
        # This is a rough proxy - actual spread requires bond yield data
        df['hy_ig_ratio'] = df['high_yield_bonds'] / df['investment_grade']
        df['hy_ig_ratio_change'] = df['hy_ig_ratio'].pct_change(periods=21) * 100

    # Gold as risk indicator
    if 'gold' in df.columns:
        df['gold_return_21d'] = df['gold'].pct_change(periods=21) * 100

    # Dollar strength
    if 'dollar_index' in df.columns:
        df['dollar_return_21d'] = df['dollar_index'].pct_change(periods=21) * 100

    return df


def resample_to_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """Resample to monthly frequency (end of month)."""
    return df.resample('M').last()


def main():
    print("=" * 60)
    print("Market Data Fetcher (yfinance)")
    print("=" * 60)

    # Fetch daily data
    df = fetch_all_market_data(TICKERS, START_DATE, END_DATE)

    if len(df) == 0:
        print("No data fetched. Check your internet connection.")
        return

    print(f"\nRaw data shape: {df.shape}")
    print(f"Date range: {df.index.min()} to {df.index.max()}")

    # Compute derived features
    print("\nComputing derived features...")
    df = compute_derived_features(df)

    # Save daily data
    daily_file = "market_data_daily.csv"
    df.to_csv(daily_file)
    print(f"Saved daily data to: {daily_file}")

    # Resample to monthly and save
    print("\nResampling to monthly...")
    df_monthly = resample_to_monthly(df)
    df_monthly.to_csv(OUTPUT_FILE)
    print(f"Saved monthly data to: {OUTPUT_FILE}")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Daily observations: {len(df)}")
    print(f"Monthly observations: {len(df_monthly)}")
    print(f"Columns: {len(df.columns)}")

    print("\nColumn list:")
    for col in sorted(df.columns):
        non_null = df[col].notna().sum()
        print(f"  {col}: {non_null} observations")

    # Quick stats on key indicators
    print("\n" + "=" * 60)
    print("KEY STATISTICS (full period)")
    print("=" * 60)

    key_cols = ['sp500', 'vix', 'treasury_10y_yield']
    for col in key_cols:
        if col in df.columns:
            print(f"\n{col}:")
            print(df[col].describe())


if __name__ == "__main__":
    main()
