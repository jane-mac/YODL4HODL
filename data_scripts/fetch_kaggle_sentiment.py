"""
Kaggle Financial Sentiment Fetcher

Downloads and processes financial sentiment datasets from Kaggle:
1. Daily Financial News for 6000+ Stocks (2009-2020)
2. US Economic News Articles (2010-2020)

Aggregates sentiment for 3-week windows before each FOMC decision.

Requires: pip install kaggle pandas
Set environment variables: KAGGLE_USERNAME, KAGGLE_KEY (or KAGGLE_API_KEY)
"""

import os
import zipfile
import shutil
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

# ============================================================================
# CONFIGURATION
# ============================================================================

# Kaggle datasets to download
DATASETS = {
    "daily_financial_news": "aaron7sun/stocknews",
    "us_economic_news": "heeraldedhia/us-economic-news-articles",
}

# Output directories
DATA_DIR = Path("kaggle_data")
DATA_DIR.mkdir(exist_ok=True)

OUTPUT_FILE = "news_sentiment.csv"

# Window before FOMC decision (days)
LOOKBACK_DAYS = 21

# Keywords for filtering Fed-related articles
FED_KEYWORDS = [
    "federal reserve", "fed ", "fomc", "interest rate", "rate hike", "rate cut",
    "monetary policy", "inflation", "deflation", "employment", "unemployment",
    "jobs report", "nonfarm payroll", "gdp", "economic growth", "recession",
    "powell", "yellen", "bernanke", "greenspan",  # Fed chairs
    "quantitative easing", "qe", "taper", "stimulus",
    "treasury", "bond yield", "yield curve",
    "central bank", "basis point", "dovish", "hawkish",
]

# ============================================================================
# KAGGLE DOWNLOAD
# ============================================================================

def setup_kaggle_credentials():
    """Set up Kaggle credentials from environment variables."""
    username = os.environ.get("KAGGLE_USERNAME")
    key = os.environ.get("KAGGLE_KEY") or os.environ.get("KAGGLE_API_KEY")

    if not username or not key:
        print("ERROR: Kaggle credentials not found.")
        print("Set KAGGLE_USERNAME and KAGGLE_KEY (or KAGGLE_API_KEY) environment variables.")
        return False

    # Kaggle library expects these specific env var names
    os.environ["KAGGLE_USERNAME"] = username
    os.environ["KAGGLE_KEY"] = key

    return True


def download_kaggle_dataset(dataset_slug: str, output_dir: Path) -> bool:
    """Download a dataset from Kaggle."""
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi

        api = KaggleApi()
        api.authenticate()

        print(f"Downloading {dataset_slug}...")
        api.dataset_download_files(dataset_slug, path=str(output_dir), unzip=True)
        print(f"  ✓ Downloaded to {output_dir}")
        return True

    except Exception as e:
        print(f"  ✗ Error downloading {dataset_slug}: {e}")
        return False


# ============================================================================
# DATA PROCESSING
# ============================================================================

def load_daily_financial_news(data_dir: Path) -> pd.DataFrame:
    """
    Load and process the Daily Financial News dataset.
    This dataset has columns like: Date, Label (sentiment), News headline
    """
    # Find the CSV files
    possible_files = [
        data_dir / "Combined_News_DJIA.csv",
        data_dir / "stocknews" / "Combined_News_DJIA.csv",
        data_dir / "RedditNews.csv",
        data_dir / "stocknews" / "RedditNews.csv",
    ]

    df_list = []

    for filepath in possible_files:
        if filepath.exists():
            print(f"Loading {filepath.name}...")
            try:
                df = pd.read_csv(filepath, encoding='utf-8', on_bad_lines='skip')
                print(f"  Columns: {list(df.columns)}")
                df_list.append((filepath.name, df))
            except Exception as e:
                print(f"  Error loading {filepath}: {e}")

    if not df_list:
        # Try to find any CSV in the directory
        for csv_file in data_dir.glob("**/*.csv"):
            print(f"Found: {csv_file}")
            try:
                df = pd.read_csv(csv_file, encoding='utf-8', nrows=5)
                print(f"  Columns: {list(df.columns)}")
            except:
                pass
        return pd.DataFrame()

    # Process Combined_News_DJIA.csv format
    # Columns: Date, Label (1=up, 0=down), Top1...Top25 (news headlines)
    processed_rows = []

    for filename, df in df_list:
        if "Combined_News" in filename:
            # This file has Date, Label, Top1-Top25 columns
            if 'Date' in df.columns and 'Label' in df.columns:
                for _, row in df.iterrows():
                    date = row['Date']
                    label = row['Label']  # 1 = market up, 0 = market down

                    # Combine all headlines for that day
                    headlines = []
                    for i in range(1, 26):
                        col = f'Top{i}'
                        if col in df.columns and pd.notna(row.get(col)):
                            headlines.append(str(row[col]))

                    combined_text = ' '.join(headlines)

                    processed_rows.append({
                        'date': date,
                        'text': combined_text,
                        'market_label': label,  # Market direction, not sentiment
                        'source': 'daily_financial_news'
                    })

        elif "Reddit" in filename:
            # Reddit news format
            if 'Date' in df.columns:
                text_col = next((c for c in df.columns if 'news' in c.lower() or 'title' in c.lower()), None)
                if text_col:
                    for _, row in df.iterrows():
                        processed_rows.append({
                            'date': row['Date'],
                            'text': str(row[text_col]),
                            'market_label': row.get('Label', None),
                            'source': 'reddit_news'
                        })

    result = pd.DataFrame(processed_rows)
    if len(result) > 0:
        result['date'] = pd.to_datetime(result['date'], errors='coerce')
        result = result.dropna(subset=['date'])
        print(f"  Loaded {len(result)} rows from Daily Financial News")

    return result


def load_us_economic_news(data_dir: Path) -> pd.DataFrame:
    """
    Load and process the US Economic News Articles dataset.
    """
    possible_files = list(data_dir.glob("**/*.csv"))

    df_list = []

    for filepath in possible_files:
        if "economic" in filepath.name.lower() or "news" in filepath.name.lower():
            print(f"Loading {filepath.name}...")
            try:
                df = pd.read_csv(filepath, encoding='utf-8', on_bad_lines='skip')
                print(f"  Columns: {list(df.columns)}")
                df_list.append(df)
            except Exception as e:
                print(f"  Error: {e}")

    if not df_list:
        # Load any CSV we find
        for filepath in possible_files[:3]:
            print(f"Trying {filepath.name}...")
            try:
                df = pd.read_csv(filepath, encoding='utf-8', on_bad_lines='skip')
                print(f"  Columns: {list(df.columns)}")
                df_list.append(df)
            except Exception as e:
                print(f"  Error: {e}")

    if not df_list:
        return pd.DataFrame()

    processed_rows = []

    for df in df_list:
        # Find date column
        date_col = next((c for c in df.columns if 'date' in c.lower()), None)
        # Find text column
        text_col = next((c for c in df.columns if any(x in c.lower() for x in ['text', 'headline', 'title', 'content', 'article'])), None)
        # Find sentiment column if exists
        sent_col = next((c for c in df.columns if 'sentiment' in c.lower()), None)

        if date_col and text_col:
            for _, row in df.iterrows():
                processed_rows.append({
                    'date': row[date_col],
                    'text': str(row[text_col]),
                    'sentiment_label': row.get(sent_col) if sent_col else None,
                    'source': 'us_economic_news'
                })

    result = pd.DataFrame(processed_rows)
    if len(result) > 0:
        result['date'] = pd.to_datetime(result['date'], errors='coerce')
        result = result.dropna(subset=['date'])
        print(f"  Loaded {len(result)} rows from US Economic News")

    return result


# ============================================================================
# SENTIMENT ANALYSIS
# ============================================================================

# Sentiment lexicons
POSITIVE_WORDS = {
    "growth", "growing", "strong", "stronger", "strength", "gain", "gains",
    "rise", "rises", "rising", "rose", "increase", "increases", "positive",
    "optimism", "optimistic", "confidence", "confident", "recovery", "improve",
    "surge", "boom", "bullish", "rally", "expansion", "robust", "healthy",
    "hire", "hiring", "jobs", "profit", "profits", "success", "record", "high",
}

NEGATIVE_WORDS = {
    "fall", "falls", "falling", "fell", "decline", "drop", "drops", "loss",
    "weak", "weaker", "weakness", "fear", "fears", "recession", "crisis",
    "crash", "plunge", "slump", "bearish", "downturn", "contraction",
    "concern", "concerns", "worry", "risk", "inflation", "unemployment",
    "layoff", "layoffs", "default", "debt", "deficit", "down", "low", "worst",
    "volatile", "volatility", "uncertainty", "slow", "slowing", "slowdown",
}

HAWKISH_WORDS = {
    "hawkish", "tighten", "tightening", "hike", "hikes", "raise", "raises",
    "restrictive", "overheating", "inflation",
}

DOVISH_WORDS = {
    "dovish", "ease", "easing", "cut", "cuts", "lower", "accommodative",
    "stimulus", "support", "patient", "gradual", "cautious",
}


def compute_sentiment(text: str) -> dict:
    """Compute sentiment scores for a piece of text."""
    if not text or not isinstance(text, str):
        return {"sentiment": 0, "hawkish": 0, "dovish": 0}

    text_lower = text.lower()
    words = set(text_lower.split())

    pos = len(words & POSITIVE_WORDS)
    neg = len(words & NEGATIVE_WORDS)
    hawk = len(words & HAWKISH_WORDS)
    dove = len(words & DOVISH_WORDS)

    total = pos + neg
    sentiment = (pos - neg) / total if total > 0 else 0

    return {
        "sentiment": sentiment,
        "positive": pos,
        "negative": neg,
        "hawkish": hawk,
        "dovish": dove,
    }


def is_fed_related(text: str) -> bool:
    """Check if text is related to Fed/economic policy."""
    if not text or not isinstance(text, str):
        return False
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in FED_KEYWORDS)


# ============================================================================
# AGGREGATION
# ============================================================================

def aggregate_sentiment_by_window(
    news_df: pd.DataFrame,
    decision_dates: list,
    lookback_days: int = 21
) -> pd.DataFrame:
    """
    For each FOMC decision date, aggregate sentiment from the preceding window.
    """
    results = []

    for decision_date in decision_dates:
        if isinstance(decision_date, str):
            decision_date = pd.to_datetime(decision_date)

        window_start = decision_date - timedelta(days=lookback_days)
        window_end = decision_date - timedelta(days=1)

        # Filter news to window
        mask = (news_df['date'] >= window_start) & (news_df['date'] <= window_end)
        window_news = news_df[mask].copy()

        # Filter for Fed-related articles
        fed_mask = window_news['text'].apply(is_fed_related)
        fed_news = window_news[fed_mask]

        # Compute sentiment for each article
        sentiments = fed_news['text'].apply(compute_sentiment).apply(pd.Series)

        if len(sentiments) > 0:
            row = {
                "decision_date": decision_date.strftime("%Y-%m-%d"),
                "window_start": window_start.strftime("%Y-%m-%d"),
                "window_end": window_end.strftime("%Y-%m-%d"),
                "total_articles": len(window_news),
                "fed_related_articles": len(fed_news),
                "avg_sentiment": sentiments['sentiment'].mean(),
                "sentiment_std": sentiments['sentiment'].std(),
                "positive_pct": (sentiments['sentiment'] > 0).mean() * 100,
                "negative_pct": (sentiments['sentiment'] < 0).mean() * 100,
                "avg_hawkish": sentiments['hawkish'].mean(),
                "avg_dovish": sentiments['dovish'].mean(),
                "hawk_dove_balance": sentiments['hawkish'].mean() - sentiments['dovish'].mean(),
            }
        else:
            row = {
                "decision_date": decision_date.strftime("%Y-%m-%d"),
                "window_start": window_start.strftime("%Y-%m-%d"),
                "window_end": window_end.strftime("%Y-%m-%d"),
                "total_articles": len(window_news),
                "fed_related_articles": 0,
                "avg_sentiment": 0,
                "sentiment_std": 0,
                "positive_pct": 50,
                "negative_pct": 50,
                "avg_hawkish": 0,
                "avg_dovish": 0,
                "hawk_dove_balance": 0,
            }

        results.append(row)
        print(f"  {decision_date.strftime('%Y-%m-%d')}: {row['fed_related_articles']} Fed-related articles, "
              f"sentiment={row['avg_sentiment']:.3f}")

    return pd.DataFrame(results)


# ============================================================================
# MAIN
# ============================================================================

def load_fomc_decisions(filepath: str = "fomc_decisions.csv") -> list:
    """Load FOMC decision dates."""
    path = Path(filepath)

    if path.exists():
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        return list(df.index)
    else:
        print(f"Warning: {filepath} not found, using sample dates")
        return pd.date_range("2010-01-01", "2020-12-31", freq="45D").tolist()


def main():
    print("=" * 60)
    print("Kaggle Financial Sentiment Fetcher")
    print("=" * 60)

    # Setup credentials
    if not setup_kaggle_credentials():
        return

    # Download datasets
    print("\n" + "-" * 40)
    print("STEP 1: Download Datasets")
    print("-" * 40)

    for name, slug in DATASETS.items():
        dataset_dir = DATA_DIR / name
        dataset_dir.mkdir(exist_ok=True)

        # Check if already downloaded
        if any(dataset_dir.glob("*.csv")):
            print(f"{name}: Already downloaded")
        else:
            download_kaggle_dataset(slug, dataset_dir)

    # Load and process datasets
    print("\n" + "-" * 40)
    print("STEP 2: Load and Process Data")
    print("-" * 40)

    all_news = []

    # Load Daily Financial News
    dfn_dir = DATA_DIR / "daily_financial_news"
    if dfn_dir.exists():
        dfn_df = load_daily_financial_news(dfn_dir)
        if len(dfn_df) > 0:
            all_news.append(dfn_df)

    # Load US Economic News
    uen_dir = DATA_DIR / "us_economic_news"
    if uen_dir.exists():
        uen_df = load_us_economic_news(uen_dir)
        if len(uen_df) > 0:
            all_news.append(uen_df)

    if not all_news:
        print("ERROR: No news data loaded. Check the downloaded files.")
        print(f"Contents of {DATA_DIR}:")
        for f in DATA_DIR.glob("**/*"):
            print(f"  {f}")
        return

    # Combine all news
    news_df = pd.concat(all_news, ignore_index=True)
    news_df = news_df.sort_values('date')
    print(f"\nCombined dataset: {len(news_df)} articles")
    print(f"Date range: {news_df['date'].min()} to {news_df['date'].max()}")

    # Load FOMC decisions
    print("\n" + "-" * 40)
    print("STEP 3: Aggregate Sentiment by FOMC Window")
    print("-" * 40)

    decision_dates = load_fomc_decisions()
    print(f"FOMC decisions: {len(decision_dates)}")

    # Filter to dates within news coverage
    news_min = news_df['date'].min()
    news_max = news_df['date'].max()

    valid_decisions = [
        d for d in decision_dates
        if news_min <= pd.to_datetime(d) <= news_max
    ]
    print(f"Decisions with news coverage: {len(valid_decisions)}")

    # Aggregate sentiment
    result_df = aggregate_sentiment_by_window(news_df, valid_decisions, LOOKBACK_DAYS)

    # Save
    result_df = result_df.set_index("decision_date")
    result_df.to_csv(OUTPUT_FILE)
    print(f"\nSaved to {OUTPUT_FILE}")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Decisions processed: {len(result_df)}")
    print(f"Date range: {result_df.index.min()} to {result_df.index.max()}")
    print(f"\nSentiment statistics:")
    print(result_df[["fed_related_articles", "avg_sentiment", "hawk_dove_balance"]].describe())

    print("\n" + "=" * 60)
    print("SAMPLE OUTPUT")
    print("=" * 60)
    print(result_df.head(10).to_string())


if __name__ == "__main__":
    main()
