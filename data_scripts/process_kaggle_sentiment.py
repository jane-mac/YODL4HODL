"""
Process Already-Downloaded Kaggle Sentiment Data

Processes the existing Kaggle financial news data and aligns with FOMC meetings.
No API calls needed - just processes local CSV files.
"""

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

LOOKBACK_DAYS = 21  # 3-week window before each decision
OUTPUT_FILE = "news_sentiment.csv"

# Keywords for filtering Fed-related articles
FED_KEYWORDS = [
    "federal reserve", "fed ", " fed", "fomc", "interest rate", "rate hike", "rate cut",
    "monetary policy", "inflation", "deflation", "employment", "unemployment",
    "jobs report", "nonfarm payroll", "gdp", "economic growth", "recession",
    "powell", "yellen", "bernanke", "greenspan",
    "quantitative easing", "qe", "taper", "stimulus",
    "treasury", "bond yield", "yield curve",
    "central bank", "basis point", "dovish", "hawkish",
]

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
    "restrictive", "overheating",
}

DOVISH_WORDS = {
    "dovish", "ease", "easing", "cut", "cuts", "accommodative",
    "stimulus", "support", "patient", "gradual", "cautious",
}


def load_kaggle_news() -> pd.DataFrame:
    """Load the downloaded Kaggle news data from multiple sources."""
    all_rows = []

    # 1. Load Combined_News_DJIA.csv (2008-2016)
    combined_file = Path("kaggle_data/daily_financial_news/Combined_News_DJIA.csv")
    if combined_file.exists():
        print(f"Loading {combined_file}...")
        df = pd.read_csv(combined_file)

        for _, row in df.iterrows():
            date = row['Date']
            # Combine all headline columns (Top1 through Top25)
            headlines = []
            for i in range(1, 26):
                col = f'Top{i}'
                if col in df.columns and pd.notna(row.get(col)):
                    headlines.append(str(row[col]))

            combined_text = ' '.join(headlines)
            all_rows.append({
                'date': pd.to_datetime(date, errors='coerce'),
                'text': combined_text,
                'source': 'combined_news_djia',
                'pre_labeled_sentiment': None,
            })

        print(f"  Loaded {len(df)} days of headlines")

    # 2. Load US-Economic-News.csv (1951-2014, pre-labeled sentiment)
    econ_file = Path("kaggle_data/us_economic_news/US-Economic-News.csv")
    if econ_file.exists():
        print(f"Loading {econ_file}...")
        try:
            df = pd.read_csv(econ_file, encoding='latin-1', on_bad_lines='skip')

            # Parse dates (M/D/YY format)
            df['date_parsed'] = pd.to_datetime(df['date'], format='%m/%d/%y', errors='coerce')

            # Fix century issue: years > current year need 100 subtracted
            from datetime import datetime
            current_year = datetime.now().year
            mask = df['date_parsed'].dt.year > current_year
            df.loc[mask, 'date_parsed'] = df.loc[mask, 'date_parsed'] - pd.DateOffset(years=100)

            for _, row in df.iterrows():
                # Combine headline and text
                headline = str(row.get('headline', '')) if pd.notna(row.get('headline')) else ''
                text = str(row.get('text', '')) if pd.notna(row.get('text')) else ''
                combined_text = f"{headline} {text}"

                # Pre-labeled sentiment (1-9 scale, convert to -1 to 1)
                positivity = row.get('positivity')
                if pd.notna(positivity):
                    # Scale: 1-9 -> -1 to 1 (5 is neutral)
                    pre_labeled = (float(positivity) - 5) / 4
                else:
                    pre_labeled = None

                all_rows.append({
                    'date': row['date_parsed'],
                    'text': combined_text,
                    'source': 'us_economic_news',
                    'pre_labeled_sentiment': pre_labeled,
                })

            print(f"  Loaded {len(df)} articles with pre-labeled sentiment")

        except Exception as e:
            print(f"  Error loading US Economic News: {e}")

    # 3. Load RedditNews.csv (2008-2016, more articles)
    reddit_file = Path("kaggle_data/daily_financial_news/RedditNews.csv")
    if reddit_file.exists():
        print(f"Loading {reddit_file}...")
        try:
            df = pd.read_csv(reddit_file, encoding='utf-8', on_bad_lines='skip')

            for _, row in df.iterrows():
                all_rows.append({
                    'date': pd.to_datetime(row['Date'], errors='coerce'),
                    'text': str(row.get('News', '')),
                    'source': 'reddit_news',
                    'pre_labeled_sentiment': None,
                })

            print(f"  Loaded {len(df)} Reddit headlines")

        except Exception as e:
            print(f"  Error loading Reddit News: {e}")

    result = pd.DataFrame(all_rows)
    if len(result) > 0:
        result = result.dropna(subset=['date'])
        result = result.sort_values('date')

    return result


def compute_sentiment(text: str) -> dict:
    """Compute sentiment scores for text."""
    if not text or not isinstance(text, str):
        return {"sentiment": 0, "positive": 0, "negative": 0, "hawkish": 0, "dovish": 0}

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


def aggregate_sentiment(news_df: pd.DataFrame, decision_dates: list) -> pd.DataFrame:
    """Aggregate sentiment for each FOMC decision window."""
    results = []

    for decision_date in decision_dates:
        if isinstance(decision_date, str):
            decision_date = pd.to_datetime(decision_date)

        window_start = decision_date - timedelta(days=LOOKBACK_DAYS)
        window_end = decision_date - timedelta(days=1)

        # Filter news to window
        mask = (news_df['date'] >= window_start) & (news_df['date'] <= window_end)
        window_news = news_df[mask].copy()

        if len(window_news) == 0:
            continue

        # Filter for Fed-related content
        fed_mask = window_news['text'].apply(is_fed_related)
        fed_news = window_news[fed_mask]

        # Compute sentiment for all articles in window (not just Fed-related)
        all_sentiments = window_news['text'].apply(compute_sentiment).apply(pd.Series)

        # Use pre-labeled sentiment where available (US Economic News dataset)
        pre_labeled = window_news['pre_labeled_sentiment'].dropna()
        if len(pre_labeled) > 0:
            pre_labeled_avg = pre_labeled.mean()
        else:
            pre_labeled_avg = None

        # Compute sentiment for Fed-related articles
        if len(fed_news) > 0:
            fed_sentiments = fed_news['text'].apply(compute_sentiment).apply(pd.Series)
            fed_avg_sentiment = fed_sentiments['sentiment'].mean()
            fed_sentiment_std = fed_sentiments['sentiment'].std()
            fed_positive_pct = (fed_sentiments['sentiment'] > 0).mean() * 100
            fed_negative_pct = (fed_sentiments['sentiment'] < 0).mean() * 100
            fed_hawkish = fed_sentiments['hawkish'].mean()
            fed_dovish = fed_sentiments['dovish'].mean()

            # Also check pre-labeled sentiment for Fed articles
            fed_pre_labeled = fed_news['pre_labeled_sentiment'].dropna()
            if len(fed_pre_labeled) > 0:
                fed_pre_labeled_avg = fed_pre_labeled.mean()
            else:
                fed_pre_labeled_avg = None
        else:
            fed_avg_sentiment = 0
            fed_sentiment_std = 0
            fed_positive_pct = 50
            fed_negative_pct = 50
            fed_hawkish = 0
            fed_dovish = 0
            fed_pre_labeled_avg = None

        row = {
            "decision_date": decision_date.strftime("%Y-%m-%d"),
            "window_start": window_start.strftime("%Y-%m-%d"),
            "window_end": window_end.strftime("%Y-%m-%d"),
            "total_articles": len(window_news),
            "fed_related_articles": len(fed_news),
            "avg_sentiment": round(fed_avg_sentiment, 4),
            "sentiment_std": round(fed_sentiment_std, 4) if pd.notna(fed_sentiment_std) else 0,
            "positive_pct": round(fed_positive_pct, 2),
            "negative_pct": round(fed_negative_pct, 2),
            "avg_hawkish": round(fed_hawkish, 4),
            "avg_dovish": round(fed_dovish, 4),
            "hawk_dove_balance": round(fed_hawkish - fed_dovish, 4),
            # Overall market sentiment (lexicon-based)
            "market_sentiment": round(all_sentiments['sentiment'].mean(), 4),
            # Pre-labeled sentiment from US Economic News (human-labeled)
            "pre_labeled_sentiment": round(pre_labeled_avg, 4) if pre_labeled_avg is not None else None,
            "fed_pre_labeled_sentiment": round(fed_pre_labeled_avg, 4) if fed_pre_labeled_avg is not None else None,
        }

        results.append(row)
        pre_label_str = f", pre-labeled={pre_labeled_avg:.3f}" if pre_labeled_avg is not None else ""
        print(f"  {decision_date.strftime('%Y-%m-%d')}: {len(window_news)} articles, {len(fed_news)} Fed-related, sentiment={fed_avg_sentiment:.3f}{pre_label_str}")

    return pd.DataFrame(results)


def main():
    print("=" * 60)
    print("Processing Kaggle Financial Sentiment")
    print("=" * 60)

    # Load news data
    print("\nLoading Kaggle news data...")
    news_df = load_kaggle_news()

    if len(news_df) == 0:
        print("ERROR: No news data found. Check kaggle_data directory.")
        return

    print(f"Total news records: {len(news_df)}")
    print(f"Date range: {news_df['date'].min().date()} to {news_df['date'].max().date()}")

    # Load FOMC decisions
    print("\nLoading FOMC decisions...")
    decisions_df = pd.read_csv("fomc_decisions.csv", index_col=0, parse_dates=True)
    print(f"Total decisions: {len(decisions_df)}")

    # Filter to decisions within news coverage
    news_min = news_df['date'].min()
    news_max = news_df['date'].max()

    valid_dates = [
        d for d in decisions_df.index
        if news_min <= d <= news_max
    ]
    print(f"Decisions with news coverage: {len(valid_dates)}")

    # Aggregate sentiment
    print("\nAggregating sentiment by FOMC window...")
    result_df = aggregate_sentiment(news_df, valid_dates)

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

    print("\nSentiment statistics:")
    print(result_df[["fed_related_articles", "avg_sentiment", "hawk_dove_balance"]].describe())

    print("\n" + "=" * 60)
    print("SAMPLE OUTPUT")
    print("=" * 60)
    print(result_df.head(15).to_string())


if __name__ == "__main__":
    main()
