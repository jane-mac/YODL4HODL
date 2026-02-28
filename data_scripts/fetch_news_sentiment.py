"""
News Sentiment Fetcher for Fed Rate Prediction

Fetches news sentiment for the 3-week window before each FOMC decision.
Uses NewsAPI (https://newsapi.org) - free tier available.

Get your free API key at: https://newsapi.org/register

Requires: pip install requests pandas
"""

import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

# Get API key from environment or set directly
NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY", "YOUR_API_KEY_HERE")

# Window before FOMC decision to analyze (days)
LOOKBACK_DAYS = 21  # 3 weeks

# Output file
OUTPUT_FILE = "news_sentiment.csv"

# NewsAPI endpoint
NEWSAPI_URL = "https://newsapi.org/v2/everything"

# Search queries for Fed-related news
SEARCH_QUERIES = [
    "federal reserve",
    "interest rate",
    "inflation",
]

# Rate limiting
REQUEST_DELAY = 1.0  # NewsAPI is more generous
MAX_RETRIES = 3
BACKOFF_FACTOR = 2.0

# ============================================================================
# SENTIMENT LEXICONS
# ============================================================================

# Economic/financial sentiment words
POSITIVE_WORDS = {
    "growth", "growing", "strong", "stronger", "strength",
    "gain", "gains", "rise", "rises", "rising", "rose",
    "increase", "increases", "increased", "increasing",
    "positive", "optimism", "optimistic", "confidence", "confident",
    "recovery", "recovering", "recover", "recovered",
    "improve", "improves", "improved", "improving", "improvement",
    "surge", "surges", "surging", "boom", "booming",
    "bullish", "rally", "rallies", "rallying",
    "expansion", "expanding", "expand",
    "robust", "healthy", "stable", "stability",
    "hire", "hiring", "hires", "jobs", "employment",
    "profit", "profits", "profitable", "earnings",
    "success", "successful", "succeed",
    "up", "higher", "high", "peak", "record",
}

NEGATIVE_WORDS = {
    "fall", "falls", "falling", "fell",
    "decline", "declines", "declined", "declining",
    "drop", "drops", "dropped", "dropping",
    "loss", "losses", "lose", "losing", "lost",
    "weak", "weaker", "weakness", "weakening",
    "fear", "fears", "feared", "fearful", "anxiety",
    "recession", "recessionary", "slowdown", "slowing",
    "crisis", "crash", "crashes", "crashed", "crashing",
    "plunge", "plunges", "plunged", "plunging",
    "slump", "slumps", "slumped",
    "bearish", "downturn", "downturns",
    "contraction", "contracting", "contract",
    "concern", "concerns", "concerned", "worry", "worried", "worries",
    "risk", "risks", "risky", "threat", "threatens",
    "inflation", "inflationary",  # Context-dependent but often negative
    "unemployment", "jobless", "layoff", "layoffs", "firing",
    "default", "defaults", "bankruptcy", "bankruptcies",
    "debt", "deficit",
    "down", "lower", "low", "bottom", "worst",
    "cut", "cuts", "cutting",  # Rate cuts can be negative signal
    "volatile", "volatility", "uncertainty", "uncertain",
}

# Hawkish vs dovish language (Fed-specific)
HAWKISH_WORDS = {
    "hawkish", "hawk", "hawks", "tighten", "tightening",
    "hike", "hikes", "hiking", "raise", "raises", "raising",
    "restrictive", "restrictiveness",
    "inflation fight", "price stability", "overheating",
}

DOVISH_WORDS = {
    "dovish", "dove", "doves", "ease", "easing", "eased",
    "cut", "cuts", "cutting", "lower", "lowering",
    "accommodative", "accommodation", "stimulus",
    "support", "supporting", "supportive",
    "patient", "patience", "gradual", "cautious",
}


# ============================================================================
# NEWSAPI FUNCTIONS
# ============================================================================

def fetch_newsapi_articles(query: str, start_date: datetime, end_date: datetime) -> list:
    """
    Fetch articles from NewsAPI for a query and date range.
    """
    if NEWSAPI_KEY == "YOUR_API_KEY_HERE":
        return []

    params = {
        "q": query,
        "from": start_date.strftime("%Y-%m-%d"),
        "to": end_date.strftime("%Y-%m-%d"),
        "language": "en",
        "sortBy": "relevancy",
        "pageSize": 100,
        "apiKey": NEWSAPI_KEY,
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(NEWSAPI_URL, params=params, timeout=30)

            if response.status_code == 429:
                wait_time = REQUEST_DELAY * (BACKOFF_FACTOR ** attempt) * 10
                print(f"    Rate limited. Waiting {wait_time:.0f}s...")
                time.sleep(wait_time)
                continue

            if response.status_code == 401:
                print("    ERROR: Invalid API key")
                return []

            if response.status_code == 426:
                print("    ERROR: Free tier only supports last 30 days")
                return []

            response.raise_for_status()
            data = response.json()

            if data.get("status") != "ok":
                print(f"    API error: {data.get('message', 'Unknown error')}")
                return []

            return data.get("articles", [])

        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = REQUEST_DELAY * (BACKOFF_FACTOR ** attempt)
                print(f"    Error: {e}. Retrying in {wait_time:.0f}s...")
                time.sleep(wait_time)
            else:
                print(f"    Failed after {MAX_RETRIES} attempts: {e}")
                return []

    return []


def analyze_sentiment(text: str) -> dict:
    """
    Analyze sentiment of text using lexicon-based approach.
    Returns sentiment scores.
    """
    if not text:
        return {"score": 0, "positive": 0, "negative": 0, "hawkish": 0, "dovish": 0}

    text_lower = text.lower()
    words = set(text_lower.split())

    positive = len(words & POSITIVE_WORDS)
    negative = len(words & NEGATIVE_WORDS)
    hawkish = len(words & HAWKISH_WORDS)
    dovish = len(words & DOVISH_WORDS)

    # Also check for phrases (not just single words)
    for phrase in POSITIVE_WORDS:
        if ' ' in phrase and phrase in text_lower:
            positive += 1
    for phrase in NEGATIVE_WORDS:
        if ' ' in phrase and phrase in text_lower:
            negative += 1

    total = positive + negative
    if total > 0:
        score = (positive - negative) / total
    else:
        score = 0

    return {
        "score": score,
        "positive": positive,
        "negative": negative,
        "hawkish": hawkish,
        "dovish": dovish,
    }


def fetch_sentiment_for_window(start_date: datetime, end_date: datetime) -> dict:
    """
    Fetch and analyze sentiment for all search queries in a date window.
    """
    all_articles = []
    all_sentiments = []

    for query in SEARCH_QUERIES:
        articles = fetch_newsapi_articles(query, start_date, end_date)
        time.sleep(REQUEST_DELAY)

        for article in articles:
            # Combine title and description for analysis
            text = f"{article.get('title', '')} {article.get('description', '')}"
            sentiment = analyze_sentiment(text)

            all_articles.append(article)
            all_sentiments.append(sentiment)

    # Aggregate results
    if not all_sentiments:
        return {
            "total_articles": 0,
            "avg_sentiment": 0,
            "positive_pct": 50,
            "negative_pct": 50,
            "avg_hawkish": 0,
            "avg_dovish": 0,
            "hawk_dove_balance": 0,
        }

    scores = [s["score"] for s in all_sentiments]
    hawkish_scores = [s["hawkish"] for s in all_sentiments]
    dovish_scores = [s["dovish"] for s in all_sentiments]

    positive_articles = sum(1 for s in scores if s > 0)
    negative_articles = sum(1 for s in scores if s < 0)
    total_classified = positive_articles + negative_articles

    return {
        "total_articles": len(all_articles),
        "avg_sentiment": sum(scores) / len(scores) if scores else 0,
        "sentiment_std": pd.Series(scores).std() if len(scores) > 1 else 0,
        "positive_pct": (positive_articles / total_classified * 100) if total_classified > 0 else 50,
        "negative_pct": (negative_articles / total_classified * 100) if total_classified > 0 else 50,
        "avg_hawkish": sum(hawkish_scores) / len(hawkish_scores) if hawkish_scores else 0,
        "avg_dovish": sum(dovish_scores) / len(dovish_scores) if dovish_scores else 0,
        "hawk_dove_balance": (sum(hawkish_scores) - sum(dovish_scores)) / len(all_sentiments) if all_sentiments else 0,
    }


# ============================================================================
# MAIN
# ============================================================================

def load_fomc_decisions(filepath: str = "fomc_decisions.csv") -> pd.DataFrame:
    """Load FOMC decision dates."""
    path = Path(filepath)

    if not path.exists():
        print(f"Warning: {filepath} not found.")
        # Return sample recent dates (NewsAPI free tier = last 30 days only)
        today = datetime.now()
        sample_dates = [
            (today - timedelta(days=7)).strftime("%Y-%m-%d"),
            (today - timedelta(days=14)).strftime("%Y-%m-%d"),
        ]
        df = pd.DataFrame({"date": pd.to_datetime(sample_dates)})
        df = df.set_index("date")
        return df

    return pd.read_csv(path, index_col=0, parse_dates=True)


def main():
    print("=" * 60)
    print("News Sentiment Fetcher (NewsAPI)")
    print("=" * 60)

    if NEWSAPI_KEY == "YOUR_API_KEY_HERE":
        print("\nERROR: Please set your NewsAPI key!")
        print("Get a free key at: https://newsapi.org/register")
        print("\nSet it via:")
        print("  1. Environment variable: export NEWSAPI_KEY='your_key'")
        print("  2. Edit this script and replace YOUR_API_KEY_HERE")
        return

    print(f"Lookback window: {LOOKBACK_DAYS} days before each decision")
    print(f"Search queries: {SEARCH_QUERIES}")

    # Load FOMC decisions
    print("\nLoading FOMC decisions...")
    decisions = load_fomc_decisions()
    print(f"Found {len(decisions)} decisions")

    # NewsAPI free tier only supports last 30 days
    # Paid tier supports historical data
    min_date = datetime.now() - timedelta(days=30)
    print(f"\nNOTE: NewsAPI free tier only covers last 30 days ({min_date.strftime('%Y-%m-%d')} onwards)")
    print("For historical data, upgrade to paid tier or use existing news_sentiment.csv\n")

    # Check for existing results to resume from
    results = []
    already_processed = set()

    if Path(OUTPUT_FILE).exists():
        try:
            existing = pd.read_csv(OUTPUT_FILE, index_col=0)
            already_processed = set(existing.index.astype(str))
            print(f"Found existing file with {len(already_processed)} decisions - will append new data")
            results = existing.reset_index().to_dict('records')
        except Exception as e:
            print(f"Could not load existing file: {e}")

    new_results = 0

    for i, decision_date in enumerate(decisions.index):
        if isinstance(decision_date, str):
            decision_date = datetime.strptime(decision_date, "%Y-%m-%d")
        elif hasattr(decision_date, 'to_pydatetime'):
            decision_date = decision_date.to_pydatetime()

        date_str = decision_date.strftime('%Y-%m-%d')

        # Skip already processed dates
        if date_str in already_processed:
            continue

        # Check if within NewsAPI range
        window_start = decision_date - timedelta(days=LOOKBACK_DAYS)
        window_end = decision_date - timedelta(days=1)

        if window_start < min_date:
            print(f"[{i+1}/{len(decisions)}] {date_str} - outside NewsAPI free tier range, skipping")
            continue

        print(f"[{i+1}/{len(decisions)}] {date_str} "
              f"(window: {window_start.strftime('%Y-%m-%d')} to {window_end.strftime('%Y-%m-%d')})")

        # Fetch sentiment
        sentiment = fetch_sentiment_for_window(window_start, window_end)

        # Store result
        row = {
            "decision_date": date_str,
            "window_start": window_start.strftime("%Y-%m-%d"),
            "window_end": window_end.strftime("%Y-%m-%d"),
            "total_articles": sentiment["total_articles"],
            "avg_sentiment": round(sentiment["avg_sentiment"], 4),
            "sentiment_std": round(sentiment.get("sentiment_std", 0), 4),
            "positive_pct": round(sentiment["positive_pct"], 2),
            "negative_pct": round(sentiment["negative_pct"], 2),
            "avg_hawkish": round(sentiment["avg_hawkish"], 4),
            "avg_dovish": round(sentiment["avg_dovish"], 4),
            "hawk_dove_balance": round(sentiment["hawk_dove_balance"], 4),
        }

        results.append(row)
        new_results += 1

        print(f"    Articles: {sentiment['total_articles']}, "
              f"Sentiment: {sentiment['avg_sentiment']:.3f}, "
              f"Hawk-Dove: {sentiment['hawk_dove_balance']:.3f}")

        # Save incrementally
        if new_results % 3 == 0:
            temp_df = pd.DataFrame(results).set_index("decision_date")
            temp_df.to_csv(OUTPUT_FILE)
            print(f"    [Checkpoint saved]")

    # Final save
    if results:
        df = pd.DataFrame(results)
        df = df.set_index("decision_date")
        df.to_csv(OUTPUT_FILE)
        print(f"\nSaved to {OUTPUT_FILE}")

        # Summary
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"Total decisions in file: {len(df)}")
        print(f"New decisions processed: {new_results}")

        if len(df) > 0:
            print(f"\nSentiment statistics:")
            print(df[["total_articles", "avg_sentiment", "positive_pct", "hawk_dove_balance"]].describe())

            print("\n" + "=" * 60)
            print("SAMPLE OUTPUT")
            print("=" * 60)
            print(df.tail(10).to_string())
    else:
        print("\nNo new results to save.")
        print("NewsAPI free tier only covers the last 30 days.")
        print("If you need historical data, consider:")
        print("  1. NewsAPI paid plan")
        print("  2. Using pre-built financial sentiment datasets")


if __name__ == "__main__":
    main()
