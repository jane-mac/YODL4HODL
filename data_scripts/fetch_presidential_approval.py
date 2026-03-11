"""
Fetch Presidential Approval Ratings for FOMC Decision Windows

For each FOMC decision date, finds all approval polls whose poll_end date falls
in the 14 days prior to (and including) the decision date, then averages the
approval rating across those polls.

Source: https://github.com/lorenzo-ruffino/approval_rate_usa_president

Output: raw_data/presidential_approval_fomc.csv
Columns: poll_date, decision_date, decision, president, approval_rating
  - poll_date      : start of the two-week lookback window (decision_date - 14 days)
  - decision_date  : FOMC meeting date
  - decision       : RAISE / LOWER / MAINTAIN
  - president      : president in office at the time
  - approval_rating: average approval % across all polls in the window
"""

import pandas as pd
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW  = ROOT / "raw_data"

POLLS_URL = (
    "https://raw.githubusercontent.com/lorenzo-ruffino/"
    "approval_rate_usa_president/main/historical_approval_polls.csv"
)

WINDOW_DAYS = 14

# ---------------------------------------------------------------------------
# Download approval polls
# ---------------------------------------------------------------------------
print("Downloading presidential approval polls...")
with urllib.request.urlopen(POLLS_URL) as resp:
    polls_raw = resp.read().decode("utf-8")

from io import StringIO
polls = pd.read_csv(StringIO(polls_raw), parse_dates=["poll_start", "poll_end"])
print(f"  Loaded {len(polls)} poll rows, {polls['president'].nunique()} presidents")

# Keep only the columns we need
polls = polls[["president", "poll_start", "poll_end", "approval"]].copy()
polls = polls.dropna(subset=["poll_end", "approval"])

# ---------------------------------------------------------------------------
# Load FOMC decisions
# ---------------------------------------------------------------------------
print("Loading FOMC decisions...")
decisions = pd.read_csv(RAW / "fomc_decisions.csv", parse_dates=["date"])
print(f"  {len(decisions)} decision rows")

# ---------------------------------------------------------------------------
# For each decision, find polls in the prior 14-day window
# ---------------------------------------------------------------------------
rows = []

for _, dec in decisions.iterrows():
    decision_date = dec["date"]
    window_start  = decision_date - pd.Timedelta(days=WINDOW_DAYS)

    # Polls where poll_end falls within [window_start, decision_date]
    in_window = polls[
        (polls["poll_end"] >= window_start) &
        (polls["poll_end"] <= decision_date)
    ]

    if in_window.empty:
        # Fall back to nearest poll ending before decision_date
        prior = polls[polls["poll_end"] <= decision_date]
        if prior.empty:
            continue
        nearest_end = prior["poll_end"].max()
        in_window = prior[prior["poll_end"] == nearest_end]

    avg_approval = round(in_window["approval"].mean(), 2)

    # Determine president: use the most frequent name in window (handles edge cases)
    president = in_window["president"].mode().iloc[0]

    rows.append({
        "poll_date"      : window_start.date(),
        "decision_date"  : decision_date.date(),
        "decision"       : dec["decision"],
        "president"      : president,
        "approval_rating": avg_approval,
    })

result = pd.DataFrame(rows)

# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------
output_path = RAW / "presidential_approval_fomc.csv"
result.to_csv(output_path, index=False)

print(f"\nDone. Saved to {output_path}")
print(f"  Rows    : {len(result)}")
print(f"  Date range: {result['decision_date'].min()} to {result['decision_date'].max()}")
print(f"\nSample output:")
print(result.head(10).to_string(index=False))
