"""
Add Presidential Approval Features to training_data_preprocessed.csv

Merges president and approval_rating from presidential_approval_fomc.csv
into training_data_preprocessed.csv on date.

Output: raw_data/training_data_preprocessed.csv (updated in-place)
"""

import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW  = ROOT / "raw_data"

print("Loading files...")
preprocessed = pd.read_csv(RAW / "training_data_preprocessed.csv", parse_dates=["date"])
approval     = pd.read_csv(RAW / "presidential_approval_fomc.csv", parse_dates=["decision_date"])

print(f"  training_data_preprocessed : {preprocessed.shape}")
print(f"  presidential_approval_fomc : {approval.shape}")

# Drop existing columns if re-running
for col in ["president", "approval_rating"]:
    if col in preprocessed.columns:
        preprocessed = preprocessed.drop(columns=[col])

approval_slim = approval[["decision_date", "president", "approval_rating"]].rename(
    columns={"decision_date": "date"}
)

merged = preprocessed.merge(approval_slim, on="date", how="left")

matched = merged["approval_rating"].notna().sum()
print(f"\nMerge results:")
print(f"  Rows matched : {matched}")
print(f"  Rows unmatched : {len(merged) - matched}")

output_path = RAW / "training_data_preprocessed.csv"
merged.to_csv(output_path, index=False)

print(f"\nDone. Saved to {output_path}")
print(f"  Rows    : {len(merged)}")
print(f"  Columns : {len(merged.columns)}")
