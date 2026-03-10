"""
Preprocess FOMC Training Dataset

Applies two transformations to training_data.csv:

1. DEDUPLICATION
   Finds all pairs of numeric columns with perfect correlation (r=1.0) and
   drops the one with more missing values. Ties are broken by keeping the
   column that appears first.

2. TREND FEATURES
   For every variable that has _3_months_prior, _2_months_prior, and
   _1_months_prior variants, the three raw look-back columns are replaced
   with two trend columns:
     {base}_change       = value_1m - value_3m  (direction of travel)
     {base}_acceleration = (value_1m - value_2m) - (value_2m - value_3m)
                           (is the move speeding up or slowing down?)

Output: raw_data/training_data_preprocessed.csv
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW  = ROOT / "raw_data"

# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
print("Loading training_data.csv...")
df = pd.read_csv(RAW / "training_data.csv", parse_dates=["date"])
print(f"  Shape: {df.shape}")

# ---------------------------------------------------------------------------
# Step 1: Deduplication
# ---------------------------------------------------------------------------
print("\nStep 1: Deduplication")

numeric_cols = df.select_dtypes(include="number").columns.tolist()
corr = df[numeric_cols].corr().abs()
upper = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))

to_drop = set()
for col in upper.columns:
    for row in upper.index:
        if upper.loc[row, col] == 1.0:
            # Keep whichever of the pair has fewer NaNs; tie goes to the one seen first
            na_col = df[col].isna().sum()
            na_row = df[row].isna().sum()
            drop = col if na_col >= na_row else row
            to_drop.add(drop)

print(f"  Dropping {len(to_drop)} duplicate columns:")
for c in sorted(to_drop):
    print(f"    {c}")

df = df.drop(columns=list(to_drop))

# ---------------------------------------------------------------------------
# Step 2: Trend features
# ---------------------------------------------------------------------------
print("\nStep 2: Trend features")

numeric_set = set(df.select_dtypes(include="number").columns)
cols = df.columns.tolist()

# Find all complete trios where all three columns are numeric
bases = []
for c in cols:
    m = re.match(r'^(.+)_3_months_prior$', c)
    if m:
        base = m.group(1)
        c3, c2, c1 = f"{base}_3_months_prior", f"{base}_2_months_prior", f"{base}_1_months_prior"
        if c3 in numeric_set and c2 in numeric_set and c1 in numeric_set:
            bases.append(base)

print(f"  Engineering trend features for {len(bases)} variable groups...")

trend_cols = {}
raw_to_drop = []

for base in bases:
    c3 = f"{base}_3_months_prior"
    c2 = f"{base}_2_months_prior"
    c1 = f"{base}_1_months_prior"

    trend_cols[f"{base}_change"]       = df[c1] - df[c3]
    trend_cols[f"{base}_acceleration"] = (df[c1] - df[c2]) - (df[c2] - df[c3])

    raw_to_drop += [c3, c2, c1]

df = df.drop(columns=raw_to_drop)
df = pd.concat([df, pd.DataFrame(trend_cols, index=df.index)], axis=1)

print(f"  Added {len(trend_cols)} trend columns, removed {len(raw_to_drop)} raw look-back columns.")

# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------
output_path = RAW / "training_data_preprocessed.csv"
df.to_csv(output_path, index=False)

print(f"\nDone.")
print(f"  Rows    : {len(df)}")
print(f"  Columns : {len(df.columns)}")
print(f"  Output  : {output_path}")
