"""
Add Board Composition Features to training_data_preprocessed.csv

For each FOMC decision date, joins fomc_membership.csv and fomc_bios.csv to add:
  - hawk_count         : number of hawks on the board
  - dove_count         : number of doves on the board
  - phd_count          : number of members with a PhD
  - lawyer_count       : number of members with a JD / law degree
  - top_university     : university most heavily represented across all board members

Output: raw_data/training_data_preprocessed.csv (updated in-place)
"""

import pandas as pd
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW  = ROOT / "raw_data"

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
print("Loading files...")
preprocessed = pd.read_csv(RAW / "training_data_preprocessed.csv", parse_dates=["date"])
membership   = pd.read_csv(RAW / "fomc_membership.csv", parse_dates=["date"])
bios         = pd.read_csv(RAW / "fomc_bios.csv")

print(f"  training_data_preprocessed : {preprocessed.shape}")
print(f"  fomc_membership            : {membership.shape}")
print(f"  fomc_bios                  : {bios.shape}")

# ---------------------------------------------------------------------------
# Build bio lookup: name -> {has_phd, has_jd_law, universities list}
# ---------------------------------------------------------------------------
def parse_bool(val):
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.strip().lower() == "true"
    return bool(val)

def parse_universities(raw):
    """Split semicolon-separated university string into a list."""
    if not isinstance(raw, str) or not raw.strip():
        return []
    return [u.strip() for u in raw.split(";") if u.strip()]

bio_lookup = {}
for _, row in bios.iterrows():
    name = str(row["name"]).strip()
    bio_lookup[name] = {
        "has_phd"    : parse_bool(row.get("has_phd", False)),
        "has_jd_law" : parse_bool(row.get("has_jd_law", False)),
        "universities": parse_universities(row.get("universities", "")),
    }

# ---------------------------------------------------------------------------
# Build membership features per date
# ---------------------------------------------------------------------------
def get_all_members(row):
    """Combine governor and president name lists into one flat list."""
    members = []
    for col in ("governor_names", "president_names"):
        cell = row.get(col, "")
        if isinstance(cell, str) and cell.strip():
            members.extend([n.strip() for n in cell.split("|") if n.strip()])
    return members

membership_features = []
unmatched_names = set()

for _, row in membership.iterrows():
    date    = row["date"]
    hawks   = row.get("hawk_count", 0)
    doves   = row.get("dove_count", 0)
    members = get_all_members(row)

    phd_count    = 0
    lawyer_count = 0
    uni_counter  = Counter()

    for name in members:
        bio = bio_lookup.get(name)
        if bio is None:
            unmatched_names.add(name)
            continue
        if bio["has_phd"]:
            phd_count += 1
        if bio["has_jd_law"]:
            lawyer_count += 1
        for uni in bio["universities"]:
            uni_counter[uni] += 1

    top_uni = uni_counter.most_common(1)[0][0] if uni_counter else None

    membership_features.append({
        "date"          : date,
        "hawk_count"    : hawks,
        "dove_count"    : doves,
        "phd_count"     : phd_count,
        "lawyer_count"  : lawyer_count,
        "top_university": top_uni,
    })

features_df = pd.DataFrame(membership_features)

if unmatched_names:
    print(f"\nWarning: {len(unmatched_names)} member name(s) not found in fomc_bios.csv:")
    for n in sorted(unmatched_names):
        print(f"  {n}")

# ---------------------------------------------------------------------------
# Merge into preprocessed on date
# Drop existing board columns if re-running
# ---------------------------------------------------------------------------
board_cols = ["hawk_count", "dove_count", "phd_count", "lawyer_count", "top_university"]
preprocessed = preprocessed.drop(columns=[c for c in board_cols if c in preprocessed.columns])

merged = preprocessed.merge(features_df, on="date", how="left")

matched = merged["phd_count"].notna().sum()
print(f"\nMerge results:")
print(f"  Rows in preprocessed : {len(preprocessed)}")
print(f"  Rows matched to board: {matched}")
print(f"  Rows unmatched       : {len(merged) - matched}")

# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------
output_path = RAW / "training_data_preprocessed.csv"
merged.to_csv(output_path, index=False)

print(f"\nDone. Saved to {output_path}")
print(f"  Rows    : {len(merged)}")
print(f"  Columns : {len(merged.columns)}")
