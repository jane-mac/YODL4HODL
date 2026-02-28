"""
FOMC Membership Mapper

Maps each FOMC rate decision to the board members serving at that time.

The FOMC consists of:
- 7 Board of Governors (appointed by President, Senate-confirmed)
- 5 voting Reserve Bank Presidents (NY Fed + 4 rotating)

This script:
1. Loads historical FOMC member data
2. Reads your FOMC decisions from fomc_decisions.csv
3. Outputs a mapping of decision -> board composition

Requires: pandas
"""

import pandas as pd
from datetime import datetime
from pathlib import Path

# ============================================================================
# HISTORICAL FED GOVERNORS DATA
# Source: https://www.federalreserve.gov/aboutthefed/bios/board/boardmembership.htm
# ============================================================================

# Format: (name, start_date, end_date, position, appointed_by)
# Position: "Chair", "Vice Chair", "Governor"
# end_date of None means currently serving

FED_GOVERNORS = [
    # Current and recent governors
    ("Jerome H. Powell", "2018-02-05", None, "Chair", "Trump"),
    ("Jerome H. Powell", "2012-05-25", "2018-02-04", "Governor", "Obama"),
    ("Philip N. Jefferson", "2023-09-13", None, "Vice Chair", "Biden"),
    ("Philip N. Jefferson", "2022-05-23", "2023-09-12", "Governor", "Biden"),
    ("Michael S. Barr", "2022-07-19", None, "Vice Chair for Supervision", "Biden"),
    ("Michelle W. Bowman", "2018-11-26", None, "Governor", "Trump"),
    ("Lisa D. Cook", "2022-05-23", None, "Governor", "Biden"),
    ("Adriana D. Kugler", "2023-09-13", None, "Governor", "Biden"),
    ("Christopher J. Waller", "2020-12-18", None, "Governor", "Trump"),

    # Recent former governors
    ("Lael Brainard", "2014-06-16", "2023-02-18", "Governor", "Obama"),
    ("Lael Brainard", "2022-01-14", "2023-02-18", "Vice Chair", "Biden"),
    ("Richard H. Clarida", "2018-09-17", "2022-01-14", "Vice Chair", "Trump"),
    ("Randal K. Quarles", "2017-10-13", "2021-12-31", "Vice Chair for Supervision", "Trump"),
    ("Nellie Liang", "2022-01-03", "2023-01-06", "Governor", "Biden"),

    # 2010s governors
    ("Janet L. Yellen", "2014-02-03", "2018-02-03", "Chair", "Obama"),
    ("Janet L. Yellen", "2010-10-04", "2014-02-03", "Vice Chair", "Obama"),
    ("Stanley Fischer", "2014-06-16", "2017-10-13", "Vice Chair", "Obama"),
    ("Daniel K. Tarullo", "2009-01-28", "2017-04-05", "Governor", "Obama"),
    ("Sarah Bloom Raskin", "2010-10-04", "2014-03-13", "Governor", "Obama"),
    ("Jerome H. Powell", "2012-05-25", "2018-02-04", "Governor", "Obama"),
    ("Jeremy C. Stein", "2012-05-30", "2014-05-28", "Governor", "Obama"),

    # Bernanke era
    ("Ben S. Bernanke", "2006-02-01", "2014-01-31", "Chair", "Bush"),
    ("Ben S. Bernanke", "2002-08-05", "2005-06-21", "Governor", "Bush"),
    ("Donald L. Kohn", "2006-06-23", "2010-09-01", "Vice Chair", "Bush"),
    ("Donald L. Kohn", "2002-08-05", "2006-06-22", "Governor", "Bush"),
    ("Kevin M. Warsh", "2006-02-24", "2011-03-31", "Governor", "Bush"),
    ("Frederic S. Mishkin", "2006-09-05", "2008-08-31", "Governor", "Bush"),
    ("Randall S. Kroszner", "2006-03-01", "2009-01-21", "Governor", "Bush"),
    ("Elizabeth A. Duke", "2008-08-05", "2013-08-31", "Governor", "Bush"),

    # Greenspan era (selected)
    ("Alan Greenspan", "1987-08-11", "2006-01-31", "Chair", "Reagan"),
    ("Roger W. Ferguson Jr.", "1999-11-05", "2006-04-28", "Vice Chair", "Clinton"),
    ("Roger W. Ferguson Jr.", "1997-11-05", "1999-11-04", "Governor", "Clinton"),
    ("Mark W. Olson", "2001-12-07", "2006-06-30", "Governor", "Bush"),
    ("Susan Schmidt Bies", "2001-12-07", "2007-03-30", "Governor", "Bush"),
    ("Edward M. Gramlich", "1997-11-05", "2005-08-31", "Governor", "Clinton"),
    ("Laurence H. Meyer", "1996-06-24", "2002-01-31", "Governor", "Clinton"),
    ("Edward W. Kelley Jr.", "1987-05-26", "2001-12-31", "Governor", "Reagan"),
    ("Alice M. Rivlin", "1996-06-25", "1999-07-16", "Vice Chair", "Clinton"),

    # Earlier governors (1990s)
    ("Lawrence B. Lindsey", "1991-11-26", "1997-02-05", "Governor", "Bush"),
    ("Susan M. Phillips", "1991-12-02", "1998-06-30", "Governor", "Bush"),
    ("John P. LaWare", "1988-08-15", "1995-04-30", "Governor", "Reagan"),
    ("David W. Mullins Jr.", "1990-05-21", "1994-02-14", "Vice Chair", "Bush"),
    ("Wayne D. Angell", "1986-02-07", "1994-02-09", "Governor", "Reagan"),
    ("Manuel H. Johnson", "1986-02-07", "1990-08-03", "Vice Chair", "Reagan"),

    # 1980s
    ("Martha R. Seger", "1984-07-02", "1991-03-11", "Governor", "Reagan"),
    ("H. Robert Heller", "1986-08-19", "1989-07-31", "Governor", "Reagan"),
    ("Preston Martin", "1982-03-31", "1986-04-30", "Vice Chair", "Reagan"),
    ("Emmett J. Rice", "1979-06-20", "1986-12-31", "Governor", "Carter"),
    ("Lyle E. Gramley", "1980-05-28", "1985-09-01", "Governor", "Carter"),
    ("J. Charles Partee", "1976-01-05", "1986-02-07", "Governor", "Ford"),
    ("Henry C. Wallich", "1974-03-08", "1986-12-15", "Governor", "Nixon"),
    ("Paul A. Volcker", "1979-08-06", "1987-08-11", "Chair", "Carter"),
    ("Frederick H. Schultz", "1979-07-27", "1982-02-11", "Vice Chair", "Carter"),
    ("Nancy H. Teeters", "1978-09-18", "1984-06-27", "Governor", "Carter"),
    ("Philip E. Coldwell", "1974-10-29", "1980-02-29", "Governor", "Ford"),
    ("Philip C. Jackson Jr.", "1975-07-14", "1978-11-17", "Governor", "Ford"),

    # 1970s
    ("Arthur F. Burns", "1970-02-01", "1978-01-31", "Chair", "Nixon"),
    ("George W. Mitchell", "1961-08-31", "1976-02-13", "Vice Chair", "Kennedy"),
    ("Robert C. Holland", "1973-06-11", "1976-05-15", "Governor", "Nixon"),
    ("Jeffrey M. Bucher", "1972-06-05", "1976-01-02", "Governor", "Nixon"),
    ("John E. Sheehan", "1972-01-04", "1975-06-01", "Governor", "Nixon"),
    ("Andrew F. Brimmer", "1966-03-09", "1974-08-31", "Governor", "Johnson"),
    ("Sherman J. Maisel", "1965-04-30", "1972-05-31", "Governor", "Johnson"),
]

# ============================================================================
# FEDERAL RESERVE BANK PRESIDENTS (FOMC voting members)
# NY Fed always votes; others rotate
# ============================================================================

FED_BANK_PRESIDENTS = [
    # New York Fed (always votes)
    ("John C. Williams", "2018-06-18", None, "New York", True),
    ("William C. Dudley", "2009-01-27", "2018-06-17", "New York", True),
    ("Timothy F. Geithner", "2003-11-17", "2009-01-26", "New York", True),
    ("William J. McDonough", "1993-07-19", "2003-07-10", "New York", True),
    ("E. Gerald Corrigan", "1985-01-01", "1993-07-18", "New York", True),

    # Other Fed Bank Presidents (selected, major recent ones)
    ("Raphael Bostic", "2017-06-05", None, "Atlanta", False),
    ("Austan Goolsbee", "2023-01-09", None, "Chicago", False),
    ("Loretta J. Mester", "2014-06-01", "2024-06-30", "Cleveland", False),
    ("Lorie K. Logan", "2022-08-22", None, "Dallas", False),
    ("Thomas I. Barkin", "2018-01-01", None, "Richmond", False),
    ("Mary C. Daly", "2018-10-01", None, "San Francisco", False),
    ("Patrick T. Harker", "2015-07-01", None, "Philadelphia", False),
    ("Neel Kashkari", "2016-01-01", None, "Minneapolis", False),
    ("James Bullard", "2008-04-01", "2023-08-14", "St. Louis", False),
    ("Esther L. George", "2011-10-01", "2023-01-31", "Kansas City", False),
    ("Charles L. Evans", "2007-09-01", "2023-01-31", "Chicago", False),
    ("Eric S. Rosengren", "2007-07-23", "2021-09-30", "Boston", False),
    ("Robert S. Kaplan", "2015-09-08", "2021-10-08", "Dallas", False),
    ("John C. Williams", "2011-03-01", "2018-06-17", "San Francisco", False),
    ("Jeffrey M. Lacker", "2004-08-01", "2017-10-04", "Richmond", False),
    ("Dennis P. Lockhart", "2007-03-01", "2017-02-28", "Atlanta", False),
    ("Narayana Kocherlakota", "2009-10-08", "2015-12-31", "Minneapolis", False),
    ("Sandra Pianalto", "2003-02-01", "2014-05-31", "Cleveland", False),
    ("Richard W. Fisher", "2005-04-04", "2015-03-19", "Dallas", False),
    ("Charles I. Plosser", "2006-08-01", "2015-03-01", "Philadelphia", False),
    ("Janet L. Yellen", "2004-06-14", "2010-10-04", "San Francisco", False),
    ("Gary H. Stern", "1985-03-18", "2009-10-07", "Minneapolis", False),
    ("Michael H. Moskow", "1994-09-01", "2007-08-31", "Chicago", False),
    ("Anthony M. Santomero", "2000-07-28", "2006-04-02", "Philadelphia", False),
    ("J. Alfred Broaddus Jr.", "1993-01-01", "2004-07-31", "Richmond", False),
    ("Robert D. McTeer Jr.", "1991-02-01", "2004-11-04", "Dallas", False),
    ("William Poole", "1998-03-23", "2008-03-31", "St. Louis", False),
    ("Jack Guynn", "1996-01-01", "2006-10-01", "Atlanta", False),
    ("Cathy E. Minehan", "1994-07-25", "2007-07-23", "Boston", False),
    ("Thomas M. Hoenig", "1991-10-01", "2011-09-30", "Kansas City", False),
]

# ============================================================================
# HAWK/DOVE CLASSIFICATION (approximate, based on public statements)
# Scale: -2 (strong dove) to +2 (strong hawk)
# ============================================================================

POLICY_STANCE = {
    # Current/recent
    "Jerome H. Powell": 0,  # Centrist/pragmatic
    "Philip N. Jefferson": 0,
    "Michelle W. Bowman": 1,  # Leans hawk
    "Lisa D. Cook": -1,  # Leans dove
    "Christopher J. Waller": 1,  # Hawk
    "Lael Brainard": -1,  # Dove
    "Richard H. Clarida": 0,

    # Bernanke era
    "Ben S. Bernanke": -1,  # Accommodative
    "Janet L. Yellen": -1,  # Dove
    "Daniel K. Tarullo": -1,
    "Stanley Fischer": 0,
    "Donald L. Kohn": 0,

    # Greenspan era - Governors
    "Alan Greenspan": 0,   # Pragmatist
    "Roger W. Ferguson Jr.": 0,
    "Wayne D. Angell": 2,          # Strong inflation hawk
    "Manuel H. Johnson": -1,       # Accommodative, often pushed for cuts
    "Martha R. Seger": -2,         # Strong dove, frequently dissented for lower rates
    "Edward W. Kelley Jr.": 0,     # Centrist
    "John P. LaWare": 0,           # Centrist
    "David W. Mullins Jr.": 0,     # Centrist/pragmatic
    "Lawrence B. Lindsey": 1,      # Supply-sider, leaned hawkish on inflation
    "Susan M. Phillips": 0,        # Centrist
    "Alice M. Rivlin": 0,          # Pragmatist/centrist
    "Laurence H. Meyer": 1,        # Inflation hawk, favored pre-emptive tightening
    "Edward M. Gramlich": -1,      # Dove, concerned with employment
    "Mark W. Olson": 0,            # Centrist
    "Susan Schmidt Bies": 0,       # Centrist
    "Randall S. Kroszner": 1,      # Leans hawk
    "Kevin M. Warsh": 1,           # Hawk

    # Volcker
    "Paul A. Volcker": 2,  # Famous hawk

    # Bank presidents - Greenspan era
    "E. Gerald Corrigan": 0,        # NY Fed, pragmatist
    "William J. McDonough": 0,      # NY Fed, pragmatist
    "Gary H. Stern": 1,             # Moderate hawk, inflation-focused
    "Robert D. McTeer Jr.": -1,     # Dove, "lone ranger" dissenter for cuts
    "Thomas M. Hoenig": 2,          # Strong hawk (already in list below but adding here)
    "William Poole": 2,             # Strong hawk
    "J. Alfred Broaddus Jr.": 2,    # Strong hawk
    "Cathy E. Minehan": 1,          # Moderate hawk
    "Michael H. Moskow": 1,         # Moderate hawk
    "Anthony M. Santomero": 0,      # Centrist
    "Jack Guynn": 0,                # Centrist
    "Timothy F. Geithner": 0,       # Pragmatist/centrist

    # Bank presidents - modern (already included below, kept for completeness)
    "James Bullard": 1,
    "Esther L. George": 2,  # Strong hawk
    "Neel Kashkari": -1,  # Dove
    "Charles L. Evans": -1,  # Dove
    "Loretta J. Mester": 1,  # Hawk
    "John C. Williams": 0,
    "Eric S. Rosengren": 0,
    "Richard W. Fisher": 2,  # Strong hawk
    "Charles I. Plosser": 2,  # Strong hawk
    "Thomas M. Hoenig": 2,  # Strong hawk
    "Narayana Kocherlakota": -1,
}


def parse_date(date_str):
    """Parse date string or return far future for None."""
    if date_str is None:
        return datetime(2099, 12, 31)
    return datetime.strptime(date_str, "%Y-%m-%d")


def get_governors_on_date(target_date: datetime) -> list:
    """Get all Fed governors serving on a specific date."""
    serving = []

    for name, start, end, position, appointed_by in FED_GOVERNORS:
        start_dt = parse_date(start)
        end_dt = parse_date(end)

        if start_dt <= target_date <= end_dt:
            serving.append({
                'name': name,
                'position': position,
                'appointed_by': appointed_by,
                'stance': POLICY_STANCE.get(name, 0),
                'type': 'Governor'
            })

    return serving


def get_bank_presidents_on_date(target_date: datetime) -> list:
    """Get all Fed Bank presidents serving on a specific date."""
    serving = []

    for name, start, end, bank, always_votes in FED_BANK_PRESIDENTS:
        start_dt = parse_date(start)
        end_dt = parse_date(end)

        if start_dt <= target_date <= end_dt:
            serving.append({
                'name': name,
                'bank': bank,
                'always_votes': always_votes,
                'stance': POLICY_STANCE.get(name, 0),
                'type': 'Bank President'
            })

    return serving


def get_fomc_composition(target_date: datetime) -> dict:
    """Get full FOMC composition for a date."""
    governors = get_governors_on_date(target_date)
    presidents = get_bank_presidents_on_date(target_date)

    # Find the chair
    chair = next((g for g in governors if g['position'] == 'Chair'), None)

    # Calculate aggregate hawk/dove score
    all_members = governors + presidents
    if all_members:
        avg_stance = sum(m['stance'] for m in all_members) / len(all_members)
    else:
        avg_stance = 0

    return {
        'date': target_date.strftime('%Y-%m-%d'),
        'chair': chair['name'] if chair else 'Unknown',
        'num_governors': len(governors),
        'num_presidents': len(presidents),
        'governors': governors,
        'bank_presidents': presidents,
        'avg_policy_stance': round(avg_stance, 2),
        'hawk_count': sum(1 for m in all_members if m['stance'] > 0),
        'dove_count': sum(1 for m in all_members if m['stance'] < 0),
    }


def load_fomc_decisions(filepath: str = "fomc_decisions.csv") -> pd.DataFrame:
    """Load FOMC decisions from previously generated file."""
    path = Path(filepath)

    if not path.exists():
        # Try alternate location
        alt_path = Path("fed_funds_target_daily.csv")
        if alt_path.exists():
            print(f"Using {alt_path} instead")
            df = pd.read_csv(alt_path, index_col=0, parse_dates=True)
            # Filter to only rate change dates
            df['change'] = df['target_rate_unified'].diff().abs()
            df = df[df['change'] > 0.001]
            return df
        else:
            print(f"Warning: {filepath} not found. Run fetch_fomc_decisions.py first.")
            return pd.DataFrame()

    return pd.read_csv(path, index_col=0, parse_dates=True)


def main():
    print("=" * 60)
    print("FOMC Membership Mapper")
    print("=" * 60)

    # Load decisions
    print("\nLoading FOMC decisions...")
    decisions = load_fomc_decisions()

    if len(decisions) == 0:
        print("\nNo decisions loaded. Creating sample with recent FOMC dates...")
        # Use some sample dates if no data file exists
        sample_dates = [
            "2024-01-31", "2023-12-13", "2023-11-01", "2023-09-20",
            "2023-07-26", "2023-06-14", "2023-05-03", "2023-03-22",
            "2022-12-14", "2022-11-02", "2022-09-21", "2022-07-27",
            "2020-03-15", "2019-10-30", "2018-12-19", "2015-12-16",
            "2008-12-16", "2006-06-29", "2000-05-16", "1994-02-04",
        ]
        decisions = pd.DataFrame({'date': sample_dates})
        decisions['date'] = pd.to_datetime(decisions['date'])
        decisions = decisions.set_index('date')

    print(f"Processing {len(decisions)} decision dates...")

    # Map each decision to FOMC composition
    results = []

    for date in decisions.index:
        if isinstance(date, str):
            date = datetime.strptime(date, '%Y-%m-%d')

        composition = get_fomc_composition(date)

        # Flatten for CSV output
        row = {
            'date': composition['date'],
            'chair': composition['chair'],
            'num_governors': composition['num_governors'],
            'num_bank_presidents': composition['num_presidents'],
            'avg_policy_stance': composition['avg_policy_stance'],
            'hawk_count': composition['hawk_count'],
            'dove_count': composition['dove_count'],
            'governor_names': '|'.join([g['name'] for g in composition['governors']]),
            'president_names': '|'.join([p['name'] for p in composition['bank_presidents']]),
        }

        results.append(row)
        print(f"  {composition['date']}: Chair={composition['chair']}, "
              f"Hawks={composition['hawk_count']}, Doves={composition['dove_count']}")

    # Create output DataFrame
    df = pd.DataFrame(results)
    df = df.set_index('date')

    # Save
    output_file = "fomc_membership.csv"
    df.to_csv(output_file)
    print(f"\nSaved to {output_file}")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total decisions mapped: {len(df)}")
    print(f"\nChairs in dataset:")
    print(df['chair'].value_counts())
    print(f"\nPolicy stance distribution:")
    print(df['avg_policy_stance'].describe())

    # Show sample
    print("\n" + "=" * 60)
    print("SAMPLE OUTPUT")
    print("=" * 60)
    print(df.head(10).to_string())


if __name__ == "__main__":
    main()
