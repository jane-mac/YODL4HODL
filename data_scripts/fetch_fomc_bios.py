"""
FOMC Member Biographical Data Collector

Fetches biographical information for each FOMC member using the Wikipedia API:
  - Universities attended (alma mater)
  - College sports participation
  - Degree types: PhD, JD/Law, MBA
  - Political family connections (relatives in government)

Outputs: raw_data/fomc_bios.csv

NOTE: Sports and political family detection uses keyword matching and is
approximate. The 'needs_review' column flags rows that may need manual checking.
"""

import re
import time
import requests
import pandas as pd
from pathlib import Path

# ---------------------------------------------------------------------------
# All unique FOMC members (governors + bank presidents) from fetch_fomc_members.py
# ---------------------------------------------------------------------------
FOMC_MEMBERS = [
    # Board of Governors
    "Jerome H. Powell",
    "Philip N. Jefferson",
    "Michael S. Barr",
    "Michelle W. Bowman",
    "Lisa D. Cook",
    "Adriana D. Kugler",
    "Christopher J. Waller",
    "Lael Brainard",
    "Richard H. Clarida",
    "Randal K. Quarles",
    "Nellie Liang",
    "Janet L. Yellen",
    "Stanley Fischer",
    "Daniel K. Tarullo",
    "Sarah Bloom Raskin",
    "Jeremy C. Stein",
    "Ben S. Bernanke",
    "Donald L. Kohn",
    "Kevin M. Warsh",
    "Frederic S. Mishkin",
    "Randall S. Kroszner",
    "Elizabeth A. Duke",
    "Alan Greenspan",
    "Roger W. Ferguson Jr.",
    "Mark W. Olson",
    "Susan Schmidt Bies",
    "Edward M. Gramlich",
    "Laurence H. Meyer",
    "Edward W. Kelley Jr.",
    "Alice M. Rivlin",
    "Lawrence B. Lindsey",
    "Susan M. Phillips",
    "John P. LaWare",
    "David W. Mullins Jr.",
    "Wayne D. Angell",
    "Manuel H. Johnson",
    "Martha R. Seger",
    "H. Robert Heller",
    "Preston Martin",
    "Emmett J. Rice",
    "Lyle E. Gramley",
    "J. Charles Partee",
    "Henry C. Wallich",
    "Paul A. Volcker",
    "Frederick H. Schultz",
    "Nancy H. Teeters",
    "Philip E. Coldwell",
    "Philip C. Jackson Jr.",
    "Arthur F. Burns",
    "George W. Mitchell",
    "Robert C. Holland",
    "Jeffrey M. Bucher",
    "John E. Sheehan",
    "Andrew F. Brimmer",
    "Sherman J. Maisel",
    # Federal Reserve Bank Presidents
    "John C. Williams",
    "William C. Dudley",
    "Timothy F. Geithner",
    "William J. McDonough",
    "E. Gerald Corrigan",
    "Raphael Bostic",
    "Austan Goolsbee",
    "Loretta J. Mester",
    "Lorie K. Logan",
    "Thomas I. Barkin",
    "Mary C. Daly",
    "Patrick T. Harker",
    "Neel Kashkari",
    "James Bullard",
    "Esther L. George",
    "Charles L. Evans",
    "Eric S. Rosengren",
    "Robert S. Kaplan",
    "Jeffrey M. Lacker",
    "Dennis P. Lockhart",
    "Narayana Kocherlakota",
    "Sandra Pianalto",
    "Richard W. Fisher",
    "Charles I. Plosser",
    "Gary H. Stern",
    "Michael H. Moskow",
    "Anthony M. Santomero",
    "J. Alfred Broaddus Jr.",
    "Robert D. McTeer Jr.",
    "William Poole",
    "Jack Guynn",
    "Cathy E. Minehan",
    "Thomas M. Hoenig",
]

WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
HEADERS = {"User-Agent": "FOMCBioCollector/1.0 jane_mac@mit.edu"}

# Degree detection: regex patterns to search in full wikitext + categories
DEGREE_PATTERNS = {
    "has_phd": [
        r'\bPh\.?D\.?\b',
        r'\bDoctor of Philosophy\b',
        r'\bD\.Phil\.?\b',
        r'\bdoctoral\b',
    ],
    "has_jd_law": [
        r'\bJ\.D\.?\b',
        r'\bJuris Doctor\b',
        r'\blaw degree\b',
        r'\bLL\.B\.?\b',
        r'\bLL\.M\.?\b',
    ],
    "has_mba": [
        r'\bM\.B\.A\.?\b',
        r'\bMaster of Business Administration\b',
    ],
}

# Sports to look for in context near college/university/varsity references
SPORTS_LIST = [
    "football", "basketball", "baseball", "tennis", "track", "cross.country",
    "swimming", "soccer", "hockey", "wrestling", "golf", "lacrosse",
    "crew", "rowing", "squash", "volleyball", "rugby",
]

# Political roles to detect in family connections
POLITICAL_ROLES = (
    r"senator|congressman|congresswoman|representative|governor|"
    r"mayor|president|judge|justice|secretary|ambassador|attorney general|"
    r"treasurer|comptroller|legislator|assemblyman|assemblywoman|"
    r"state senator|state representative|cabinet"
)

# Family relationship words
FAMILY_WORDS = (
    r"father|mother|uncle|aunt|brother|sister|son|daughter|"
    r"grandfather|grandmother|wife|husband|spouse|parent|cousin|nephew|niece|"
    r"in-law|father-in-law|mother-in-law"
)


# ---------------------------------------------------------------------------
# Wikipedia helpers
# ---------------------------------------------------------------------------

def search_wikipedia(name: str) -> str:
    """Return the most likely Wikipedia page title for a person's name."""
    params = {
        "action": "opensearch",
        "search": name,
        "limit": 5,
        "format": "json",
        "redirects": "resolve",
    }
    try:
        r = requests.get(WIKIPEDIA_API, params=params, headers=HEADERS, timeout=10)
        r.raise_for_status()
        results = r.json()
        if results and results[1]:
            return results[1][0]
    except Exception as e:
        print(f"    [search error] {e}")
    return ""


def fetch_page(title: str) -> tuple:
    """Return (wikitext, categories_list) for a Wikipedia title."""
    params = {
        "action": "parse",
        "page": title,
        "prop": "wikitext|categories",
        "format": "json",
    }
    try:
        r = requests.get(WIKIPEDIA_API, params=params, headers=HEADERS, timeout=15)
        r.raise_for_status()
        data = r.json()
        if "parse" in data:
            wikitext = data["parse"].get("wikitext", {}).get("*", "")
            categories = [c["*"] for c in data["parse"].get("categories", [])]
            return wikitext, categories
    except Exception as e:
        print(f"    [fetch error] {e}")
    return "", []


# ---------------------------------------------------------------------------
# Extraction helpers
# ---------------------------------------------------------------------------

def clean_wikitext(text: str) -> str:
    """Strip common wiki markup from a string."""
    # [[link|display]] -> display, [[link]] -> link
    text = re.sub(r'\[\[(?:[^|\]]*\|)?([^\]]+)\]\]', r'\1', text)
    # {{template|...}} -> remove
    text = re.sub(r'\{\{[^}]*\}\}', '', text)
    # Remove bold/italic markers
    text = re.sub(r"'{2,3}", '', text)
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    # Collapse whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def extract_infobox_field(wikitext: str, field: str) -> str:
    """Pull a named field out of a Wikipedia infobox."""
    # Match the field up to the next | or }} (handles multi-line values)
    pattern = rf'\|\s*{re.escape(field)}\s*=\s*((?:[^|{{}}]|\n(?!\s*\|))+)'
    m = re.search(pattern, wikitext, re.IGNORECASE)
    if m:
        raw = m.group(1).strip()
        return clean_wikitext(raw)
    return ""


def extract_universities(wikitext: str, categories: list) -> str:
    """
    Combine alma_mater infobox field + alumni categories to build a
    semicolon-separated list of universities attended.
    """
    universities = []

    # 1. Infobox alma_mater field
    alma = extract_infobox_field(wikitext, "alma_mater")
    if alma:
        # Split on common list delimiters used in wikitext
        for part in re.split(r'[;•*\n]|<br\s*/?>', alma):
            part = part.strip(' ,')
            if part and len(part) > 3:
                # Exclude lone degree abbreviations
                if not re.fullmatch(r'[A-Z][A-Za-z.]{1,5}', part):
                    universities.append(part)

    # 2. Wikipedia "XYZ alumni" categories
    for cat in categories:
        m = re.match(r'^(.+?)\s+[Aa]lumni$', cat)
        if m:
            univ = m.group(1).replace('_', ' ')
            # Skip generic entries
            if re.search(r'^(Former|American|Living|People)', univ):
                continue
            if len(univ) < 5:
                continue
            universities.append(univ)

    # Deduplicate preserving order, case-insensitively
    seen = set()
    deduped = []
    for u in universities:
        key = u.lower()
        if key not in seen:
            seen.add(key)
            deduped.append(u)

    return "; ".join(deduped)


def detect_degrees(wikitext: str, categories: list) -> dict:
    """Return {degree_key: bool} by scanning wikitext and categories."""
    full_text = wikitext + " " + " ".join(categories)
    return {
        key: any(re.search(pat, full_text, re.IGNORECASE) for pat in patterns)
        for key, patterns in DEGREE_PATTERNS.items()
    }


def detect_college_sports(wikitext: str) -> str:
    """
    Look for sports played at college level.
    Searches a window of text near 'college', 'varsity', 'university', etc.
    Returns semicolon-separated sport names or empty string.
    """
    # Pull up to 4000 chars from early-life / education sections
    match = re.search(
        r'(?:early life|education|biography|personal life|background).{0,4000}',
        wikitext, re.IGNORECASE | re.DOTALL
    )
    search_text = match.group(0) if match else wikitext[:4000]

    found = []
    college_context = re.compile(
        r'(?:college|university|varsity|undergraduate|student|team|athletic|played)',
        re.IGNORECASE
    )

    for sport in SPORTS_LIST:
        sport_re = re.compile(rf'\b{sport}\b', re.IGNORECASE)
        for m in sport_re.finditer(search_text):
            # Check if there's a college-context word within 150 chars
            window_start = max(0, m.start() - 150)
            window_end = min(len(search_text), m.end() + 150)
            window = search_text[window_start:window_end]
            if college_context.search(window):
                found.append(sport.replace('.', ''))
                break

    return "; ".join(dict.fromkeys(found))  # dedup, preserve order


def detect_political_family(wikitext: str) -> str:
    """
    Scan for mentions of family members holding government/political roles.
    Returns up to 3 cleaned text snippets describing any connections found.
    """
    # Focus on personal life / family / early life sections
    match = re.search(
        r'(?:personal life|family|early life|biography).{0,5000}',
        wikitext, re.IGNORECASE | re.DOTALL
    )
    search_text = match.group(0) if match else wikitext[:6000]

    # Pattern: family word near political role (within ~80 chars either direction)
    pattern = re.compile(
        rf'(?:{FAMILY_WORDS}).{{0,80}}(?:{POLITICAL_ROLES})'
        rf'|(?:{POLITICAL_ROLES}).{{0,80}}(?:{FAMILY_WORDS})',
        re.IGNORECASE
    )

    snippets = []
    seen = set()
    for m in pattern.finditer(search_text):
        # Grab a bit of context around the match
        start = max(0, m.start() - 20)
        end = min(len(search_text), m.end() + 20)
        raw = search_text[start:end]
        cleaned = clean_wikitext(raw).strip()
        # Skip very short or duplicate snippets
        if len(cleaned) > 20 and cleaned not in seen:
            seen.add(cleaned)
            snippets.append(cleaned)
        if len(snippets) >= 3:
            break

    return "; ".join(snippets)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def process_member(name: str) -> dict:
    """Fetch Wikipedia data and extract bio fields for one FOMC member."""
    row = {
        "name": name,
        "wikipedia_title": "",
        "universities": "",
        "has_phd": False,
        "has_jd_law": False,
        "has_mba": False,
        "college_sports": "",
        "political_family_connections": "",
        "needs_review": "",
    }

    title = search_wikipedia(name)
    if not title:
        row["needs_review"] = "Wikipedia page not found"
        return row

    row["wikipedia_title"] = title

    wikitext, categories = fetch_page(title)
    if not wikitext:
        row["needs_review"] = "Could not fetch page content"
        return row

    # Warn if the page title doesn't look like the right person
    # (e.g. disambiguation page or wrong person)
    if "disambiguation" in " ".join(categories).lower():
        row["needs_review"] = f"Disambiguation page — check manually: {title}"
        return row

    row["universities"] = extract_universities(wikitext, categories)
    row.update(detect_degrees(wikitext, categories))
    row["college_sports"] = detect_college_sports(wikitext)
    row["political_family_connections"] = detect_political_family(wikitext)

    # Flag if no universities found (may need manual lookup)
    if not row["universities"]:
        row["needs_review"] = "No university data found — check manually"

    return row


def main():
    print("=" * 60)
    print("FOMC Member Biographical Data Collector")
    print("=" * 60)

    unique_members = list(dict.fromkeys(FOMC_MEMBERS))
    print(f"\nProcessing {len(unique_members)} unique FOMC members...\n")

    results = []
    for i, name in enumerate(unique_members, 1):
        print(f"[{i:>2}/{len(unique_members)}] {name}")
        data = process_member(name)
        results.append(data)
        time.sleep(0.4)   # be polite to Wikipedia's servers

    df = pd.DataFrame(results)

    output_path = Path("raw_data/fomc_bios.csv")
    df.to_csv(output_path, index=False)
    print(f"\nSaved to {output_path}")

    # Summary stats
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total members processed : {len(df)}")
    print(f"Has PhD                 : {df['has_phd'].sum()}")
    print(f"Has JD / Law degree     : {df['has_jd_law'].sum()}")
    print(f"Has MBA                 : {df['has_mba'].sum()}")
    print(f"College sports detected : {(df['college_sports'] != '').sum()}")
    print(f"Political family links  : {(df['political_family_connections'] != '').sum()}")
    print(f"Needs manual review     : {(df['needs_review'] != '').sum()}")

    print("\nSample (first 10 rows):")
    display_cols = ["name", "universities", "has_phd", "has_jd_law", "has_mba", "college_sports"]
    print(df[display_cols].head(10).to_string(index=False))


if __name__ == "__main__":
    main()
