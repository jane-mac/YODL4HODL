"""
FOMC Document Fetcher

Downloads and extracts text from:
1. FOMC Statements (1994-present) - from federalreserve.gov
2. FOMC Minutes (1993-present) - from federalreserve.gov
3. Historical transcripts (1936-present) - from FRASER

For a Fed rate prediction model, the most useful are:
- Statements: Released immediately after each meeting
- Minutes: Released ~3 weeks after each meeting

Requires: pip install requests beautifulsoup4 pymupdf
"""

import os
import re
import time
import json
from pathlib import Path
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Optional: for PDF text extraction
try:
    import pymupdf  # PyMuPDF
    HAS_PYMUPDF = True
except ImportError:
    HAS_PYMUPDF = False
    print("Note: pymupdf not installed. PDF text extraction disabled.")
    print("Install with: pip install pymupdf")

# ============================================================================
# CONFIGURATION
# ============================================================================

OUTPUT_DIR = Path("fomc_documents")
OUTPUT_DIR.mkdir(exist_ok=True)

# Subdirectories
STATEMENTS_DIR = OUTPUT_DIR / "statements"
MINUTES_DIR = OUTPUT_DIR / "minutes"
TRANSCRIPTS_DIR = OUTPUT_DIR / "transcripts"

for d in [STATEMENTS_DIR, MINUTES_DIR, TRANSCRIPTS_DIR]:
    d.mkdir(exist_ok=True)

# Browser headers
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}

# Rate limiting
REQUEST_DELAY = 1.0  # seconds between requests


# ============================================================================
# FOMC STATEMENTS (1994-present)
# ============================================================================

def fetch_fomc_statements(session: requests.Session) -> list:
    """
    Fetch all FOMC statements from federalreserve.gov
    Returns list of dicts with date, url, and text content.
    """
    print("\n" + "=" * 60)
    print("Fetching FOMC Statements")
    print("=" * 60)

    statements = []
    base_url = "https://www.federalreserve.gov"

    # The Fed's monetary policy page lists all statements
    calendars_url = f"{base_url}/monetarypolicy/fomccalendars.htm"

    print(f"Fetching calendar page: {calendars_url}")
    response = session.get(calendars_url)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, 'html.parser')

    # Find all statement links
    # Statements are linked with text containing "Statement" or as PDF/HTML links
    statement_links = []

    # Look for links in the calendar tables
    for link in soup.find_all('a', href=True):
        href = link.get('href', '')
        text = link.get_text(strip=True).lower()

        # Match statement links
        if 'statement' in text or 'monetary20' in href or 'fomcpres' in href:
            full_url = urljoin(base_url, href)
            if full_url not in [s['url'] for s in statement_links]:
                statement_links.append({
                    'url': full_url,
                    'link_text': link.get_text(strip=True)
                })

    print(f"Found {len(statement_links)} statement links on calendar page")

    # Also fetch historical statements archive
    historical_url = f"{base_url}/monetarypolicy/fomc_historical_year.htm"
    print(f"Fetching historical archive: {historical_url}")

    try:
        response = session.get(historical_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find year links
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if 'fomchistorical' in href and href.endswith('.htm'):
                year_url = urljoin(base_url, href)
                print(f"  Fetching year page: {year_url}")

                year_response = session.get(year_url)
                year_soup = BeautifulSoup(year_response.content, 'html.parser')

                for stmt_link in year_soup.find_all('a', href=True):
                    stmt_href = stmt_link.get('href', '')
                    stmt_text = stmt_link.get_text(strip=True).lower()

                    if 'statement' in stmt_text or 'press release' in stmt_text:
                        full_url = urljoin(base_url, stmt_href)
                        if full_url not in [s['url'] for s in statement_links]:
                            statement_links.append({
                                'url': full_url,
                                'link_text': stmt_link.get_text(strip=True)
                            })

                time.sleep(REQUEST_DELAY)

    except Exception as e:
        print(f"  Warning: Could not fetch historical archive: {e}")

    print(f"\nTotal statement links found: {len(statement_links)}")

    # Download and extract text from each statement
    for i, stmt in enumerate(statement_links, 1):
        url = stmt['url']
        print(f"[{i}/{len(statement_links)}] {url}")

        try:
            # Extract date from URL
            date_match = re.search(r'(\d{8})', url)
            if date_match:
                date_str = date_match.group(1)
                date = datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
            else:
                date = "unknown"

            # Fetch the statement
            response = session.get(url)
            response.raise_for_status()

            if url.endswith('.pdf'):
                # Save PDF and extract text
                pdf_path = STATEMENTS_DIR / f"statement_{date}.pdf"
                with open(pdf_path, 'wb') as f:
                    f.write(response.content)

                text = extract_text_from_pdf(pdf_path) if HAS_PYMUPDF else ""
            else:
                # Parse HTML
                soup = BeautifulSoup(response.content, 'html.parser')

                # Find the main content div
                content_div = soup.find('div', class_='col-xs-12')
                if content_div:
                    text = content_div.get_text(separator='\n', strip=True)
                else:
                    text = soup.get_text(separator='\n', strip=True)

            statements.append({
                'date': date,
                'url': url,
                'text': text[:50000],  # Truncate very long texts
                'type': 'statement'
            })

            # Save text file
            text_path = STATEMENTS_DIR / f"statement_{date}.txt"
            with open(text_path, 'w', encoding='utf-8') as f:
                f.write(text)

            time.sleep(REQUEST_DELAY)

        except Exception as e:
            print(f"  Error: {e}")

    return statements


# ============================================================================
# FOMC MINUTES (1993-present)
# ============================================================================

def fetch_fomc_minutes(session: requests.Session) -> list:
    """
    Fetch all FOMC minutes from federalreserve.gov
    """
    print("\n" + "=" * 60)
    print("Fetching FOMC Minutes")
    print("=" * 60)

    minutes = []
    base_url = "https://www.federalreserve.gov"

    # Minutes archive page
    archive_url = f"{base_url}/monetarypolicy/fomccalendars.htm"

    print(f"Fetching: {archive_url}")
    response = session.get(archive_url)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, 'html.parser')

    # Find minutes links
    minutes_links = []

    for link in soup.find_all('a', href=True):
        href = link.get('href', '')
        text = link.get_text(strip=True).lower()

        if 'minutes' in text and ('fomcminutes' in href or 'minutes20' in href):
            full_url = urljoin(base_url, href)
            if full_url not in [m['url'] for m in minutes_links]:
                minutes_links.append({
                    'url': full_url,
                    'link_text': link.get_text(strip=True)
                })

    # Also check historical pages
    for year in range(1993, datetime.now().year + 1):
        hist_url = f"{base_url}/monetarypolicy/fomchistorical{year}.htm"
        try:
            response = session.get(hist_url)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')

                for link in soup.find_all('a', href=True):
                    href = link.get('href', '')
                    text = link.get_text(strip=True).lower()

                    if 'minutes' in text:
                        full_url = urljoin(base_url, href)
                        if full_url not in [m['url'] for m in minutes_links]:
                            minutes_links.append({
                                'url': full_url,
                                'link_text': link.get_text(strip=True)
                            })

            time.sleep(REQUEST_DELAY / 2)
        except:
            pass

    print(f"Found {len(minutes_links)} minutes links")

    # Download and extract
    for i, mins in enumerate(minutes_links, 1):
        url = mins['url']
        print(f"[{i}/{len(minutes_links)}] {url}")

        try:
            date_match = re.search(r'(\d{8})', url)
            if date_match:
                date_str = date_match.group(1)
                date = datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
            else:
                date = f"unknown_{i}"

            response = session.get(url)
            response.raise_for_status()

            if url.endswith('.pdf'):
                pdf_path = MINUTES_DIR / f"minutes_{date}.pdf"
                with open(pdf_path, 'wb') as f:
                    f.write(response.content)
                text = extract_text_from_pdf(pdf_path) if HAS_PYMUPDF else ""
            else:
                soup = BeautifulSoup(response.content, 'html.parser')
                content_div = soup.find('div', class_='col-xs-12')
                if content_div:
                    text = content_div.get_text(separator='\n', strip=True)
                else:
                    text = soup.get_text(separator='\n', strip=True)

            minutes.append({
                'date': date,
                'url': url,
                'text': text[:100000],
                'type': 'minutes'
            })

            text_path = MINUTES_DIR / f"minutes_{date}.txt"
            with open(text_path, 'w', encoding='utf-8') as f:
                f.write(text)

            time.sleep(REQUEST_DELAY)

        except Exception as e:
            print(f"  Error: {e}")

    return minutes


# ============================================================================
# FRASER HISTORICAL TRANSCRIPTS
# ============================================================================

def fetch_fraser_transcripts(session: requests.Session, decades: list = None) -> list:
    """
    Fetch historical FOMC transcripts from FRASER (St. Louis Fed archive)
    These are detailed meeting transcripts released with a 5-year lag.
    """
    print("\n" + "=" * 60)
    print("Fetching FRASER Historical Transcripts")
    print("=" * 60)

    if decades is None:
        # Focus on decades most relevant for modern Fed policy
        decades = ['2010s', '2000s', '1990s', '1980s', '1970s']

    transcripts = []
    base_url = "https://fraser.stlouisfed.org/title/federal-open-market-committee-meeting-minutes-transcripts-documents-677"

    for decade in decades:
        print(f"\nProcessing {decade}...")
        browse_url = f"{base_url}?browse={decade}"

        try:
            response = session.get(browse_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            # Find meeting links
            meeting_links = []
            for link in soup.find_all('a', class_='list-item'):
                href = link.get('href')
                if href and '/meeting-' in href:
                    full_url = urljoin(base_url, href)
                    meeting_links.append(full_url)

            print(f"  Found {len(meeting_links)} meetings")

            # Process each meeting
            for meeting_url in meeting_links[:5]:  # Limit for demo
                time.sleep(REQUEST_DELAY)

                try:
                    response = session.get(meeting_url)
                    content = response.text

                    # Find PDF links in page content
                    pattern = r'https?://fraser\.stlouisfed\.org/docs/historical/FOMC/[^"\'>\s]*\.pdf'
                    pdf_urls = list(set(re.findall(pattern, content)))

                    # Filter for transcripts (not minutes)
                    transcript_pdfs = [u for u in pdf_urls if 'transcript' in u.lower()]

                    for pdf_url in transcript_pdfs:
                        filename = pdf_url.split('/')[-1]
                        pdf_path = TRANSCRIPTS_DIR / filename

                        if not pdf_path.exists():
                            print(f"    Downloading: {filename}")
                            pdf_response = session.get(pdf_url)
                            with open(pdf_path, 'wb') as f:
                                f.write(pdf_response.content)
                            time.sleep(REQUEST_DELAY)

                        # Extract text
                        if HAS_PYMUPDF and pdf_path.exists():
                            text = extract_text_from_pdf(pdf_path)

                            # Parse date from filename
                            date_match = re.search(r'(\d{8})', filename)
                            date = date_match.group(1) if date_match else "unknown"

                            transcripts.append({
                                'date': date,
                                'filename': filename,
                                'text': text[:100000],
                                'type': 'transcript'
                            })

                except Exception as e:
                    print(f"    Error processing meeting: {e}")

        except Exception as e:
            print(f"  Error fetching {decade}: {e}")

        time.sleep(REQUEST_DELAY)

    return transcripts


# ============================================================================
# PDF TEXT EXTRACTION
# ============================================================================

def extract_text_from_pdf(pdf_path: Path) -> str:
    """Extract text from PDF using PyMuPDF"""
    if not HAS_PYMUPDF:
        return ""

    try:
        doc = pymupdf.open(str(pdf_path))
        text = ""
        for page in doc:
            text += page.get_text()
        doc.close()
        return text
    except Exception as e:
        print(f"  Error extracting text from {pdf_path}: {e}")
        return ""


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 60)
    print("FOMC Document Fetcher")
    print("=" * 60)
    print(f"Output directory: {OUTPUT_DIR.absolute()}")
    print(f"PyMuPDF available: {HAS_PYMUPDF}")

    session = requests.Session()
    session.headers.update(HEADERS)

    all_documents = []

    # Fetch statements (most important for rate prediction)
    statements = fetch_fomc_statements(session)
    all_documents.extend(statements)
    print(f"\nFetched {len(statements)} statements")

    # Fetch minutes
    minutes = fetch_fomc_minutes(session)
    all_documents.extend(minutes)
    print(f"Fetched {len(minutes)} minutes")

    # Optionally fetch historical transcripts (large download)
    # Uncomment to enable:
    # transcripts = fetch_fraser_transcripts(session)
    # all_documents.extend(transcripts)

    # Save combined dataset as JSON
    output_file = OUTPUT_DIR / "fomc_documents.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_documents, f, indent=2, ensure_ascii=False)

    print(f"\n" + "=" * 60)
    print("COMPLETE")
    print("=" * 60)
    print(f"Total documents: {len(all_documents)}")
    print(f"  Statements: {len([d for d in all_documents if d['type'] == 'statement'])}")
    print(f"  Minutes: {len([d for d in all_documents if d['type'] == 'minutes'])}")
    print(f"\nOutput files:")
    print(f"  {output_file}")
    print(f"  {STATEMENTS_DIR}/*.txt")
    print(f"  {MINUTES_DIR}/*.txt")


if __name__ == "__main__":
    main()
