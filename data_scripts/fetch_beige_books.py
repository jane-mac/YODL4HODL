"""
Beige Book Fetcher

Downloads the Federal Reserve's Beige Book (Summary of Commentary on
Current Economic Conditions) for all FOMC meetings from 1990 to present.

The Beige Book is published ~2 weeks before each FOMC meeting (8x per year).
Source: federalreserve.gov

Files are saved as-is (HTML or PDF) with no text processing.

Requires: pip install requests beautifulsoup4
"""

import re
import time
import json
from pathlib import Path
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


# ============================================================================
# CONFIGURATION
# ============================================================================

OUTPUT_DIR = Path("fomc_documents") / "beige_books"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://www.federalreserve.gov"
START_YEAR = 1990

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}

REQUEST_DELAY = 1.0  # seconds between requests


# ============================================================================
# LINK DISCOVERY
# ============================================================================

def discover_beige_book_links(session: requests.Session) -> list:
    """
    Discover all Beige Book links from the Fed's archive pages.
    Returns list of dicts with 'url', 'date', and 'year'.
    """
    print("\n" + "=" * 60)
    print("Discovering Beige Book links")
    print("=" * 60)

    all_links = []
    seen_urls = set()

    # --- Current/recent books (calendar page) ---
    calendar_url = f"{BASE_URL}/monetarypolicy/fomccalendars.htm"
    print(f"\nChecking calendar page: {calendar_url}")
    try:
        resp = session.get(calendar_url)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, 'html.parser')
        for link in soup.find_all('a', href=True):
            text = link.get_text(strip=True).lower()
            href = link['href']
            if 'beige' in text or 'beigebook' in href.lower() or 'beige-book' in href.lower():
                full_url = urljoin(BASE_URL, href)
                if full_url not in seen_urls:
                    seen_urls.add(full_url)
                    all_links.append({'url': full_url, 'source': 'calendar'})
        print(f"  Found {len(all_links)} links on calendar page")
    except Exception as e:
        print(f"  Warning: {e}")

    time.sleep(REQUEST_DELAY)

    # --- Historical year pages ---
    current_year = datetime.now().year
    print(f"\nScraping historical year pages ({START_YEAR}–{current_year})...")

    for year in range(START_YEAR, current_year + 1):
        hist_url = f"{BASE_URL}/monetarypolicy/fomchistorical{year}.htm"
        try:
            resp = session.get(hist_url)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.content, 'html.parser')
            found_this_year = 0
            for link in soup.find_all('a', href=True):
                text = link.get_text(strip=True).lower()
                href = link['href']
                if 'beige' in text or 'beigebook' in href.lower() or 'beige-book' in href.lower():
                    full_url = urljoin(BASE_URL, href)
                    if full_url not in seen_urls:
                        seen_urls.add(full_url)
                        all_links.append({'url': full_url, 'source': f'historical_{year}'})
                        found_this_year += 1
            if found_this_year:
                print(f"  {year}: {found_this_year} links")
        except Exception as e:
            print(f"  {year}: Error - {e}")
        time.sleep(REQUEST_DELAY / 2)

    # --- Dedicated beige book archive page ---
    archive_urls = [
        f"{BASE_URL}/monetarypolicy/beigebook/beigebook.htm",
        f"{BASE_URL}/monetarypolicy/beigebook/",
        f"{BASE_URL}/monetarypolicy/beigebook",
    ]
    for archive_url in archive_urls:
        print(f"\nChecking archive: {archive_url}")
        try:
            resp = session.get(archive_url)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.content, 'html.parser')
            found = 0
            for link in soup.find_all('a', href=True):
                href = link['href']
                full_url = urljoin(BASE_URL, href)
                # Accept any link that points to a beige book page
                if (
                    'beigebook' in full_url.lower()
                    or 'beige-book' in full_url.lower()
                ) and full_url not in seen_urls:
                    seen_urls.add(full_url)
                    all_links.append({'url': full_url, 'source': 'archive'})
                    found += 1
            print(f"  Found {found} additional links")
        except Exception as e:
            print(f"  Warning: {e}")
        time.sleep(REQUEST_DELAY)

    # --- Try URL pattern for years where scraping found nothing ---
    # Beige books published ~Jan, Mar, Apr, Jun, Jul, Sep, Oct, Dec
    # (approx 2 weeks before 8 FOMC meetings)
    # We'll try both known URL patterns and pick up any we missed.
    beige_months = [1, 3, 4, 6, 7, 9, 10, 12]
    pattern_found = 0
    print(f"\nTrying URL patterns for missing years...")
    for year in range(START_YEAR, current_year + 1):
        for month in beige_months:
            yyyymm = f"{year}{month:02d}"
            candidates = [
                f"{BASE_URL}/monetarypolicy/beige-book-{yyyymm}.htm",
                f"{BASE_URL}/monetarypolicy/beigebook/beigebook{yyyymm}.htm",
            ]
            for url in candidates:
                if url not in seen_urls:
                    seen_urls.add(url)
                    all_links.append({'url': url, 'source': 'pattern', 'year': year, 'month': month})
                    pattern_found += 1
    print(f"  Added {pattern_found} pattern-based candidates")

    print(f"\nTotal candidate URLs: {len(all_links)}")
    return all_links


def parse_date_from_url(url: str) -> Optional[str]:
    """Try to extract a YYYY-MM-DD date from a Beige Book URL."""
    # Match 8-digit date like 20240117
    m = re.search(r'(\d{8})', url)
    if m:
        try:
            return datetime.strptime(m.group(1), '%Y%m%d').strftime('%Y-%m-%d')
        except ValueError:
            pass
    # Match 6-digit YYYYMM like beigebook202401
    m = re.search(r'(\d{6})', url)
    if m:
        try:
            return datetime.strptime(m.group(1) + '01', '%Y%m%d').strftime('%Y-%m')
        except ValueError:
            pass
    return None


# ============================================================================
# DOWNLOAD
# ============================================================================

def fetch_beige_books(session: requests.Session, links: list) -> list:
    """
    Download each Beige Book candidate URL and save the raw file (HTML or PDF).
    Skips 404s. Returns list of result dicts.
    """
    print("\n" + "=" * 60)
    print("Downloading Beige Books")
    print("=" * 60)

    results = []
    saved = set()  # track saved dates to avoid duplicates

    for i, entry in enumerate(links, 1):
        url = entry['url']

        try:
            resp = session.get(url, timeout=15)
            if resp.status_code == 404:
                continue  # silently skip pattern candidates that don't exist
            resp.raise_for_status()
        except requests.exceptions.RequestException:
            continue

        # Determine file type and extension
        is_pdf = url.lower().endswith('.pdf') or 'application/pdf' in resp.headers.get('Content-Type', '')
        ext = 'pdf' if is_pdf else 'html'

        # Parse date
        date_str = parse_date_from_url(url)

        # Deduplicate on date
        dedup_key = date_str or url
        if dedup_key in saved:
            continue
        saved.add(dedup_key)

        print(f"  [{len(results)+1}] {date_str or 'unknown'} — {url}")

        # Save raw file
        safe_date = (date_str or 'unknown').replace('-', '')
        filename = f"beige_book_{safe_date}.{ext}"
        file_path = OUTPUT_DIR / filename
        with open(file_path, 'wb') as f:
            f.write(resp.content)

        results.append({
            'date': date_str,
            'url': url,
            'filename': filename,
            'size_bytes': len(resp.content),
        })

        time.sleep(REQUEST_DELAY)

    return results


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 60)
    print("Beige Book Fetcher")
    print(f"Range: {START_YEAR} – {datetime.now().year}")
    print("=" * 60)
    print(f"Output directory: {OUTPUT_DIR.absolute()}")

    session = requests.Session()
    session.headers.update(HEADERS)

    # Discover links
    links = discover_beige_book_links(session)

    # Download
    results = fetch_beige_books(session, links)

    # Sort by date
    results.sort(key=lambda x: x['date'] or '')

    # Save index JSON
    index_path = OUTPUT_DIR / "beige_books_index.json"
    with open(index_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print("\n" + "=" * 60)
    print("COMPLETE")
    print("=" * 60)
    print(f"Beige books downloaded: {len(results)}")
    if results:
        print(f"Date range: {results[0]['date']} to {results[-1]['date']}")
    print(f"Index saved to: {index_path}")
    print(f"Files saved to: {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
