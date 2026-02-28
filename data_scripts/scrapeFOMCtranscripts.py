#!/usr/bin/env python3
"""
FOMC Transcript Downloader
Downloads all PDF transcripts from the FRASER website
"""
import dropbox
import requests
from bs4 import BeautifulSoup
import time
import os
from pathlib import Path
from urllib.parse import urljoin
import pymupdf

# Create output directory
OUTPUT_DIR = Path("fomc_transcripts")
OUTPUT_DIR.mkdir(exist_ok=True)
DROPBOX_ACCESS_TOKEN = os.environ["DROPBOX_TOKEN"]

# Browser-like headers to avoid 403 errors
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:147.0) Gecko/20100101 Firefox/147.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Cache-Control': 'max-age=0'
}


def upload_to_dropbox(local_path, dropbox_path=None):
    """
    Upload a local file to Dropbox.
    
    local_path   - path to the file on your laptop
    dropbox_path - destination path in Dropbox (defaults to /filename)
    """
    if dropbox_path is None:
        dropbox_path = "/" + os.path.basename(local_path)

    dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)

    with open(local_path, "rb") as f:
        dbx.files_upload(
            f.read(),
            dropbox_path,
            mode=dropbox.files.WriteMode("overwrite")  # or "add" to avoid overwriting
        )
    print(f"Uploaded {local_path} → Dropbox:{dropbox_path}")

# Example usage
upload_to_dropbox("/Users/you/output/report.csv", "/reports/report.csv")

def get_meeting_links(url, session):
    """Extract all meeting links from the browse page"""
    print(f"Fetching browse page: {url}")
    
    response = session.get(url)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find all meeting links - they have class "list-item" and href to meeting pages
    meeting_links = []
    for link in soup.find_all('a', class_='list-item'):
        href = link.get('href')
        if href and '/meeting-' in href:
            full_url = urljoin(url, href)
            last = full_url.rfind('-')
            dataNum = full_url[last+1:]
            print(dataNum)
            
            meeting_links.append(url+'#'+dataNum)
    
    print(f"  Found {len(meeting_links)} meetings")
    return meeting_links

def get_pdf_links_from_meeting(meeting_url, session):
    """Extract PDF links from a specific meeting page"""
    try:
        response = session.get(meeting_url)
        response.raise_for_status()
        
        # Look for PDF URLs in the JSON metadata embedded in the page
        content = response.text
        pdf_links = []
        
        # Use regex to find all URLs ending in .pdf in the metadata
        import re
        # Pattern to match URLs in the metadata that end in .pdf
        pattern = r'http:\\/\\/fraser\.stlouisfed\.org\\/docs\\/historical\\/FOMC\\/meetingdocuments\\/[^"]*\.pdf'
        
        matches = re.findall(pattern, content)
        
        for match in matches:
            # Unescape the URL (replace \/ with /)
            clean_url = match.replace('\\/', '/')
            # Convert http to https
            clean_url = clean_url.replace('http://', 'https://')
            pdf_links.append(clean_url)
        
        # Remove duplicates while preserving order
        pdf_links = list(dict.fromkeys(pdf_links))
        
        return pdf_links
    except Exception as e:
        print(f"  Error fetching meeting page {meeting_url}: {e}")
        return []

def download_pdf(session, url, output_dir):
    """Download a single PDF file"""
    filename = url.split('/')[-1]
    filepath = output_dir / filename
    
    
    # Skip if already downloaded
    if filepath.exists():
        print(f"  ✓ Already exists: {filename}")
        return True
    
    try:
        print(f"  Downloading: {filename}")
        
        # Add referer header for the specific PDF request
        headers = HEADERS.copy()
        headers['Referer'] = 'https://fraser.stlouisfed.org/title/federal-open-market-committee-meeting-minutes-transcripts-documents-677'
        
        response = session.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Save the PDF
        with open(filepath, 'wb') as f:
            f.write(response.content)
        
        print(f"  ✓ Downloaded: {filename} ({len(response.content)} bytes)")
        return True
        
    except Exception as e:
        print(f"  ✗ Error downloading {filename}: {e}")
        return False

def main():
    # Base URL - you can modify this to target different decades
    base_url = "https://fraser.stlouisfed.org/title/federal-open-market-committee-meeting-minutes-transcripts-documents-677"
    
    # Get different decade pages if needed
#        '2000s', '2010s', '2020s', '1990s', '1980s', '1970s', '1960s', '1950s', '1940s',
    decades = ['1930s']
    
    print("FOMC Transcript Downloader")
    print("=" * 50)
    print(f"Output directory: {OUTPUT_DIR.absolute()}\n")
    
    # Create session
    session = requests.Session()
    session.headers.update(HEADERS)
    
    all_pdf_links = set()  # Use set to avoid duplicates
    
    # Step 1: Collect all meeting links from all decades
    print("Step 1: Collecting meeting links...")
    all_meeting_links = []
    
    for decade in decades:
        url = f"{base_url}?browse={decade}"
        try:
            meeting_links = get_meeting_links(url, session)
            all_meeting_links.extend(meeting_links)
            time.sleep(1)  # Be nice to the server
        except Exception as e:
            print(f"Error fetching {decade}: {e}")
    
    print(f"\nTotal meetings found: {len(all_meeting_links)}")
    
    # Step 2: Visit each meeting page to get PDF links
    print("\nStep 2: Extracting PDF links from each meeting...")
    meeting_url = all_meeting_links[1]
    i=1
    #for i, meeting_url in enumerate(all_meeting_links, 1):
    print(f"[{i}/{len(all_meeting_links)}] {meeting_url.split('/')[-1]}")
    pdf_links = get_pdf_links_from_meeting(meeting_url, session)
    if pdf_links:
        print(f"  Found {len(pdf_links)} PDF(s)")
        all_pdf_links.update(pdf_links)
    time.sleep(1)  # Be polite
    
    print(f"\nTotal unique PDFs found: {len(all_pdf_links)}")
    print("\nStep 3: Starting downloads...\n")

    onlyMinutestranscripts = list(filter(lambda word: "rg82_fomcminutes" in word, all_pdf_links))
    
    print(onlyMinutestranscripts)

    # Step 3: Download all PDFs
    successful = 0
    failed = 0
    
#    for i, pdf_url in enumerate(sorted(all_pdf_links), 1):
#        print(f"[{i}/{len(all_pdf_links)}]")
#        if download_pdf(session, pdf_url, OUTPUT_DIR):
#            successful += 1
#        else:
#            failed += 1
#        
#        # Be polite - wait between downloads
#        time.sleep(2)
    
    print("\n" + "=" * 50)
    print(f"Download complete!")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Files saved to: {OUTPUT_DIR.absolute()}")

if __name__ == "__main__":
    main()
