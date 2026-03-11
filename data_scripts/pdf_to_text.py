"""
Convert all PDFs in a folder to .txt files.

Usage:
    python3 pdf_to_text.py <folder>

Output folder: <folder>_txt  (created next to the input folder)

Requires: pymupdf  (pip install pymupdf)
"""
## https://github.com/pymupdf/PyMuPDF

import sys
import pymupdf
from pathlib import Path


def pdf_to_text(pdf_path: Path, output_path: Path) -> None:
    doc = pymupdf.open(pdf_path)
    text = "\n".join(page.get_text() for page in doc)
    output_path.write_text(text, encoding="utf-8")


def convert_folder(folder: Path) -> None:
    pdfs = sorted(folder.glob("*.pdf"))
    if not pdfs:
        print(f"No PDF files found in {folder}")
        return

    output_folder = folder.parent / (folder.name + "_txt")
    output_folder.mkdir(exist_ok=True)
    print(f"Output folder: {output_folder}")

    for pdf in pdfs:
        out = output_folder / (pdf.stem + ".txt")
        print(f"  {pdf.name} -> {out.name}")
        pdf_to_text(pdf, out)

    print(f"\nDone. Converted {len(pdfs)} file(s).")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 pdf_to_text.py <folder>")
        sys.exit(1)

    folder = Path(sys.argv[1]).expanduser().resolve()

    if not folder.is_dir():
        print(f"Error: {folder} is not a directory.")
        sys.exit(1)

    convert_folder(folder)
