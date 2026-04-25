from __future__ import annotations

import re
from pathlib import Path

import pdfplumber


def extract_pdf_tables(file_path: str | Path) -> dict:
    """Extract table-oriented PDF content into a simple JSON-serializable shape."""
    pdf_path = Path(file_path)
    pdf_data = {"source_file": str(pdf_path), "pages": []}

    with pdfplumber.open(pdf_path) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            page_data = {
                "page_number": page_number,
                "tables": [table for table in page.extract_tables() if table],
            }
            pdf_data["pages"].append(page_data)

    return pdf_data


def extract_pdf_text(file_path: str | Path) -> str:
    """Extract concatenated page text for deterministic metadata parsing."""
    pdf_path = Path(file_path)
    pages: list[str] = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            if page_text:
                pages.append(page_text)

    return "\n".join(pages)


def extract_property_metadata(file_path: str | Path) -> dict[str, str]:
    """Extract stable property metadata fields that should live on Property."""
    text = extract_pdf_text(file_path)

    def match_group(pattern: str) -> str | None:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            return None
        return match.group(1).strip()

    metadata: dict[str, str] = {}
    address = match_group(r"PHYSICAL ADDRESS:\s*(.+?)\s+OWNER NAME:")
    sctm = match_group(r"SCTM:\s*([0-9.\-]+)")
    item_number = match_group(r"ITEM NUMBER:\s*([0-9]+)")

    if address:
        metadata["address"] = address
    if sctm:
        metadata["sctm"] = sctm
    if item_number:
        metadata["itemNumber"] = item_number

    return metadata
