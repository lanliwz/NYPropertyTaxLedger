from __future__ import annotations

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
