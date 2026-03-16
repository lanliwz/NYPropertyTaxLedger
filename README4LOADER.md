# README4LOADER

`README4LOADER.md` documents the PDF loader workflow for `NYPropertyTaxLedger`.

## Purpose

The loader imports archived-style property tax bill PDFs into Neo4j database `tax62n`.

Default tax PDF folder:

```text
/Users/weizhang/Downloads/tax-62n
```

The current loader pipeline is implemented in:

- [ny_property_tax_ledger/pdf_extract.py](/Users/weizhang/github/NYPropertyTaxLedger/ny_property_tax_ledger/pdf_extract.py)
- [ny_property_tax_ledger/tax_pdf_to_cypher.py](/Users/weizhang/github/NYPropertyTaxLedger/ny_property_tax_ledger/tax_pdf_to_cypher.py)
- [ny_property_tax_ledger/load_tax_pdfs.py](/Users/weizhang/github/NYPropertyTaxLedger/ny_property_tax_ledger/load_tax_pdfs.py)

It was modernized from the archived workflow in:

- `/Users/weizhang/github/neo4j-onto2ai-toolset/archive/out_of_scope/2026-02-18/tax62_chatbot/taxbill_loader62n.py`
- `/Users/weizhang/github/neo4j-onto2ai-toolset/archive/out_of_scope/2026-02-18/ai_tools/pdf2graph.py`
- `/Users/weizhang/github/neo4j-onto2ai-toolset/archive/out_of_scope/2026-02-18/ai_tools/extract_pdf_data.py`

## Data Flow

1. Read each PDF from the target folder.
2. Extract table data from each page with `pdfplumber`.
3. Infer the canonical tax year from the filename when it contains a year range like `2025-2026`.
4. Send the extracted structure to the configured chat model.
5. Ask the model to generate Cypher for:
   - `TaxStatement`
   - `Levy`
   - `Payment`
   - `Owner`
   - `Property`
6. Normalize and repair common Cypher issues before execution.
7. Split schema statements from data statements.
8. Execute each file in its own Neo4j transaction against database `tax62n`.
9. Append one `LedgerBlock` and multiple `LedgerEntry` nodes as immutable blockchain-style history for the imported property/tax-year snapshot.

## Environment

Set these variables before running the loader:

```bash
export OPENAI_API_KEY=...
export OPENAI_MODEL=openai:gpt-4o
export NEO4J_URI=bolt://localhost:7687
export NEO4J_USERNAME=neo4j
export NEO4J_PASSWORD=...
export NEO4J_TAX_DB_NAME=tax62n
export TAX_FILE_FOLDER=/Users/weizhang/Downloads/tax-62n
```

## Install

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Install the package from the repo checkout:

```bash
source venv/bin/activate
python -m pip install .
```

## Run Loader

Load all PDFs recursively from a folder:

```bash
source venv/bin/activate
python -m ny_property_tax_ledger.load_tax_pdfs /path/to/tax-pdfs \
  --uri "$NEO4J_URI" \
  --username "$NEO4J_USERNAME" \
  --password "$NEO4J_PASSWORD" \
  --database "${NEO4J_TAX_DB_NAME:-tax62n}"
```

Dry run without writing to Neo4j:

```bash
source venv/bin/activate
python -m ny_property_tax_ledger.load_tax_pdfs /path/to/tax-pdfs \
  --uri "$NEO4J_URI" \
  --username "$NEO4J_USERNAME" \
  --password "$NEO4J_PASSWORD" \
  --dry-run
```

If you omit the folder argument, the loader uses `TAX_FILE_FOLDER`.

Example with the configured default folder:

```bash
source venv/bin/activate
python -m ny_property_tax_ledger.load_tax_pdfs \
  --uri "$NEO4J_URI" \
  --username "$NEO4J_USERNAME" \
  --password "$NEO4J_PASSWORD" \
  --database "${NEO4J_TAX_DB_NAME:-tax62n}"
```

Installed console-script form:

```bash
source venv/bin/activate
nypropertytaxledger-load-pdfs \
  --uri "$NEO4J_URI" \
  --username "$NEO4J_USERNAME" \
  --password "$NEO4J_PASSWORD" \
  --database "${NEO4J_TAX_DB_NAME:-tax62n}"
```

Current default-folder PDF set:

```text
62n-2019-2020-property-tax.pdf
62n-2020-2021-property-tax.pdf
62n-2022-2023-property-tax.pdf
62n-2023-2024-property-tax.pdf
62n-2024-2025-property-tax.pdf
62n-2025-2026-property-tax.pdf
```

Top-level folder only:

```bash
source venv/bin/activate
python -m ny_property_tax_ledger.load_tax_pdfs /path/to/tax-pdfs \
  --uri "$NEO4J_URI" \
  --username "$NEO4J_USERNAME" \
  --password "$NEO4J_PASSWORD" \
  --no-recursive
```

## Expected Output

The loader prints a summary dictionary like:

```python
{"files": 12, "statements": 84, "errors": []}
```

If a file fails, the error is added to the `errors` list with the source filename.

After the current year-normalization fix, a clean load into `testdb` or `tax62n` should produce `TaxStatement.year` values like:

```text
2019-2020
2020-2021
2022-2023
2023-2024
2024-2025
2025-2026
```

The same clean load now also appends blockchain-style history:

- `LedgerBlock` nodes linked to the property with `LEDGER_FOR`
- `LedgerEntry` nodes linked to the block with `CONTAINS`
- `PREVIOUS_BLOCK` links between consecutive import blocks
- `FOR_PROPERTY` and `FOR_TAX_STATEMENT` links from entries to the projection graph

## Notes

- The loader still relies on LLM-generated Cypher, but it now adds deterministic year normalization, Cypher cleanup, retry/repair, per-file transaction safety, and append-only ledger history.
- `--dry-run` is the safest first step for a new PDF batch.
- The generated Cypher targets the archived tax-bill graph shape, not the newer `Account` / `TaxBilling` / `TaxPayment` split model.
- The append-only ledger is the system of record; `TaxStatement`, `Levy`, and `Payment` are the compatibility projection.
- If needed, the next improvement would be saving generated Cypher to disk before execution for review and replay.
