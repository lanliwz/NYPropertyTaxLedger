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
3. Send the extracted structure to the configured chat model.
4. Ask the model to generate Cypher for:
   - `TaxStatement`
   - `Levy`
   - `Payment`
   - `Owner`
   - `Property`
5. Split the generated Cypher into statements.
6. Execute the statements against Neo4j database `tax62n`.

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

## Notes

- The loader currently relies on LLM-generated Cypher, so results depend on the consistency of the PDF structure.
- `--dry-run` is the safest first step for a new PDF batch.
- The generated Cypher targets the archived tax-bill graph shape, not the newer `Account` / `TaxBilling` / `TaxPayment` split model.
- If needed, the next improvement would be saving generated Cypher to disk before execution for review and replay.
