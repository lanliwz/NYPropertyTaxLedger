# NYPropertyTaxLedger

`NYPropertyTaxLedger` is a small Streamlit and LangGraph app for exploring property tax billing and payment data stored in Neo4j.

It was initialized from the archived `tax62_chatbot` prototype and cleaned up into a standalone repository layout.

## Features

- Streamlit chat UI for tax questions
- LangGraph workflow for guard, Cypher generation, query execution, and answer synthesis
- Neo4j-backed tax billing and payment analysis
- Simple CLI conversation runner for quick testing

## Project Layout

```text
NYPropertyTaxLedger/
  app.py
  requirements.txt
  ny_property_tax_ledger/
    connect.py
    graph.py
    query_examples.py
    cli_chatbot.py
```

## Environment

Set these environment variables before running the app:

```bash
export OPENAI_API_KEY=...
export Neo4jFinDBUrl=bolt://localhost:7687
export Neo4jFinDBUserName=neo4j
export Neo4jFinDBPassword=...
export NEO4J_TAX_DB_NAME=taxjc
```

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
streamlit run app.py
```

## Notes

- The app expects the Neo4j graph schema to be available through `langchain_neo4j.Neo4jGraph`.
- The sample Cypher few-shots target the current `taxjc` split model with `Account`, `TaxBilling`, and `TaxPayment`.
