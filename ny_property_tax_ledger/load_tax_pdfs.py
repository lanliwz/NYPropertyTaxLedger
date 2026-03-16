from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

from ny_property_tax_ledger.config import TAX_FILE_FOLDER
from ny_property_tax_ledger.pdf_extract import extract_pdf_tables
from ny_property_tax_ledger.property_ledger import (
    build_property_ledger_block,
    build_property_ledger_entries,
    build_source_payload_hash,
)
from ny_property_tax_ledger.tax_pdf_to_cypher import (
    generate_cypher_for_tax_bill,
    infer_tax_year_from_filename,
    repair_cypher_for_tax_bill,
)


def _iter_pdf_files(folder: Path, recursive: bool) -> list[Path]:
    pattern = "**/*.pdf" if recursive else "*.pdf"
    return sorted(path for path in folder.glob(pattern) if path.is_file())


def _split_cypher_statements(cypher_script: str) -> list[str]:
    return [statement.strip() for statement in cypher_script.split(";") if statement.strip()]


def _execute_statements_in_transaction(tx, statements: list[str]) -> None:
    for statement in statements:
        tx.run(statement).consume()


def _append_property_ledger(tx, property_address, tax_year, ledger_block, ledger_entries):
    existing_block = tx.run(
        """
        MATCH (b:LedgerBlock {blockId: $blockId})
        RETURN b.blockId AS blockId
        """,
        blockId=ledger_block["blockId"],
    ).single()

    previous_tip = None
    if existing_block is None:
        previous_tip = tx.run(
            """
            MATCH (p:Property {address: $address})
            OPTIONAL MATCH (tip:LedgerBlock)-[:LEDGER_FOR]->(p)
            WHERE NOT EXISTS { MATCH (:LedgerBlock)-[:PREVIOUS_BLOCK]->(tip) }
            RETURN tip.blockId AS blockId,
                   tip.blockHash AS blockHash,
                   coalesce(tip.blockHeight, -1) AS blockHeight
            LIMIT 1
            """,
            address=property_address,
        ).single()

    previous_hash = previous_tip["blockHash"] if previous_tip and previous_tip["blockId"] else None
    block_height = previous_tip["blockHeight"] + 1 if previous_tip and previous_tip["blockId"] else 0
    block_hash = hashlib.sha1(
        json.dumps(
            {
                "blockId": ledger_block["blockId"],
                "prevHash": previous_hash,
                "sourceHash": ledger_block["sourceHash"],
                "blockHeight": block_height,
                "entryCount": ledger_block["entryCount"],
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()

    block_properties = dict(ledger_block)
    block_properties["prevHash"] = previous_hash
    block_properties["blockHeight"] = block_height
    block_properties["blockHash"] = block_hash

    tx.run(
        """
        MERGE (b:LedgerBlock {blockId: $blockId})
        ON CREATE SET b += $block_properties
        WITH b
        MATCH (p:Property {address: $address})
        MERGE (b)-[:LEDGER_FOR]->(p)
        WITH b
        MATCH (t:TaxStatement {year: $taxYear})<-[:HAS_TAX_STATEMENT]-(:Property {address: $address})
        MERGE (b)-[:SNAPSHOT_OF]->(t)
        """,
        blockId=block_properties["blockId"],
        block_properties=block_properties,
        address=property_address,
        taxYear=tax_year,
    )

    if previous_tip and previous_tip["blockId"]:
        tx.run(
            """
            MATCH (b:LedgerBlock {blockId: $blockId})
            MATCH (prev:LedgerBlock {blockId: $prevBlockId})
            MERGE (b)-[:PREVIOUS_BLOCK]->(prev)
            """,
            blockId=block_properties["blockId"],
            prevBlockId=previous_tip["blockId"],
        )

    if ledger_entries:
        tx.run(
            """
            UNWIND $entries AS entry
            MERGE (e:LedgerEntry {entryId: entry.entryId})
            ON CREATE SET e += entry
            WITH e, entry
            MATCH (b:LedgerBlock {blockId: $blockId})
            MERGE (b)-[:CONTAINS]->(e)
            WITH e, entry
            MATCH (p:Property {address: $address})
            MERGE (e)-[:FOR_PROPERTY]->(p)
            WITH e, entry
            MATCH (t:TaxStatement {year: $taxYear})<-[:HAS_TAX_STATEMENT]-(:Property {address: $address})
            MERGE (e)-[:FOR_TAX_STATEMENT]->(t)
            """,
            entries=ledger_entries,
            blockId=block_properties["blockId"],
            address=property_address,
            taxYear=tax_year,
        )


def _partition_statements(statements: list[str]) -> tuple[list[str], list[str]]:
    schema_statements: list[str] = []
    data_statements: list[str] = []
    for statement in statements:
        if statement.lstrip().upper().startswith("CREATE CONSTRAINT"):
            schema_statements.append(statement)
        else:
            data_statements.append(statement)
    return schema_statements, data_statements


def _load_projection_snapshot(tx, property_address: str, tax_year: str) -> dict:
    levy_rows = tx.run(
        """
        MATCH (:Property {address: $address})-[:HAS_TAX_STATEMENT]->(:TaxStatement {year: $taxYear})-[:INCLUDES]->(l:Levy)
        RETURN l.uuid AS uuid,
               l.description AS description,
               l.tax_amount_with_exemptions AS tax_amount_with_exemptions,
               l.tax_amount_without_exemptions AS tax_amount_without_exemptions
        ORDER BY l.description
        """,
        address=property_address,
        taxYear=tax_year,
    )
    payment_rows = tx.run(
        """
        MATCH (:Property {address: $address})-[:HAS_TAX_STATEMENT]->(:TaxStatement {year: $taxYear})-[:HAS_PAYMENT]->(p:Payment)
        RETURN p.payment_date AS payment_date,
               p.amount AS amount,
               p.amount_paid AS amount_paid,
               p.payor AS payor
        ORDER BY p.payment_date, p.payor
        """,
        address=property_address,
        taxYear=tax_year,
    )
    return {
        "levies": [record.data() for record in levy_rows],
        "payments": [record.data() for record in payment_rows],
    }


def _extract_projection_hints(file_path: Path, cypher_script: str) -> tuple[str, str]:
    address_match = None
    import re
    address_match = re.search(r'address:\s*"([^"]+)"', cypher_script)
    property_address = address_match.group(1) if address_match else None
    tax_year = infer_tax_year_from_filename(file_path)
    if not property_address or not tax_year:
        raise ValueError(f"Could not infer property address or tax year for {file_path}")
    return property_address, tax_year


def _load_single_file(session, file_path: Path, run_id: str, max_attempts: int = 3) -> int:
    cypher_script = generate_cypher_for_tax_bill(file_path)
    property_address, tax_year = _extract_projection_hints(file_path, cypher_script)
    pdf_data = extract_pdf_tables(file_path)
    source_payload_hash = build_source_payload_hash(pdf_data)
    loaded_at = datetime.utcnow().isoformat(timespec="seconds")
    last_error = ""

    for attempt in range(1, max_attempts + 1):
        statements = _split_cypher_statements(cypher_script)
        schema_statements, data_statements = _partition_statements(statements)
        try:
            for statement in schema_statements:
                session.run(statement).consume()
            session.execute_write(_execute_statements_in_transaction, data_statements)
            snapshot = session.execute_read(_load_projection_snapshot, property_address, tax_year)
            block_seed = build_property_ledger_block(
                property_address=property_address,
                tax_year=tax_year,
                source_payload_hash=source_payload_hash,
                run_id=run_id,
                ledger_entries=[],
                loaded_at=loaded_at,
            )
            ledger_entries = build_property_ledger_entries(
                property_address=property_address,
                tax_year=tax_year,
                levies=snapshot["levies"],
                payments=snapshot["payments"],
                block_id=block_seed["blockId"],
                loaded_at=loaded_at,
            )
            ledger_block = build_property_ledger_block(
                property_address=property_address,
                tax_year=tax_year,
                source_payload_hash=source_payload_hash,
                run_id=run_id,
                ledger_entries=ledger_entries,
                loaded_at=loaded_at,
            )
            ledger_entries = build_property_ledger_entries(
                property_address=property_address,
                tax_year=tax_year,
                levies=snapshot["levies"],
                payments=snapshot["payments"],
                block_id=ledger_block["blockId"],
                loaded_at=loaded_at,
            )
            session.execute_write(
                _append_property_ledger,
                property_address,
                tax_year,
                ledger_block,
                ledger_entries,
            )
            return len(statements)
        except Neo4jError as exc:
            last_error = str(exc)
            if attempt == max_attempts:
                raise
            cypher_script = repair_cypher_for_tax_bill(file_path, cypher_script, last_error)
            property_address, tax_year = _extract_projection_hints(file_path, cypher_script)

    raise RuntimeError(f"Failed to load {file_path}: {last_error}")


def load_tax_pdfs(
    folder_path: str | Path,
    neo4j_uri: str,
    neo4j_username: str,
    neo4j_password: str,
    database: str = "tax62n",
    recursive: bool = True,
    dry_run: bool = False,
) -> dict:
    folder = Path(folder_path)
    if not folder.is_dir():
        raise ValueError(f"{folder} is not a valid directory")

    files = _iter_pdf_files(folder, recursive=recursive)
    summary = {"files": len(files), "statements": 0, "errors": []}
    run_id = str(uuid4())

    if dry_run:
        for file_path in files:
            statements = _split_cypher_statements(generate_cypher_for_tax_bill(file_path))
            summary["statements"] += len(statements)
        return summary

    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_username, neo4j_password))
    try:
        with driver.session(database=database) as session:
            for file_path in files:
                try:
                    summary["statements"] += _load_single_file(session, file_path, run_id=run_id)
                except Exception as exc:
                    summary["errors"].append({"file": str(file_path), "error": str(exc)})
        summary["run_id"] = run_id
        return summary
    finally:
        driver.close()


def main():
    parser = argparse.ArgumentParser(description="Load property tax PDFs into Neo4j.")
    parser.add_argument(
        "folder",
        nargs="?",
        default=TAX_FILE_FOLDER,
        help=f"Folder containing tax PDF files (default: {TAX_FILE_FOLDER})",
    )
    parser.add_argument("--uri", default="bolt://localhost:7687", help="Neo4j URI")
    parser.add_argument("--username", default="neo4j", help="Neo4j username")
    parser.add_argument("--password", required=True, help="Neo4j password")
    parser.add_argument("--database", default="tax62n", help="Neo4j database name")
    parser.add_argument(
        "--no-recursive",
        action="store_true",
        help="Only scan the top-level folder for PDFs",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate Cypher without writing to Neo4j",
    )
    args = parser.parse_args()

    summary = load_tax_pdfs(
        folder_path=args.folder,
        neo4j_uri=args.uri,
        neo4j_username=args.username,
        neo4j_password=args.password,
        database=args.database,
        recursive=not args.no_recursive,
        dry_run=args.dry_run,
    )
    print(summary)


if __name__ == "__main__":
    main()
