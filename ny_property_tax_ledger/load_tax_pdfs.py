from __future__ import annotations

import argparse
from pathlib import Path

from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

from ny_property_tax_ledger.config import TAX_FILE_FOLDER
from ny_property_tax_ledger.tax_pdf_to_cypher import (
    generate_cypher_for_tax_bill,
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


def _partition_statements(statements: list[str]) -> tuple[list[str], list[str]]:
    schema_statements: list[str] = []
    data_statements: list[str] = []
    for statement in statements:
        if statement.lstrip().upper().startswith("CREATE CONSTRAINT"):
            schema_statements.append(statement)
        else:
            data_statements.append(statement)
    return schema_statements, data_statements


def _load_single_file(session, file_path: Path, max_attempts: int = 3) -> int:
    cypher_script = generate_cypher_for_tax_bill(file_path)
    last_error = ""

    for attempt in range(1, max_attempts + 1):
        statements = _split_cypher_statements(cypher_script)
        schema_statements, data_statements = _partition_statements(statements)
        try:
            for statement in schema_statements:
                session.run(statement).consume()
            session.execute_write(_execute_statements_in_transaction, data_statements)
            return len(statements)
        except Neo4jError as exc:
            last_error = str(exc)
            if attempt == max_attempts:
                raise
            cypher_script = repair_cypher_for_tax_bill(file_path, cypher_script, last_error)

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
                    summary["statements"] += _load_single_file(session, file_path)
                except Exception as exc:
                    summary["errors"].append({"file": str(file_path), "error": str(exc)})
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
