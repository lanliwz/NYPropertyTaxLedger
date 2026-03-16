from __future__ import annotations

import json
import re
from pathlib import Path
from textwrap import dedent

from langchain_core.language_models.chat_models import BaseChatModel

from ny_property_tax_ledger.connect import llm
from ny_property_tax_ledger.pdf_extract import extract_pdf_tables


def _extract_message_text(response) -> str:
    content = getattr(response, "content", response)
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                parts.append(item.get("text", ""))
            else:
                parts.append(str(item))
        return "\n".join(part for part in parts if part).strip()
    return str(content).strip()


def infer_tax_year_from_filename(file_path: str | Path) -> str | None:
    match = re.search(r"(\d{4})-(\d{4})", Path(file_path).name)
    if not match:
        return None
    return f"{match.group(1)}-{match.group(2)}"


def apply_tax_year_override(cypher_script: str, tax_year: str | None) -> str:
    if not tax_year:
        return cypher_script
    return re.sub(
        r"(\byear:\s*)(\"[^\"]*\"|\d{4})",
        lambda m: f'{m.group(1)}"{tax_year}"',
        cypher_script,
        count=1,
    )


def normalize_cypher_script(cypher_script: str) -> str:
    """Apply small deterministic repairs to common model output issues."""
    cleaned = cypher_script.replace("```cypher", "").replace("```", "").strip()
    cypher_keywords = (
        "CREATE ",
        "MERGE ",
        "MATCH ",
        "WITH ",
        "UNWIND ",
        "SET ",
        "RETURN ",
        "OPTIONAL MATCH ",
    )
    raw_lines = cleaned.splitlines()
    start_index = 0
    for idx, line in enumerate(raw_lines):
        if line.lstrip().upper().startswith(cypher_keywords):
            start_index = idx
            break
    cleaned = "\n".join(line.rstrip() for line in raw_lines[start_index:]).strip()
    lines = [line.rstrip() for line in cleaned.splitlines()]

    normalized: list[str] = []
    saw_unwind = False
    ownership_lines: list[str] = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            normalized.append("")
            continue

        if stripped == "MERGE (owner)-[:OWNS]->(property)":
            if saw_unwind:
                ownership_lines.append(line)
                continue

        if re.match(r"UNWIND\s+data\.(levies|payments)\s+AS\s+\w+", stripped):
            previous = ""
            for existing in reversed(normalized):
                existing_stripped = existing.strip()
                if existing_stripped:
                    previous = existing_stripped
                    break
            if not (previous.startswith("WITH ") or previous.endswith("AS data")):
                normalized.append("WITH data, taxStatement")
            saw_unwind = True

        normalized.append(line)

    if ownership_lines:
        insert_at = None
        for idx, line in enumerate(normalized):
            stripped = line.strip()
            if "MERGE (property)-[:HAS_TAX_STATEMENT]->" in stripped or (
                stripped.startswith("MERGE (taxStatement:TaxStatement")
            ):
                insert_at = idx + 1
        if insert_at is None:
            insert_at = len(normalized)
        for ownership_line in ownership_lines:
            normalized.insert(insert_at, ownership_line)
            insert_at += 1

    return "\n".join(normalized).strip()


def _build_generation_prompt(payload: str) -> str:
    return dedent(
        f"""
        Task: Generate a Neo4j Cypher script for a property tax PDF.

        Target graph schema:
        (:TaxStatement {{year}})
        (:Levy {{uuid, description, tax_amount_with_exemptions, tax_amount_without_exemptions}})
        (:Payment {{payment_date, amount, amount_paid, payor}})
        (:Owner {{name}})
        (:Property {{address}})
        (:TaxStatement)-[:INCLUDES]->(:Levy)
        (:TaxStatement)-[:HAS_PAYMENT]->(:Payment)
        (:Property)-[:HAS_TAX_STATEMENT]->(:TaxStatement)
        (:Owner)-[:OWNS]->(:Property)

        Requirements:
        - Return only Cypher. No markdown, comments, or explanations.
        - Include only valid Cypher statements separated by semicolons.
        - Every node pattern in every MERGE or MATCH must include a label.
        - Never create unlabeled nodes.
        - Always define variables before reusing them in a later WITH clause.
        - Use MERGE for nodes and relationships.
        - Use CREATE CONSTRAINT IF NOT EXISTS FOR ... REQUIRE ... IS UNIQUE; for uniqueness constraints.
        - Ignore page and table structures as graph entities.
        - Use exactly one top-level WITH {{...}} AS data map for extracted values per file.
        - When creating Levy ids, use:
          WITH *, apoc.util.md5([data.year, data.address, levy_data.description]) AS levy_uuid
        - Keep owner, property, and taxStatement in scope before any later WITH that needs them.
        - If there are multiple payments, UNWIND them from data.payments.
        - If there is one payment, either store it in data.payment or a one-item data.payments list.
        - Ensure all variables referenced in SET, MERGE, WITH, and UNWIND are defined.
        - If the source filename contains a tax year range like 2025-2026, use that exact range for data.year.

        PDF content:
        {payload}
        """
    ).strip()


def _build_repair_prompt(payload: str, bad_cypher: str, error_message: str) -> str:
    return dedent(
        f"""
        Task: Repair invalid Neo4j Cypher for a property tax PDF.

        The previous Cypher failed in Neo4j with this error:
        {error_message}

        Repair rules:
        - Return only corrected Cypher.
        - Preserve the intended tax data from the PDF content.
        - Keep the same target graph schema:
          (:TaxStatement {{year}})
          (:Levy {{uuid, description, tax_amount_with_exemptions, tax_amount_without_exemptions}})
          (:Payment {{payment_date, amount, amount_paid, payor}})
          (:Owner {{name}})
          (:Property {{address}})
        - Every node pattern must include a label.
        - Never use variables in WITH unless they were defined earlier in scope.
        - Make the script executable as-is in Neo4j.

        Previous invalid Cypher:
        {bad_cypher}

        PDF content:
        {payload}
        """
    ).strip()


def generate_cypher_for_tax_bill(
    file_path: str | Path,
    model: BaseChatModel | None = None,
) -> str:
    """Generate Cypher statements for a property tax PDF using the configured chat model."""
    pdf_data = extract_pdf_tables(file_path)
    payload = json.dumps(pdf_data, ensure_ascii=True)
    tax_year = infer_tax_year_from_filename(file_path)

    active_model = model or llm
    response = active_model.invoke(_build_generation_prompt(payload))
    cypher = normalize_cypher_script(_extract_message_text(response))
    return apply_tax_year_override(cypher, tax_year)


def repair_cypher_for_tax_bill(
    file_path: str | Path,
    bad_cypher: str,
    error_message: str,
    model: BaseChatModel | None = None,
) -> str:
    """Repair invalid Cypher using the source PDF content plus Neo4j error feedback."""
    pdf_data = extract_pdf_tables(file_path)
    payload = json.dumps(pdf_data, ensure_ascii=True)
    tax_year = infer_tax_year_from_filename(file_path)
    active_model = model or llm
    response = active_model.invoke(_build_repair_prompt(payload, bad_cypher, error_message))
    cypher = normalize_cypher_script(_extract_message_text(response))
    return apply_tax_year_override(cypher, tax_year)
