from __future__ import annotations

import hashlib
import json
from datetime import datetime


def _sha1_payload(payload) -> str:
    return hashlib.sha1(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def build_source_payload_hash(pdf_data: dict) -> str:
    return _sha1_payload(pdf_data)


def build_property_ledger_entries(
    property_address: str,
    tax_year: str,
    levies: list[dict],
    payments: list[dict],
    block_id: str,
    loaded_at: str | None = None,
) -> list[dict]:
    created_at = loaded_at or datetime.utcnow().isoformat(timespec="seconds")
    entries: list[dict] = []

    event_rows = [("LEVY", row) for row in levies] + [("PAYMENT", row) for row in payments]
    for ordinal, (event_type, row) in enumerate(event_rows, start=1):
        canonical_payload = {
            "blockId": block_id,
            "propertyAddress": property_address,
            "taxYear": tax_year,
            "eventType": event_type,
            "ordinal": ordinal,
            "row": row,
        }
        entry_hash = _sha1_payload(canonical_payload)
        entry = dict(row)
        entry.update(
            {
                "entryId": entry_hash,
                "entryHash": entry_hash,
                "blockId": block_id,
                "propertyAddress": property_address,
                "taxYear": tax_year,
                "eventType": event_type,
                "ordinal": ordinal,
                "createdAt": created_at,
                "ledgerVersion": 1,
            }
        )
        entries.append(entry)

    return entries


def build_property_ledger_block(
    property_address: str,
    tax_year: str,
    source_payload_hash: str,
    run_id: str,
    ledger_entries: list[dict],
    loaded_at: str | None = None,
) -> dict:
    created_at = loaded_at or datetime.utcnow().isoformat(timespec="seconds")
    entry_hashes = [entry["entryHash"] for entry in ledger_entries]
    block_fingerprint = {
        "propertyAddress": property_address,
        "taxYear": tax_year,
        "runId": run_id,
        "loadedAt": created_at,
        "sourceHash": source_payload_hash,
        "entryHashes": entry_hashes,
    }
    block_id = _sha1_payload(block_fingerprint)

    return {
        "blockId": block_id,
        "propertyAddress": property_address,
        "taxYear": tax_year,
        "chainScope": "PROPERTY",
        "sourceSystem": "PropertyTaxPdfLoader",
        "runId": run_id,
        "sourceHash": source_payload_hash,
        "entryCount": len(entry_hashes),
        "createdAt": created_at,
        "ledgerVersion": 1,
    }
