from __future__ import annotations

import argparse
import hashlib
import json
import os

from neo4j import GraphDatabase


def _compute_block_hash(block_id, prev_hash, source_hash, block_height, entry_count):
    return hashlib.sha1(
        json.dumps(
            {
                "blockId": block_id,
                "prevHash": prev_hash,
                "sourceHash": source_hash,
                "blockHeight": block_height,
                "entryCount": entry_count,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def verify_property_ledger(database: str = "tax62n") -> int:
    uri = os.getenv("NEO4J_URI")
    username = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")
    if not uri or not username or not password:
        raise RuntimeError("Missing NEO4J_URI, NEO4J_USERNAME, or NEO4J_PASSWORD.")

    driver = GraphDatabase.driver(uri, auth=(username, password))
    query = """
    MATCH (p:Property)
    OPTIONAL MATCH (b:LedgerBlock)-[:LEDGER_FOR]->(p)
    OPTIONAL MATCH (b)-[:CONTAINS]->(e:LedgerEntry)
    OPTIONAL MATCH (b)-[prev_rel]->(prev:LedgerBlock)
    WHERE prev_rel IS NULL OR type(prev_rel) = 'PREVIOUS_BLOCK'
    RETURN p.address AS propertyAddress,
           b.blockId AS blockId,
           b.blockHash AS blockHash,
           properties(b)['prevHash'] AS prevHash,
           b.sourceHash AS sourceHash,
           b.blockHeight AS blockHeight,
           b.entryCount AS entryCount,
           count(DISTINCT e) AS actualEntryCount,
           collect(DISTINCT prev.blockId) AS prevBlockIds
    ORDER BY propertyAddress, blockHeight, blockId
    """

    try:
        with driver.session(database=database) as session:
            records = [record.data() for record in session.run(query)]
    finally:
        driver.close()

    chains: dict[str, list[dict]] = {}
    for record in records:
        chains.setdefault(record["propertyAddress"], []).append(record)

    failures: list[str] = []
    for property_address, chain in chains.items():
        blocks = [record for record in chain if record["blockId"]]
        expected_prev_block_id = None
        expected_prev_hash = None

        for expected_height, block in enumerate(blocks):
            prev_block_ids = [value for value in block["prevBlockIds"] if value is not None]
            if block["blockHeight"] != expected_height:
                failures.append(
                    f"{property_address} block {block['blockId']}: expected blockHeight "
                    f"{expected_height}, found {block['blockHeight']}"
                )
            if block["entryCount"] != block["actualEntryCount"]:
                failures.append(
                    f"{property_address} block {block['blockId']}: entryCount={block['entryCount']} "
                    f"but contains {block['actualEntryCount']} LedgerEntry nodes"
                )
            if expected_height == 0:
                if block["prevHash"] is not None:
                    failures.append(
                        f"{property_address} genesis block {block['blockId']}: prevHash should be null"
                    )
                if prev_block_ids:
                    failures.append(
                        f"{property_address} genesis block {block['blockId']}: unexpected PREVIOUS_BLOCK link"
                    )
            else:
                if block["prevHash"] != expected_prev_hash:
                    failures.append(
                        f"{property_address} block {block['blockId']}: prevHash mismatch"
                    )
                if prev_block_ids != [expected_prev_block_id]:
                    failures.append(
                        f"{property_address} block {block['blockId']}: PREVIOUS_BLOCK mismatch"
                    )

            expected_hash = _compute_block_hash(
                block["blockId"],
                block["prevHash"],
                block["sourceHash"],
                block["blockHeight"],
                block["entryCount"],
            )
            if block["blockHash"] != expected_hash:
                failures.append(
                    f"{property_address} block {block['blockId']}: blockHash mismatch"
                )
            expected_prev_block_id = block["blockId"]
            expected_prev_hash = block["blockHash"]

        print(
            f"{property_address}: verified {len(blocks)} block(s), "
            f"{sum(block['actualEntryCount'] for block in blocks)} entry link(s)"
        )

    if failures:
        print("\nLedger verification failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print(f"\nLedger verification passed for database '{database}'.")
    return 0


def main():
    parser = argparse.ArgumentParser(description="Verify property LedgerBlock/LedgerEntry chains.")
    parser.add_argument("--database", default="tax62n", help="Neo4j database to verify.")
    args = parser.parse_args()
    raise SystemExit(verify_property_ledger(database=args.database))


if __name__ == "__main__":
    main()
