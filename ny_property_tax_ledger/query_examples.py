examples = [
    {
        "question": "What is the total billed amount in 2025?",
        "query": (
            "MATCH (b:TaxBilling) "
            "WHERE b.Year = '2025' "
            "RETURN round(coalesce(sum(b.Billed), 0.0) * 100) / 100.0 AS total_billed"
        ),
    },
    {
        "question": "What is the total paid amount in 2025?",
        "query": (
            "MATCH (p:TaxPayment) "
            "WHERE p.Year = '2025' "
            "RETURN round(abs(coalesce(sum(p.Paid), 0.0)) * 100) / 100.0 AS total_paid"
        ),
    },
    {
        "question": "Show billed and paid totals by account for 2025.",
        "query": (
            "MATCH (a:Account) "
            "OPTIONAL MATCH (b:TaxBilling)-[:BILL_FOR]->(a) WHERE b.Year = '2025' "
            "WITH a, round(coalesce(sum(b.Billed), 0.0) * 100) / 100.0 AS billed_total "
            "OPTIONAL MATCH (p:TaxPayment)-[:PAYMENT_FOR]->(a) WHERE p.Year = '2025' "
            "RETURN a.accountId AS accountId, "
            "a.propertyLocation AS propertyLocation, "
            "billed_total, "
            "round(abs(coalesce(sum(p.Paid), 0.0)) * 100) / 100.0 AS paid_total "
            "ORDER BY billed_total DESC"
        ),
    },
    {
        "question": "What are the quarterly billing totals for 2025?",
        "query": (
            "MATCH (b:TaxBilling) "
            "WHERE b.Year = '2025' "
            "RETURN b.Qtr AS quarter, "
            "round(coalesce(sum(b.Billed), 0.0) * 100) / 100.0 AS billed_total "
            "ORDER BY quarter"
        ),
    },
    {
        "question": "What are the quarterly payment totals for 2025?",
        "query": (
            "MATCH (p:TaxPayment) "
            "WHERE p.Year = '2025' "
            "RETURN p.Qtr AS quarter, "
            "round(abs(coalesce(sum(p.Paid), 0.0)) * 100) / 100.0 AS paid_total "
            "ORDER BY quarter"
        ),
    },
]
