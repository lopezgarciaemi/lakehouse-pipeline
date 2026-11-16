"""Incremental OLTP PostgreSQL extractor and synthetic chaos generator."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy import create_engine, text


def extract_oltp_accounts(db_uri: str) -> list[dict[str, Any]]:
    """Extract current customer account snapshots from PostgreSQL OLTP."""
    engine = create_engine(db_uri)
    query = text(
        """
        SELECT
            account_id,
            customer_name,
            account_tier,
            kyc_status,
            country_code,
            risk_segment,
            updated_at
        FROM accounts
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query).mappings().all()
    return [dict(r) for r in rows]


def extract_oltp_transactions(db_uri: str, execution_date: date) -> list[dict[str, Any]]:
    """Extract incremental transactions for a single UTC logical day [dt, dt + 1d)."""
    start_ts = datetime.combine(execution_date, datetime.min.time(), tzinfo=UTC)
    end_ts = start_ts + timedelta(days=1)
    engine = create_engine(db_uri)
    query = text(
        """
        SELECT
            tx_id,
            account_id,
            merchant_category,
            tx_type,
            currency,
            CAST(amount AS DOUBLE PRECISION) AS amount,
            status,
            tx_timestamp
        FROM transactions
        WHERE tx_timestamp >= :start_ts
          AND tx_timestamp < :end_ts
        ORDER BY tx_timestamp ASC
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"start_ts": start_ts, "end_ts": end_ts}).mappings().all()
    return [dict(r) for r in rows]


def generate_synthetic_oltp_batch(
    execution_date: date,
    inject_anomalies: bool = True,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Generate realistic multi-currency OLTP accounts and transactions for local runs.

    When `inject_anomalies=True`, injects 1 duplicate tx_id and 2 contract-violating
    rows (negative amount and unsupported currency) to demonstrate deduplication and
    quarantine routing end-to-end.
    """
    base_ts = datetime.combine(execution_date, datetime.min.time(), tzinfo=UTC)
    accounts = [
        {
            "account_id": "ACC-1001",
            "customer_name": "Grupo Industrial Bajio SA",
            "account_tier": "ENTERPRISE",
            "kyc_status": "VERIFIED",
            "country_code": "MX",
            "risk_segment": "LOW",
            "updated_at": (base_ts - timedelta(days=5)).isoformat(),
        },
        {
            "account_id": "ACC-1002",
            "customer_name": "Fintech Horizon LLC",
            "account_tier": "GROWTH",
            "kyc_status": "VERIFIED",
            "country_code": "US",
            "risk_segment": "MEDIUM",
            "updated_at": (base_ts - timedelta(days=3)).isoformat(),
        },
        {
            "account_id": "ACC-1003",
            "customer_name": "Importadora Queretaro",
            "account_tier": "STANDARD",
            "kyc_status": "VERIFIED",
            "country_code": "MX",
            "risk_segment": "LOW",
            "updated_at": (base_ts - timedelta(days=2)).isoformat(),
        },
        {
            "account_id": "ACC-1004",
            "customer_name": "EuroTrade Logistics GmbH",
            "account_tier": "ENTERPRISE",
            "kyc_status": "PENDING_REVIEW",
            "country_code": "DE",
            "risk_segment": "HIGH",
            "updated_at": (base_ts - timedelta(days=1)).isoformat(),
        },
        {
            "account_id": "ACC-1005",
            "customer_name": "Pacific Treasury Corp",
            "account_tier": "GROWTH",
            "kyc_status": "VERIFIED",
            "country_code": "CA",
            "risk_segment": "LOW",
            "updated_at": base_ts.isoformat(),
        },
    ]

    transactions: list[dict[str, Any]] = [
        {
            "tx_id": f"TX-{execution_date.strftime('%Y%m%d')}-001",
            "account_id": "ACC-1001",
            "merchant_category": "CLOUD_INFRA",
            "tx_type": "DEBIT",
            "currency": "USD",
            "amount": 1450.50,
            "status": "COMPLETED",
            "tx_timestamp": (base_ts + timedelta(hours=9)).isoformat(),
        },
        {
            "tx_id": f"TX-{execution_date.strftime('%Y%m%d')}-002",
            "account_id": "ACC-1001",
            "merchant_category": "SUPPLIER_SETTLEMENT",
            "tx_type": "TRANSFER",
            "currency": "MXN",
            "amount": 48500.00,
            "status": "COMPLETED",
            "tx_timestamp": (base_ts + timedelta(hours=11)).isoformat(),
        },
        {
            "tx_id": f"TX-{execution_date.strftime('%Y%m%d')}-003",
            "account_id": "ACC-1002",
            "merchant_category": "FX_DESK",
            "tx_type": "FX_SETTLEMENT",
            "currency": "EUR",
            "amount": 3200.75,
            "status": "COMPLETED",
            "tx_timestamp": (base_ts + timedelta(hours=13)).isoformat(),
        },
        {
            "tx_id": f"TX-{execution_date.strftime('%Y%m%d')}-004",
            "account_id": "ACC-1003",
            "merchant_category": "LOGISTICS",
            "tx_type": "DEBIT",
            "currency": "MXN",
            "amount": 12950.25,
            "status": "COMPLETED",
            "tx_timestamp": (base_ts + timedelta(hours=15)).isoformat(),
        },
        {
            "tx_id": f"TX-{execution_date.strftime('%Y%m%d')}-005",
            "account_id": "ACC-1004",
            "merchant_category": "TREASURY",
            "tx_type": "CREDIT",
            "currency": "GBP",
            "amount": 2100.00,
            "status": "PENDING",
            "tx_timestamp": (base_ts + timedelta(hours=16)).isoformat(),
        },
        {
            "tx_id": f"TX-{execution_date.strftime('%Y%m%d')}-006",
            "account_id": "ACC-1005",
            "merchant_category": "SAAS_BILLING",
            "tx_type": "DEBIT",
            "currency": "CAD",
            "amount": 890.40,
            "status": "COMPLETED",
            "tx_timestamp": (base_ts + timedelta(hours=18)).isoformat(),
        },
        {
            "tx_id": f"TX-{execution_date.strftime('%Y%m%d')}-007",
            "account_id": "ACC-1002",
            "merchant_category": "PAYROLL",
            "tx_type": "TRANSFER",
            "currency": "USD",
            "amount": 6400.00,
            "status": "COMPLETED",
            "tx_timestamp": (base_ts + timedelta(hours=19)).isoformat(),
        },
        {
            "tx_id": f"TX-{execution_date.strftime('%Y%m%d')}-008",
            "account_id": "ACC-1005",
            "merchant_category": "HARDWARE",
            "tx_type": "DEBIT",
            "currency": "JPY",
            "amount": 185000.00,
            "status": "COMPLETED",
            "tx_timestamp": (base_ts + timedelta(hours=20)).isoformat(),
        },
    ]

    if inject_anomalies:
        transactions.extend(
            [
                # Intentional duplicate tx_id from upstream webhook retry
                {
                    "tx_id": f"TX-{execution_date.strftime('%Y%m%d')}-002",
                    "account_id": "ACC-1001",
                    "merchant_category": "SUPPLIER_SETTLEMENT",
                    "tx_type": "TRANSFER",
                    "currency": "MXN",
                    "amount": 48500.00,
                    "status": "COMPLETED",
                    "tx_timestamp": (base_ts + timedelta(hours=11, minutes=5)).isoformat(),
                },
                # Intentional contract breach #1: negative amount -> Quarantine
                {
                    "tx_id": f"TX-{execution_date.strftime('%Y%m%d')}-BAD1",
                    "account_id": "ACC-1003",
                    "merchant_category": "REFUND_GLITCH",
                    "tx_type": "DEBIT",
                    "currency": "USD",
                    "amount": -250.00,
                    "status": "FAILED",
                    "tx_timestamp": (base_ts + timedelta(hours=21)).isoformat(),
                },
                # Intentional contract breach #2: unknown currency -> Quarantine
                {
                    "tx_id": f"TX-{execution_date.strftime('%Y%m%d')}-BAD2",
                    "account_id": "ACC-1004",
                    "merchant_category": "UNKNOWN_VENDOR",
                    "tx_type": "DEBIT",
                    "currency": "XXX",
                    "amount": 100.00,
                    "status": "COMPLETED",
                    "tx_timestamp": (base_ts + timedelta(hours=22)).isoformat(),
                },
            ]
        )

    return accounts, transactions
