"""Tests for Pydantic data contracts, quarantine routing, and circuit breaker."""

import pytest

from src.quality.contracts import (
    DataQualityThresholdError,
    TransactionRecord,
    validate_and_split_records,
)


def _valid_tx(tx_id: str = "TX-1") -> dict:
    return {
        "tx_id": tx_id,
        "account_id": "ACC-1001",
        "merchant_category": "CLOUD_INFRA",
        "tx_type": "DEBIT",
        "currency": "USD",
        "amount": 150.0,
        "status": "COMPLETED",
        "tx_timestamp": "2026-11-15T10:00:00+00:00",
    }


def test_validate_routes_bad_records_to_quarantine_within_threshold() -> None:
    rows = [
        _valid_tx("TX-1"),
        _valid_tx("TX-2"),
        _valid_tx("TX-3"),
        _valid_tx("TX-4"),
        {**_valid_tx("TX-BAD"), "amount": -50.0, "currency": "XXX"},
    ]

    result = validate_and_split_records(
        rows,
        TransactionRecord,
        dataset="transactions",
        max_quarantine_ratio=0.25,
    )

    assert len(result.valid_rows) == 4
    assert len(result.quarantine_rows) == 1
    assert result.quarantine_rows[0]["tx_id"] == "TX-BAD"
    assert "amount" in result.quarantine_rows[0]["_rejection_reason"]
    assert "currency" in result.quarantine_rows[0]["_rejection_reason"]


def test_validate_triggers_circuit_breaker_when_threshold_exceeded() -> None:
    rows = [
        _valid_tx("TX-1"),
        {**_valid_tx("TX-BAD1"), "amount": -10.0},
        {**_valid_tx("TX-BAD2"), "currency": "INVALID"},
    ]

    with pytest.raises(DataQualityThresholdError, match="circuit-breaker"):
        validate_and_split_records(
            rows,
            TransactionRecord,
            dataset="transactions",
            max_quarantine_ratio=0.25,
        )
