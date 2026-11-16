"""End-to-end local Lakehouse test (Bronze Parquet + Quarantine + DuckDB Silver/Gold)."""

from datetime import date
from pathlib import Path

import duckdb

from src.config import Settings
from src.pipelines.local_runner import run_local_pipeline


def test_local_lakehouse_e2e_deduplicates_and_converts_currencies(tmp_path: Path) -> None:
    settings = Settings(
        storage_backend="local",
        local_lakehouse_dir=tmp_path / "lakehouse",
        duckdb_path=tmp_path / "warehouse" / "test.duckdb",
        max_quarantine_ratio=0.30,
    )
    exec_date = date(2026, 11, 15)

    # Run twice to verify full end-to-end idempotency
    run_local_pipeline(exec_date, use_mock_api=True, settings=settings)
    summary = run_local_pipeline(exec_date, use_mock_api=True, settings=settings)

    # Synthetic batch has 8 unique valid rows + 1 duplicate valid row (9 valid) + 2 invalid rows
    assert summary["bronze_transactions"] == 9
    assert summary["quarantine_transactions"] == 2
    # Silver/Gold deduplicates the repeated tx_id, leaving 8 unique transactions
    assert summary["fct_transactions"] == 8

    con = duckdb.connect(str(settings.duckdb_path), read_only=True)
    try:
        null_fx = con.execute(
            "SELECT COUNT(*) FROM gold.fct_transactions "
            "WHERE amount_usd IS NULL OR amount_mxn IS NULL"
        ).fetchone()[0]
        assert null_fx == 0

        currencies = {
            r[0]
            for r in con.execute(
                "SELECT DISTINCT currency FROM gold.mart_daily_currency_exposure"
            ).fetchall()
        }
        assert currencies == {"USD", "MXN", "EUR", "GBP", "CAD", "JPY"}
    finally:
        con.close()
