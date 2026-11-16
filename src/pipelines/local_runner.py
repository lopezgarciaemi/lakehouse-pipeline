"""Local end-to-end Bronze -> Quarantine -> DuckDB Gold runner (no Docker required)."""

from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb

from src.alerts import send_alert
from src.config import Settings, get_settings
from src.extractors.fx_api import fetch_fx_rates
from src.extractors.oltp_postgres import generate_synthetic_oltp_batch
from src.quality.contracts import (
    AccountRecord,
    FxRateRecord,
    TransactionRecord,
    validate_and_split_records,
)
from src.storage import LakehouseStorage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


def build_duckdb_warehouse(lakehouse_dir: Path, duckdb_path: Path) -> dict[str, int]:
    """Build Silver & Gold analytical tables in DuckDB directly from Bronze Parquet."""
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    base = lakehouse_dir.resolve().as_posix()

    con = duckdb.connect(str(duckdb_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
        con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

        con.execute(
            f"""
            CREATE OR REPLACE VIEW silver.stg_fx_rates AS
            SELECT
                CAST(rate_date AS DATE) AS rate_date,
                CAST(ingestion_date AS DATE) AS ingestion_date,
                UPPER(base_currency) AS base_currency,
                UPPER(target_currency) AS target_currency,
                CAST(rate AS DOUBLE) AS rate_per_usd,
                source
            FROM read_parquet('{base}/bronze/fx_rates/dt=*/*.parquet', hive_partitioning = true)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY ingestion_date, target_currency
                ORDER BY rate_date DESC
            ) = 1;
            """
        )

        con.execute(
            f"""
            CREATE OR REPLACE VIEW silver.stg_accounts AS
            SELECT
                account_id,
                customer_name,
                account_tier,
                kyc_status,
                country_code,
                risk_segment,
                CAST(updated_at AS TIMESTAMPTZ) AS updated_at
            FROM read_parquet('{base}/bronze/accounts/dt=*/*.parquet', hive_partitioning = true)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY account_id
                ORDER BY updated_at DESC, dt DESC
            ) = 1;
            """
        )

        con.execute(
            f"""
            CREATE OR REPLACE VIEW silver.stg_transactions AS
            SELECT
                tx_id,
                account_id,
                merchant_category,
                tx_type,
                UPPER(currency) AS currency,
                CAST(amount AS DOUBLE) AS original_amount,
                status,
                CAST(tx_timestamp AS TIMESTAMPTZ) AS tx_timestamp,
                CAST(tx_timestamp AS DATE) AS tx_date
            FROM read_parquet('{base}/bronze/transactions/dt=*/*.parquet', hive_partitioning = true)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY tx_id
                ORDER BY tx_timestamp ASC
            ) = 1;
            """
        )

        con.execute(
            """
            CREATE OR REPLACE TABLE gold.dim_accounts AS
            SELECT
                account_id,
                customer_name,
                account_tier,
                kyc_status,
                country_code,
                risk_segment,
                updated_at
            FROM silver.stg_accounts;
            """
        )

        con.execute(
            """
            CREATE OR REPLACE TABLE gold.fct_transactions AS
            WITH fx_pivot AS (
                SELECT
                    ingestion_date,
                    target_currency,
                    rate_per_usd,
                    MAX(CASE WHEN target_currency = 'MXN' THEN rate_per_usd END)
                        OVER (PARTITION BY ingestion_date) AS usd_to_mxn_rate
                FROM silver.stg_fx_rates
            )
            SELECT
                t.tx_id,
                t.account_id,
                a.customer_name,
                a.account_tier,
                a.risk_segment,
                t.merchant_category,
                t.tx_type,
                t.currency,
                t.original_amount,
                ROUND(t.original_amount / fx.rate_per_usd, 4) AS amount_usd,
                ROUND((t.original_amount / fx.rate_per_usd) * fx.usd_to_mxn_rate, 2) AS amount_mxn,
                t.status,
                t.tx_date,
                t.tx_timestamp
            FROM silver.stg_transactions t
            LEFT JOIN silver.stg_accounts a
                ON t.account_id = a.account_id
            LEFT JOIN fx_pivot fx
                ON t.tx_date = fx.ingestion_date
                AND t.currency = fx.target_currency;
            """
        )

        con.execute(
            """
            CREATE OR REPLACE TABLE gold.mart_daily_currency_exposure AS
            SELECT
                tx_date,
                currency,
                COUNT(*) AS transaction_count,
                SUM(original_amount) AS total_original_amount,
                ROUND(SUM(amount_usd), 2) AS total_usd_equivalent,
                ROUND(SUM(amount_mxn), 2) AS total_mxn_equivalent
            FROM gold.fct_transactions
            WHERE status <> 'FAILED'
            GROUP BY tx_date, currency
            ORDER BY tx_date DESC, total_usd_equivalent DESC;
            """
        )

        fct_count = con.execute("SELECT COUNT(*) FROM gold.fct_transactions").fetchone()[0]
        mart_count = con.execute(
            "SELECT COUNT(*) FROM gold.mart_daily_currency_exposure"
        ).fetchone()[0]
        return {"fct_transactions": int(fct_count), "mart_daily_currency_exposure": int(mart_count)}
    finally:
        con.close()


def run_local_pipeline(
    execution_date: date,
    use_mock_api: bool = False,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Execute full Bronze + Quarantine + DuckDB Gold pipeline locally."""
    cfg = settings or get_settings()
    storage = LakehouseStorage(cfg)

    fx_raw = fetch_fx_rates(
        execution_date=execution_date,
        base_url=cfg.fx_api_base_url,
        use_mock=use_mock_api,
    )
    accounts_raw, tx_raw = generate_synthetic_oltp_batch(
        execution_date=execution_date,
        inject_anomalies=True,
    )

    fx_val = validate_and_split_records(
        fx_raw, FxRateRecord, dataset="fx_rates", max_quarantine_ratio=cfg.max_quarantine_ratio
    )
    acc_val = validate_and_split_records(
        accounts_raw,
        AccountRecord,
        dataset="accounts",
        max_quarantine_ratio=cfg.max_quarantine_ratio,
    )
    tx_val = validate_and_split_records(
        tx_raw,
        TransactionRecord,
        dataset="transactions",
        max_quarantine_ratio=cfg.max_quarantine_ratio,
    )

    storage.write_parquet_partition(fx_val.valid_rows, "bronze", "fx_rates", execution_date)
    storage.write_parquet_partition(acc_val.valid_rows, "bronze", "accounts", execution_date)
    storage.write_parquet_partition(tx_val.valid_rows, "bronze", "transactions", execution_date)

    if tx_val.quarantine_rows:
        storage.write_parquet_partition(
            tx_val.quarantine_rows, "quarantine", "transactions", execution_date
        )
        send_alert(
            title="Quarantine Records Detected",
            message=f"{len(tx_val.quarantine_rows)} invalid transactions routed to quarantine.",
            severity="WARNING",
            context={"execution_date": execution_date.isoformat()},
            webhook_url=cfg.alert_webhook_url,
        )

    counts = build_duckdb_warehouse(cfg.local_lakehouse_dir, cfg.duckdb_path)
    summary = {
        "execution_date": execution_date.isoformat(),
        "bronze_fx_rates": len(fx_val.valid_rows),
        "bronze_accounts": len(acc_val.valid_rows),
        "bronze_transactions": len(tx_val.valid_rows),
        "quarantine_transactions": len(tx_val.quarantine_rows),
        **counts,
    }
    logger.info("Pipeline completed: %s", summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Financial Lakehouse Pipeline locally.")
    parser.add_argument(
        "--date",
        default=(date.today() - timedelta(days=1)).isoformat(),
        help="Logical execution date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--mock-api",
        action="store_true",
        help="Use deterministic mock FX rates instead of live Frankfurter API.",
    )
    args = parser.parse_args()
    run_local_pipeline(date.fromisoformat(args.date), use_mock_api=args.mock_api)


if __name__ == "__main__":
    main()
