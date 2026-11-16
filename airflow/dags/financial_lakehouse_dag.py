"""Airflow DAG orchestrating the Financial Lakehouse ELT Pipeline (TaskFlow API)."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from src.alerts import send_alert
from src.config import get_settings
from src.extractors.fx_api import fetch_fx_rates
from src.extractors.oltp_postgres import extract_oltp_accounts, extract_oltp_transactions
from src.quality.contracts import (
    AccountRecord,
    FxRateRecord,
    TransactionRecord,
    validate_and_split_records,
)
from src.storage import LakehouseStorage


def notify_task_failure(context: dict[str, Any]) -> None:
    """Airflow on_failure_callback sending structured alert payloads."""
    ti = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown_dag"
    task_id = ti.task_id if ti else "unknown_task"
    execution_date = context.get("ds", "unknown_date")
    exception = str(context.get("exception", ""))

    send_alert(
        title=f"Airflow Task Failed: {dag_id}.{task_id}",
        message=f"Execution failed for logical date {execution_date}: {exception}",
        severity="ERROR",
        context={
            "dag_id": dag_id,
            "task_id": task_id,
            "execution_date": execution_date,
        },
    )


DEFAULT_ARGS = {
    "owner": "emiliano.lopez",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "on_failure_callback": notify_task_failure,
}


@dag(
    dag_id="financial_lakehouse_daily",
    description=(
        "Idempotent daily Financial Lakehouse ELT: "
        "REST FX API + Postgres OLTP -> MinIO S3 -> DuckDB + dbt"
    ),
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["data-engineering", "lakehouse", "dbt", "duckdb", "minio"],
)
def financial_lakehouse_daily() -> None:
    """Orchestrate parallel extraction, contract validation, and dbt warehouse modeling."""

    @task
    def extract_and_validate_fx_rates(ds: str | None = None) -> dict[str, Any]:
        assert ds is not None
        settings = get_settings()
        storage = LakehouseStorage(settings)
        exec_date = date.fromisoformat(ds)

        raw_rates = fetch_fx_rates(execution_date=exec_date, base_url=settings.fx_api_base_url)
        validation = validate_and_split_records(
            raw_rates,
            FxRateRecord,
            dataset="fx_rates",
            max_quarantine_ratio=settings.max_quarantine_ratio,
        )
        uri = storage.write_parquet_partition(
            validation.valid_rows, layer="bronze", dataset="fx_rates", partition_date=exec_date
        )
        if validation.quarantine_rows:
            storage.write_parquet_partition(
                validation.quarantine_rows,
                layer="quarantine",
                dataset="fx_rates",
                partition_date=exec_date,
            )
        return {
            "valid": len(validation.valid_rows),
            "quarantine": len(validation.quarantine_rows),
            "uri": uri,
        }

    @task
    def extract_and_validate_oltp(ds: str | None = None) -> dict[str, Any]:
        assert ds is not None
        settings = get_settings()
        storage = LakehouseStorage(settings)
        exec_date = date.fromisoformat(ds)

        raw_accounts = extract_oltp_accounts(settings.oltp_db_uri)
        acc_val = validate_and_split_records(
            raw_accounts,
            AccountRecord,
            dataset="accounts",
            max_quarantine_ratio=settings.max_quarantine_ratio,
        )
        acc_uri = storage.write_parquet_partition(
            acc_val.valid_rows, layer="bronze", dataset="accounts", partition_date=exec_date
        )

        raw_tx = extract_oltp_transactions(settings.oltp_db_uri, execution_date=exec_date)
        tx_val = validate_and_split_records(
            raw_tx,
            TransactionRecord,
            dataset="transactions",
            max_quarantine_ratio=settings.max_quarantine_ratio,
        )
        tx_uri = storage.write_parquet_partition(
            tx_val.valid_rows, layer="bronze", dataset="transactions", partition_date=exec_date
        )
        if tx_val.quarantine_rows:
            storage.write_parquet_partition(
                tx_val.quarantine_rows,
                layer="quarantine",
                dataset="transactions",
                partition_date=exec_date,
            )
            send_alert(
                title="OLTP Quarantine Records Routed",
                message=f"{len(tx_val.quarantine_rows)} invalid transactions isolated on {ds}.",
                severity="WARNING",
                context={"ds": ds, "invalid_count": len(tx_val.quarantine_rows)},
            )

        return {
            "accounts_valid": len(acc_val.valid_rows),
            "tx_valid": len(tx_val.valid_rows),
            "tx_quarantine": len(tx_val.quarantine_rows),
            "accounts_uri": acc_uri,
            "tx_uri": tx_uri,
        }

    dbt_env = {"DBT_LAKEHOUSE_ROOT": f"s3://{get_settings().s3_bucket}"}

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot_scd2",
        bash_command="cd /opt/airflow/dbt && dbt snapshot --profiles-dir . --target docker_s3",
        env=dbt_env,
        append_env=True,
    )

    dbt_run = BashOperator(
        task_id="dbt_run_medallion_models",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir . --target docker_s3",
        env=dbt_env,
        append_env=True,
    )

    dbt_test = BashOperator(
        task_id="dbt_test_data_quality",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir . --target docker_s3",
        env=dbt_env,
        append_env=True,
    )

    fx_done = extract_and_validate_fx_rates()
    oltp_done = extract_and_validate_oltp()

    [fx_done, oltp_done] >> dbt_run >> dbt_snapshot >> dbt_test


financial_lakehouse_daily()
