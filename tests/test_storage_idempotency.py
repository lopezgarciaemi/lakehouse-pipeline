"""Tests for idempotent Parquet date-partition overwrites."""

from datetime import date
from pathlib import Path

from src.config import Settings
from src.storage import LakehouseStorage


def test_partition_overwrite_is_idempotent(tmp_path: Path) -> None:
    settings = Settings(storage_backend="local", local_lakehouse_dir=tmp_path)
    storage = LakehouseStorage(settings)
    dt = date(2026, 11, 15)

    first_batch = [{"tx_id": "TX-1", "amount": 100.0}, {"tx_id": "TX-2", "amount": 200.0}]
    storage.write_parquet_partition(
        first_batch, layer="bronze", dataset="transactions", partition_date=dt
    )

    # Re-run / backfill for the same date must overwrite, never append/duplicate
    backfill_batch = [{"tx_id": "TX-1", "amount": 100.0}, {"tx_id": "TX-2", "amount": 250.0}]
    storage.write_parquet_partition(
        backfill_batch, layer="bronze", dataset="transactions", partition_date=dt
    )

    df = storage.read_parquet_partition(layer="bronze", dataset="transactions", partition_date=dt)
    assert len(df) == 2
    assert df.loc[df["tx_id"] == "TX-2", "amount"].iloc[0] == 250.0
