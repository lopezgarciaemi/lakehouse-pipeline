"""Idempotent Parquet partition storage supporting local filesystem and MinIO S3."""

from __future__ import annotations

import io
from datetime import date
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config import Settings, get_settings


class LakehouseStorage:
    """Read/write date-partitioned Parquet datasets idempotently across local or S3 backends."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    @staticmethod
    def build_partition_key(layer: str, dataset: str, partition_date: date) -> str:
        """Return canonical Hive-style partition object key."""
        dt_str = partition_date.isoformat()
        return f"{layer}/{dataset}/dt={dt_str}/{dataset}.parquet"

    def write_parquet_partition(
        self,
        records: list[dict[str, Any]],
        layer: str,
        dataset: str,
        partition_date: date,
    ) -> str:
        """Atomically write (overwrite) a single date partition as Parquet.

        Overwriting only the target `dt=YYYY-MM-DD` partition makes Airflow backfills
        and task retries strictly idempotent without duplicating records.
        """
        if not records:
            return ""

        df = pd.DataFrame(records)
        table = pa.Table.from_pandas(df, preserve_index=False)
        rel_key = self.build_partition_key(layer, dataset, partition_date)

        if self.settings.storage_backend == "s3":
            return self._write_s3(table, rel_key)
        return self._write_local(table, rel_key)

    def read_parquet_partition(
        self,
        layer: str,
        dataset: str,
        partition_date: date,
    ) -> pd.DataFrame:
        """Read a single date partition back into a DataFrame."""
        rel_key = self.build_partition_key(layer, dataset, partition_date)
        if self.settings.storage_backend == "s3":
            client = self._get_s3_client()
            obj = client.get_object(Bucket=self.settings.s3_bucket, Key=rel_key)
            buf = io.BytesIO(obj["Body"].read())
            return pq.read_table(buf).to_pandas()

        target = self.settings.local_lakehouse_dir / rel_key
        return pq.read_table(target).to_pandas()

    def _write_local(self, table: pa.Table, rel_key: str) -> str:
        target = self.settings.local_lakehouse_dir / rel_key
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp_target = target.with_suffix(".parquet.tmp")
        pq.write_table(table, tmp_target, compression="snappy")
        tmp_target.replace(target)
        return str(target)

    def _write_s3(self, table: pa.Table, rel_key: str) -> str:
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        client = self._get_s3_client()
        client.put_object(
            Bucket=self.settings.s3_bucket,
            Key=rel_key,
            Body=buf.getvalue(),
            ContentType="application/octet-stream",
        )
        return f"s3://{self.settings.s3_bucket}/{rel_key}"

    def _get_s3_client(self) -> Any:
        import boto3

        return boto3.client(
            "s3",
            endpoint_url=self.settings.s3_endpoint_url,
            aws_access_key_id=self.settings.s3_access_key,
            aws_secret_access_key=self.settings.s3_secret_key,
        )

    def ensure_bucket_exists(self) -> None:
        """Create target S3 bucket idempotently when running in S3 mode."""
        if self.settings.storage_backend != "s3":
            return
        client = self._get_s3_client()
        existing = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
        if self.settings.s3_bucket not in existing:
            client.create_bucket(Bucket=self.settings.s3_bucket)
