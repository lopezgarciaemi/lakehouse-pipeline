"""Pydantic data contracts and quarantine-based validation gatekeeper."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationError, field_validator

SUPPORTED_CURRENCIES = {"USD", "MXN", "EUR", "GBP", "CAD", "JPY"}


class DataQualityThresholdError(RuntimeError):
    """Raised when the invalid-record ratio breaches the circuit-breaker threshold."""


class FxRateRecord(BaseModel):
    """Data contract for daily FX reference rates."""

    rate_date: date
    ingestion_date: date
    base_currency: str = Field(min_length=3, max_length=3)
    target_currency: str = Field(min_length=3, max_length=3)
    rate: float = Field(gt=0)
    source: str = Field(min_length=1)

    @field_validator("base_currency", "target_currency")
    @classmethod
    def validate_currency(cls, value: str) -> str:
        upper = value.strip().upper()
        if upper not in SUPPORTED_CURRENCIES:
            raise ValueError(f"Unsupported currency code: {value}")
        return upper


class AccountRecord(BaseModel):
    """Data contract for fintech customer accounts."""

    account_id: str = Field(min_length=1)
    customer_name: str = Field(min_length=1)
    account_tier: Literal["STANDARD", "GROWTH", "ENTERPRISE"]
    kyc_status: Literal["VERIFIED", "PENDING_REVIEW", "REJECTED"]
    country_code: str = Field(min_length=2, max_length=2)
    risk_segment: Literal["LOW", "MEDIUM", "HIGH"]
    updated_at: datetime


class TransactionRecord(BaseModel):
    """Data contract for multi-currency financial transactions."""

    tx_id: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    merchant_category: str = Field(min_length=1)
    tx_type: Literal["DEBIT", "CREDIT", "TRANSFER", "FX_SETTLEMENT"]
    currency: str = Field(min_length=3, max_length=3)
    amount: float = Field(gt=0)
    status: Literal["COMPLETED", "PENDING", "FAILED", "REVERSED"]
    tx_timestamp: datetime

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, value: str) -> str:
        upper = value.strip().upper()
        if upper not in SUPPORTED_CURRENCIES:
            raise ValueError(f"Unsupported currency code: {value}")
        return upper


@dataclass(slots=True)
class ValidationResult:
    """Split output of a contract validation pass."""

    dataset: str
    valid_rows: list[dict[str, Any]]
    quarantine_rows: list[dict[str, Any]]
    total_count: int

    @property
    def invalid_ratio(self) -> float:
        if self.total_count == 0:
            return 0.0
        return len(self.quarantine_rows) / self.total_count


def validate_and_split_records(
    rows: list[dict[str, Any]],
    model_cls: type[BaseModel],
    dataset: str,
    max_quarantine_ratio: float = 0.25,
) -> ValidationResult:
    """Validate rows against a Pydantic contract, routing bad records to quarantine.

    Raises DataQualityThresholdError if the failure ratio exceeds `max_quarantine_ratio`
    to protect downstream Gold models from systemic upstream schema corruption.
    """
    valid_rows: list[dict[str, Any]] = []
    quarantine_rows: list[dict[str, Any]] = []
    rejected_at = datetime.now(tz=UTC).isoformat()

    for row in rows:
        try:
            validated = model_cls.model_validate(row)
            valid_rows.append(validated.model_dump(mode="json"))
        except ValidationError as exc:
            quarantine_entry = {str(k): ("" if v is None else str(v)) for k, v in row.items()}
            quarantine_entry["_dataset"] = dataset
            quarantine_entry["_rejection_reason"] = "; ".join(
                f"{'.'.join(str(loc) for loc in err['loc'])}: {err['msg']}"
                for err in exc.errors()
            )
            quarantine_entry["_rejected_at_utc"] = rejected_at
            quarantine_rows.append(quarantine_entry)

    result = ValidationResult(
        dataset=dataset,
        valid_rows=valid_rows,
        quarantine_rows=quarantine_rows,
        total_count=len(rows),
    )

    if result.total_count > 0 and result.invalid_ratio > max_quarantine_ratio:
        raise DataQualityThresholdError(
            f"[{dataset}] Quarantine ratio {result.invalid_ratio:.1%} exceeded "
            f"circuit-breaker threshold {max_quarantine_ratio:.1%} "
            f"({len(quarantine_rows)}/{result.total_count} invalid rows)."
        )

    return result
