"""Resilient FX reference-rate extractor using the Frankfurter public REST API."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

logger = logging.getLogger(__name__)

DEFAULT_TARGETS = ("MXN", "EUR", "GBP", "CAD", "JPY")


class FxApiRateLimitError(requests.RequestException):
    """Raised when the upstream FX API returns HTTP 429 or transient 5xx."""


def get_mock_fx_payload(execution_date: date, base_currency: str = "USD") -> dict[str, Any]:
    """Deterministic fallback payload for offline local runs and CI."""
    return {
        "amount": 1.0,
        "base": base_currency,
        "date": execution_date.isoformat(),
        "rates": {
            "MXN": 20.15,
            "EUR": 0.942,
            "GBP": 0.789,
            "CAD": 1.402,
            "JPY": 154.30,
        },
    }


def normalize_fx_payload(
    payload: dict[str, Any],
    ingestion_date: date,
    source: str = "frankfurter_ecb",
) -> list[dict[str, Any]]:
    """Normalize raw Frankfurter JSON response into tabular FX rate records."""
    base = str(payload["base"]).upper()
    rate_date = str(payload["date"])
    rates: dict[str, float] = payload.get("rates", {})

    rows: list[dict[str, Any]] = [
        {
            "rate_date": rate_date,
            "ingestion_date": ingestion_date.isoformat(),
            "base_currency": base,
            "target_currency": base,
            "rate": 1.0,
            "source": source,
        }
    ]
    for target_currency, rate_value in rates.items():
        rows.append(
            {
                "rate_date": rate_date,
                "ingestion_date": ingestion_date.isoformat(),
                "base_currency": base,
                "target_currency": str(target_currency).upper(),
                "rate": float(rate_value),
                "source": source,
            }
        )
    return rows


@retry(
    retry=retry_if_exception_type((requests.RequestException, FxApiRateLimitError)),
    stop=stop_after_attempt(4),
    wait=wait_exponential_jitter(initial=1, max=10),
    reraise=True,
)
def _request_fx_json(url: str, params: dict[str, str], timeout: int = 10) -> dict[str, Any]:
    response = requests.get(url, params=params, timeout=timeout)
    if response.status_code == 429 or response.status_code >= 500:
        raise FxApiRateLimitError(
            f"Transient upstream status {response.status_code} from {url}"
        )
    response.raise_for_status()
    return response.json()


def fetch_fx_rates(
    execution_date: date,
    base_currency: str = "USD",
    target_currencies: tuple[str, ...] = DEFAULT_TARGETS,
    base_url: str = "https://api.frankfurter.app",
    use_mock: bool = False,
) -> list[dict[str, Any]]:
    """Fetch daily FX rates for `execution_date` with retry/backoff resilience."""
    if use_mock:
        payload = get_mock_fx_payload(execution_date, base_currency=base_currency)
        return normalize_fx_payload(payload, ingestion_date=execution_date, source="mock_ecb")

    url = f"{base_url.rstrip('/')}/{execution_date.isoformat()}"
    params = {
        "from": base_currency.upper(),
        "to": ",".join(c.upper() for c in target_currencies if c.upper() != base_currency.upper()),
    }
    logger.info("Fetching FX rates from %s with params=%s", url, params)
    payload = _request_fx_json(url, params=params)
    return normalize_fx_payload(payload, ingestion_date=execution_date)
