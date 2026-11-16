"""Tests for resilient FX API extraction and payload normalization."""

from datetime import date
from unittest.mock import MagicMock

import pytest
import requests

from src.extractors.fx_api import fetch_fx_rates, normalize_fx_payload


def test_normalize_fx_payload_includes_base_and_targets() -> None:
    payload = {
        "amount": 1.0,
        "base": "USD",
        "date": "2026-11-14",
        "rates": {"MXN": 20.1, "EUR": 0.94},
    }
    rows = normalize_fx_payload(payload, ingestion_date=date(2026, 11, 15))
    currencies = {r["target_currency"]: r["rate"] for r in rows}

    assert currencies["USD"] == 1.0
    assert currencies["MXN"] == 20.1
    assert currencies["EUR"] == 0.94
    assert all(r["ingestion_date"] == "2026-11-15" for r in rows)


def test_fetch_fx_rates_retries_on_429_then_succeeds(mocker: MagicMock) -> None:
    rate_limited = MagicMock()
    rate_limited.status_code = 429

    ok_resp = MagicMock()
    ok_resp.status_code = 200
    ok_resp.json.return_value = {
        "amount": 1.0,
        "base": "USD",
        "date": "2026-11-15",
        "rates": {"MXN": 20.25},
    }

    mock_get = mocker.patch(
        "src.extractors.fx_api.requests.get",
        side_effect=[rate_limited, ok_resp],
    )
    mocker.patch("time.sleep", return_value=None)

    rows = fetch_fx_rates(execution_date=date(2026, 11, 15), target_currencies=("MXN",))

    assert mock_get.call_count == 2
    assert any(r["target_currency"] == "MXN" and r["rate"] == 20.25 for r in rows)


def test_fetch_fx_rates_raises_after_exhausting_retries(mocker: MagicMock) -> None:
    mocker.patch(
        "src.extractors.fx_api.requests.get",
        side_effect=requests.Timeout("API timeout"),
    )
    mocker.patch("time.sleep", return_value=None)

    with pytest.raises(requests.Timeout):
        fetch_fx_rates(execution_date=date(2026, 11, 15))
