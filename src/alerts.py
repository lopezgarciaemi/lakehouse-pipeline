"""Structured operational alerting with optional webhook delivery."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import requests

from src.config import get_settings

logger = logging.getLogger(__name__)


def send_alert(
    title: str,
    message: str,
    severity: str = "ERROR",
    context: dict[str, Any] | None = None,
    webhook_url: str | None = None,
) -> bool:
    """Emit a structured alert log and optionally post to a Slack/Discord/Teams webhook."""
    url = webhook_url if webhook_url is not None else get_settings().alert_webhook_url
    payload = {
        "timestamp_utc": datetime.now(tz=UTC).isoformat(),
        "severity": severity,
        "title": title,
        "message": message,
        "context": context or {},
    }

    log_fn = logger.error if severity == "ERROR" else logger.info
    log_fn("ALERT [%s] %s: %s | context=%s", severity, title, message, payload["context"])

    if not url:
        return False

    try:
        # Format compatible with Slack/Discord/generic JSON webhooks
        body = {
            "text": f"[{severity}] *{title}*\n{message}\n```{payload['context']}```",
            "content": f"**[{severity}] {title}**\n{message}",
            "metadata": payload,
        }
        resp = requests.post(url, json=body, timeout=5)
        resp.raise_for_status()
        return True
    except requests.RequestException as exc:
        logger.warning("Webhook delivery failed (pipeline state unaffected): %s", exc)
        return False
