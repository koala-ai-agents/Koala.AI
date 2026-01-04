"""Output adapters for Kola.

Currently includes a simple webhook sender. This can be extended to S3,
databases, or other sinks.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


class OutputError(Exception):
    pass


def send_webhook(
    url: str,
    payload: Dict[str, Any],
    headers: Dict[str, str] | None = None,
    timeout: float = 5.0,
) -> int:
    """Send a JSON payload to a webhook URL. Returns HTTP status code.

    Raises OutputError on failures.
    """
    try:
        import requests  # type: ignore[import-untyped]

        resp = requests.post(url, json=payload, headers=headers or {}, timeout=timeout)
        resp.raise_for_status()
        return int(resp.status_code)
    except Exception as e:
        raise OutputError(str(e)) from e


def send_to_file(path: str, payload: Dict[str, Any]) -> None:
    """Write payload as JSON to a file (append mode). Useful as a simple output adapter for demos.

    The function ensures parent directories exist.
    """
    try:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False))
            fh.write("\n")
    except Exception as e:
        raise OutputError(str(e)) from e
