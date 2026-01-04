"""OpenAI-compatible LLM client adapter.

Supports providers that expose the OpenAI API surface (e.g., Groq, OpenRouter,
Azure OpenAI). Minimal chat interface with streaming optional.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

try:
    import httpx
except Exception:  # pragma: no cover - optional dep may be missing
    httpx = None  # type: ignore


class LLMError(Exception):
    pass


class LLMClient:
    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.openai.com/v1",
        timeout: float = 30.0,
    ):
        if httpx is None:
            raise LLMError(
                "httpx not installed. Install with extras: pip install '.[llm]'"
            )
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._client = httpx.Client(timeout=timeout)

    def chat(
        self,
        model: str,
        messages: List[Dict[str, str]],
        temperature: Optional[float] = None,
        stream: bool = False,
    ) -> Any:
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload: Dict[str, Any] = {
            "model": model,
            "messages": messages,
            "stream": stream,
        }
        if temperature is not None:
            payload["temperature"] = temperature

        try:
            if stream:
                return self._stream_chat(url, headers, payload)
            resp = self._client.post(url, headers=headers, json=payload)
            try:
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:  # type: ignore[name-defined]
                # Include response body for easier debugging
                raise LLMError(f"{e} | body={resp.text}")
            return resp.json()
        except httpx.HTTPError as e:  # type: ignore[name-defined]
            raise LLMError(str(e))

    def _stream_chat(self, url: str, headers: Dict[str, str], payload: Dict[str, Any]):
        def gen():
            with self._client.stream("POST", url, headers=headers, json=payload) as r:
                r.raise_for_status()
                for line in r.iter_lines():
                    if not line:
                        continue
                    if line.startswith(b"data:"):
                        data = line[len(b"data:") :].strip()
                        if data == b"[DONE]":
                            break
                        yield json.loads(data)

        return gen()
