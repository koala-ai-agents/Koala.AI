"""Provider-level exception hierarchy.

Distinct exception types let callers do targeted retry logic (retry
`RateLimitError` and `ProviderServerError`, but not `AuthenticationError`)
without brittle string matching on error text.
"""

from __future__ import annotations


class ProviderError(Exception):
    """Base for all provider-related errors."""

    def __init__(
        self,
        message: str,
        *,
        status: int | None = None,
        body: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.body = body


class AuthenticationError(ProviderError):
    """401 or 403 from the provider — bad or missing API key."""


class RateLimitError(ProviderError):
    """429 from the provider — rate-limited. Retry with backoff."""


class BadRequestError(ProviderError):
    """4xx from the provider — malformed request, bad model name, etc."""


class ProviderServerError(ProviderError):
    """5xx from the provider. Usually transient; retryable."""


class ProviderTimeoutError(ProviderError):
    """The HTTP request timed out."""


class ProviderConnectionError(ProviderError):
    """Could not connect to the provider (DNS, network, TLS, etc.)."""


class MissingApiKey(ProviderError):
    """No API key could be resolved via explicit arg, provider env, or LLM_API_KEY."""
