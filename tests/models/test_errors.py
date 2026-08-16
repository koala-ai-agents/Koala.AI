"""Tests for koala.models.errors — the exception hierarchy."""

from __future__ import annotations

from koala.models import (
    AuthenticationError,
    BadRequestError,
    MissingApiKey,
    ProviderConnectionError,
    ProviderError,
    ProviderServerError,
    ProviderTimeoutError,
    RateLimitError,
)


def test_every_provider_error_subclasses_provider_error() -> None:
    for cls in (
        AuthenticationError,
        RateLimitError,
        BadRequestError,
        ProviderServerError,
        ProviderTimeoutError,
        ProviderConnectionError,
        MissingApiKey,
    ):
        assert issubclass(cls, ProviderError)


def test_error_carries_status_and_body_attributes() -> None:
    e = RateLimitError("too many", status=429, body='{"error": "rate limit"}')
    assert e.status == 429
    assert e.body == '{"error": "rate limit"}'
    assert "too many" in str(e)


def test_error_defaults_status_and_body_to_none() -> None:
    e = ProviderError("boom")
    assert e.status is None
    assert e.body is None
