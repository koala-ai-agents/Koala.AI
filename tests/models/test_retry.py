from __future__ import annotations

import httpx
import pytest

from koala.core.retry import RetryPolicy, parse_retry_after, retry_async
from koala.models.errors import (
    AuthenticationError,
    RateLimitError,
)
from koala.models.universal import UniversalProvider


def test_parse_retry_after():
    assert parse_retry_after(None) is None
    assert parse_retry_after("") is None
    assert parse_retry_after("15") == 15.0
    assert parse_retry_after("0") == 0.0
    assert parse_retry_after("-5") == 0.0
    assert parse_retry_after("invalid-value") is None


def test_compute_delay_jitter_and_cap():
    policy = RetryPolicy(initial_delay=1.0, max_delay=10.0, backoff_factor=2.0, jitter=False)
    assert policy.compute_delay(0) == 1.0
    assert policy.compute_delay(1) == 2.0
    assert policy.compute_delay(2) == 4.0
    assert policy.compute_delay(3) == 8.0
    assert policy.compute_delay(4) == 10.0  # capped at max_delay

    # With retry_after higher than computed
    assert policy.compute_delay(0, retry_after=5.0) == 5.0
    # retry_after capped at max_delay
    assert policy.compute_delay(0, retry_after=15.0) == 10.0


@pytest.mark.asyncio
async def test_retry_async_success_after_failures():
    attempts = 0

    async def flaky():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RateLimitError("429 rate limited", status=429)
        return "success"

    policy = RetryPolicy(max_retries=3, initial_delay=0.01, jitter=False)

    def is_retryable(e):
        return (isinstance(e, RateLimitError), None)

    result = await retry_async(flaky, policy, is_retryable=is_retryable)
    assert result == "success"
    assert attempts == 3


@pytest.mark.asyncio
async def test_retry_async_non_retryable_fails_immediately():
    attempts = 0

    async def bad_key():
        nonlocal attempts
        attempts += 1
        raise AuthenticationError("401 unauthorized", status=401)

    policy = RetryPolicy(max_retries=3, initial_delay=0.01)

    def is_retryable(e):
        return (isinstance(e, RateLimitError), None)

    with pytest.raises(AuthenticationError):
        await retry_async(bad_key, policy, is_retryable=is_retryable)
    assert attempts == 1


@pytest.mark.asyncio
async def test_universal_provider_retries_transient_429():
    call_count = 0

    def mock_handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(429, headers={"Retry-After": "0.01"}, json={"error": "rate limited"})
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"role": "assistant", "content": "hello"}}],
                "usage": {"prompt_tokens": 10, "completion_tokens": 5},
            },
        )

    transport = httpx.MockTransport(mock_handler)
    policy = RetryPolicy(max_retries=2, initial_delay=0.01, jitter=False)
    provider = UniversalProvider(
        slug="test",
        base_url="https://api.test.com/v1",
        api_key="sk-test",
        transport=transport,
        retry_policy=policy,
    )

    from koala.models.settings import ChatSettings

    msg, usage = await provider.chat(
        model_name="test-model",
        messages=[],
        settings=ChatSettings(),
    )
    assert msg.text == "hello"
    assert call_count == 2
