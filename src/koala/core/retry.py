"""Automatic retry utilities with jittered exponential backoff and Retry-After support.

Zero external dependencies — implemented using Python stdlib (asyncio, random, time).
Follows standard full-jitter exponential backoff (AWS / Vercel AI SDK pattern):
    sleep = min(max_delay, uniform(0, initial_delay * (backoff_factor ** attempt)))
"""

from __future__ import annotations

import asyncio
import email.utils
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar

T = TypeVar("T")

# Default HTTP status codes that indicate a transient, retryable failure.
DEFAULT_RETRYABLE_STATUS_CODES: frozenset[int] = frozenset(
    {
        408,  # Request Timeout
        429,  # Too Many Requests / Rate Limit
        500,  # Internal Server Error
        502,  # Bad Gateway
        503,  # Service Unavailable
        504,  # Gateway Timeout
    }
)


def parse_retry_after(header_value: str | None) -> float | None:
    """Parse a standard HTTP `Retry-After` header value.

    Can be either integer seconds (e.g. "120") or an HTTP-date format
    (e.g. "Wed, 21 Oct 2026 07:28:00 GMT"). Returns seconds to wait,
    or None if invalid/absent.
    """
    if not header_value:
        return None
    header_str = header_value.strip()
    # Try integer seconds first
    try:
        sec = float(header_str)
        return max(0.0, sec)
    except ValueError:
        pass

    # Try HTTP date
    try:
        target_time = email.utils.parsedate_to_datetime(header_str).timestamp()
        diff = target_time - time.time()
        return max(0.0, diff)
    except Exception:
        return None


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Configuration for automatic retry with exponential backoff and jitter.

    Args:
        max_retries: Maximum number of retry attempts (0 disables retries).
        initial_delay: Base delay in seconds before the first retry attempt.
        max_delay: Upper cap on retry delay in seconds.
        backoff_factor: Multiplier applied per attempt (default 2.0).
        jitter: If True, uses full jitter to avoid thundering herd problems.
        retryable_status_codes: HTTP status codes to retry on.
    """

    max_retries: int = 3
    initial_delay: float = 0.5
    max_delay: float = 30.0
    backoff_factor: float = 2.0
    jitter: bool = True
    retryable_status_codes: frozenset[int] = DEFAULT_RETRYABLE_STATUS_CODES

    def compute_delay(
        self, attempt: int, retry_after: float | None = None
    ) -> float:
        """Compute the delay for a given attempt (0-indexed).

        If `retry_after` is provided and exceeds the computed backoff,
        `retry_after` takes precedence, capped at `max_delay`.
        """
        base = self.initial_delay * (self.backoff_factor**attempt)
        capped = min(self.max_delay, base)
        if self.jitter:
            delay = random.uniform(0.0, capped)
        else:
            delay = capped

        if retry_after is not None and retry_after > 0:
            delay = min(self.max_delay, max(delay, retry_after))
        return delay


async def retry_async(
    func: Callable[[], Awaitable[T]],
    policy: RetryPolicy,
    *,
    is_retryable: Callable[[Exception], tuple[bool, float | None]],
    on_retry: Callable[[Exception, int, float], None] | None = None,
) -> T:
    """Execute an async callable with automatic retry governed by RetryPolicy.

    Args:
        func: Zero-argument async function to call.
        policy: The RetryPolicy to enforce.
        is_retryable: Predicate returning (retryable: bool, retry_after_sec: float | None).
        on_retry: Optional hook called before sleeping on each retry attempt.
    """
    attempt = 0
    while True:
        try:
            return await func()
        except Exception as exc:
            if attempt >= policy.max_retries:
                raise

            retryable, retry_after = is_retryable(exc)
            if not retryable:
                raise

            delay = policy.compute_delay(attempt, retry_after)
            if on_retry is not None:
                on_retry(exc, attempt + 1, delay)

            await asyncio.sleep(delay)
            attempt += 1
