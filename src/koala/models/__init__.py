"""Koala models package (L2).

Exports the user-facing `Model` class, the abstract `BaseProvider`, the
concrete `UniversalProvider` for OpenAI-compatible endpoints, the built-in
provider registry, chat settings, and the provider error hierarchy.
"""

from __future__ import annotations

from .base import BaseProvider
from .errors import (
    AuthenticationError,
    BadRequestError,
    MissingApiKey,
    ProviderConnectionError,
    ProviderError,
    ProviderServerError,
    ProviderTimeoutError,
    RateLimitError,
)
from .keys import UNIVERSAL_ENV_KEY, resolve_api_key
from .model import Model
from .registry import (
    BUILTIN_PROVIDERS,
    ProviderProfile,
    get_provider_profile,
    list_providers,
    register_provider,
    unregister_provider,
)
from .settings import ChatSettings
from .universal import UniversalProvider

__all__ = [
    # Main user-facing class
    "Model",
    # Providers
    "BaseProvider",
    "UniversalProvider",
    # Registry
    "ProviderProfile",
    "BUILTIN_PROVIDERS",
    "register_provider",
    "unregister_provider",
    "get_provider_profile",
    "list_providers",
    # Settings
    "ChatSettings",
    # Key resolution
    "UNIVERSAL_ENV_KEY",
    "resolve_api_key",
    # Errors
    "ProviderError",
    "AuthenticationError",
    "RateLimitError",
    "BadRequestError",
    "ProviderTimeoutError",
    "ProviderConnectionError",
    "ProviderServerError",
    "MissingApiKey",
]
