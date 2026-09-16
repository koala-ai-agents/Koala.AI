# `koala.models`

L2 — model wrapper and provider layer. See the [Models guide](../guide/models.md),
[Providers](../getting-started/providers.md), and [Retries & resilience](../guide/resilience.md).
Both `Model` and `UniversalProvider` accept a `retry_policy: RetryPolicy` from `koala.core`.

## Model

::: koala.models.model.Model

## Provider base

::: koala.models.base.BaseProvider

## Universal provider

::: koala.models.universal.UniversalProvider

## Registry

::: koala.models.registry.ProviderProfile
::: koala.models.registry.BUILTIN_PROVIDERS
::: koala.models.registry.register_provider
::: koala.models.registry.unregister_provider
::: koala.models.registry.get_provider_profile
::: koala.models.registry.list_providers

## Chat settings

::: koala.models.settings.ChatSettings

## API keys

::: koala.models.keys.UNIVERSAL_ENV_KEY
::: koala.models.keys.resolve_api_key

## Errors

::: koala.models.errors.ProviderError
::: koala.models.errors.AuthenticationError
::: koala.models.errors.RateLimitError
::: koala.models.errors.BadRequestError
::: koala.models.errors.ProviderTimeoutError
::: koala.models.errors.ProviderConnectionError
::: koala.models.errors.ProviderServerError
::: koala.models.errors.MissingApiKey
