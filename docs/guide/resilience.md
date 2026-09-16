# Retries & Resilience

Production AI systems interact with external network services subject to rate limits (HTTP 429), server hiccups (HTTP 502/503), timeouts, and occasional schema drift from non-deterministic models.

Koala provides a multi-layer resilience system:

1. **Transport Layer**: Automatic exponential backoff with full jitter and `Retry-After` compliance.
2. **Tool Execution Layer**: Tool-driven `ModelRetry` allowing functions to ask the model to re-try with corrected parameters.
3. **Structured Output Layer**: An automated reflection loop (`max_output_retries`) that feeds Pydantic validation errors back to the model for self-correction.

---

## 1. Automatic Retries & Backoff

Every `Model` in Koala is equipped with a `RetryPolicy`. Whenever an upstream provider returns a transient error (such as a 429 rate limit or 503 service unavailable), Koala retries automatically using jittered exponential backoff.

### Default Policy
By default, Koala automatically retries:
- **HTTP status codes**: `408`, `429`, `500`, `502`, `503`, `504`
- **Network exceptions**: `httpx.TimeoutException`, `httpx.ConnectError`
- **Backoff schedule**: Exponential backoff with full random jitter to prevent thundering herd spikes.
- **Header compliance**: Honors RFC 7231 `Retry-After` headers (both integer seconds and RFC 1123 HTTP dates).

### Customizing `RetryPolicy`

```python
from koala import Model
from koala.core import RetryPolicy

# Custom retry policy: up to 5 attempts, with max 60s cap
aggressive_retry = RetryPolicy(
    max_retries=5,
    initial_delay=1.0,
    max_delay=60.0,
    backoff_factor=2.0,
    jitter=True,
    retryable_statuses={429, 500, 502, 503, 504},
)

# Attach directly to the model
model = Model(
    "groq/llama-3.3-70b-versatile",
    retry_policy=aggressive_retry,
)
```

### Disabling Retries
To disable retries for fail-fast scenarios or unit tests:

```python
no_retries = RetryPolicy(max_retries=0)
model = Model("openai/gpt-4o-mini", retry_policy=no_retries)
```

---

## 2. Tool-Driven Retries (`ModelRetry`)

Sometimes a tool is invoked with valid types, but the provided arguments fail domain-level business logic (e.g. "Order #123 does not exist", or "Search query was too broad").

Instead of crashing or failing the agent run, your tool can raise `ModelRetry`:

```python
from koala import tool
from koala.core import ModelRetry


@tool
def cancel_order(order_id: str) -> str:
    """Cancel a customer order by its 8-character ID."""
    if not order_id.startswith("ORD-"):
        # Tell the model what was wrong so it can self-correct!
        raise ModelRetry(
            f"Invalid order ID format '{order_id}'. Order IDs must start with 'ORD-' (e.g. 'ORD-98412')."
        )
    ...
    return f"Order {order_id} successfully cancelled."
```

### What Happens Behind the Scenes:
1. `FunctionTool` catches `ModelRetry` and surfaces it as a tool error message: `"Tool requested retry: <message>"`.
2. The agent loop appends this error message to the conversation history.
3. The LLM receives the feedback on its next turn, reads the instruction, and re-invokes the tool with the corrected arguments.

---

## 3. Structured Output Reflection Loop

When requiring structured output (`output_type=YourSchema`), the model may occasionally omit a required field or fail validation constraints.

Instead of terminating with an error, set `max_output_retries`:

```python
from pydantic import BaseModel, Field
from koala import Agent


class UserProfile(BaseModel):
    username: str = Field(min_length=3)
    age: int = Field(ge=18, description="Must be at least 18.")
    skills: list[str] = Field(min_length=1)


agent = Agent(
    "openai/gpt-4o-mini",
    instructions="Extract user profile information.",
    output_type=UserProfile,
    max_output_retries=2,   # Allow up to 2 self-correction reflection turns
)

result = agent.run("Name is John, age 16, likes Python")
```

### Reflection Lifecycle:
1. **Model Generation**: The LLM outputs a payload (e.g. with `age=16`).
2. **Validation Failure**: Pydantic detects `age < 18` and raises `OutputParseError`.
3. **Automated Feedback Injection**: The agent intercepts the error and automatically appends a targeted prompt to the model:
   ```text
   Your response did not match the required schema. Validation errors:
   - age: Input should be greater than or equal to 18
   Please correct the errors and return the valid JSON object strictly matching the schema.
   ```
4. **Self-Correction**: The LLM re-evaluates, corrects the issue (or explains the conflict), and returns valid structured output.
