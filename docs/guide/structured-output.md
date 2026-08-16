# Structured output

Set `output_type=` to any Pydantic `BaseModel` subclass. Koala:

1. Uses the provider's native `response_format` when the model advertises
   `Capability.STRUCTURED_OUTPUT`.
2. Falls back to prompt-engineered JSON-schema hint otherwise.
3. Validates the final response into an instance of your model.

`result.output` is always a validated instance — you don't `json.loads`
anything yourself.

## Basic

```python
from pydantic import BaseModel, Field
from koala import Agent


class MovieRec(BaseModel):
    title: str = Field(description="The movie title.")
    year: int = Field(description="Release year.")
    genre: str = Field(description="Primary genre.")
    reason: str = Field(description="Why the user would like it.")


agent = Agent(
    "openai/gpt-4o-mini",
    instructions="Recommend one movie based on the user's taste.",
    output_type=MovieRec,
)

result = agent.run("I like slow-burn sci-fi.")
rec: MovieRec = result.output       # validated instance
print(rec.title, rec.year)
```

## Native vs prompt fallback

**Native**: When `Capability.STRUCTURED_OUTPUT in model.capabilities`,
Koala builds an OpenAI-style `response_format`:

```python
{
    "type": "json_schema",
    "json_schema": {
        "name": "MovieRec",
        "schema": <model_json_schema>,
        "strict": true,
    },
}
```

The provider constrains the model to emit exactly that shape. Providers
with this capability today: `openai`, `groq`, `xai`.

**Prompt fallback**: When the model doesn't advertise native support,
Koala appends a schema hint to the system prompt:

```
Return your answer strictly as a JSON object matching this schema:
{...}
Only the JSON — no prose, no code fences.
```

Then parses the assistant message text with `json.loads(...)` and
`YourModel.model_validate(...)`. If parsing fails, an `OutputParseError`
is raised and surfaced as `stop_reason="error"` on the `RunResult`.

## Nested and complex models

Full Pydantic support — nested models, `Optional`, `Literal`, unions,
enums, lists:

```python
from typing import Literal
from pydantic import BaseModel


class Address(BaseModel):
    street: str
    city: str
    country: str


class Contact(BaseModel):
    name: str
    email: str | None = None
    priority: Literal["low", "medium", "high"] = "medium"
    address: Address
    tags: list[str] = []


agent = Agent(
    "openai/gpt-4o-mini",
    instructions="Extract contact info from the user's message.",
    output_type=Contact,
)
```

## With tools

Structured output composes with tool calls. The model can call tools
during the loop and still return the final answer in the required shape:

```python
class WeatherReport(BaseModel):
    city: str
    temp_c: float
    condition: str


agent = Agent(
    "openai/gpt-4o-mini",
    instructions="Look up the weather and return a structured report.",
    tools=[get_weather],
    output_type=WeatherReport,
)

result = agent.run("What's the weather in Tokyo?")
print(result.output.temp_c)   # 18.0
```

## Validation errors

If the model produces JSON that doesn't validate:

```python
result = agent.run("...")
if result.stop_reason == "error":
    print(result.error)        # "Failed to parse output for agent 'x': ..."
```

The turn ends with a fatal `Error` event and `stop_reason="error"`. To
handle it gracefully, catch it and either retry with adjusted instructions
or fall back to raw text.

## Tips

- Pydantic `Field(description="...")` shows up as JSON-schema
  `description` and directly influences generation quality with native
  structured output.
- If a field is `Optional`, the model will often omit it. Make it
  required if it needs to be present.

## Reference

Under the hood, `koala.agents.output` builds the response format and
parses results:

- `build_response_format(model: type[BaseModel]) -> dict` — the native
  OpenAI-style envelope.
- `build_prompt_schema_hint(model: type[BaseModel]) -> str` — the
  system-prompt fallback.
- `parse_output(text: str, model: type[BaseModel], agent_name: str) -> BaseModel`
  — validate; raises `OutputParseError` on failure.

See the [API reference for `koala.agents`](../reference/agents.md).
