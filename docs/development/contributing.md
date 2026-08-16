# Contributing

Contributions are welcome. This page documents the actual workflow —
what the Makefile does, what CI would run if it existed, and where the
seams are for extending Koala without patching the framework itself.

## Setup

```bash
git clone https://github.com/PR-HARIHARAN/Koala.AI
cd Koala.AI

uv venv
uv pip install -e ".[dev,mcp,otel,docs]"
```

Python 3.12+ required.

## Local development loop

```bash
make lint       # ruff check
make lint-fix   # ruff check --fix
make format     # black + isort
make type-check # mypy
make test       # pytest with coverage
make test-fast  # pytest --no-cov
```

Or the full CI-shaped pipeline:

```bash
make ci   # format-check + lint + type-check + test
```

## Documentation

The site is built with `mkdocs` + `mkdocs-material` +
`mkdocstrings-python`.

```bash
# Live preview at http://127.0.0.1:8000
mkdocs serve

# One-shot build (writes to site/)
mkdocs build

# Strict build — fails on any warning (broken links, missing refs)
mkdocs build --strict
```

Docs live under `docs/`. See the existing pages for style. Grounding rule:
every claim in the docs must be checkable against source under
`src/koala/`. If a feature isn't implemented, the docs say so.

## Project conventions

- **New API only under `koala.<subpackage>`.** Anything in
  `koala._legacy/` is quarantined and off-limits — don't extend it.
- **Every layer speaks the same event stream.** New primitives should
  emit / consume `koala.core.events.Event` types, not custom callback
  shapes.
- **Async first.** Sync methods are thin `asyncio.run()` wrappers.
- **Protocols over ABCs** where structural typing suffices (`Behavior`,
  `ApprovalRule`, `Runnable`, `Channel`, `Checkpointer`). ABCs where a
  base class is doing real work (`BaseTool`, `BaseAgent`, `BaseMemory`,
  `BaseProvider`).
- **Google-style docstrings.** Google format is what `parse_google_docstring`
  understands, so tool docstrings need it. Everything else is free to
  match, and mkdocstrings surfaces docstrings verbatim.
- **Runtime imports of optional deps.** Modules like
  `koala.tools.mcp` and `koala.observability.otel` import their optional
  deps inside functions or with a `try/except ImportError` guard so
  the base install stays two-dep.

## Where to extend

- **New provider** — `koala.core.register_provider(...)`. If it needs a
  non-OpenAI wire, subclass `BaseProvider` and inject via
  `Model(provider_instance=...)`.
- **New tool source** — build a `BaseTool` subclass or wire an MCP
  server. Nothing else to hook.
- **New memory backend** — implement `BaseMemory`. Four async methods.
- **New checkpoint backend** — implement the `Checkpointer` Protocol.
  Five async methods.
- **New behavior** — any class with a `name` attribute and
  `apply(spec: AgentSpec)` method.
- **New approval rule** — any class with a `check(ctx, call, /)` method
  returning `"allow" | "deny" | "ask" | None`.
- **New agent** — subclass `BaseAgent` and implement `astream`. Only
  do this if you're replacing the whole tool-calling loop.

Each of these is documented in the corresponding
[Guides](../guide/agents.md) page — check the "Custom X" section.

## Git safety

- Prefer specific `git add <file>` over `git add -A`.
- Never commit `koala_runs.db`, `.coverage`, `htmlcov/`, `.ruff_cache/`,
  `.pytest_cache/` — all are `.gitignore`'d.
- Never commit real API keys. Use `.env.local` (gitignored) and
  `os.environ.get(...)` in code.

## Pre-commit hooks

The repo ships `.pre-commit-config.yaml` with `ruff`, `black`, `isort`,
trailing-whitespace, and YAML validation. Install it:

```bash
uv pip install pre-commit
pre-commit install
```

Hooks run on every `git commit`. `pre-commit run --all-files` runs them
on demand.

## Reporting bugs

Open an issue at [github.com/PR-HARIHARAN/Koala.AI/issues](https://github.com/PR-HARIHARAN/Koala.AI/issues)
with:

1. Python version + OS.
2. Extras installed (`[dev,mcp,otel,docs]`?).
3. A minimal reproducer — ideally using `ScriptedProvider` from
   `tests/agents/conftest.py` so nobody needs credentials.
4. What you expected vs what happened.

## Sending a change

1. Fork.
2. Branch from `main`.
3. Make the change with tests.
4. `make ci` — all green locally.
5. Open a PR. Small, focused PRs land faster than sprawling ones.

## Code of conduct

Be respectful. Discussion happens in issues and PRs; personal attacks,
harassment, or discriminatory language get you shown the door.
