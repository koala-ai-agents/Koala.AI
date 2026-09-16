# Koala Framework — common developer tasks.
.PHONY: help install test test-fast test-integration lint lint-fix format format-check type-check pre-commit clean all ci

help:  ## Show this help message
	@echo "Koala Framework - Available Commands"
	@echo "===================================="
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:  ## Install package with dev dependencies (uv preferred)
	uv pip install -e ".[dev,mcp,otel]"

test:  ## Run all tests with coverage (excludes integration tests unless env is set)
	pytest tests/

test-fast:  ## Run tests without coverage (faster)
	pytest tests/ --no-cov

test-integration:  ## Run env-gated live-provider integration tests
	pytest tests/integration/ -m integration --no-cov -v

lint:  ## Run linter (ruff)
	ruff check src/koala tests examples

lint-fix:  ## Run linter and auto-fix issues
	ruff check --fix src/koala tests examples

format:  ## Format code with black and isort
	black src/koala tests examples
	isort src/koala tests examples

format-check:  ## Check formatting without modifying files
	black --check src/koala tests examples
	isort --check-only src/koala tests examples

type-check:  ## Run type checker (mypy)
	mypy src/koala

pre-commit:  ## Run all pre-commit hooks
	pre-commit run --all-files

clean:  ## Clean build artifacts and cache
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf build/ dist/ .coverage htmlcov/ .pytest_cache/ .ruff_cache/ .mypy_cache/ site/

build: clean  ## Build wheel and source distributions
	uv build

check-dist: build  ## Validate built distributions with twine
	uvx twine check dist/*

publish-test: check-dist  ## Publish distribution to TestPyPI
	uv publish --publish-url https://test.pypi.org/legacy/

publish: check-dist  ## Publish distribution to PyPI
	uv publish

all: clean format lint test  ## Run full CI pipeline (format, lint, test)

ci: format-check lint type-check test  ## Run CI checks without modifying files
