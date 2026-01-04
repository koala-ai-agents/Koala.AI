# Koala Framework - Makefile for common tasks
.PHONY: help install test lint format clean all pre-commit

help:  ## Show this help message
	@echo "🐨 Koala Framework - Available Commands"
	@echo "========================================"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:  ## Install package with dev dependencies
	pip install -e ".[dev,airflow,llm]"

test:  ## Run tests with coverage
	pytest tests/ -v --cov=src/koala --cov-report=term-missing --cov-report=html

test-fast:  ## Run tests without coverage (faster)
	pytest tests/ -v

lint:  ## Run linter (ruff)
	ruff check src/ tests/ cookbook/

lint-fix:  ## Run linter and fix issues
	ruff check --fix src/ tests/ cookbook/

format:  ## Format code with black and isort
	black src/ tests/ cookbook/
	isort src/ tests/ cookbook/

format-check:  ## Check formatting without modifying files
	black --check src/ tests/ cookbook/
	isort --check-only src/ tests/ cookbook/

type-check:  ## Run type checker (mypy)
	mypy src/koala

pre-commit:  ## Run all pre-commit hooks
	pre-commit run --all-files

clean:  ## Clean build artifacts and cache
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf build/ dist/ .coverage htmlcov/ .pytest_cache/ .ruff_cache/ .mypy_cache/

clean-logs:  ## Clean Airflow logs
	rm -rf logs/
	mkdir -p logs

clean-all: clean clean-logs  ## Clean everything including logs

docker-up:  ## Start Airflow with Docker Compose
	docker-compose up -d

docker-down:  ## Stop Airflow
	docker-compose down

docker-restart:  ## Restart Airflow
	docker-compose restart

docker-logs:  ## Show Airflow logs
	docker-compose logs -f

airflow-test:  ## Test Airflow DAG generation
	python cookbook/web_search_agent_airflow_clean.py

all: clean format lint test  ## Run full CI pipeline (format, lint, test)

ci: format-check lint type-check test  ## Run CI checks without modifying files
