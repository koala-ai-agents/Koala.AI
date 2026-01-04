#!/bin/bash
# filepath: e:\01_Projects\02_Main\02_Intel_Industrial_Program\kola\setup.sh

set -e  # Exit on error

echo "🐨 Koala Framework Setup for WSL"
echo "================================"

# Step 1: Create/activate virtual environment
./create_environment.sh

# Step 2: Install koala packages in editable mode with dev dependencies
echo ""
echo "📦 Installing Koala packages with dev dependencies..."
pip install -e ".[dev,airflow,llm]"

# Step 3: Install plugins if they exist
if [ -d "plugins" ]; then
    echo "📦 Installing plugins..."
    if [ -f "plugins/setup.py" ] || [ -f "plugins/pyproject.toml" ]; then
        pip install -e plugins
    else
        echo "⚠️  No setup.py or pyproject.toml found in plugins, skipping"
    fi
fi

# Step 4: Create .env if not exists
if [ ! -f .env ]; then
    echo ""
    echo "⚙️  Creating .env file..."
    if [ -f .env.example ]; then
        cp .env.example .env
        echo "✅ .env created from .env.example"
    else
        cat > .env << 'EOF'
# LLM Configuration
LLM_API_KEY=your_api_key_here
LLM_BASE_URL=https://api.groq.com/openai/v1
LLM_MODEL=llama-3.3-70b-versatile

# Airflow Configuration (optional)
AIRFLOW_URL=http://localhost:8080
AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=admin
EOF
        echo "✅ .env created with defaults"
    fi
    echo "⚠️  Please edit .env with your actual API keys!"
else
    echo "✅ .env already exists"
fi

# Step 5: Create required directories
echo ""
echo "📁 Creating required directories..."
mkdir -p dags logs config

# Step 6: Setup pre-commit hooks
echo ""
read -p "Install pre-commit hooks? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🔧 Installing pre-commit hooks..."
    pre-commit install
    echo "🔄 Running pre-commit on all files (first run)..."
    pre-commit run --all-files || true
    echo "✅ Pre-commit hooks installed and configured"
fi

# Step 7: Verify installation
echo ""
echo "🔍 Verifying installation..."
echo "  Python: $(python --version)"
echo "  Pytest: $(pytest --version | head -n1)"
echo "  Ruff: $(ruff --version)"
echo "  Black: $(black --version)"

# Step 8: Run tests to verify everything works
echo ""
read -p "Run tests to verify installation? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🧪 Running tests..."
    pytest tests/ -v || echo "⚠️  Some tests failed (this might be expected)"
fi

echo ""
echo "🎉 Setup complete!"
echo ""
echo "📋 Next steps:"
echo "  1. Edit .env with your LLM API keys"
echo "  2. Run tests: pytest tests/ -v"
echo "  3. Run linter: ruff check src/ tests/"
echo "  4. Format code: black src/ tests/"
echo "  5. Start Airflow: docker-compose up -d"
echo "  6. Access Airflow UI: http://localhost:8080"
echo ""
echo "💡 Useful commands:"
echo "  - Run tests: pytest"
echo "  - Run tests with coverage: pytest --cov"
echo "  - Check code style: ruff check ."
echo "  - Format code: black ."
echo "  - Type check: mypy src/"
echo "  - Pre-commit check: pre-commit run --all-files"
