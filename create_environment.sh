#!/bin/bash
# filepath: e:\01_Projects\02_Main\02_Intel_Industrial_Program\kola\create_environment.sh

set -e

VENV_NAME="airflow_env"

echo "🐨 Koala Virtual Environment Setup"
echo "===================================="

if [ -d "$VENV_NAME" ]; then
    echo "✅ Virtual environment '$VENV_NAME' already exists"
    echo "🔄 Activating virtual environment..."
    source "$VENV_NAME/bin/activate"
    echo "✅ Activated: $VENV_NAME"
    echo "📍 Python: $(which python)"
    echo "📦 Pip: $(which pip)"
else
    echo "📦 Creating virtual environment: $VENV_NAME"
    python3 -m venv "$VENV_NAME"

    echo "🔄 Activating virtual environment..."
    source "$VENV_NAME/bin/activate"

    echo "⬆️  Upgrading pip..."
    pip install --upgrade pip

    echo "📥 Installing requirements from requirements.txt..."
    if [ -f "requirements.txt" ]; then
        pip install -r requirements.txt
        echo "✅ Requirements installed"
    else
        echo "⚠️  requirements.txt not found, skipping"
    fi

    echo "✅ Virtual environment created and activated"
fi

echo ""
echo "🎉 Ready to go!"
echo "💡 To activate later: source $VENV_NAME/bin/activate"
