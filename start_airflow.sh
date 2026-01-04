#!/bin/bash
# Airflow startup script
# This script creates/activates virtual environment, installs dependencies, and starts Airflow

# Check if virtual environment exists
if [ ! -d "airflow_env" ]; then
    echo "============================================"
    echo "Virtual environment 'airflow_env' not found."
    echo "Creating virtual environment with Python 3.12..."
    echo "============================================"

    # Create virtual environment with Python 3.12
    python3.12 -m venv airflow_env

    # Activate the virtual environment
    source airflow_env/bin/activate

    # Upgrade pip
    echo "Upgrading pip..."
    pip install --upgrade pip

    # Install requirements
    echo "Installing requirements from requirements.txt..."
    pip install -r requirements.txt

    # Set AIRFLOW_HOME
    export AIRFLOW_HOME=~/airflow

    # Initialize Airflow database
    echo "Initializing Airflow database..."
    airflow db init

    echo "============================================"
    echo "Virtual environment setup complete!"
    echo "Please check $AIRFLOW_HOME for admin password after Airflow starts."
    echo "============================================"
else
    echo "============================================"
    echo "Virtual environment 'airflow_env' found."
    echo "Activating virtual environment..."
    echo "============================================"

    # Activate the virtual environment
    source airflow_env/bin/activate
fi

# Set up Airflow environment variables (use double underscores for config overrides)
export AIRFLOW__CORE__AUTH_MANAGER="airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"

# Set JWT secret for API authentication (use double underscores)
export AIRFLOW__API_AUTH__JWT_SECRET="your-fixed-secret-key-change-this-in-production-make-it-long-and-secure"

# Set AIRFLOW_HOME to the native installation (not the docker one)
export AIRFLOW_HOME=~/airflow

# Display the admin password (if it exists)
if [ -f "$AIRFLOW_HOME/standalone_admin_password.txt" ]; then
    echo "============================================"
    echo "Airflow Admin Credentials:"
    echo "Username: admin"
    echo "Password: $(cat $AIRFLOW_HOME/standalone_admin_password.txt)"
    echo "============================================"
else
    echo "============================================"
    echo "Note: Admin password will be generated on first run."
    echo "Check $AIRFLOW_HOME/standalone_admin_password.txt after startup."
    echo "============================================"
fi

# Start Airflow in standalone mode
echo "Starting Airflow standalone..."
airflow standalone
