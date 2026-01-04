# Custom Airflow image that installs project requirements at build time
FROM apache/airflow:3.1.5

# Switch to root to copy files and fix permissions
USER root

# Copy files that need to be installed
COPY src/koala /opt/airflow/src/koala
COPY requirements.txt /requirements.txt
COPY plugins /opt/airflow/koala_plugins_src

# Create expected directories (volumes will mount over these at runtime)
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/config /opt/airflow/plugins

# Fix ownership of all copied files so airflow user can access them
RUN chown -R airflow:root /opt/airflow/src /opt/airflow/koala_plugins_src

# Switch to airflow user before running pip
USER airflow

# Install local Koala framework into the image
RUN pip install --no-cache-dir -e /opt/airflow/src/koala

# Install koala plugins as a proper package
RUN pip install --no-cache-dir -e /opt/airflow/koala_plugins_src

# Copy requirements and install them (including providers and extras)
RUN pip install --no-cache-dir -r /requirements.txt

# Environment is ready - plugins directory will be empty (preventing Airflow from loading .py files as plugins)
