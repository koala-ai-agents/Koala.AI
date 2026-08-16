# Custom Airflow image with Koala installed.
#
# What this adds on top of `apache/airflow:3.1.5`:
#   1. Koala framework code at /opt/koala.
#   2. Koala installed in editable mode → pulls httpx + pydantic from
#      pyproject.toml as its runtime deps.
#
# That's all Koala needs on the Airflow worker. Anything more is added
# per-flow via _PIP_ADDITIONAL_REQUIREMENTS in docker-compose.yaml (quick
# checks) or by extending this Dockerfile (production).
FROM apache/airflow:3.1.5

USER root

# Copy the Koala source tree into the image.
# LICENSE is required — pyproject.toml declares `license = {file = "LICENSE"}`,
# and Hatchling refuses to build the editable install without it.
COPY pyproject.toml /opt/koala/pyproject.toml
COPY README.md /opt/koala/README.md
COPY LICENSE /opt/koala/LICENSE
COPY src /opt/koala/src

# Airflow expects these dirs to exist. Volumes mount over them at runtime,
# but pre-creating avoids a first-boot permission race.
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/config /opt/airflow/plugins

# The airflow user needs to own the Koala tree so pip -e can write build
# artifacts into it.
RUN chown -R airflow:root /opt/koala

USER airflow

# Install Koala in editable mode. Runtime deps (httpx, pydantic) come from
# pyproject.toml automatically.
RUN pip install --no-cache-dir -e /opt/koala
