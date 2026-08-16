"""User Flow modules deployed to Airflow.

Anything importable at ``koala_flows.*`` is available to both the host
process (which generates the DAG file + JSON spec) and to Airflow workers
(which execute the DAG tasks). Airflow puts ``/opt/airflow/dags`` on
``PYTHONPATH`` inside every container, so the workers can resolve
``koala_flows.<module>:<action>`` action refs recorded in the spec.
"""
