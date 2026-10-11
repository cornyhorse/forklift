"""Airflow integration of forklift-client (extra ``airflow``: Apache Airflow 3.2 or newer).

- :class:`~forklift_client.airflow.hooks.ForkliftHook`: a client from an Airflow connection of
  type ``forklift`` (host: the gateway's URL, password: an API token).
- :class:`~forklift_client.airflow.operators.ForkliftJobOperator`: run a dataset or submit a job
  and wait for it, in the worker or deferred to the triggerer.
- :class:`~forklift_client.airflow.sensors.ForkliftJobSensor`: wait for a job to end (poke,
  reschedule or deferrable).
- :class:`~forklift_client.airflow.triggers.ForkliftJobTrigger`: the trigger both defer to.

Airflow finds them through the ``apache_airflow_provider`` entry point
(:func:`forklift_client.airflow.provider.get_provider_info`). This module imports nothing from
Airflow, so the entry point stays cheap to load. See docs/platform/airflow.md.
"""
