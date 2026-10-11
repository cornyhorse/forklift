"""The provider information Airflow reads through the ``apache_airflow_provider`` entry point.

It registers the ``forklift`` connection type with its form (declaratively, as Airflow 3.2 and
newer expect: ``conn-fields`` and ``ui-field-behaviour``) and lists the hook, operator, sensor
and trigger modules. It must not import Airflow: Airflow loads it while it reads its own
configuration.
"""

from __future__ import annotations

from typing import Any, Dict

from forklift_client import __version__

INTEGRATION = "Forklift"
DOCS = "https://github.com/cornyhorse/forklift/blob/main/docs/platform/airflow.md"


def _modules(name: str) -> list:
    return [
        {"integration-name": INTEGRATION, "python-modules": [f"forklift_client.airflow.{name}"]}
    ]


def get_provider_info() -> Dict[str, Any]:
    return {
        "package-name": "forklift-client",
        "name": "Forklift",
        "description": (
            f"forklift-client {__version__}: run forklift jobs and datasets from Airflow and wait "
            "for them (operator, deferrable sensor and trigger)."
        ),
        "integrations": [
            {"integration-name": INTEGRATION, "external-doc-url": DOCS, "tags": ["software"]}
        ],
        "hooks": _modules("hooks"),
        "operators": _modules("operators"),
        "sensors": _modules("sensors"),
        "triggers": _modules("triggers"),
        "connection-types": [
            {
                "connection-type": "forklift",
                "hook-class-name": "forklift_client.airflow.hooks.ForkliftHook",
                "hook-name": "Forklift",
                "conn-fields": {
                    "ca_file": {
                        "label": "CA bundle",
                        "description": "A PEM file to verify the gateway's and the object store's "
                        "certificates with (default: the system's CA certificates)",
                        "schema": {"type": ["string", "null"]},
                    },
                    "timeout": {
                        "label": "Timeout (seconds)",
                        "description": "Socket timeout of each request",
                        "schema": {"type": ["number", "null"], "default": 30},
                    },
                },
                "ui-field-behaviour": {
                    "hidden-fields": ["schema", "login", "port", "extra"],
                    "relabeling": {"host": "Base URL", "password": "API token"},
                    "placeholders": {
                        "host": "https://forklift.example.org",
                        "password": "fkl_...",
                    },
                },
            }
        ],
    }
