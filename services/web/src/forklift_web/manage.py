"""The ``forklift-web`` command: Django's manage.py for the gateway.

forklift-web migrate
forklift-web bootstrap_admin --username admin --email admin@example.org
forklift-web create_worker_token --name batch-pool
forklift-web sweep_retention [--dry-run] [--every SECONDS]
forklift-web requeue_expired_leases
forklift-web rotate_secrets
forklift-web export_openapi contracts/openapi.json [--internal] [--check]
"""

from __future__ import annotations

import os
import sys


def main(argv=None) -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "forklift_web.settings")
    from django.core.management import execute_from_command_line

    execute_from_command_line(argv if argv is not None else sys.argv)
