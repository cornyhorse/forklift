"""Write the OpenAPI document of /api/v1 (or /internal/v1), or check a checked-in copy."""

from __future__ import annotations

import json
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from forklift_web.api import api as public_api
from forklift_web.internal import internal_api


def render(internal: bool = False) -> str:
    api, prefix = (internal_api, "/internal/v1/") if internal else (public_api, "/api/v1/")
    return json.dumps(api.get_openapi_schema(path_prefix=prefix), indent=2, sort_keys=True) + "\n"


class Command(BaseCommand):
    help = (
        "Write the OpenAPI document of the public API (contracts/openapi.json in the "
        "repository) or, with --internal, of the internal one; --check only compares."
    )

    def add_arguments(self, parser):
        parser.add_argument("path", help="File to write (or check)")
        parser.add_argument("--internal", action="store_true", help="The internal API instead")
        parser.add_argument(
            "--check", action="store_true", help="Exit with an error if the file is out of date"
        )

    def handle(self, *args, path, internal, check, **options):
        text = render(internal)
        target = Path(path)
        if check:
            if not target.is_file() or target.read_text(encoding="utf-8") != text:
                raise CommandError(
                    f"{target} is out of date; regenerate it with: forklift-web export_openapi "
                    f"{target}{' --internal' if internal else ''}"
                )
            self.stdout.write(f"{target} is up to date.")
            return
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
        self.stdout.write(f"Wrote {target}.")
