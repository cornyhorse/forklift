"""The JSON Schemas of the job contract, generated from :mod:`forklift.jobs.spec` and ``.result``.

The schemas are checked in under ``contracts/`` at the root of the repository; a test fails when
they differ from what this module generates. After changing the contract, regenerate them::

    python -m forklift.jobs.contract            # writes contracts/*.schema.json
    python -m forklift.jobs.contract --check    # exit 1 if the files are out of date

Both take the target directory as an optional argument (default ``contracts``).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

from ._model import json_schema
from .result import JobResult
from .spec import JobSpec

_BASE_ID = "https://github.com/cornyhorse/forklift/blob/main/contracts/"

#: File name -> generated schema
SCHEMA_FILES = {
    "jobspec.schema.json": lambda: json_schema(
        JobSpec, schema_id=_BASE_ID + "jobspec.schema.json", title="Forklift JobSpec v1"
    ),
    "jobresult.schema.json": lambda: json_schema(
        JobResult, schema_id=_BASE_ID + "jobresult.schema.json", title="Forklift JobResult v1"
    ),
}


def jobspec_schema() -> Dict[str, Any]:
    """The JSON Schema of :class:`~forklift.jobs.spec.JobSpec`."""
    return SCHEMA_FILES["jobspec.schema.json"]()


def jobresult_schema() -> Dict[str, Any]:
    """The JSON Schema of :class:`~forklift.jobs.result.JobResult`."""
    return SCHEMA_FILES["jobresult.schema.json"]()


def render(schema: Dict[str, Any]) -> str:
    """The text of a schema file (stable formatting, so diffs show real changes only)."""
    return json.dumps(schema, indent=2, ensure_ascii=False) + "\n"


def write_schemas(directory: Path) -> None:
    """Write every schema file into ``directory`` (created when missing)."""
    directory.mkdir(parents=True, exist_ok=True)
    for name, build in SCHEMA_FILES.items():
        (directory / name).write_text(render(build()), encoding="utf-8")


def outdated_schemas(directory: Path) -> Dict[str, str]:
    """Schema files in ``directory`` that are missing or differ from the generated ones."""
    stale = {}
    for name, build in SCHEMA_FILES.items():
        path = directory / name
        if not path.is_file():
            stale[name] = "missing"
        elif path.read_text(encoding="utf-8") != render(build()):
            stale[name] = "differs from the generated schema"
    return stale


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Write the schema files, or with ``--check`` report the ones that are out of date."""
    parser = argparse.ArgumentParser(
        "python -m forklift.jobs.contract", description="Generate the job contract's JSON Schemas"
    )
    parser.add_argument("directory", nargs="?", default="contracts", help="default: contracts")
    parser.add_argument(
        "--check", action="store_true", help="only check that the files are up to date"
    )
    args = parser.parse_args(argv)
    directory = Path(args.directory)
    if args.check:
        stale = outdated_schemas(directory)
        for name, problem in stale.items():
            print(f"{directory / name}: {problem}", file=sys.stderr)
        if stale:
            print(
                "Regenerate the job contract schemas with: python -m forklift.jobs.contract",
                file=sys.stderr,
            )
        return 1 if stale else 0
    write_schemas(directory)
    for name in SCHEMA_FILES:
        print(f"Wrote {directory / name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
