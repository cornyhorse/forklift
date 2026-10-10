"""Helpers shared by the worker's integration tests."""

from __future__ import annotations

import subprocess
import sys
from functools import lru_cache
from typing import Any

REAL_ENGINE = [sys.executable, "-I", "-m", "forklift"]

# Two valid rows and one whose id is not an integer (it goes to bad_rows).
CSV = b"id,name\n1,alice\n2,bob\nx,carol\n"
SCHEMA = {
    "type": "object",
    "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
    "required": ["id"],
}


@lru_cache(maxsize=1)
def engine_has_run_job() -> bool:
    done = subprocess.run([*REAL_ENGINE, "run-job", "--help"], capture_output=True, timeout=120)
    return done.returncode == 0


def csv_spec(job_id: str, location: dict[str, Any]) -> dict[str, Any]:
    """A run of the CSV above with its schema; the gateway's lease sets the job_id."""
    return {
        "spec_version": 1,
        "job_id": job_id,
        "kind": "run",
        "input": {"format": "csv", "location": location},
        "schema": SCHEMA,
        "output": {"location": {"type": "file", "path": "out/"}},
    }
