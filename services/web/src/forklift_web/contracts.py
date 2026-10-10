"""The job contract's JSON Schemas (``contracts/jobspec.schema.json``, ``jobresult.schema.json``).

The engine publishes them (``forklift.jobs``); the gateway validates every spec it builds and
every result a worker reports against them, so the two cannot drift silently. They are found in
``FORKLIFT_CONTRACTS_DIR``, else in the copy packaged with forklift_web (the Docker image puts
it there), else in the repository's ``contracts/`` directory (a development checkout).
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Optional

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from jsonschema import Draft202012Validator
from jsonschema.validators import validator_for

JOBSPEC = "jobspec.schema.json"
JOBRESULT = "jobresult.schema.json"

_PACKAGED = Path(__file__).resolve().parent / "contracts"
_REPOSITORY = Path(__file__).resolve().parents[4] / "contracts"


class ContractViolation(Exception):
    """A document does not match its contract; ``errors`` lists where and why."""

    def __init__(self, name: str, errors: list):
        self.errors = errors
        super().__init__(f"The document does not match {name}: " + "; ".join(errors))


def contracts_dir() -> Path:
    if settings.FORKLIFT_CONTRACTS_DIR:
        return Path(settings.FORKLIFT_CONTRACTS_DIR)
    if (_PACKAGED / JOBSPEC).is_file():
        return _PACKAGED
    return _REPOSITORY


@lru_cache(maxsize=None)
def _validator(path: str):
    try:
        schema = json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ImproperlyConfigured(
            f"The job contract {path} is missing; set FORKLIFT_CONTRACTS_DIR to the directory "
            "with jobspec.schema.json and jobresult.schema.json."
        ) from None
    cls = validator_for(schema, default=Draft202012Validator)
    cls.check_schema(schema)
    return cls(schema)


# Keywords whose jsonschema messages name only property names, never the document's values.
_SAFE_MESSAGES = {"required", "additionalProperties", "dependentRequired"}


def _depth(error) -> tuple:
    return len(error.absolute_path), error.validator != "type"


def _describe(error) -> str:
    if error.context:  # anyOf / oneOf: say why the alternative that got furthest failed
        return _describe(max(error.context, key=_depth))
    where = "/".join(str(part) for part in error.absolute_path) or "(document)"
    if error.validator in _SAFE_MESSAGES:
        return f"{where}: {error.message}"
    expected = repr(error.validator_value)
    if len(expected) > 120:
        expected = expected[:117] + "..."
    return f"{where}: fails '{error.validator}' (expected {expected})"


def validate(name: str, document: dict, *, directory: Optional[Path] = None) -> None:
    """Raise ContractViolation unless ``document`` matches the contract file ``name``.

    The messages give the location and the JSON Schema keyword that failed, not the value (a
    spec carries presigned URLs and connection strings, which must not be repeated).
    """
    validator = _validator(str((directory or contracts_dir()) / name))
    errors = sorted(validator.iter_errors(document), key=lambda error: list(error.absolute_path))
    if errors:
        raise ContractViolation(name, [_describe(error) for error in errors[:20]])


def clear_cache() -> None:
    _validator.cache_clear()
