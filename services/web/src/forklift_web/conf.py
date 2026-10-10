"""Reading settings from environment variables.

Every deployment setting of the gateway is an environment variable named ``FORKLIFT_...``
(the same names in Docker Compose and Helm); ``settings.py`` reads them through these helpers so
that a malformed value fails at start-up with a message naming the variable.
"""

from __future__ import annotations

import os
from typing import Mapping, Optional

from django.core.exceptions import ImproperlyConfigured

_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off", ""}


def env_str(name: str, default: Optional[str] = None, *, environ: Mapping[str, str] = os.environ):
    """The variable's value, ``default`` when it is unset; unset without a default is an error."""
    value = environ.get(name)
    if value is not None:
        return value
    if default is None:
        raise ImproperlyConfigured(f"The environment variable {name} must be set.")
    return default


def env_optional(name: str, *, environ: Mapping[str, str] = os.environ) -> Optional[str]:
    """The variable's value, or None when it is unset or empty."""
    return environ.get(name) or None


def env_bool(name: str, default: bool, *, environ: Mapping[str, str] = os.environ) -> bool:
    value = environ.get(name)
    if value is None:
        return default
    lowered = value.strip().lower()
    if lowered in _TRUE:
        return True
    if lowered in _FALSE:
        return False
    raise ImproperlyConfigured(
        f"The environment variable {name} must be a boolean (1/0, true/false, yes/no, on/off); "
        f"got {value!r}."
    )


def env_int(
    name: str,
    default: int,
    *,
    minimum: Optional[int] = None,
    environ: Mapping[str, str] = os.environ,
) -> int:
    value = environ.get(name)
    if value is None or value.strip() == "":
        return default
    try:
        number = int(value)
    except ValueError:
        raise ImproperlyConfigured(
            f"The environment variable {name} must be an integer; got {value!r}."
        ) from None
    if minimum is not None and number < minimum:
        raise ImproperlyConfigured(
            f"The environment variable {name} must be at least {minimum}; got {number}."
        )
    return number


def env_list(name: str, default: str = "", *, environ: Mapping[str, str] = os.environ) -> list:
    """A comma-separated list; surrounding whitespace and empty items are dropped."""
    value = environ.get(name, default)
    return [item.strip() for item in value.split(",") if item.strip()]
