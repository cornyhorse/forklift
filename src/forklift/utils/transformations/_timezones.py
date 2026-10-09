"""Time zone lookup used by the datetime transformations and their configuration.

Prefers the standard library (``zoneinfo``, Python 3.9+) and falls back to ``pytz`` if it is
importable. ``pytz`` is not a declared dependency of this package.
"""

from __future__ import annotations

import datetime
from typing import Optional


def resolve_timezone(name: str) -> datetime.tzinfo:
    """Return a ``tzinfo`` for an IANA time zone name.

    Raises:
        ValueError: if the name is not a known time zone (typos must not silently null a column)
        ImportError: if neither ``zoneinfo`` nor ``pytz`` is available
    """
    if not isinstance(name, str) or not name.strip():
        raise ValueError("timezone must be a non-empty IANA time zone name")

    zoneinfo_module = None
    try:
        import zoneinfo as zoneinfo_module  # type: ignore[no-redef]
    except ImportError:
        pass

    if zoneinfo_module is not None:
        try:
            return zoneinfo_module.ZoneInfo(name)
        except (zoneinfo_module.ZoneInfoNotFoundError, ValueError, OSError):
            pass  # unknown name, or no system tz database: let pytz have a go

    pytz_module = None
    try:
        import pytz as pytz_module  # type: ignore[no-redef]
    except ImportError:
        pass

    if pytz_module is not None:
        try:
            return pytz_module.timezone(name)
        except pytz_module.UnknownTimeZoneError:
            pass

    if zoneinfo_module is None and pytz_module is None:
        raise ImportError(
            "Time zone support needs the standard library 'zoneinfo' module (Python 3.9+) "
            "or the 'pytz' package; neither is available."
        )
    raise ValueError(
        f"Unknown timezone: {name!r}. Use an IANA name such as 'America/New_York' "
        "(on systems without a time zone database install the 'tzdata' package)."
    )


def validate_timezone(name: Optional[str]) -> None:
    """Raise ``ValueError`` if ``name`` is set but not a usable time zone."""
    if name:
        resolve_timezone(name)
