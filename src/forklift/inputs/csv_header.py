from __future__ import annotations
from typing import List, Optional, Pattern

__all__ = [
    "stability_scan_skip_rows",
    "atomic_regex_skip_rows",
]

def stability_scan_skip_rows(source_path: str, *, encoding: str, keywords: Optional[List[str]]) -> int:
    """Return number of leading lines to skip using stability_scan rules.

    Lines starting with '#" are always skipped. If keywords provided, the first
    non-comment line containing *all* keywords (case-insensitive) is treated as header.
    Otherwise the first non-comment line is the header. Returns count of lines to skip
    before the header line (0 if header already first line).
    """
    lowered_keywords = [k.lower() for k in (keywords or []) if isinstance(k, str)]
    skip_rows = 0
    try:
        with open(source_path, "r", encoding=encoding) as f:
            while True:
                line = f.readline()
                if not line:
                    break
                stripped = line.rstrip("\n")
                if stripped.startswith("#"):
                    skip_rows += 1
                    continue
                header_lower = stripped.lower()
                if not lowered_keywords or all(kw in header_lower for kw in lowered_keywords):
                    break  # found header
                # treat as metadata line if it doesn't satisfy keywords
                skip_rows += 1
    except OSError:
        return 0
    return skip_rows

def atomic_regex_skip_rows(source_path: str, *, encoding: str, regex: Pattern[str], separator: str) -> int:
    """Return number of leading lines to skip until a regex match appears in first field."""
    skip = 0
    try:
        with open(source_path, "r", encoding=encoding) as f:
            while True:
                line = f.readline()
                if not line:
                    break
                first_field = line.rstrip("\n").split(separator, 1)[0]
                if regex.search(first_field):
                    break
                skip += 1
    except OSError:
        return 0
    return skip

