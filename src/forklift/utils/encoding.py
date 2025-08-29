from __future__ import annotations
from typing import List, TextIO

def open_text_auto(path: str, encodings: List[str] | None = None) -> TextIO:
    """Open a text file trying multiple encodings in order.

    Attempts each encoding until one succeeds; on total failure falls back to
    ``utf-8`` with ``errors="replace"`` so downstream parsing does not crash.

    :param path: Filesystem path to open.
    :param encodings: Ordered list of candidate encodings. Defaults to
        ``["utf-8-sig", "utf-8", "cp1252", "latin-1"]``.
    :return: Text IO handle opened for reading with universal newline disabled.
    """
    encs = encodings or ["utf-8-sig", "utf-8", "cp1252", "latin-1"]
    last_err: Exception | None = None
    for enc in encs:
        try:
            return open(path, "r", encoding=enc, newline="")
        except Exception as e:  # pragma: no cover - defensive
            last_err = e
            continue
    return open(path, "r", encoding="utf-8", errors="replace", newline="")