from __future__ import annotations
from typing import List, TextIO

def open_text_auto(path: str, encodings: List[str] | None = None) -> TextIO:
    """
    Open text using a list of preferred encodings; fall back to utf-8/replace

    :param path:
    :param encodings:
    :return:
    """

    encs = encodings or ["utf-8-sig", "utf-8", "cp1252", "latin-1"]
    last_err: Exception | None = None
    for enc in encs:
        try:
            return open(path, "r", encoding=enc, newline="")
        except Exception as e:
            last_err = e
            continue
    # final fallback so tests don't crash on weird files
    return open(path, "r", encoding="utf-8", errors="replace", newline="")