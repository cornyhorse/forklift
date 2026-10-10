"""Structured logs: one JSON object per line on stderr (or plain text for development).

Log records carry job ids, attempts, counts, codes and host names; never cell values, connection
strings, tokens or presigned URLs. Context goes in ``extra={...}`` and becomes top-level keys.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from typing import IO, Any

LOGGER_NAME = "forklift_worker"

# Attributes every LogRecord has; anything else on a record came from ``extra``.
_STANDARD = set(vars(logging.LogRecord("", 0, "", 0, "", None, None))) | {"message", "asctime"}


def logger(name: str = "") -> logging.Logger:
    return logging.getLogger(f"{LOGGER_NAME}.{name}" if name else LOGGER_NAME)


def _extras(record: logging.LogRecord) -> dict[str, Any]:
    return {key: value for key, value in vars(record).items() if key not in _STANDARD}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "time": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
        }
        entry.update(_extras(record))
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str, sort_keys=False)


class TextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        context = " ".join(f"{key}={value}" for key, value in _extras(record).items())
        line = f"{record.levelname.lower():7} {record.getMessage()}"
        text = f"{line} [{context}]" if context else line
        if record.exc_info:
            text += "\n" + self.formatException(record.exc_info)
        return text


def configure(level: str, fmt: str, stream: IO[str] | None = None) -> logging.Handler:
    """Send this package's logs to ``stream`` (stderr) at ``level`` in ``fmt`` (json or text)."""
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setFormatter(JsonFormatter() if fmt == "json" else TextFormatter())
    root = logging.getLogger(LOGGER_NAME)
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(level.upper())
    root.propagate = False
    return handler
