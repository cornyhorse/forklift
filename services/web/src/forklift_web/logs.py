"""Structured JSON logs (design section 10.3): one object per line, never cell values or secrets.

Messages logged by the gateway name objects by id and count things; they never include request
bodies, connection secrets, tokens or presigned URLs.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

# Attributes every LogRecord has; anything else came from ``extra=`` and is logged as a field.
_STANDARD = set(vars(logging.makeLogRecord({}))) | {"message", "asctime", "taskName"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "time": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in vars(record).items():
            if key not in _STANDARD:
                entry[key] = value
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)
