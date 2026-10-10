"""Remove secrets from text the worker passes on (engine stderr, error messages, warnings).

A job's secrets are the connection strings in its ``sql``/``sql_table`` locations (and the
passwords inside them) and its presigned URLs (whose query strings are signatures). Generic
patterns catch ``Pwd=...``/``Password=...`` pairs, URL user info and S3 signature parameters
whatever job they came from.
"""

from __future__ import annotations

import re
from typing import Any, Iterable, Iterator
from urllib.parse import urlsplit

REDACTED = "[redacted]"
_MIN_SECRET = 4  # shorter fragments are not replaced on their own: they would garble any text

_PATTERNS = (
    # ODBC / ADO style key=value pairs: Pwd=...; Password=...; (up to the next ';')
    (re.compile(r"(?i)\b(pwd|password|passwd)(\s*=\s*)(\{[^}]*\}|[^;\s]*)"), r"\1\2" + REDACTED),
    # user:password@ in URLs
    (re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://[^/\s:@]*:)[^/\s@]+@"), r"\1" + REDACTED + "@"),
    # SigV4 / SigV2 query parameters of presigned URLs
    (
        re.compile(
            r"(?i)\b(X-Amz-Signature|X-Amz-Credential|X-Amz-Security-Token|Signature)=[^&\s\"']+"
        ),
        r"\1=" + REDACTED,
    ),
)
_ODBC_PASSWORD = re.compile(r"(?i)(?:^|;)\s*(?:pwd|password|passwd)\s*=\s*(\{[^}]*\}|[^;]*)")


def _locations(spec: Any) -> Iterator[dict]:
    """Every location-like object in the spec's input and output."""
    if not isinstance(spec, dict):
        return
    stack = [spec.get("input"), spec.get("output")]
    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            if isinstance(item.get("type"), str):
                yield item
            stack.extend(item.values())
        elif isinstance(item, list):
            stack.extend(item)


def secrets_of(spec: Any) -> list[str]:
    """The literal secrets in a job spec, longest first."""
    found: set[str] = set()
    for location in _locations(spec):
        connection = location.get("connection_string")
        if isinstance(connection, str) and connection:
            found.add(connection)
            for match in _ODBC_PASSWORD.finditer(connection):
                found.add(match.group(1).strip("{}"))
            try:
                password = urlsplit(connection).password
            except ValueError:
                password = None
            if password:
                found.add(password)
        url = location.get("url")
        if isinstance(url, str) and url:
            found.add(url)
            query = urlsplit(url).query
            if query:
                found.add(query)
    return sorted((s for s in found if len(s) >= _MIN_SECRET), key=len, reverse=True)


class Redactor:
    def __init__(self, secrets: Iterable[str] = ()):
        self.secrets = [s for s in secrets if len(s) >= _MIN_SECRET]

    def add(self, secrets: Iterable[str]) -> None:
        """Redact ``secrets`` too from now on (a fresh presigned URL, for example)."""
        known = {*self.secrets, *(s for s in secrets if len(s) >= _MIN_SECRET)}
        self.secrets = sorted(known, key=len, reverse=True)  # a URL before its query string

    def __call__(self, text: str) -> str:
        for secret in self.secrets:
            text = text.replace(secret, REDACTED)
        for pattern, replacement in _PATTERNS:
            text = pattern.sub(replacement, text)
        return text

    def deep(self, value: Any) -> Any:
        """``value`` with every string in it (keys excepted) redacted."""
        if isinstance(value, str):
            return self(value)
        if isinstance(value, list):
            return [self.deep(item) for item in value]
        if isinstance(value, dict):
            return {key: self.deep(item) for key, item in value.items()}
        return value
