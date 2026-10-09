"""Redaction of secrets in database connection strings and the text derived from them.

Connection strings routinely carry credentials (``PWD=...``). Anything written to disk or to a
log (``metadata.json``, error messages) must go through :func:`redact_connection_string`, which
keeps host, database and driver for provenance and replaces secret values with ``***``.

Recognised forms:

* ODBC / ADO style ``key=value;key=value`` strings, including braced values that may contain
  ``;`` (``Pwd={a;b}``, ``}}`` escapes a closing brace)
* URLs: the password part of ``scheme://user:pass@host/db`` and secret query parameters
  (``?password=...&sslkey=...``)

A key is secret when it contains ``pwd``, ``pass``, ``secret``, ``token`` or ``key``
(case-insensitive), e.g. ``PWD``, ``Password``, ``PassPhrase``, ``ClientSecret``, ``AccessToken``,
``AccountKey``, ``SSLKey``. This deliberately errs on the side of redacting too much.

Limitations: a password containing an unencoded ``/``, ``?`` or ``#`` inside a URL cannot be told
apart from the rest of the URL; percent-encode such passwords.
"""

from __future__ import annotations

import re
from typing import List, Optional, Tuple

REDACTED = "***"
_MIN_SCRUB_LENGTH = 4

_SECRET_KEY_FRAGMENTS = ("pwd", "pass", "secret", "token", "key")
_URL_AUTHORITY = re.compile(r"(?P<scheme>[A-Za-z][A-Za-z0-9+.\-]*://)(?P<authority>[^/?#\s]*)")
_URL_QUERY_SECRET = re.compile(
    r"(?P<prefix>[?&;])(?P<key>[^=&;#\s]*(?:pwd|pass|secret|token|key)[^=&;#\s]*)"
    r"=(?P<value>[^&;#\s]*)",
    re.IGNORECASE,
)


def _is_secret_key(key: str) -> bool:
    lowered = key.strip().lower()
    return any(fragment in lowered for fragment in _SECRET_KEY_FRAGMENTS)


def _value_end(text: str, start: int) -> int:
    """Index just after the value starting at ``start`` (braced values may contain ``;``)."""
    i = start
    while i < len(text) and text[i] in " \t":
        i += 1
    if i < len(text) and text[i] == "{":
        i += 1
        while i < len(text):
            if text[i] == "}":
                if i + 1 < len(text) and text[i + 1] == "}":  # escaped closing brace
                    i += 2
                    continue
                i += 1
                break
            i += 1
        else:
            return len(text)  # unterminated brace: treat the remainder as the value
    # Anything after a closing brace up to the next separator belongs to the same value
    semicolon = text.find(";", i)
    return len(text) if semicolon == -1 else semicolon


def _redact_pairs(text: str, secrets: List[str]) -> str:
    """Redact secret ``key=value`` pairs of an ODBC style string."""
    out: List[str] = []
    pos = 0
    while pos < len(text):
        equals = text.find("=", pos)
        semicolon = text.find(";", pos)
        if equals == -1 or (semicolon != -1 and semicolon < equals):
            # Segment without "=": copy up to and including the separator
            end = len(text) if semicolon == -1 else semicolon + 1
            out.append(text[pos:end])
            pos = end
            continue
        key = text[pos:equals]
        end = _value_end(text, equals + 1)
        if _is_secret_key(key):
            secrets.append(text[equals + 1 : end].strip())
            out.append(f"{key}={REDACTED}")
        else:
            out.append(text[pos:end])
        if end < len(text):  # keep the separator
            out.append(";")
            end += 1
        pos = end
    return "".join(out)


def _redact_url_userinfo(match: "re.Match[str]", secrets: List[str]) -> str:
    authority = match.group("authority")
    userinfo, at, host = authority.rpartition("@")
    if not at or ":" not in userinfo:
        return match.group(0)
    user, _, password = userinfo.partition(":")
    secrets.append(password)
    return f"{match.group('scheme')}{user}:{REDACTED}@{host}"


def _redact(text: str) -> Tuple[str, List[str]]:
    secrets: List[str] = []
    result = _redact_pairs(text, secrets)

    def _query(match: "re.Match[str]") -> str:
        if match.group("value") != REDACTED:
            secrets.append(match.group("value"))
        return f"{match.group('prefix')}{match.group('key')}={REDACTED}"

    result = _URL_AUTHORITY.sub(lambda m: _redact_url_userinfo(m, secrets), result)
    result = _URL_QUERY_SECRET.sub(_query, result)
    return result, secrets


def redact_connection_string(connection_string: Optional[str]) -> Optional[str]:
    """Return ``connection_string`` with every secret value replaced by ``***``.

    Host, database, driver and user name are kept. ``None`` is returned unchanged.

    Example:
        >>> redact_connection_string("Driver={X};Server=db;Uid=bob;Pwd={a;b};Database=d")
        'Driver={X};Server=db;Uid=bob;Pwd=***;Database=d'
    """
    if connection_string is None:
        return None
    return _redact(str(connection_string))[0]


def scrub_secrets(text: str, connection_string: Optional[str]) -> str:
    """Remove the connection string and its secret values from free text (error messages)."""
    if not connection_string or not text:
        return text
    redacted, secrets = _redact(str(connection_string))
    text = text.replace(str(connection_string), redacted)
    candidates = set()
    for secret in secrets:
        candidates.add(secret)
        if secret.startswith("{") and secret.endswith("}"):  # also the unbraced form
            candidates.add(secret[1:-1].replace("}}", "}"))
    # Very short values are skipped: replacing "a" everywhere would only destroy the message
    for secret in sorted(
        (c for c in candidates if len(c) >= _MIN_SCRUB_LENGTH), key=len, reverse=True
    ):
        text = text.replace(secret, REDACTED)
    return text
