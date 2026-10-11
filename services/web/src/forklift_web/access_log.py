"""gunicorn's access log, without what the single sign-on callback's URL carries.

The provider sends the browser back to ``/accounts/oidc/callback/?code=...&state=...``. The
code and the state are short-lived and single-use, and the code is worthless without the PKCE
verifier and the client secret, but they do not belong in a log. The image's gunicorn logs
through :class:`AccessLog` (``--logger-class forklift_web.access_log.AccessLog``), which writes
the callback's query as ``[redacted]``, in the request line and in a ``Referer`` that names the
callback (the callback's pages also ask browsers to send none); everything else is logged as
gunicorn logs it.
"""

from __future__ import annotations

from urllib.parse import urlsplit

from gunicorn.glogging import Logger

REDACTED = "[redacted]"
# Without its trailing slash too: that request is redirected, query and all, to the one with it.
REDACTED_PATH = "/accounts/oidc/callback"


def _redacts(path: str) -> bool:
    return path.rstrip("/") == REDACTED_PATH


class AccessLog(Logger):
    def atoms(self, resp, req, environ, request_time):
        atoms = super().atoms(resp, req, environ, request_time)
        path = environ.get("PATH_INFO") or ""
        if _redacts(path) and environ.get("QUERY_STRING"):
            uri = environ.get("RAW_URI", path).partition("?")[0] + "?" + REDACTED
            atoms["r"] = f"{environ['REQUEST_METHOD']} {uri} {environ['SERVER_PROTOCOL']}"
            atoms["q"] = REDACTED
        referer = atoms.get("f", "-")
        if "?" in referer and _redacts(urlsplit(referer).path):
            atoms["f"] = referer.partition("?")[0] + "?" + REDACTED
        return atoms
