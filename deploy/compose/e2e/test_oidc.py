"""End-to-end: single sign-on through Dex, a real OpenID Provider (start the stack with
e2e/oidc.yml as the comment at its top says, and set FORKLIFT_E2E_DEX_URL).

A person signs in the way a browser does: the "Sign in with Dex" link on the sign-in page, Dex's
own login form, back at the gateway signed in, with the role their Dex group maps to; an admin
sees the linked sign-in and the audit log has it. Someone in no mapped group is refused, and so
is a second use of a callback. Standard library, plus the session fixtures of test_stack.py.
"""

from __future__ import annotations

import html
import http.client
import http.cookiejar
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request

import pytest
from test_stack import admin, settings  # noqa: F401  (the session fixtures)

# The issuer's host as the gateway (on the stack's network) reaches it; the test sends the
# browser's requests for it to the published port instead.
DEX_HOST = "dex:5556"
ISSUER = f"http://{DEX_HOST}/dex"
AUTHOR = ("ada@example.org", "forklift-e2e-author")
NOBODY = ("nobody@example.org", "forklift-e2e-nobody")


def _dex_address() -> str:
    url = os.environ.get("FORKLIFT_E2E_DEX_URL")
    if not url:
        pytest.skip("start the stack with e2e/oidc.yml and set FORKLIFT_E2E_DEX_URL")
    return urllib.parse.urlsplit(url).netloc


class Back(Exception):
    """The provider sent the browser back to the gateway (raised where ``stop_at`` says)."""

    def __init__(self, url: str):
        super().__init__(url)
        self.url = url


class Browser:
    """Keeps cookies and follows redirects like a browser; requests for dex:5556 go to the
    address Dex is published on. ``last`` is the URL the latest request ended at; with
    ``stop_at`` set, a redirect to a URL that starts with it raises Back instead."""

    def __init__(self, dex_address: str):
        browser = self

        class Connections(urllib.request.HTTPHandler):
            def http_open(self, request):
                def connect(host, **kwargs):
                    return http.client.HTTPConnection(
                        dex_address if host == DEX_HOST else host, **kwargs
                    )

                return self.do_open(connect, request)

        class Redirects(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, request, fp, code, msg, headers, newurl):
                if browser.stop_at and newurl.startswith(browser.stop_at):
                    raise Back(newurl)
                return super().redirect_request(request, fp, code, msg, headers, newurl)

        self.cookies = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            Connections(), Redirects(), urllib.request.HTTPCookieProcessor(self.cookies)
        )
        self.last = ""
        self.stop_at = ""

    def open(self, url: str, form: dict = None, *, headers: dict = None) -> tuple:
        """(status, page) at the end of the redirects."""
        data = urllib.parse.urlencode(form).encode() if form is not None else None
        request = urllib.request.Request(url, data=data, headers=headers or {})
        try:
            with self.opener.open(request, timeout=30) as response:
                self.last = response.geturl()
                return response.status, response.read().decode()
        except urllib.error.HTTPError as error:
            self.last = error.geturl()
            return error.code, error.read().decode()

    def sign_in(self, base_url: str, account: tuple, next_path: str = "/") -> tuple:
        """From the sign-in page through Dex's login form; (status, page) where it ends."""
        status, page = self.open(f"{base_url}/accounts/login/?next={next_path}")
        assert status == 200, page[:500]
        link = re.search(r'<a class="button" href="([^"]+)">Sign in with Dex</a>', page)
        assert link, "the sign-in page has no single sign-on button"
        status, page = self.open(urllib.parse.urljoin(base_url, html.unescape(link.group(1))))
        assert status == 200 and self.last.startswith(f"{ISSUER}/auth/local"), self.last
        action = re.search(r'<form[^>]*action="([^"]+)"', page).group(1)
        login, password = account
        return self.open(
            urllib.parse.urljoin(self.last, html.unescape(action)),
            {"login": login, "password": password},
        )

    def sign_out(self, base_url: str) -> None:
        self.open(f"{base_url}/accounts/login/")
        csrf = next(cookie.value for cookie in self.cookies if cookie.name == "csrftoken")
        status, _ = self.open(
            f"{base_url}/accounts/logout/",
            {"csrfmiddlewaretoken": csrf},
            headers={"Referer": f"{base_url}/"},
        )
        assert (status, self.last) == (200, f"{base_url}/accounts/login/")

    def me(self, base_url: str):
        status, page = self.open(f"{base_url}/api/v1/me")
        return json.loads(page) if status == 200 else None


def _user(admin, username: str) -> dict:  # noqa: F811
    _, found = admin.request("GET", "/api/v1/admin/users?limit=500")
    return next(user for user in found["items"] if user["username"] == username)


def test_signing_in_through_dex(admin, settings):  # noqa: F811
    base = settings["FORKLIFT_PUBLIC_URL"].rstrip("/")
    browser = Browser(_dex_address())
    status, page = browser.sign_in(base, AUTHOR, next_path="/schemas/")
    assert (status, browser.last) == (200, f"{base}/schemas/"), page[:500]
    me = browser.me(base)
    assert (me["user"]["username"], me["user"]["role"], me["user"]["email"]) == (
        "ada",
        "author",
        "ada@example.org",
    )
    assert "schemas:write" in me["scopes"]

    user = _user(admin, "ada")
    _, identities = admin.request("GET", f"/api/v1/admin/users/{user['id']}/identities")
    (identity,) = identities["items"]
    assert (identity["issuer"], identity["linked_by"]) == (ISSUER, "created")
    assert identity["subject"] and identity["last_used_at"]
    _, logins = admin.request(
        "GET", f"/api/v1/admin/audit?action=user.login&actor_id={user['id']}"
    )
    details = logins["items"][0]["details"]
    assert (details["method"], details["issuer"]) == ("oidc", ISSUER)

    # Signing out ends the session; signing in again finds the same account by its subject
    browser.sign_out(base)
    assert browser.me(base) is None
    status, _ = browser.sign_in(base, AUTHOR)
    assert status == 200 and browser.me(base)["user"]["id"] == user["id"]


def test_someone_in_no_mapped_group_is_refused(admin, settings):  # noqa: F811
    base = settings["FORKLIFT_PUBLIC_URL"].rstrip("/")
    browser = Browser(_dex_address())
    status, page = browser.sign_in(base, NOBODY)
    assert status == 403, page[:500]
    assert "Your Dex account has no role in Forklift" in html.unescape(page)
    assert browser.last.startswith(f"{base}/accounts/oidc/callback/")
    assert browser.me(base) is None
    _, refusals = admin.request("GET", "/api/v1/admin/audit?action=user.oidc_login_refused")
    assert any(
        entry["details"].get("reason") == "no_role" and entry["details"]["issuer"] == ISSUER
        for entry in refusals["items"]
    )
    _, users = admin.request("GET", "/api/v1/admin/users?limit=500")
    assert "nobody" not in {user["username"] for user in users["items"]}


def test_a_callback_works_once(settings):  # noqa: F811
    base = settings["FORKLIFT_PUBLIC_URL"].rstrip("/")
    browser = Browser(_dex_address())
    browser.stop_at = f"{base}/accounts/oidc/callback/"
    with pytest.raises(Back) as back:
        browser.sign_in(base, AUTHOR)
    browser.stop_at = ""
    assert browser.open(back.value.url)[0] == 200
    assert browser.me(base)["user"]["username"] == "ada"
    # the same callback again, in this browser or another one: refused
    browser.sign_out(base)
    for replay in (browser, Browser(_dex_address())):
        status, page = replay.open(back.value.url)
        assert status == 400 and "This sign-in could not be finished" in html.unescape(page)
        assert replay.me(base) is None
