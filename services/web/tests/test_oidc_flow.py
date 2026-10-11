"""Single sign-on through the gateway's pages against a fake provider (a real TLS server with
real keys): the authorization request, the callback and its single-use state, accounts created
and linked, roles from claims, every refusal (audited, never with a token), the sign-in page
with and without the password form, and signing out at the provider."""

from __future__ import annotations

import re
import urllib.parse
from datetime import timedelta

import pytest
from django.contrib.auth import authenticate
from django.contrib.sessions.models import Session
from django.db import IntegrityError
from django.test import Client
from django.utils import timezone
from oidc_support import (  # noqa: F401
    CLIENT_ID,
    REMOVE,
    challenge,
    finish,
    idp,
    sign_in,
    sso,
    start,
)

from forklift_web.core.choices import OidcLink, Role
from forklift_web.core.models import ApiToken, AuditLog, OidcIdentity, User
from forklift_web.policy import Actor
from forklift_web.services import oidc, retention

pytestmark = pytest.mark.django_db


def signed_in_as(client: Client):
    """The user the client's session belongs to (None when signed out)."""
    user = client.get("/api/v1/me")
    return user.json()["user"]["username"] if user.status_code == 200 else None


def entries(action: str) -> list:
    return list(AuditLog.objects.filter(action=action).order_by("id"))


def refusal(response, status: int, message: str) -> str:
    """The sign-in page with the refusal ``message``; returns the page."""
    page = response.content.decode()
    assert response.status_code == status, page
    if response.wsgi_request.path == "/accounts/oidc/callback/":
        assert response["Referrer-Policy"] == "no-referrer"  # the URL has the code and state
    assert message in page.replace("&#x27;", "'"), page
    assert 'href="/accounts/oidc/login/"' in page  # start again
    return page


def no_secrets(idp, *texts) -> None:  # noqa: F811
    """Neither codes, tokens, states, nonces nor the client secret appear in ``texts``."""
    values = list(idp.grants) + list(idp.access_tokens)
    for request in idp.requests:
        values += [request.query.get(key) for key in ("state", "nonce", "code_challenge")]
    values = [value for value in values if value]
    for text in texts:
        assert not any(value in text for value in values)
        assert "the-tests-client-secret" not in text


# --------------------------------------------------------------------------- the flow


def test_the_authorization_request(client, idp, sso):  # noqa: F811
    location = start(client, next_url="/jobs/")
    parts = urllib.parse.urlsplit(location)
    assert f"{parts.scheme}://{parts.netloc}{parts.path}" == idp.url("/auth")
    query = dict(urllib.parse.parse_qsl(parts.query))
    assert query["response_type"] == "code" and query["client_id"] == CLIENT_ID
    assert query["redirect_uri"] == "http://testserver/accounts/oidc/callback/"
    assert query["scope"] == "openid email profile"
    assert query["code_challenge_method"] == "S256"
    assert len(query["state"]) >= 43 and len(query["nonce"]) >= 43
    (flow,) = client.session[oidc.FLOWS_KEY].values()
    assert client.session[oidc.FLOWS_KEY].keys() == {query["state"]}
    assert flow["nonce"] == query["nonce"] and flow["next"] == "/jobs/"
    assert challenge(flow["verifier"]) == query["code_challenge"] and len(flow["verifier"]) == 64
    assert client.session.get_expiry_age() == oidc.FLOW_SECONDS  # a visitor's session
    assert "no-cache" in client.get("/accounts/oidc/login/")["Cache-Control"]


def test_the_redirect_uri_is_under_the_public_url(client, idp, sso, settings):  # noqa: F811
    settings.FORKLIFT_PUBLIC_URL = "https://forklift.example.org"
    query = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(start(client)).query))
    assert query["redirect_uri"] == "https://forklift.example.org/accounts/oidc/callback/"


def test_an_endpoint_with_a_query_of_its_own(client, idp, sso):  # noqa: F811
    idp.discovery_overrides["authorization_endpoint"] = idp.url("/auth?p=b2c_1_signin")
    location = start(client)
    assert location.startswith(idp.url("/auth?p=b2c_1_signin&response_type=code"))


def test_a_first_sign_in_creates_the_account(client, idp, sso, settings):  # noqa: F811
    person = idp.person("alice", groups=["forklift-authors", "forklift-viewers", "other"])
    response = sign_in(client, idp, person)
    assert response.status_code == 302 and response["Location"] == "/"
    assert response["Referrer-Policy"] == "no-referrer"
    user = User.objects.get(username="user-alice")
    assert (user.role, user.email, user.first_name, user.last_name) == (
        Role.AUTHOR,
        "alice@example.org",
        "Ada",
        "Lovelace",
    )
    assert not user.has_usable_password() and user.last_login is not None
    identity = OidcIdentity.objects.get()
    assert (identity.user, identity.issuer, identity.subject, identity.linked_by) == (
        user,
        idp.issuer,
        "alice",
        OidcLink.CREATED,
    )
    assert identity.last_used_at is not None
    assert signed_in_as(client) == "user-alice"
    assert client.session[oidc.SIGNED_IN_KEY] == {"issuer": idp.issuer, "subject": "alice"}
    assert oidc.FLOWS_KEY not in client.session
    assert client.session.get_expiry_age() == settings.SESSION_COOKIE_AGE  # a sign-in's
    (created,) = entries("user.create")
    assert created.actor is None and created.actor_label == "system:oidc"
    assert created.details == {
        "source": "oidc",
        "role": "author",
        "issuer": idp.issuer,
        "subject": "alice",
        "password_set": False,
    }
    (login,) = entries("user.login")
    assert login.actor == user
    assert login.details == {"method": "oidc", "issuer": idp.issuer, "subject": "alice"}
    # the gateway authenticated with the secret and proved the PKCE verifier
    (exchange,) = idp.seen("/token")
    assert b"code_verifier=" in exchange.body and "authorization" in exchange.headers
    no_secrets(idp, *[str(entry.details) for entry in AuditLog.objects.all()])


def test_the_next_sign_in_finds_the_account_by_its_subject(client, idp, sso):  # noqa: F811
    person = idp.person("alice")
    sign_in(client, idp, person)
    client.post("/accounts/logout/")
    person.claims["preferred_username"] = "renamed"  # usernames do not identify anyone
    person.claims["given_name"] = "Augusta"
    sign_in(client, idp, person)
    assert signed_in_as(client) == "user-alice"
    assert User.objects.count() == 1 and OidcIdentity.objects.count() == 1
    assert User.objects.get().first_name == "Augusta"
    assert entries("user.login")[-1].details["updated"] == ["first_name"]


def test_next_is_kept_and_only_for_this_site(client, idp, sso):  # noqa: F811
    person = idp.person("alice")
    assert finish(client, idp.log_in(start(client, "/jobs/?page=2"), person))["Location"] == (
        "/jobs/?page=2"
    )
    other = Client()
    assert finish(other, idp.log_in(start(other, "https://evil.example/"), person))[
        "Location"
    ] == ("/")


def test_signed_in_people_go_straight_on(client, idp, sso):  # noqa: F811
    sign_in(client, idp, idp.person("alice"))
    response = client.get("/accounts/oidc/login/?next=/jobs/")
    assert response.status_code == 302 and response["Location"] == "/jobs/"


def age_flows(client, seconds: float) -> None:
    session = client.session
    flows = session[oidc.FLOWS_KEY]
    for flow in flows.values():
        flow["started_at"] -= seconds
    session[oidc.FLOWS_KEY] = flows
    session.save()


def test_several_tabs_and_old_flows(client, idp, sso):  # noqa: F811
    person = idp.person("alice")
    first = start(client)
    for _ in range(oidc.MAX_FLOWS + 1):
        latest = start(client)
    assert len(client.session[oidc.FLOWS_KEY]) == oidc.MAX_FLOWS  # the oldest went
    refusal(finish(client, idp.log_in(first, person)), 400, "it was already used")
    age_flows(client, oidc.FLOW_SECONDS)
    refusal(finish(client, idp.log_in(latest, person)), 400, "more than 10 minutes ago")
    start(client)  # flows that are too old are dropped when the next one starts
    assert len(client.session[oidc.FLOWS_KEY]) == 1
    assert entries("user.oidc_login_refused") == []  # nobody's identity is involved yet


def test_unfinished_sign_ins_leave_sessions_the_sweeper_deletes(client, idp, sso):  # noqa
    start(client)
    signed_in = Client()
    sign_in(signed_in, idp, idp.person("alice"))
    later = timezone.now() + timedelta(seconds=oidc.FLOW_SECONDS + 1)
    system = Actor.for_system("tests")
    assert retention.sweep(system, dry_run=True, now=later).sessions == 1
    assert Session.objects.count() == 2
    assert retention.sweep(system, now=later).sessions == 1
    assert signed_in_as(signed_in) == "user-alice"  # the signed-in session stays
    assert entries("retention.purge") == []  # housekeeping, not audited


def test_a_state_is_used_once(client, idp, sso):  # noqa: F811
    back = idp.log_in(start(client), idp.person("alice"))
    assert finish(client, back).status_code == 302
    client.post("/accounts/logout/")
    refusal(finish(client, back), 400, "This sign-in could not be finished")
    assert User.objects.count() == 1


def test_a_state_from_another_browser_is_refused(client, idp, sso):  # noqa: F811
    back = idp.log_in(start(Client()), idp.person("alice"))  # an attacker's own sign-in
    refusal(finish(client, back), 400, "or it was started in another browser")
    assert signed_in_as(client) is None and idp.seen("/token") == []


def test_a_callback_without_a_state(client, idp, sso):  # noqa: F811
    refusal(client.get("/accounts/oidc/callback/?code=abc"), 400, "Start again.")


def test_the_providers_error_answer(client, idp, sso):  # noqa: F811
    state = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(start(client)).query))["state"]
    response = client.get(
        "/accounts/oidc/callback/",
        {"state": state, "error": "access_denied", "error_description": "<b>Pay here</b>"},
    )
    page = refusal(response, 403, "Fake IdP did not sign you in (access_denied): you cancelled")
    assert "Pay here" not in page  # the description is anyone's text: logged, not shown
    state = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(start(client)).query))["state"]
    response = client.get("/accounts/oidc/callback/", {"state": state, "error": "made-up"})
    refusal(response, 403, "Fake IdP did not sign you in (error).")
    state = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(start(client)).query))["state"]
    response = client.get("/accounts/oidc/callback/", {"state": state, "error": "login_required"})
    refusal(response, 403, "(login_required)")
    assert [entry.details["error"] for entry in entries("user.oidc_login_refused")] == [
        "access_denied",
        "error",
        "login_required",
    ]
    assert entries("user.oidc_login_refused")[0].details == {
        "reason": "provider_error",
        "issuer": idp.issuer,
        "error": "access_denied",
    }


def test_a_callback_without_a_code(client, idp, sso):  # noqa: F811
    state = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(start(client)).query))["state"]
    response = client.get("/accounts/oidc/callback/", {"state": state})
    refusal(response, 400, "without an authorization code")
    assert entries("user.oidc_login_refused")[0].details["reason"] == "no_code"


def test_a_code_the_provider_does_not_accept(client, idp, sso):  # noqa: F811
    back = idp.log_in(start(client), idp.person("alice"))
    idp.grants.clear()  # expired at the provider
    response = finish(client, back)
    refusal(response, 502, "Fake IdP refused to complete the sign-in (invalid_grant)")
    (entry,) = entries("user.oidc_login_refused")
    assert entry.details == {"reason": "token_exchange_failed", "issuer": idp.issuer}


def test_a_token_that_fails_a_check(client, idp, sso, caplog):  # noqa: F811
    idp.token_claims = {"aud": "another-client"}
    response = sign_in(client, idp, idp.person("alice"))
    refusal(response, 403, "The ID token from Fake IdP was issued for another client (aud)")
    (entry,) = entries("user.oidc_login_refused")
    assert entry.details["reason"] == "wrong_audience"
    assert signed_in_as(client) is None and User.objects.count() == 0
    no_secrets(idp, response.content.decode(), caplog.text, str(entry.details))


def test_a_code_from_another_sign_in(client, idp, sso):  # noqa: F811
    person = idp.person("alice")
    back = idp.log_in(start(client), person)
    other = idp.log_in(start(Client()), person)
    # another browser's code with this browser's state: PKCE makes the provider refuse it
    injected = re.sub(r"code=[^&]+", re.search(r"code=[^&]+", other).group(0), back)
    refusal(finish(client, injected), 502, "refused to complete the sign-in (invalid_grant)")


def test_a_token_meant_for_another_sign_in(client, idp, sso):  # noqa: F811
    idp.token_claims = {"nonce": "the nonce of another sign-in"}
    refusal(sign_in(client, idp, idp.person("alice")), 403, "does not carry this sign-in's nonce")
    assert entries("user.oidc_login_refused")[0].details["reason"] == "nonce_mismatch"


def test_a_provider_that_cannot_be_reached_when_starting(client, idp, sso):  # noqa: F811
    idp.raw["/.well-known/openid-configuration"] = (503, {}, b"")
    response = client.get("/accounts/oidc/login/")
    page = refusal(response, 502, "Signing in with Fake IdP is not possible right now")
    assert "answered 503" in page


def test_userinfo_claims_are_added(client, idp, sso):  # noqa: F811
    sso(USERINFO="true")
    person = idp.person("alice", groups=["forklift-viewers"])
    idp.token_claims = {"groups": REMOVE}  # only the UserInfo endpoint knows the groups
    person.claims["groups"] = ["forklift-operators"]
    sign_in(client, idp, person)
    assert User.objects.get().role == Role.OPERATOR
    idp.userinfo_answer = (200, "application/json", b'{"sub": "mallory"}')
    client.post("/accounts/logout/")
    response = sign_in(client, idp, person)
    refusal(response, 403, "names another person (sub) than the UserInfo endpoint does")


def test_the_userinfo_endpoint_failing(client, idp, sso):  # noqa: F811
    sso(USERINFO="true")
    idp.userinfo_answer = (500, "application/json", b"{}")
    refusal(sign_in(client, idp, idp.person("alice")), 502, "localhost answered 500")


def test_two_first_sign_ins_at_once(client, idp, sso, monkeypatch):  # noqa: F811
    real, calls = oidc._sign_in, []

    def raced(*args):
        calls.append(1)
        if len(calls) == 1:
            raise IntegrityError("duplicate key value violates oidc_identity_one_per_sub")
        return real(*args)

    monkeypatch.setattr(oidc, "_sign_in", raced)
    assert sign_in(client, idp, idp.person("alice")).status_code == 302
    assert len(calls) == 2 and signed_in_as(client) == "user-alice"


def test_not_configured(client, settings):
    settings.FORKLIFT_OIDC = None
    assert client.get("/accounts/oidc/login/").status_code == 404
    assert client.get("/accounts/oidc/callback/?state=x").status_code == 404
    assert b"Sign in with" not in client.get("/accounts/login/").content


# --------------------------------------------------------------------------- roles


def test_roles_come_from_the_claim(client, idp, sso):  # noqa: F811
    person = idp.person("alice", groups=["forklift-admins", "forklift-viewers"])
    sign_in(client, idp, person)
    user = User.objects.get()
    assert user.role == Role.ADMIN
    client.post("/accounts/logout/")
    person.claims["groups"] = ["forklift-operators"]
    sign_in(client, idp, person)  # synchronised at every sign-in, even the last admin
    user.refresh_from_db()
    assert user.role == Role.OPERATOR
    (change,) = entries("user.update")
    assert change.actor_label == "system:oidc"
    assert change.details == {
        "changed": {"role": {"from": "admin", "to": "operator"}},
        "source": "oidc",
        "claim": "groups",
    }
    assert entries("user.login")[-1].details["updated"] == ["role"]


def test_without_synchronising_admins_manage_roles(client, idp, sso):  # noqa: F811
    sso(SYNC_ROLE="false")
    person = idp.person("alice", groups=["forklift-authors"])
    sign_in(client, idp, person)
    user = User.objects.get()
    user.role = Role.ADMIN
    user.save()
    client.post("/accounts/logout/")
    person.claims["groups"] = []  # no mapped role any more: it is not asked again
    sign_in(client, idp, person)
    user.refresh_from_db()
    assert user.role == Role.ADMIN and entries("user.update") == []


def test_a_role_claim_inside_an_object(client, idp, sso):  # noqa: F811
    sso(ROLE_CLAIM="realm_access.roles", ADMIN_GROUPS="boss", VIEWER_GROUPS=None)
    person = idp.person("alice", realm_access={"roles": ["boss"]})
    sign_in(client, idp, person)
    assert User.objects.get().role == Role.ADMIN


def test_a_role_claim_that_is_a_string_or_a_dotted_name(client, idp, sso):  # noqa: F811
    sso(ROLE_CLAIM="https://example.org/role", AUTHOR_GROUPS="writer")
    sign_in(client, idp, idp.person("alice", **{"https://example.org/role": "writer"}))
    assert User.objects.get().role == Role.AUTHOR


def test_the_default_role(client, idp, sso):  # noqa: F811
    sso(DEFAULT_ROLE="operator")
    sign_in(client, idp, idp.person("alice", groups={"not": "a list"}))
    assert User.objects.get().role == Role.OPERATOR


def test_no_mapped_role_and_no_default_is_refused(client, idp, sso):  # noqa: F811
    person = idp.person("alice", groups=["accounting"])
    response = sign_in(client, idp, person)
    refusal(response, 403, "Your Fake IdP account has no role in Forklift")
    assert User.objects.count() == 0
    (entry,) = entries("user.oidc_login_refused")
    assert entry.details == {"reason": "no_role", "issuer": idp.issuer, "subject": "alice"}
    # and an existing account loses access when its groups no longer map to a role
    person.claims["groups"] = ["forklift-viewers"]
    sign_in(client, idp, person)
    client.post("/accounts/logout/")
    person.claims["groups"] = []
    refusal(sign_in(client, idp, person), 403, "has no role in Forklift")
    assert User.objects.get().role == Role.VIEWER  # unchanged
    assert entries("user.oidc_login_refused")[-1].details["username"] == "user-alice"


# --------------------------------------------------------------------------- accounts


def test_no_account_is_created_when_creating_is_off(client, idp, sso):  # noqa: F811
    sso(CREATE_USERS="false")
    response = sign_in(client, idp, idp.person("alice"))
    refusal(response, 403, "does not create accounts at the first sign-in")
    assert User.objects.count() == 0
    assert entries("user.oidc_login_refused")[0].details["reason"] == "no_account"


def test_usernames_never_link(client, idp, sso, make_user):  # noqa: F811
    local = make_user(Role.ADMIN, username="User-Alice", email="alice@example.org")
    response = sign_in(client, idp, idp.person("alice"))
    refusal(response, 403, "A Forklift account named 'user-alice' already exists")
    assert not local.oidc_identities.exists() and signed_in_as(client) is None
    assert entries("user.oidc_login_refused")[0].details == {
        "reason": "username_taken",
        "issuer": idp.issuer,
        "subject": "alice",
        "username": "user-alice",
    }


def test_the_username_claim(client, idp, sso):  # noqa: F811
    sso(USERNAME_CLAIM="email")
    sign_in(client, idp, idp.person("alice"))
    assert User.objects.get().username == "alice@example.org"


@pytest.mark.parametrize(
    "value, message",
    [
        (REMOVE, "did not send the 'preferred_username' claim"),
        ("  ", "did not send the 'preferred_username' claim"),
        (42, "did not send the 'preferred_username' claim"),
        ("has spaces", "is not a valid Forklift username"),
        ("x" * 151, "is not a valid Forklift username"),
    ],
)
def test_unusable_usernames(client, idp, sso, value, message):  # noqa: F811
    person = idp.person("alice")
    if value is REMOVE:
        del person.claims["preferred_username"]
    else:
        person.claims["preferred_username"] = value
    refusal(sign_in(client, idp, person), 403, message)
    assert User.objects.count() == 0


def test_an_unverified_email_is_not_copied(client, idp, sso):  # noqa: F811
    sign_in(client, idp, idp.person("alice", email_verified=False, given_name=None))
    user = User.objects.get()
    assert (user.email, user.first_name) == ("", "")


def test_a_deactivated_account_is_refused(client, idp, sso):  # noqa: F811
    person = idp.person("alice")
    sign_in(client, idp, person)
    User.objects.update(is_active=False)
    client.post("/accounts/logout/")
    refusal(sign_in(client, idp, person), 403, "Your Forklift account 'user-alice' is deactivated")
    assert entries("user.oidc_login_refused")[0].details["reason"] == "inactive"


def test_service_accounts_never_sign_in_through_the_provider(client, idp, sso):  # noqa: F811
    person = idp.person("alice")
    sign_in(client, idp, person)
    User.objects.update(is_service_account=True)
    client.post("/accounts/logout/")
    refusal(sign_in(client, idp, person), 403, "is a service account; it signs in with API")


# --------------------------------------------------------------------------- linking by email


def test_a_verified_email_links_only_when_allowed(client, idp, sso, make_user):  # noqa: F811
    local = make_user(Role.AUTHOR, username="ada", email="Alice@Example.org")
    person = idp.person("alice")
    sign_in(client, idp, person)  # not allowed: a new account
    assert signed_in_as(client) == "user-alice" and not local.oidc_identities.exists()


def test_linking_by_a_verified_email(client, idp, sso, make_user):  # noqa: F811
    sso(LINK_VERIFIED_EMAIL="true", SYNC_ROLE="false")
    local = make_user(Role.AUTHOR, username="ada", email="Alice@Example.org")
    sign_in(client, idp, idp.person("alice"))
    assert signed_in_as(client) == "ada"
    identity = local.oidc_identities.get()
    assert (identity.subject, identity.linked_by) == ("alice", OidcLink.EMAIL)
    (link,) = entries("user.oidc_link")
    assert link.details == {"issuer": idp.issuer, "subject": "alice", "by": "verified_email"}
    assert link.object_id == str(local.pk)
    assert authenticate(username="ada", password="correct horse battery staple")  # unchanged


def test_linking_falls_back_to_a_new_account(client, idp, sso, make_user):  # noqa: F811
    sso(LINK_VERIFIED_EMAIL="true")
    make_user(Role.AUTHOR, username="ada", email="somebody-else@example.org")
    sign_in(client, idp, idp.person("alice"))
    identity = OidcIdentity.objects.get()
    assert (identity.user.username, identity.linked_by) == ("user-alice", OidcLink.CREATED)
    assert str(identity) == f"alice at {idp.issuer}"


def test_an_unverified_email_never_links(client, idp, sso, make_user):  # noqa: F811
    sso(LINK_VERIFIED_EMAIL="true")
    local = make_user(Role.AUTHOR, username="ada", email="alice@example.org")
    for verified in (False, "true", None):
        person = idp.person(
            f"alice-{verified}", email="alice@example.org", email_verified=verified
        )
        sign_in(Client(), idp, person)
    assert not local.oidc_identities.exists() and User.objects.count() == 4


@pytest.mark.parametrize(
    "setup, message, reason",
    [
        ("two accounts", "Several Forklift accounts have the email address", "email_ambiguous"),
        ("service account", "is a service account; it signs in with API", "service_account"),
        (
            "already linked",
            "is already linked to another Fake IdP sign-in",
            "email_already_linked",
        ),
    ],
)
def test_email_links_that_are_refused(
    client, idp, sso, make_user, setup, message, reason  # noqa: F811
):
    sso(LINK_VERIFIED_EMAIL="true")
    local = make_user(
        Role.AUTHOR,
        username="ada",
        email="alice@example.org",
        is_service_account=setup == "service account",
    )
    if setup == "two accounts":
        make_user(Role.VIEWER, username="ada2", email="ALICE@example.org")
    if setup == "already linked":
        OidcIdentity.objects.create(
            user=local, issuer=idp.issuer, subject="someone-else", linked_by=OidcLink.EMAIL
        )
    refusal(sign_in(client, idp, idp.person("alice")), 403, message)
    assert entries("user.oidc_login_refused")[0].details["reason"] == reason
    assert entries("user.oidc_link") == []
    assert not OidcIdentity.objects.filter(subject="alice").exists()


def test_a_refused_sign_in_leaves_no_link(client, idp, sso, make_user):  # noqa: F811
    sso(LINK_VERIFIED_EMAIL="true")
    make_user(Role.AUTHOR, username="ada", email="alice@example.org", is_active=False)
    refusal(sign_in(client, idp, idp.person("alice")), 403, "'ada' is deactivated")
    assert OidcIdentity.objects.count() == 0 and entries("user.oidc_link") == []


# --------------------------------------------------------------------------- the sign-in page


def test_the_sign_in_page_offers_both(client, idp, sso):  # noqa: F811
    page = client.get("/accounts/login/?next=/jobs/").content.decode()
    assert 'href="/accounts/oidc/login/?next=%2Fjobs%2F">Sign in with Fake IdP</a>' in page
    assert 'name="password"' in page and "Or with a Forklift password" in page


def test_password_sign_in_turned_off(client, idp, sso, settings, make_user):  # noqa: F811
    settings.FORKLIFT_PASSWORD_SIGN_IN = False
    page = client.get("/accounts/login/").content.decode()
    assert "Sign in with Fake IdP" in page and 'name="password"' not in page
    user = make_user(Role.ADMIN)
    response = client.post(
        "/accounts/login/",
        {"username": user.username, "password": "correct horse battery staple"},
    )
    refusal(response, 403, "Signing in with a password is turned off here; sign in with Fake IdP")
    assert signed_in_as(client) is None
    assert entries("user.login") == [] and entries("user.login_failed") == []  # never checked


def test_api_tokens_keep_working_for_people_who_sign_in_through_the_provider(
    client, idp, sso, settings  # noqa: F811
):
    settings.FORKLIFT_PASSWORD_SIGN_IN = False
    sign_in(client, idp, idp.person("alice"))
    csrf = client.cookies["csrftoken"].value
    created = client.post(
        "/api/v1/tokens",
        {"name": "ci", "scopes": ["jobs:read"]},
        content_type="application/json",
        HTTP_X_CSRFTOKEN=csrf,
    )
    assert created.status_code == 201, created.content
    me = Client().get("/api/v1/me", HTTP_AUTHORIZATION=f"Bearer {created.json()['token']}")
    assert me.json()["user"]["username"] == "user-alice"
    assert ApiToken.objects.get().owner.username == "user-alice"


def test_people_without_a_password_have_nothing_to_change(client, idp, sso):  # noqa: F811
    sign_in(client, idp, idp.person("alice"))
    assert 'href="/account/password/"' not in client.get("/").content.decode()
    response = client.get("/account/password/")
    assert response.status_code == 403
    assert "Your account has no password here" in response.content.decode()


# --------------------------------------------------------------------------- signing out


def test_signing_out_ends_the_session_only(client, idp, sso):  # noqa: F811
    sign_in(client, idp, idp.person("alice"))
    response = client.post("/accounts/logout/")
    assert response.status_code == 302 and response["Location"] == "/accounts/login/"
    assert signed_in_as(client) is None and idp.logouts == []
    assert "id_token" not in str(client.session.items())


def test_signing_out_at_the_provider_too(client, idp, sso, settings):  # noqa: F811
    sso(PROVIDER_LOGOUT="true")
    settings.FORKLIFT_PUBLIC_URL = "https://forklift.example.org"
    sign_in(client, idp, idp.person("alice"))
    id_token = client.session[oidc.SIGNED_IN_KEY]["id_token"]
    response = client.post("/accounts/logout/")
    assert response.status_code == 200 and signed_in_as(client) is None
    assert "no-store" in response["Cache-Control"]
    page = response.content.decode()
    target = re.search(r'<meta http-equiv="refresh" content="0; url=([^"]+)">', page).group(1)
    target = target.replace("&amp;", "&")
    parts = urllib.parse.urlsplit(target)
    assert f"{parts.scheme}://{parts.netloc}{parts.path}" == idp.url("/logout")
    assert dict(urllib.parse.parse_qsl(parts.query)) == {
        "client_id": CLIENT_ID,
        "post_logout_redirect_uri": "https://forklift.example.org/accounts/login/",
        "id_token_hint": id_token,
    }
    assert "Continue to sign out of Fake IdP" in page


def test_provider_logout_turned_on_after_signing_in(client, idp, sso):  # noqa: F811
    sign_in(client, idp, idp.person("alice"))  # no ID token kept for the hint
    sso(PROVIDER_LOGOUT="true")
    page = client.post("/accounts/logout/").content.decode()
    target = re.search(r'content="0; url=([^"]+)"', page).group(1).replace("&amp;", "&")
    query = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(target).query))
    assert query == {
        "client_id": CLIENT_ID,
        "post_logout_redirect_uri": "http://testserver/accounts/login/",
    }


def test_no_provider_logout_without_an_end_session_endpoint(client, idp, sso):  # noqa: F811
    sso(PROVIDER_LOGOUT="true")
    sign_in(client, idp, idp.person("alice"))
    idp.discovery_overrides["end_session_endpoint"] = REMOVE
    oidc.providers.reset()
    assert client.post("/accounts/logout/").status_code == 302


def test_no_provider_logout_when_the_provider_is_down(client, idp, sso):  # noqa: F811
    sso(PROVIDER_LOGOUT="true")
    sign_in(client, idp, idp.person("alice"))
    oidc.providers.reset()
    idp.raw["/.well-known/openid-configuration"] = (500, {}, b"")
    assert client.post("/accounts/logout/").status_code == 302
    assert signed_in_as(client) is None


def test_no_provider_logout_for_password_sessions(client, idp, sso, make_user):  # noqa: F811
    sso(PROVIDER_LOGOUT="true")
    client.force_login(make_user(Role.VIEWER))
    assert client.post("/accounts/logout/").status_code == 302
    # nor for a session from another issuer (the configuration changed since)
    sign_in(client, idp, idp.person("alice"))
    session = client.session
    session[oidc.SIGNED_IN_KEY] = {"issuer": "https://old.example.org", "subject": "alice"}
    session.save()
    assert client.post("/accounts/logout/").status_code == 302
