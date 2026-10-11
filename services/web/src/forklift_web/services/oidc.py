"""Signing in through the OpenID Provider: the authorization code flow, and what a sign-in does
with accounts.

**The flow** (OpenID Connect Core 1.0, the authorization code flow with PKCE): :func:`begin`
keeps a ``state``, a ``nonce`` and a PKCE verifier for this browser in its session (server side;
the cookie holds only the session key) and returns the provider's authorization URL with the
state, the nonce and the S256 challenge. :func:`complete` takes the state back out of the
session, so each one is used once (and only by the browser that started it, which defeats
login CSRF), within :data:`FLOW_SECONDS`; it exchanges the code with the verifier, checks the
ID token (forklift_web.oidc.id_token) and, with FORKLIFT_OIDC_USERINFO, adds the UserInfo
endpoint's claims (whose ``sub`` must be the token's).

**Accounts.** A sign-in finds its account by (issuer, ``sub``) through an
:class:`~forklift_web.core.models.OidcIdentity`; never by username. Without one:

1. with FORKLIFT_OIDC_LINK_VERIFIED_EMAIL, an ``email`` the provider marks verified
   (``email_verified`` is JSON true) links the identity to the one account with that address,
   unless that account is a service account or already linked to another identity at this
   issuer (several accounts with the address: refused);
2. else, with FORKLIFT_OIDC_CREATE_USERS (the default), a new account is created, named after
   FORKLIFT_OIDC_USERNAME_CLAIM, with an unusable password; a name another account has (in any
   letter case) is refused rather than shared;
3. else the sign-in is refused.

The role comes from FORKLIFT_OIDC_ROLE_CLAIM: the most powerful role any of its values maps to,
else FORKLIFT_OIDC_DEFAULT_ROLE; with neither the sign-in is refused. A new account gets it; with
FORKLIFT_OIDC_SYNC_ROLE (the default) every sign-in sets it again, so the provider decides roles
(even the last admin's); without, admins manage roles in Forklift after the first sign-in.
Deactivated accounts and service accounts are refused. Names (``given_name``, ``family_name``)
and a verified email are copied at each sign-in.

**Audit.** ``user.login`` (``method: oidc``), ``user.create`` and ``user.update`` (``source:
oidc``), ``user.oidc_link`` and ``user.oidc_login_refused`` with a reason code. Neither the audit
log nor the log ever holds a token, a code, a state or a nonce.
"""

from __future__ import annotations

import base64
import hashlib
import logging
import secrets
import time
from dataclasses import dataclass, field
from typing import Optional

from django.conf import settings
from django.contrib.auth.validators import UnicodeUsernameValidator
from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from django.urls import reverse
from django.utils import timezone

from forklift_web.core.choices import OidcLink
from forklift_web.core.models import OidcIdentity, User
from forklift_web.oidc import provider as providers
from forklift_web.oidc.config import OidcConfig
from forklift_web.oidc.id_token import InvalidIdToken, validate
from forklift_web.oidc.provider import ProviderError, with_query
from forklift_web.policy import ALL_SCOPES, Actor
from forklift_web.services import audit

logger = logging.getLogger(__name__)

FLOWS_KEY = "forklift_oidc_flows"  # session: state -> the flow it started
SIGNED_IN_KEY = "forklift_oidc"  # session: how this session signed in
FLOW_SECONDS = 600
MAX_FLOWS = 5  # sign-ins started in other tabs of the same browser
NAME_LENGTH = 150
# Errors of the authorization endpoint (RFC 6749 §4.1.2.1, OpenID Connect Core §3.1.2.6) that
# are shown by name; any other value is shown as "error" (the URL can be made up by anyone).
PROVIDER_ERRORS = {
    "access_denied",
    "invalid_request",
    "invalid_scope",
    "unauthorized_client",
    "unsupported_response_type",
    "server_error",
    "temporarily_unavailable",
    "interaction_required",
    "login_required",
    "account_selection_required",
    "consent_required",
    "invalid_request_uri",
    "invalid_request_object",
    "request_not_supported",
    "request_uri_not_supported",
    "registration_not_supported",
}


class SignInRefused(Exception):
    """A sign-in that did not go through: ``message`` for the person, ``status`` for the page."""

    def __init__(self, message: str, *, status: int = 403):
        super().__init__(message)
        self.message = message
        self.status = status


class _Refusal(SignInRefused):
    """A refusal still to be audited: raised inside the account transaction, recorded by
    :func:`complete` once that transaction has been rolled back."""

    def __init__(self, reason: str, message: str, *, status: int, details: dict):
        super().__init__(message, status=status)
        self.reason = reason
        self.details = details


@dataclass
class SignIn:
    """A sign-in that went through: the account, where to go next, and for the session."""

    user: User
    next_url: str
    session: dict
    audit_details: dict = field(default_factory=dict)


def config() -> Optional[OidcConfig]:
    return settings.FORKLIFT_OIDC


def public_url(request, name: str) -> str:
    """An absolute URL of the gateway, under FORKLIFT_PUBLIC_URL when it is set."""
    path = reverse(name)
    base = settings.FORKLIFT_PUBLIC_URL
    return base + path if base else request.build_absolute_uri(path)


def _challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def _system(request) -> Actor:
    """The gateway acting on a sign-in (who creates accounts and applies roles from claims)."""
    return Actor(
        scopes=ALL_SCOPES,
        system="oidc",
        request_id=getattr(request, "request_id", ""),
        ip=getattr(request, "client_ip", None),
    )


def _anonymous(request) -> Actor:
    return Actor.anonymous(
        request_id=getattr(request, "request_id", ""), ip=getattr(request, "client_ip", None)
    )


# --------------------------------------------------------------------------- the flow


def begin(request, next_url: str) -> str:
    """Start a sign-in: keep its state, nonce and PKCE verifier in the session; returns the
    provider's authorization URL. ProviderError when the provider cannot be used."""
    conf = config()
    metadata = providers.get(conf).metadata()
    state, nonce = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
    verifier = secrets.token_urlsafe(48)  # 64 characters (RFC 7636: 43 to 128)
    redirect_uri = public_url(request, "forklift-oidc-callback")
    now = time.time()
    flows = {
        key: flow
        for key, flow in request.session.get(FLOWS_KEY, {}).items()
        if now - flow["started_at"] < FLOW_SECONDS
    }
    for key in sorted(flows, key=lambda key: flows[key]["started_at"])[: -MAX_FLOWS + 1]:
        del flows[key]
    flows[state] = {
        "nonce": nonce,
        "verifier": verifier,
        "redirect_uri": redirect_uri,
        "next": next_url,
        "started_at": now,
    }
    request.session[FLOWS_KEY] = flows
    # Until the sign-in is finished the session is a visitor's: it ends with the flows in it
    # (the sweeper deletes it), not after FORKLIFT_SESSION_SECONDS.
    request.session.set_expiry(FLOW_SECONDS)
    return with_query(
        metadata.authorization_endpoint,
        {
            "response_type": "code",
            "client_id": conf.client_id,
            "redirect_uri": redirect_uri,
            "scope": " ".join(conf.scopes),
            "state": state,
            "nonce": nonce,
            "code_challenge": _challenge(verifier),
            "code_challenge_method": "S256",
        },
    )


def _refuse(reason: str, message: str, *, status: int = 403, **details):
    raise _Refusal(reason, message, status=status, details=details)


def _record(request, refusal: _Refusal) -> SignInRefused:
    """Audit and log a refused sign-in (never a token, a code, a state or a nonce)."""
    conf = config()
    details = {"reason": refusal.reason, "issuer": conf.issuer, **refusal.details}
    audit.record(_anonymous(request), "user.oidc_login_refused", details=details)
    logger.warning("Single sign-on refused", extra=details)
    return SignInRefused(refusal.message, status=refusal.status)


def _take_flow(request) -> Optional[dict]:
    """The flow the callback's state names, removed from the session (each state is used once,
    whatever happens next); None when it is unknown or too old."""
    state = request.GET.get("state", "")
    flows = dict(request.session.get(FLOWS_KEY, {}))
    flow = flows.pop(state, None) if state else None
    request.session[FLOWS_KEY] = flows
    if flow is None or time.time() - flow["started_at"] >= FLOW_SECONDS:
        return None
    return flow


def complete(request) -> SignIn:
    """Finish a sign-in at the callback; SignInRefused (audited once the callback's state was
    this browser's) when it does not go through."""
    conf = config()
    flow = _take_flow(request)
    if flow is None:
        # Not audited: anyone can call the callback with made-up values, and no identity is
        # involved yet.
        logger.info("Single sign-on callback without a known state", extra={"issuer": conf.issuer})
        raise SignInRefused(
            "This sign-in could not be finished: it was already used, it was started more than "
            f"{FLOW_SECONDS // 60} minutes ago, or it was started in another browser. Start "
            "again.",
            status=400,
        )
    try:
        claims, id_token = _claims(request, conf, flow)
        try:
            return _sign_in(request, conf, claims, id_token, flow["next"])
        except IntegrityError:
            # Two first sign-ins of one person at once: the other one created the account.
            return _sign_in(request, conf, claims, id_token, flow["next"])
    except _Refusal as refusal:
        raise _record(request, refusal) from None


def _claims(request, conf: OidcConfig, flow: dict) -> tuple:
    """(the checked claims, the raw ID token) the callback's code is exchanged for."""
    name = conf.provider_name
    error = request.GET.get("error")
    if error is not None:
        shown = error if error in PROVIDER_ERRORS else "error"
        logger.info(
            "The provider answered the sign-in with an error",
            extra={
                "error": error[:64],
                "error_description": request.GET.get("error_description", "")[:200],
            },
        )
        if shown == "access_denied":
            message = (
                f"{name} did not sign you in (access_denied): you cancelled, or {name} refused "
                "the sign-in. Start again, or ask an admin."
            )
        else:
            message = (
                f"{name} did not sign you in ({shown}). Start again, or ask an admin if this "
                "keeps happening."
            )
        _refuse("provider_error", message, error=shown)
    code = request.GET.get("code")
    if not code:
        _refuse(
            "no_code",
            f"{name} sent you back without an authorization code. Start again.",
            status=400,
        )
    provider = providers.get(conf)
    try:
        tokens = provider.exchange(code, flow["redirect_uri"], flow["verifier"])
        claims = validate(provider, tokens["id_token"], nonce=flow["nonce"])
        if conf.userinfo:
            claims = _with_userinfo(provider, tokens.get("access_token"), claims)
    except ProviderError as failure:
        logger.warning("Single sign-on failed at the provider", extra={"detail": failure.detail})
        _refuse(failure.reason, failure.message, status=502)
    except InvalidIdToken as invalid:
        _refuse(
            invalid.reason,
            f"The ID token from {name} {invalid.problem}, so the sign-in was refused. If this "
            "keeps happening, an admin finds the details in the gateway's log.",
        )
    return claims, tokens["id_token"]


def _with_userinfo(provider, access_token, claims: dict) -> dict:
    found = provider.userinfo(access_token)
    if found.get("sub") != claims["sub"]:
        raise InvalidIdToken(
            "names another person (sub) than the UserInfo endpoint does", "userinfo_mismatch"
        )
    return {**found, **claims}


# --------------------------------------------------------------------------- claims


def claim(claims: dict, name: str):
    """A claim by its name, or by a dotted path into nested objects (``realm_access.roles``)
    when no claim has the whole name (some providers use URLs, with dots, as claim names)."""
    if name in claims:
        return claims[name]
    value = claims
    for part in name.split("."):
        if not isinstance(value, dict) or part not in value:
            return None
        value = value[part]
    return value


def claim_values(claims: dict, name: str) -> list:
    """The strings of a claim that holds a string or a list."""
    value = claim(claims, name)
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, str)]
    return []


def verified_email(claims: dict) -> str:
    """The email address, when the provider says it is verified (JSON true); else ""."""
    email = claims.get("email")
    if claims.get("email_verified") is True and isinstance(email, str) and "@" in email:
        return email.strip()[:254]
    return ""


def _text(claims: dict, name: str) -> str:
    value = claims.get(name)
    return value.strip()[:NAME_LENGTH] if isinstance(value, str) else ""


# --------------------------------------------------------------------------- accounts


def _no_role(conf: OidcConfig) -> str:
    return (
        f"Your {conf.provider_name} account has no role in Forklift: none of the values of its "
        f"{conf.role_claim!r} claim is mapped to a role here. Ask an admin to give you access."
    )


@transaction.atomic
def _sign_in(request, conf: OidcConfig, claims: dict, id_token: str, next_url: str) -> SignIn:
    subject = claims["sub"]
    who = {"subject": subject}
    identity = (
        OidcIdentity.objects.select_for_update()
        .select_related("user")
        .filter(issuer=conf.issuer, subject=subject)
        .first()
    )
    role = conf.role_for(claim_values(claims, conf.role_claim))
    if identity is None:
        identity = _link_by_email(request, conf, claims) or _create(request, conf, claims, role)
    user = identity.user
    who["username"] = user.username
    if not user.is_active:
        _refuse(
            "inactive",
            f"Your Forklift account {user.username!r} is deactivated; ask an admin.",
            **who,
        )
    if user.is_service_account:
        _refuse(
            "service_account",
            f"{user.username!r} is a service account; it signs in with API tokens only.",
            **who,
        )
    if conf.sync_role and role is None:
        _refuse("no_role", _no_role(conf), **who)
    changed = _update(request, conf, user, claims, role if conf.sync_role else None)
    identity.last_used_at = timezone.now()
    identity.save(update_fields=["last_used_at"])
    session = {"issuer": conf.issuer, "subject": subject}
    if conf.provider_logout:
        session["id_token"] = id_token  # the hint for the provider's logout (server side)
    details = {"method": "oidc", "issuer": conf.issuer, "subject": subject}
    if changed:
        details["updated"] = changed
    return SignIn(user=user, next_url=next_url, session=session, audit_details=details)


def _link_by_email(request, conf: OidcConfig, claims: dict) -> Optional[OidcIdentity]:
    email = verified_email(claims) if conf.link_verified_email else ""
    if not email:
        return None
    found = list(User.objects.select_for_update().filter(email__iexact=email)[:2])
    who = {"subject": claims["sub"]}
    if not found:
        return None
    if len(found) > 1:
        _refuse(
            "email_ambiguous",
            f"Several Forklift accounts have the email address {conf.provider_name} gave for "
            "you, so none of them was linked to your sign-in; ask an admin.",
            **who,
        )
    user = found[0]
    who["username"] = user.username
    if user.is_service_account:
        _refuse(
            "service_account",
            f"The Forklift account with your email address, {user.username!r}, is a service "
            "account; it signs in with API tokens only.",
            **who,
        )
    if user.oidc_identities.filter(issuer=conf.issuer).exists():
        _refuse(
            "email_already_linked",
            f"The Forklift account with your email address, {user.username!r}, is already "
            f"linked to another {conf.provider_name} sign-in; ask an admin.",
            **who,
        )
    identity = OidcIdentity.objects.create(
        user=user, issuer=conf.issuer, subject=claims["sub"], linked_by=OidcLink.EMAIL
    )
    audit.record(
        _system(request),
        "user.oidc_link",
        user,
        {"issuer": conf.issuer, "subject": claims["sub"], "by": "verified_email"},
    )
    return identity


def _username(conf: OidcConfig, claims: dict) -> str:
    value = claim(claims, conf.username_claim)
    who = {"subject": claims["sub"]}
    if not isinstance(value, str) or not value.strip():
        _refuse(
            "no_username",
            f"{conf.provider_name} did not send the {conf.username_claim!r} claim, which this "
            "installation names new accounts after; ask an admin.",
            **who,
        )
    username = value.strip()
    try:
        UnicodeUsernameValidator()(username)
        if len(username) > NAME_LENGTH:
            raise ValidationError("too long")
    except ValidationError:
        _refuse(
            "invalid_username",
            f"The {conf.username_claim!r} claim from {conf.provider_name} is not a valid "
            f"Forklift username (at most {NAME_LENGTH} letters, digits and @ . + - _); ask an "
            "admin.",
            **who,
        )
    return username


def _create(request, conf: OidcConfig, claims: dict, role: Optional[str]) -> OidcIdentity:
    who = {"subject": claims["sub"]}
    if not conf.create_users:
        _refuse(
            "no_account",
            f"There is no Forklift account for your {conf.provider_name} sign-in, and this "
            "installation does not create accounts at the first sign-in; ask an admin for one.",
            **who,
        )
    if role is None:
        _refuse("no_role", _no_role(conf), **who)
    username = _username(conf, claims)
    if User.objects.filter(username__iexact=username).exists():
        _refuse(
            "username_taken",
            f"A Forklift account named {username!r} already exists and is not linked to your "
            f"{conf.provider_name} sign-in, so none was created; ask an admin.",
            username=username,
            **who,
        )
    user = User(
        username=username,
        role=role,
        email=verified_email(claims),
        first_name=_text(claims, "given_name"),
        last_name=_text(claims, "family_name"),
    )
    user.set_unusable_password()
    user.save()
    identity = OidcIdentity.objects.create(
        user=user, issuer=conf.issuer, subject=claims["sub"], linked_by=OidcLink.CREATED
    )
    audit.record(
        _system(request),
        "user.create",
        user,
        {
            "source": "oidc",
            "role": role,
            "issuer": conf.issuer,
            "subject": claims["sub"],
            "password_set": False,
        },
    )
    return identity


def _update(request, conf: OidcConfig, user: User, claims: dict, role: Optional[str]) -> list:
    """Copy names, a verified email and (``role``, when syncing) the role from the claims;
    returns the names of the fields that changed. A role change is audited."""
    wanted = {"first_name": _text(claims, "given_name"), "last_name": _text(claims, "family_name")}
    email = verified_email(claims)
    if email:
        wanted["email"] = email
    changed = [key for key, value in wanted.items() if value and getattr(user, key) != value]
    for key in changed:
        setattr(user, key, wanted[key])
    if role is not None and role != user.role:
        audit.record(
            _system(request),
            "user.update",
            user,
            {
                "changed": {"role": {"from": user.role, "to": role}},
                "source": "oidc",
                "claim": conf.role_claim,
            },
        )
        logger.info(
            "Role set from the provider's claims",
            extra={"username": user.username, "from": user.role, "to": role},
        )
        user.role = role
        changed.append("role")
    if changed:
        user.save(update_fields=changed)
    return changed


# --------------------------------------------------------------------------- signing out


def provider_logout_url(request, signed_in: Optional[dict]) -> Optional[str]:
    """Where to send the browser after signing out of Forklift, to sign out at the provider
    too (RP-Initiated Logout 1.0): only with FORKLIFT_OIDC_PROVIDER_LOGOUT, for a session that
    signed in through this provider, and when the provider has an end_session_endpoint."""
    conf = config()
    if conf is None or not conf.provider_logout or not signed_in:
        return None
    if signed_in.get("issuer") != conf.issuer:
        return None
    try:
        endpoint = providers.get(conf).metadata().end_session_endpoint
    except ProviderError as failure:
        logger.warning(
            "Signing out at the provider is not possible", extra={"detail": failure.detail}
        )
        return None
    if endpoint is None:
        return None
    params = {
        "client_id": conf.client_id,
        "post_logout_redirect_uri": public_url(request, "forklift-login"),
    }
    if signed_in.get("id_token"):
        params["id_token_hint"] = signed_in["id_token"]
    return with_query(endpoint, params)
