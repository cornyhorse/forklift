"""The single sign-on settings, read from ``FORKLIFT_OIDC_*`` when the gateway starts.

``FORKLIFT_OIDC_ISSUER`` turns single sign-on on; every other ``FORKLIFT_OIDC_*`` variable needs
it. Settings that cannot work together fail at start-up (ImproperlyConfigured) with a message
naming the variables; messages never show the client secret. An empty variable counts as unset,
so a deployment can pass every name through (``${FORKLIFT_OIDC_SCOPES:-}`` in Compose).

``FORKLIFT_PASSWORD_SIGN_IN`` (default true) keeps the password form; it can be turned off only
while single sign-on is on.
"""

from __future__ import annotations

import os
import re
import ssl
from dataclasses import dataclass, field
from typing import Mapping, Optional
from urllib.parse import urlsplit

from django.core.exceptions import ImproperlyConfigured

from forklift_web.conf import env_bool

PREFIX = "FORKLIFT_OIDC_"
# Most powerful first: a person whose claim values map to several roles gets the first.
ROLES = ("admin", "author", "operator", "viewer")
DEFAULT_ROLES = ("viewer", "operator", "author", "none")
TOKEN_AUTH_METHODS = ("client_secret_basic", "client_secret_post")
# Asymmetric signatures only: never "none", never HMAC (an HS256 token "signed" with the
# provider's public key would verify with that key as the secret).
ALGORITHMS = (
    "RS256",
    "RS384",
    "RS512",
    "PS256",
    "PS384",
    "PS512",
    "ES256",
    "ES384",
    "ES512",
    "EdDSA",
)
DEFAULT_SCOPES = "openid email profile"
MAX_ISSUER_LENGTH = 255
_CLAIM_NAME = re.compile(r"^\S{1,200}$")

NAMES = {
    "ISSUER",
    "CLIENT_ID",
    "CLIENT_SECRET",
    "CLIENT_SECRET_FILE",
    "TOKEN_AUTH_METHOD",
    "SCOPES",
    "PROVIDER_NAME",
    "USERNAME_CLAIM",
    "ROLE_CLAIM",
    "ADMIN_GROUPS",
    "AUTHOR_GROUPS",
    "OPERATOR_GROUPS",
    "VIEWER_GROUPS",
    "DEFAULT_ROLE",
    "CREATE_USERS",
    "SYNC_ROLE",
    "LINK_VERIFIED_EMAIL",
    "USERINFO",
    "ALGORITHMS",
    "CA_FILE",
    "ALLOW_HTTP",
    "PROVIDER_LOGOUT",
}


@dataclass(frozen=True)
class OidcConfig:
    """Single sign-on as configured (hashable, so per-configuration caches can key on it).

    ``role_values`` pairs each claim value with the role it grants; ``default_role`` is the role
    of a person none of whose values is mapped (None: such a person is refused).
    """

    issuer: str
    client_id: str
    client_secret: str = field(repr=False)
    token_auth_method: str = "client_secret_basic"
    scopes: tuple = ("openid", "email", "profile")
    provider_name: str = "SSO"
    username_claim: str = "preferred_username"
    role_claim: str = "groups"
    role_values: tuple = ()
    default_role: Optional[str] = None
    create_users: bool = True
    sync_role: bool = True
    link_verified_email: bool = False
    userinfo: bool = False
    algorithms: tuple = ALGORITHMS
    ca_file: Optional[str] = None
    allow_http: bool = False
    provider_logout: bool = False

    @property
    def discovery_url(self) -> str:
        """``<issuer>/.well-known/openid-configuration`` (OpenID Connect Discovery 1.0, §4)."""
        return self.issuer.rstrip("/") + "/.well-known/openid-configuration"

    def role_for(self, values) -> Optional[str]:
        """The most powerful role any of ``values`` maps to, else the default role (or None)."""
        granted = {role for value, role in self.role_values if value in values}
        return next((role for role in ROLES if role in granted), self.default_role)


def _set(environ: Mapping[str, str]) -> dict:
    """The variables that have a value: an empty one counts as unset."""
    return {name: value for name, value in environ.items() if value.strip()}


def _var(name: str) -> str:
    return PREFIX + name


def _list(value: Optional[str]) -> list:
    """Items separated by commas or white space, without repeats, in their order."""
    return list(dict.fromkeys(item for item in re.split(r"[\s,]+", value or "") if item))


def _bool(values: dict, name: str, default: bool) -> bool:
    return env_bool(_var(name), default, environ=values)


def _check_names(values: dict) -> None:
    unknown = sorted(name for name in values if name.startswith(PREFIX))
    unknown = [name for name in unknown if name[len(PREFIX) :] not in NAMES]
    if unknown:
        raise ImproperlyConfigured(
            f"Unknown single sign-on settings: {', '.join(unknown)} (known: "
            + ", ".join(sorted(_var(name) for name in NAMES))
            + ")."
        )


def _issuer(value: str, allow_http: bool) -> str:
    name = _var("ISSUER")
    parts = urlsplit(value)
    schemes = ("https", "http") if allow_http else ("https",)
    if parts.scheme == "http" and not allow_http:
        raise ImproperlyConfigured(
            f"{name} must be an https:// URL; an http:// issuer is accepted only with "
            f"{_var('ALLOW_HTTP')}=true, which is for development (tokens and the client "
            "secret would cross the network in clear text)."
        )
    if (
        parts.scheme not in schemes
        or not parts.hostname
        or "@" in parts.netloc
        or parts.query
        or parts.fragment
        or len(value) > MAX_ISSUER_LENGTH
        or any(char.isspace() for char in value)
    ):
        raise ImproperlyConfigured(
            f"{name} must be the provider's issuer URL exactly as its discovery document "
            f"states it (https://host[/path], at most {MAX_ISSUER_LENGTH} characters, without "
            f"a query, a fragment or a user name); got {value!r}."
        )
    return value


def _client_secret(values: dict) -> str:
    inline, path = values.get(_var("CLIENT_SECRET")), values.get(_var("CLIENT_SECRET_FILE"))
    if inline and path:
        raise ImproperlyConfigured(
            f"Set {_var('CLIENT_SECRET')} or {_var('CLIENT_SECRET_FILE')}, not both."
        )
    if inline:
        return inline.strip()
    if not path:
        raise ImproperlyConfigured(
            f"Single sign-on needs the client secret the provider issued for this client: set "
            f"{_var('CLIENT_SECRET_FILE')} (a file that holds it, e.g. a mounted secret) or "
            f"{_var('CLIENT_SECRET')}."
        )
    try:
        with open(path, encoding="utf-8") as handle:
            secret = handle.read().strip()
    except (OSError, UnicodeDecodeError) as error:
        reason = error.strerror if isinstance(error, OSError) else "it is not UTF-8 text"
        raise ImproperlyConfigured(
            f"{_var('CLIENT_SECRET_FILE')} names {path}, which cannot be read: {reason}."
        ) from None
    if not secret:
        raise ImproperlyConfigured(f"{_var('CLIENT_SECRET_FILE')} names {path}, which is empty.")
    return secret


def _claim(values: dict, name: str, default: str) -> str:
    claim = values.get(_var(name), default).strip()
    if not _CLAIM_NAME.match(claim):
        raise ImproperlyConfigured(
            f"{_var(name)} must be a claim name without spaces (at most 200 characters); got "
            f"{claim!r}."
        )
    return claim


def _role_values(values: dict) -> tuple:
    found: dict = {}
    for role in ROLES:
        name = _var(f"{role.upper()}_GROUPS")
        for value in _list(values.get(name)):
            if value in found:
                raise ImproperlyConfigured(
                    f"The claim value {value!r} is listed for two roles ({found[value]} and "
                    f"{role}); give each value one role ({name} and the other "
                    f"{_var('*_GROUPS')})."
                )
            found[value] = role
    return tuple(found.items())


def _default_role(values: dict, role_values: tuple) -> Optional[str]:
    name = _var("DEFAULT_ROLE")
    role = values.get(name, "none").strip().lower()
    if role == "admin":
        raise ImproperlyConfigured(
            f"{name} cannot be admin: it would make everyone the provider signs in an admin. "
            f"List the admins' claim values in {_var('ADMIN_GROUPS')} instead."
        )
    if role not in DEFAULT_ROLES:
        raise ImproperlyConfigured(
            f"{name} must be one of {', '.join(DEFAULT_ROLES)}; got {role!r}."
        )
    if role == "none" and not role_values:
        raise ImproperlyConfigured(
            f"Single sign-on could not let anyone in: {name} is none (people without a mapped "
            f"role are refused) and no {_var('ADMIN_GROUPS')}, {_var('AUTHOR_GROUPS')}, "
            f"{_var('OPERATOR_GROUPS')} or {_var('VIEWER_GROUPS')} maps a value of the "
            f"{_var('ROLE_CLAIM')} claim to a role. Map some, or set a default role."
        )
    return None if role == "none" else role


def _algorithms(values: dict) -> tuple:
    name = _var("ALGORITHMS")
    chosen = _list(values.get(name)) or list(ALGORITHMS)
    refused = [alg for alg in chosen if alg not in ALGORITHMS]
    if refused:
        raise ImproperlyConfigured(
            f"{name} may list only asymmetric signature algorithms ({', '.join(ALGORITHMS)}); "
            f"refused: {', '.join(refused)} ('none' and HMAC algorithms such as HS256 are never "
            "accepted for ID tokens)."
        )
    return tuple(chosen)


def _scopes(values: dict) -> tuple:
    scopes = _list(values.get(_var("SCOPES"), DEFAULT_SCOPES))
    if "openid" not in scopes:
        raise ImproperlyConfigured(
            f"{_var('SCOPES')} must include openid (without it the provider sends no ID token); "
            f"got {' '.join(scopes)!r}."
        )
    return tuple(scopes)


def _ca_file(values: dict) -> Optional[str]:
    path = values.get(_var("CA_FILE"))
    if path is None:
        return None
    try:
        ssl.create_default_context(cafile=path)
    except (OSError, ssl.SSLError) as error:
        raise ImproperlyConfigured(
            f"{_var('CA_FILE')} names {path}, which is not a readable file of PEM CA "
            f"certificates ({error.strerror or getattr(error, 'reason', None) or error})."
        ) from None
    return path


def load(environ: Mapping[str, str] = os.environ) -> Optional[OidcConfig]:
    """The single sign-on configuration, or None when ``FORKLIFT_OIDC_ISSUER`` is unset."""
    values = _set(environ)
    _check_names(values)
    issuer = values.get(_var("ISSUER"))
    if issuer is None:
        others = sorted(name for name in values if name.startswith(PREFIX))
        if others:
            raise ImproperlyConfigured(
                f"{', '.join(others)} {'is' if len(others) == 1 else 'are'} set but "
                f"{_var('ISSUER')} is not: set the issuer to turn single sign-on on, or remove "
                "the other single sign-on settings."
            )
        return None
    allow_http = _bool(values, "ALLOW_HTTP", False)
    client_id = values.get(_var("CLIENT_ID"))
    if client_id is None:
        raise ImproperlyConfigured(
            f"{_var('ISSUER')} is set, so {_var('CLIENT_ID')} must be too: the client id the "
            "provider issued for this Forklift installation."
        )
    method = values.get(_var("TOKEN_AUTH_METHOD"), TOKEN_AUTH_METHODS[0]).strip()
    if method not in TOKEN_AUTH_METHODS:
        raise ImproperlyConfigured(
            f"{_var('TOKEN_AUTH_METHOD')} must be one of {', '.join(TOKEN_AUTH_METHODS)}; got "
            f"{method!r}."
        )
    role_values = _role_values(values)
    return OidcConfig(
        issuer=_issuer(issuer.strip(), allow_http),
        client_id=client_id.strip(),
        client_secret=_client_secret(values),
        token_auth_method=method,
        scopes=_scopes(values),
        provider_name=values.get(_var("PROVIDER_NAME"), "SSO").strip(),
        username_claim=_claim(values, "USERNAME_CLAIM", "preferred_username"),
        role_claim=_claim(values, "ROLE_CLAIM", "groups"),
        role_values=role_values,
        default_role=_default_role(values, role_values),
        create_users=_bool(values, "CREATE_USERS", True),
        sync_role=_bool(values, "SYNC_ROLE", True),
        link_verified_email=_bool(values, "LINK_VERIFIED_EMAIL", False),
        userinfo=_bool(values, "USERINFO", False),
        algorithms=_algorithms(values),
        ca_file=_ca_file(values),
        allow_http=allow_http,
        provider_logout=_bool(values, "PROVIDER_LOGOUT", False),
    )


def password_sign_in(oidc: Optional[OidcConfig], environ: Mapping[str, str] = os.environ) -> bool:
    """Whether the sign-in page keeps its password form (``FORKLIFT_PASSWORD_SIGN_IN``)."""
    enabled = env_bool("FORKLIFT_PASSWORD_SIGN_IN", True, environ=_set(environ))
    if not enabled and oidc is None:
        raise ImproperlyConfigured(
            "FORKLIFT_PASSWORD_SIGN_IN is false, but single sign-on is not configured "
            f"({_var('ISSUER')}): nobody could sign in to the web interface. Configure single "
            "sign-on, or keep password sign-in."
        )
    return enabled
