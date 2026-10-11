"""The single sign-on settings: defaults, every value they take, and every combination that
cannot work, which stops the gateway at start-up with a message naming the variables."""

from __future__ import annotations

import importlib

import pytest
from django.core.exceptions import ImproperlyConfigured
from webhook_support import certificates

from forklift_web.core.choices import Role
from forklift_web.oidc import config
from forklift_web.oidc.config import OidcConfig, load, password_sign_in

ISSUER = "https://idp.example.org/realms/forklift"


def environ(**values) -> dict:
    """FORKLIFT_OIDC_* variables (names without the prefix) for a minimal valid setup."""
    base = {"ISSUER": ISSUER, "CLIENT_ID": "forklift", "CLIENT_SECRET": "s3cr3t-value"}
    base["VIEWER_GROUPS"] = "everyone"
    base.update(values)
    return {f"FORKLIFT_OIDC_{key}": value for key, value in base.items() if value is not None}


def refused(match: str, **values):
    with pytest.raises(ImproperlyConfigured, match=match) as error:
        load(environ(**values))
    assert "s3cr3t-value" not in str(error.value)
    return str(error.value)


def test_unset_means_no_single_sign_on():
    assert load({}) is None
    assert load({"FORKLIFT_OIDC_ISSUER": "", "FORKLIFT_OIDC_SCOPES": "  "}) is None


def test_defaults():
    found = load(environ())
    assert found == OidcConfig(
        issuer=ISSUER,
        client_id="forklift",
        client_secret="s3cr3t-value",
        token_auth_method="client_secret_basic",
        scopes=("openid", "email", "profile"),
        provider_name="SSO",
        username_claim="preferred_username",
        role_claim="groups",
        role_values=(("everyone", "viewer"),),
        default_role=None,
        create_users=True,
        sync_role=True,
        link_verified_email=False,
        userinfo=False,
        algorithms=config.ALGORITHMS,
        ca_file=None,
        allow_http=False,
        provider_logout=False,
    )
    assert found.discovery_url == ISSUER + "/.well-known/openid-configuration"
    assert "s3cr3t-value" not in repr(found)
    hash(found)  # per-configuration caches key on it


def test_every_setting(tmp_path):
    secret_file = tmp_path / "secret"
    secret_file.write_text("from-a-file\n")
    ca_pem, _, _ = certificates(tmp_path)
    ca_file = tmp_path / "ca.pem"
    ca_file.write_text(ca_pem)
    found = load(
        environ(
            ISSUER="https://idp.example.org/application/o/forklift/",
            CLIENT_SECRET=None,
            CLIENT_SECRET_FILE=str(secret_file),
            TOKEN_AUTH_METHOD="client_secret_post",
            SCOPES="openid, email groups  openid",
            PROVIDER_NAME=" Keycloak ",
            USERNAME_CLAIM="email",
            ROLE_CLAIM="realm_access.roles",
            ADMIN_GROUPS="admins, ops",
            AUTHOR_GROUPS="authors",
            OPERATOR_GROUPS="operators",
            VIEWER_GROUPS="",
            DEFAULT_ROLE="Viewer",
            CREATE_USERS="false",
            SYNC_ROLE="no",
            LINK_VERIFIED_EMAIL="yes",
            USERINFO="1",
            ALGORITHMS="ES256,RS256",
            CA_FILE=str(ca_file),
            ALLOW_HTTP="on",
            PROVIDER_LOGOUT="true",
        )
    )
    assert found.issuer.endswith("/forklift/")
    assert found.discovery_url == (
        "https://idp.example.org/application/o/forklift/.well-known/openid-configuration"
    )
    assert found.client_secret == "from-a-file"
    assert (found.token_auth_method, found.scopes) == (
        "client_secret_post",
        ("openid", "email", "groups"),
    )
    assert (found.provider_name, found.username_claim, found.role_claim) == (
        "Keycloak",
        "email",
        "realm_access.roles",
    )
    assert dict(found.role_values) == {
        "admins": "admin",
        "ops": "admin",
        "authors": "author",
        "operators": "operator",
    }
    assert found.default_role == "viewer"
    assert (found.create_users, found.sync_role, found.link_verified_email) == (False, False, True)
    assert (found.userinfo, found.allow_http, found.provider_logout) == (True, True, True)
    assert found.algorithms == ("ES256", "RS256")
    assert found.ca_file == str(ca_file)


def test_the_most_powerful_mapped_role_wins_else_the_default():
    found = load(environ(ADMIN_GROUPS="a", AUTHOR_GROUPS="b", VIEWER_GROUPS="c"))
    assert found.role_for(["c", "b", "x"]) == "author"
    assert found.role_for(["a", "c"]) == "admin"
    assert found.role_for(["x"]) is None
    assert load(environ(DEFAULT_ROLE="operator")).role_for([]) == "operator"
    assert config.ROLES == tuple(reversed(Role.values))


def test_the_issuer_must_be_https_unless_development_allows_http():
    message = refused("must be an https:// URL", ISSUER="http://localhost:8080/realms/x")
    assert "FORKLIFT_OIDC_ALLOW_HTTP=true" in message and "development" in message
    found = load(environ(ISSUER="http://localhost:8080/realms/x", ALLOW_HTTP="true"))
    assert found.issuer == "http://localhost:8080/realms/x"


@pytest.mark.parametrize(
    "issuer",
    [
        "idp.example.org",
        "ftp://idp.example.org",
        "https://",
        "https://user@idp.example.org",
        "https://idp.example.org/?tenant=1",
        "https://idp.example.org/#x",
        "https://idp.example.org/" + "x" * 300,
        "https://idp.example.org/a b",
    ],
)
def test_malformed_issuers_are_refused(issuer):
    refused("must be the provider's issuer URL exactly", ISSUER=issuer)


def test_settings_without_an_issuer_are_refused():
    with pytest.raises(ImproperlyConfigured, match="FORKLIFT_OIDC_CLIENT_ID is set but"):
        load({"FORKLIFT_OIDC_CLIENT_ID": "forklift"})
    with pytest.raises(
        ImproperlyConfigured, match="FORKLIFT_OIDC_CLIENT_ID, FORKLIFT_OIDC_SCOPES are"
    ):
        load({"FORKLIFT_OIDC_CLIENT_ID": "forklift", "FORKLIFT_OIDC_SCOPES": "openid"})


def test_misspelt_settings_are_refused():
    message = refused(
        "Unknown single sign-on settings: FORKLIFT_OIDC_ADMIN_GROUP ", ADMIN_GROUP="x"
    )
    assert "FORKLIFT_OIDC_ADMIN_GROUPS" in message  # the known names are listed
    assert load({**environ(), "FORKLIFT_OIDC_TYPO": ""}) is not None  # empty: not set


def test_the_client_id_and_one_secret_are_required(tmp_path):
    refused("FORKLIFT_OIDC_CLIENT_ID must be too", CLIENT_ID=None)
    refused("needs the client secret", CLIENT_SECRET=None)
    path = tmp_path / "secret"
    path.write_text("x")
    refused("not both", CLIENT_SECRET_FILE=str(path))
    refused("which cannot be read: No such file", CLIENT_SECRET=None, CLIENT_SECRET_FILE="/nope")
    path.write_text("\n  \n")
    refused("which is empty", CLIENT_SECRET=None, CLIENT_SECRET_FILE=str(path))
    path.write_bytes(b"\xff\xfe")
    refused("it is not UTF-8 text", CLIENT_SECRET=None, CLIENT_SECRET_FILE=str(path))


def test_scopes_need_openid_and_methods_are_known():
    refused("must include openid", SCOPES="email profile")
    refused("TOKEN_AUTH_METHOD must be one of client_secret_basic", TOKEN_AUTH_METHOD="none")


@pytest.mark.parametrize("claim", ["with space", "x" * 201])
def test_claim_names_are_checked(claim):
    refused("USERNAME_CLAIM must be a claim name", USERNAME_CLAIM=claim)
    refused("ROLE_CLAIM must be a claim name", ROLE_CLAIM=claim)


def test_role_mapping_must_let_someone_in_and_be_unambiguous():
    message = refused("could not let anyone in", VIEWER_GROUPS=None)
    assert "FORKLIFT_OIDC_ADMIN_GROUPS" in message and "set a default role" in message
    assert load(environ(VIEWER_GROUPS=None, DEFAULT_ROLE="viewer")).role_values == ()
    refused(
        "listed for two roles \\(admin and viewer\\)", ADMIN_GROUPS="staff", VIEWER_GROUPS="staff"
    )
    refused("DEFAULT_ROLE cannot be admin", DEFAULT_ROLE="admin")
    refused("DEFAULT_ROLE must be one of viewer, operator, author, none", DEFAULT_ROLE="guest")
    assert load(environ(DEFAULT_ROLE="none")).default_role is None


def test_only_asymmetric_algorithms():
    message = refused("refused: none, HS256", ALGORITHMS="RS256,none,HS256")
    assert "never" in message
    refused("refused: RS1", ALGORITHMS="RS1")


def test_booleans_are_checked():
    refused("FORKLIFT_OIDC_SYNC_ROLE must be a boolean", SYNC_ROLE="sometimes")


def test_the_ca_file_must_hold_certificates(tmp_path):
    refused("CA_FILE names /nope, which is not a readable file .*No such file", CA_FILE="/nope")
    junk = tmp_path / "junk.pem"
    junk.write_text("not a certificate")
    refused("which is not a readable file of PEM CA certificates", CA_FILE=str(junk))


def test_password_sign_in_can_be_turned_off_only_with_single_sign_on():
    conf = load(environ())
    assert password_sign_in(None, {}) is True
    assert password_sign_in(conf, {"FORKLIFT_PASSWORD_SIGN_IN": ""}) is True
    assert password_sign_in(conf, {"FORKLIFT_PASSWORD_SIGN_IN": "false"}) is False
    with pytest.raises(ImproperlyConfigured, match="nobody could sign in to the web interface"):
        password_sign_in(None, {"FORKLIFT_PASSWORD_SIGN_IN": "false"})


def test_the_settings_module_reads_and_checks_them(monkeypatch):
    for name, value in environ(PROVIDER_NAME="Keycloak").items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("FORKLIFT_PASSWORD_SIGN_IN", "false")
    module = importlib.import_module("forklift_web.settings")
    try:
        reloaded = importlib.reload(module)
        assert reloaded.FORKLIFT_OIDC.provider_name == "Keycloak"
        assert reloaded.FORKLIFT_PASSWORD_SIGN_IN is False
        monkeypatch.setenv("FORKLIFT_OIDC_SCOPES", "email")
        with pytest.raises(ImproperlyConfigured, match="must include openid"):
            importlib.reload(module)
    finally:
        monkeypatch.undo()
        importlib.reload(module)
    assert module.FORKLIFT_OIDC is None and module.FORKLIFT_PASSWORD_SIGN_IN is True
