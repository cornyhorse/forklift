"""The OpenID Provider client against a fake provider on a real TLS socket: discovery and its
checks, the cache, the key set and its rate-limited refresh, the token and UserInfo endpoints,
and providers that are down, slow, huge, redirecting or not speaking JSON or HTTP."""

from __future__ import annotations

import base64
import json
import socket
import threading

import pytest
from oidc_support import CLIENT_ID, CLIENT_SECRET, REMOVE, FakeProvider, Grant, idp  # noqa: F401

from forklift_web.oidc import provider as providers
from forklift_web.oidc.provider import Provider, ProviderError, with_query

DISCOVERY = "/.well-known/openid-configuration"


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def make(idp, clock=None, **overrides) -> Provider:  # noqa: F811
    return Provider(idp.config(**overrides), clock=clock or Clock())


def failure(call, match: str) -> ProviderError:
    with pytest.raises(ProviderError, match=match) as error:
        call()
    assert CLIENT_SECRET not in error.value.message + error.value.detail
    return error.value


# --------------------------------------------------------------------------- discovery


def test_discovery_is_cached_for_an_hour(idp):  # noqa: F811
    clock = Clock()
    provider = make(idp, clock)
    metadata = provider.metadata()
    assert (metadata.issuer, metadata.token_endpoint, metadata.jwks_uri) == (
        idp.issuer,
        idp.url("/token"),
        idp.url("/certs"),
    )
    assert metadata.userinfo_endpoint == idp.url("/userinfo")
    assert metadata.end_session_endpoint == idp.url("/logout")
    # what both the provider and the configuration allow, in the configuration's order
    assert metadata.algorithms == ("RS256", "PS256", "ES256")
    clock.now += 3599
    assert provider.metadata() is metadata
    clock.now += 1
    provider.metadata()
    assert len(idp.seen(DISCOVERY)) == 2
    request = idp.seen(DISCOVERY)[0]
    assert request.headers["accept"] == "application/json"
    assert request.headers["user-agent"].startswith("forklift-web/")


def test_the_issuer_must_match_exactly(idp):  # noqa: F811
    idp.discovery_overrides["issuer"] = idp.issuer + "/"
    error = failure(make(idp).metadata, "names another issuer than this installation")
    assert error.reason == "provider_misconfigured"
    assert f"names the issuer '{idp.issuer}/'" in error.detail
    assert "a trailing slash counts" in error.detail
    assert "An admin finds the details in the gateway's log" in error.message


@pytest.mark.parametrize("key", ["authorization_endpoint", "token_endpoint", "jwks_uri"])
def test_required_endpoints(idp, key):  # noqa: F811
    idp.discovery_overrides[key] = REMOVE
    failure(make(idp).metadata, f"has no {key}")
    idp.discovery_overrides[key] = 42
    failure(make(idp).metadata, f"has no {key}")


@pytest.mark.parametrize(
    "url, problem",
    [
        ("http://localhost:1/token", "is not an https:// URL"),
        ("https://someone@localhost/token", "without a user name"),
        ("https://localhost:99999/token", "with a host"),
        ("https:///token", "with a host"),
    ],
)
def test_endpoints_must_be_usable_urls(idp, url, problem):  # noqa: F811
    idp.discovery_overrides["token_endpoint"] = url
    error = failure(make(idp).metadata, "the provider's token endpoint is a URL this installation")
    assert problem in error.detail and error.reason == "provider_misconfigured"


def test_optional_endpoints(idp):  # noqa: F811
    idp.discovery_overrides.update(userinfo_endpoint=REMOVE, end_session_endpoint="")
    metadata = make(idp).metadata()
    assert (metadata.userinfo_endpoint, metadata.end_session_endpoint) == (None, None)
    failure(make(idp, USERINFO="true").metadata, "the provider has no UserInfo endpoint")
    idp.discovery_overrides["end_session_endpoint"] = "http://localhost/logout"
    failure(make(idp).metadata, "the provider's end session endpoint is a URL")


@pytest.mark.parametrize(
    "key, offered, needed",
    [
        ("response_types_supported", ["id_token"], "the authorization code flow"),
        ("code_challenge_methods_supported", ["plain"], "PKCE with S256"),
        ("token_endpoint_auth_methods_supported", ["private_key_jwt"], "client_secret_basic"),
    ],
)
def test_what_the_provider_must_support(idp, key, offered, needed):  # noqa: F811
    idp.discovery_overrides[key] = offered
    error = failure(make(idp).metadata, f"the provider does not offer {needed}")
    assert repr(offered) in error.detail
    idp.discovery_overrides[key] = REMOVE  # not published: assumed
    make(idp).metadata()


def test_the_algorithms_both_sides_allow(idp):  # noqa: F811
    idp.discovery_overrides["id_token_signing_alg_values_supported"] = ["HS256", "ES512"]
    error = failure(make(idp, ALGORITHMS="RS256").metadata, "algorithms this installation does")
    assert "signs ID tokens with HS256, ES512; FORKLIFT_OIDC_ALGORITHMS allows RS256" in (
        error.detail
    )
    idp.discovery_overrides["id_token_signing_alg_values_supported"] = REMOVE
    assert make(idp, ALGORITHMS="ES256,RS256").metadata().algorithms == ("ES256", "RS256")


# --------------------------------------------------------------------------- the connection


def test_tls_is_verified(idp):  # noqa: F811
    error = failure(make(idp, CA_FILE=None).metadata, "could not be verified")
    assert "FORKLIFT_OIDC_CA_FILE" in error.detail
    assert error.message.startswith("Signing in with Fake IdP is not possible right now:")
    assert error.reason == "provider_unavailable"


def test_a_plain_http_server_behind_an_https_issuer(tmp_path):
    plain = FakeProvider(tmp_path, tls=False)
    try:
        issuer = plain.issuer.replace("http://", "https://")
        provider = Provider(plain.config(ISSUER=issuer, ALLOW_HTTP=None))
        failure(provider.metadata, "failed the TLS handshake")
    finally:
        plain.stop()


def test_http_only_with_the_development_switch(tmp_path):
    plain = FakeProvider(tmp_path, tls=False)
    try:
        assert Provider(plain.config()).metadata().issuer.startswith("http://")
    finally:
        plain.stop()


def test_a_provider_that_is_down(idp):  # noqa: F811
    with socket.socket() as unused:
        unused.bind(("127.0.0.1", 0))
        port = unused.getsockname()[1]
    provider = make(idp, ISSUER=f"https://localhost:{port}/realms/forklift")
    error = failure(provider.metadata, "localhost refused the connection")
    assert "ConnectionRefusedError" in error.detail


def test_a_host_that_does_not_resolve(idp):  # noqa: F811
    provider = make(idp, ISSUER="https://idp.invalid/realms/forklift")
    failure(provider.metadata, "idp.invalid could not be reached")


def test_a_provider_that_is_too_slow(idp, monkeypatch):  # noqa: F811
    monkeypatch.setattr(providers, "TOTAL_SECONDS", 0.3)
    idp.delay[DISCOVERY] = 2
    error = failure(make(idp).metadata, "localhost did not answer in time")
    assert "no complete answer within 0.3 seconds" in error.detail


@pytest.mark.parametrize("how", ["length", "stream"])
def test_answers_are_at_most_a_mebibyte(idp, how):  # noqa: F811
    idp.huge[DISCOVERY] = how
    error = failure(make(idp).metadata, "sent an answer larger than 1024 KiB")
    assert "more than 1048576 bytes" in error.detail


def test_redirects_are_not_followed(idp):  # noqa: F811
    idp.raw[DISCOVERY] = (302, {"Location": idp.url("/elsewhere")}, b"")
    error = failure(make(idp).metadata, r"answered with a redirect \(302\), which is not followed")
    assert "give the URL it redirects to" in error.detail
    assert idp.seen("/elsewhere") == []


@pytest.mark.parametrize(
    "status, body, problem",
    [
        (500, b"{}", "answered 500"),
        (200, b"<html>", "did not answer with a JSON object"),
        (200, b"[1, 2]", "did not answer with a JSON object"),
        (200, b"\xff\xfe", "did not answer with a JSON object"),
    ],
)
def test_answers_that_are_not_a_json_document(idp, status, body, problem):  # noqa: F811
    idp.raw[DISCOVERY] = (status, {"Content-Type": "text/html"}, body)
    failure(make(idp).metadata, problem)


def test_an_answer_that_is_not_http(idp):  # noqa: F811
    listener = socket.create_server(("127.0.0.1", 0))

    def answer():
        connection, _ = listener.accept()
        connection.recv(65536)
        connection.sendall(b"SSH-2.0-not-http\r\n\r\n")
        connection.close()

    thread = threading.Thread(target=answer, daemon=True)
    thread.start()
    port = listener.getsockname()[1]
    try:
        provider = make(idp, ISSUER=f"http://localhost:{port}/x", ALLOW_HTTP="true")
        failure(provider.metadata, "did not answer with valid HTTP")
    finally:
        thread.join(5)
        listener.close()


def test_with_query_keeps_an_endpoints_own_query():
    assert with_query("https://idp/auth?p=b2c_1", {"a": "1 2"}) == "https://idp/auth?p=b2c_1&a=1+2"
    assert with_query("https://idp/auth", {"a": "b"}) == "https://idp/auth?a=b"


def test_one_provider_per_configuration(idp):  # noqa: F811
    conf = idp.config()
    assert providers.get(conf) is providers.get(idp.config())
    providers.reset()
    assert providers.get(conf) is not None


# --------------------------------------------------------------------------- keys


def test_keys_are_cached_and_refreshed_at_most_once_a_minute(idp):  # noqa: F811
    clock = Clock()
    provider = make(idp, clock)
    keys, fresh = provider.keys()
    assert fresh and [key["kid"] for key in keys] == ["rsa-1", "ec-1"]
    assert provider.keys() == (keys, False)
    clock.now += 59
    assert provider.keys(refresh=True) == (keys, False)  # too soon
    clock.now += 1
    idp.rotate()
    keys, fresh = provider.keys(refresh=True)
    assert fresh and [key["kid"] for key in keys] == ["rsa-3"]
    clock.now += 3599
    assert provider.keys()[1] is False
    clock.now += 1
    assert provider.keys()[1] is True  # an hour old
    assert len(idp.seen("/certs")) == 3


def test_a_key_set_without_keys(idp):  # noqa: F811
    idp.raw["/certs"] = (200, {"Content-Type": "application/json"}, b'{"keys": "none"}')
    failure(make(idp).keys, "the provider's key set has no keys")
    idp.raw["/certs"] = (200, {}, b'{"keys": [1, {"kid": "a"}]}')
    assert make(idp).keys()[0] == [{"kid": "a"}]


def test_a_failed_fetch_is_not_cached(idp):  # noqa: F811
    provider = make(idp)
    idp.raw["/certs"] = (503, {}, b"")
    failure(provider.keys, "answered 503")
    del idp.raw["/certs"]
    assert provider.keys()[1] is True


def test_waiting_too_long_for_another_fetch(idp, monkeypatch):  # noqa: F811
    monkeypatch.setattr(providers, "CONNECT_SECONDS", 0.05)
    monkeypatch.setattr(providers, "TOTAL_SECONDS", 0.05)
    provider = make(idp)
    with provider._keys_lock:  # another request is fetching them
        failure(provider.keys, "localhost is slow to send its keys")


# --------------------------------------------------------------------------- tokens


def grant(idp, code: str, verifier: str = "v" * 43, redirect_uri="https://f/cb"):  # noqa: F811
    from oidc_support import challenge

    person = idp.person("alice")
    idp.grants[code] = Grant(CLIENT_ID, redirect_uri, challenge(verifier), "n-1", "openid", person)
    return verifier


def test_the_code_is_exchanged_with_basic_authentication(idp):  # noqa: F811
    verifier = grant(idp, "code-1")
    answer = make(idp).exchange("code-1", "https://f/cb", verifier)
    assert isinstance(answer["id_token"], str) and answer["token_type"] == "Bearer"
    (request,) = idp.seen("/token")
    assert request.method == "POST"
    assert request.headers["content-type"] == "application/x-www-form-urlencoded"
    assert b"client_secret" not in request.body
    pair = base64.b64decode(request.headers["authorization"][6:]).decode()
    assert pair == f"{CLIENT_ID}:{CLIENT_SECRET}"


def test_or_with_the_secret_in_the_form(idp):  # noqa: F811
    idp.expected_auth = "client_secret_post"
    verifier = grant(idp, "code-1")
    provider = make(idp, TOKEN_AUTH_METHOD="client_secret_post")
    assert "id_token" in provider.exchange("code-1", "https://f/cb", verifier)
    assert "authorization" not in idp.seen("/token")[0].headers


def test_client_credentials_are_form_encoded_for_basic_authentication(idp, monkeypatch):  # noqa
    monkeypatch.setattr("oidc_support.CLIENT_SECRET", "a:b c%")
    verifier = grant(idp, "code-1")
    provider = make(idp, CLIENT_SECRET="a:b c%")
    assert "id_token" in provider.exchange("code-1", "https://f/cb", verifier)
    pair = base64.b64decode(idp.seen("/token")[0].headers["authorization"][6:]).decode()
    assert pair == f"{CLIENT_ID}:a%3Ab%20c%25"


def test_a_wrong_client_secret(idp):  # noqa: F811
    verifier = grant(idp, "code-1")
    provider = make(idp, CLIENT_SECRET="wrong")
    error = failure(
        lambda: provider.exchange("code-1", "https://f/cb", verifier),
        r"Fake IdP refused to complete the sign-in \(invalid_client\)",
    )
    assert error.reason == "token_exchange_failed"
    assert "answered 401: invalid_client. invalid_client usually means a wrong" in error.detail


def test_a_code_the_provider_refuses(idp):  # noqa: F811
    grant(idp, "code-1")
    provider = make(idp)
    error = failure(
        lambda: provider.exchange("code-1", "https://f/cb", "w" * 43), r"\(invalid_grant\)"
    )
    assert "answered 400: invalid_grant (Code not valid)." in error.detail
    assert "code-1" not in error.message + error.detail


def test_error_answers_without_json(idp):  # noqa: F811
    idp.raw["/token"] = (400, {}, b"Bad Request")
    error = failure(lambda: make(idp).exchange("c", "https://f/cb", "v"), r"\(no error code\)")
    assert error.detail.endswith("answered 400: no error code.")
    idp.raw["/token"] = (
        400,
        {},
        json.dumps({"error": "x" * 100, "error_description": 5}).encode(),
    )
    error = failure(lambda: make(idp).exchange("c", "https://f/cb", "v"), "refused to complete")
    assert "x" * 64 + ")" in error.message and "x" * 65 not in error.message


def test_answers_without_an_id_token(idp):  # noqa: F811
    verifier = grant(idp, "code-1")
    idp.token_answer = (200, {"access_token": "a", "token_type": "Bearer"})
    error = failure(
        lambda: make(idp).exchange("code-1", "https://f/cb", verifier), "without an ID token"
    )
    assert "is the openid scope granted" in error.detail
    idp.raw["/token"] = (500, {}, b"{}")
    failure(lambda: make(idp).exchange("c", "https://f/cb", "v"), "answered 500")


def test_userinfo(idp):  # noqa: F811
    person = idp.person("alice", groups=["a", "b"])
    idp.access_tokens["at-1"] = person
    claims = make(idp, USERINFO="true").userinfo("at-1")
    assert claims["sub"] == "alice" and claims["groups"] == ["a", "b"]
    assert idp.seen("/userinfo")[0].headers["authorization"] == "Bearer at-1"
    failure(lambda: make(idp).userinfo("unknown"), "answered 401")
    failure(lambda: make(idp).userinfo(None), "without an access token")
    idp.userinfo_answer = (200, "application/jwt; charset=utf-8", b"e30.e30.")
    error = failure(lambda: make(idp).userinfo("at-1"), "signs its UserInfo answers")
    assert "answer UserInfo requests with JSON" in error.detail
