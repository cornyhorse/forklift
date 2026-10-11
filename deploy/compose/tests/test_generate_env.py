"""generate_env.py: the .env files it prints for the default stack, TLS and an external store.

python -m pytest deploy/compose/tests -o addopts= --cov=generate_env --cov-branch
"""

from __future__ import annotations

import base64
import runpy
import sys
from pathlib import Path

import pytest

COMPOSE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(COMPOSE))

import generate_env  # noqa: E402

SECRETS = (
    "FORKLIFT_SECRET_KEY",
    "FORKLIFT_SECRETS_KEYS",
    "FORKLIFT_DB_PASSWORD",
    "FORKLIFT_STORE_ACCESS_KEY",
    "FORKLIFT_STORE_SECRET_KEY",
    "FORKLIFT_ADMIN_PASSWORD",
)


def env(capsys, *argv: str) -> dict:
    generate_env.main(list(argv))
    text = capsys.readouterr().out
    values = {}
    for line in text.splitlines():
        if line and not line.startswith("#"):
            key, _, value = line.partition("=")
            assert key not in values, f"{key} twice"
            values[key] = value
    return values


def refused(capsys, *argv: str) -> str:
    with pytest.raises(SystemExit) as caught:
        generate_env.main(list(argv))
    assert caught.value.code == 2
    return capsys.readouterr().err


def test_the_default_stack_gets_fresh_secrets_and_localhost(capsys):
    first, second = env(capsys), env(capsys)

    for name in SECRETS:
        assert first[name] and first[name] != second[name], name
    assert len(base64.urlsafe_b64decode(first["FORKLIFT_SECRETS_KEYS"])) == 32
    assert first["FORKLIFT_STORE_ACCESS_KEY"].startswith("forklift-")
    assert first["FORKLIFT_ADMIN_USERNAME"] == "admin"
    assert first["FORKLIFT_PUBLIC_URL"] == "http://localhost:8080"
    assert first["FORKLIFT_PUBLIC_STORE_URL"] == "http://localhost:9000"
    assert first["FORKLIFT_ALLOWED_HOSTS"] == "localhost,127.0.0.1,gateway"
    assert not [key for key in first if "DOMAIN" in key or key.startswith("COMPOSE_")]


def test_tls_with_acme_certificates(capsys):
    values = env(capsys, "--domain", "Forklift.Example.org.", "--acme-email", "ops@example.org")

    assert values["COMPOSE_PROFILES"] == "caddy"
    assert values["FORKLIFT_DOMAIN"] == "forklift.example.org"
    assert values["FORKLIFT_STORE_DOMAIN"] == "store.forklift.example.org"
    assert (values["FORKLIFT_TLS"], values["FORKLIFT_ACME_EMAIL"]) == ("acme", "ops@example.org")
    assert values["FORKLIFT_PUBLIC_URL"] == "https://forklift.example.org"
    assert values["FORKLIFT_PUBLIC_STORE_URL"] == "https://store.forklift.example.org"
    assert values["FORKLIFT_ALLOWED_HOSTS"] == "forklift.example.org,localhost,127.0.0.1,gateway"
    assert values["FORKLIFT_SECURE_COOKIES"] == values["FORKLIFT_BEHIND_TLS_PROXY"] == "true"
    assert values["FORKLIFT_TRUSTED_PROXIES"] == "1"
    assert values["FORKLIFT_BIND_ADDRESS"] == "127.0.0.1"
    assert (values["FORKLIFT_HTTPS_HOST_PORT"], values["FORKLIFT_HTTP_HOST_PORT"]) == ("443", "80")


def test_tls_with_the_internal_ca_on_other_ports(capsys):
    values = env(
        capsys,
        "--domain=forklift-edge.test",
        "--store-domain=objects.forklift-edge.test",
        "--internal-ca",
        "--https-port=48443",
        "--http-port=48480",
    )

    assert (values["FORKLIFT_TLS"], values["FORKLIFT_ACME_EMAIL"]) == ("internal", "")
    assert values["FORKLIFT_PUBLIC_URL"] == "https://forklift-edge.test:48443"
    assert values["FORKLIFT_PUBLIC_STORE_URL"] == "https://objects.forklift-edge.test:48443"
    assert values["FORKLIFT_HTTPS_HOST_PORT"] == "48443"


def test_an_external_store(capsys):
    values = env(
        capsys,
        "--store-url",
        "HTTPS://S3.eu-west-1.amazonaws.com:443/",
        "--store-bucket",
        "my-forklift",
        "--store-region",
        "eu-west-1",
    )

    assert values["FORKLIFT_STORE_URL"] == "https://s3.eu-west-1.amazonaws.com"
    assert values["FORKLIFT_STORE_HOST"] == "s3.eu-west-1.amazonaws.com"
    assert values["FORKLIFT_PUBLIC_STORE_URL"] == "https://s3.eu-west-1.amazonaws.com"
    assert (values["FORKLIFT_STORE_BUCKET"], values["FORKLIFT_STORE_REGION"]) == (
        "my-forklift",
        "eu-west-1",
    )
    assert values["FORKLIFT_STORE_ACCESS_KEY"] == values["FORKLIFT_STORE_SECRET_KEY"] == ""
    assert "COMPOSE_PROFILES" not in values, "external-store.yml starts the egress proxy itself"
    assert values["FORKLIFT_PUBLIC_URL"] == "http://localhost:8080"


def test_an_external_store_on_another_port_seen_elsewhere_by_browsers(capsys):
    values = env(
        capsys,
        "--store-url=http://[FD00::5]:9000",
        "--public-store-url=http://localhost:48290",
    )

    assert values["FORKLIFT_STORE_URL"] == "http://[fd00::5]:9000"
    assert values["FORKLIFT_STORE_HOST"] == "[fd00::5]:9000"
    assert values["FORKLIFT_PUBLIC_STORE_URL"] == "http://localhost:48290"


def test_tls_with_an_external_https_store(capsys):
    values = env(
        capsys, "--domain=forklift.example.org", "--internal-ca", "--store-url=https://s3.example"
    )

    assert values["FORKLIFT_STORE_DOMAIN"] == "", "the store is not behind Caddy"
    assert values["FORKLIFT_PUBLIC_STORE_URL"] == "https://s3.example"
    assert values["FORKLIFT_PUBLIC_URL"] == "https://forklift.example.org"


@pytest.mark.parametrize(
    "argv, message",
    [
        (["--domain", "forklift.example.org"], "--domain needs --acme-email"),
        (["--domain", "bad host", "--internal-ca"], "'bad host' is not a host name"),
        (["--domain", "a.example", "--acme-email", "nobody"], "'nobody' is not an email"),
        (["--domain", "a.example", "--acme-email", "a@b.c", "--internal-ca"], "not allowed with"),
        (["--store-domain", "s.example"], "--store-domain needs --domain"),
        (["--acme-email", "a@b.c"], "--acme-email needs --domain"),
        (["--internal-ca"], "--internal-ca, --https-port and --http-port need --domain"),
        (["--https-port", "8443"], "--internal-ca, --https-port and --http-port need --domain"),
        (["--http-port", "8080"], "--internal-ca, --https-port and --http-port need --domain"),
        (["--domain", "a.example", "--internal-ca", "--https-port", "0"], "'0' is not a port"),
        (["--domain", "a.example", "--internal-ca", "--http-port", "x"], "'x' is not a port"),
        (
            ["--domain", "a.example", "--store-domain", "a.example", "--internal-ca"],
            "--store-domain must differ from --domain",
        ),
        (["--public-store-url", "https://s3.example"], "--public-store-url needs --store-url"),
        (["--store-bucket", "b"], "--store-bucket and --store-region need --store-url"),
        (["--store-region", "eu-west-1"], "--store-bucket and --store-region need --store-url"),
        (["--store-url", "ftp://s3.example"], "is not an http:// or https:// URL"),
        (["--store-url", "https://s3.example/bucket"], "is not an http:// or https:// URL"),
        (["--store-url", "https://key@s3.example"], "is not an http:// or https:// URL"),
        (["--store-url", "https://s3.example:99999"], "is not an http:// or https:// URL"),
        (["--store-url", "https://s3.example?x=1"], "is not an http:// or https:// URL"),
        (["--store-url", "https://s3.example#x"], "is not an http:// or https:// URL"),
        (["--store-url", "https://"], "is not an http:// or https:// URL"),
        (["--store-url", "https://s3.example", "--store-bucket", "a b"], "has spaces, quotes"),
        (["--store-url", "https://s3.example", "--store-region", " "], "is empty"),
        (
            ["--domain", "a.example", "--internal-ca", "--store-url", "https://s3.example"]
            + ["--store-domain", "s.example"],
            "--store-domain and --store-url exclude each other",
        ),
        (
            ["--domain", "a.example", "--internal-ca", "--store-url", "http://store:9000"],
            "browsers cannot upload to http://store:9000 (mixed content)",
        ),
        (
            ["--domain", "a.example", "--internal-ca", "--store-url", "https://s3.example"]
            + ["--public-store-url", "http://store.example"],
            "browsers cannot upload to http://store.example (mixed content)",
        ),
    ],
)
def test_contradictions_and_bad_values_are_explained(capsys, argv, message):
    assert message in refused(capsys, *argv)


def test_it_runs_as_a_script(capsys, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["generate_env.py", "--domain=a.example", "--internal-ca"])
    runpy.run_path(str(COMPOSE / "generate_env.py"), run_name="__main__")
    assert "FORKLIFT_DOMAIN=a.example" in capsys.readouterr().out
