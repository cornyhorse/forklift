"""Print a .env file with fresh random secrets for the Compose stack.

    python deploy/compose/generate_env.py > deploy/compose/.env

TLS in front of the gateway and the store (the caddy profile), with certificates from Let's
Encrypt (ZeroSSL as a fallback) or from Caddy's own CA for local and test installations:

    python deploy/compose/generate_env.py --domain forklift.example.org \\
        --acme-email ops@example.org > deploy/compose/.env
    python deploy/compose/generate_env.py --domain forklift.test --internal-ca \\
        > deploy/compose/.env

An object store outside the stack (start it with external-store.yml as well), which workers
reach through the egress proxy; fill in the store's credentials afterwards:

    python deploy/compose/generate_env.py --store-url https://s3.eu-west-1.amazonaws.com \\
        --store-bucket my-forklift --store-region eu-west-1 > deploy/compose/.env

Standard library only. Keep the file private: it holds the admin password, the database
password, the store's credentials and the keys that encrypt connection secrets.
"""

from __future__ import annotations

import argparse
import base64
import re
import secrets
import sys
from typing import Optional, Sequence
from urllib.parse import urlsplit

_LABEL = r"[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?"
_DOMAIN = re.compile(rf"^(?=.{{1,253}}$){_LABEL}(\.{_LABEL})*$")
_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_DEFAULT_PORTS = {"http": 80, "https": 443}


def token(nbytes: int = 24) -> str:
    return secrets.token_urlsafe(nbytes)


def _domain(text: str) -> str:
    name = text.strip().lower().rstrip(".")
    if not _DOMAIN.match(name):
        raise argparse.ArgumentTypeError(
            f"{text!r} is not a host name (for example forklift.example.org)"
        )
    return name


def _email(text: str) -> str:
    if not _EMAIL.match(text.strip()):
        raise argparse.ArgumentTypeError(f"{text!r} is not an email address")
    return text.strip()


def _port(text: str) -> int:
    if not text.strip().isdigit() or not 1 <= int(text) <= 65535:
        raise argparse.ArgumentTypeError(f"{text!r} is not a port (1 to 65535)")
    return int(text)


def _store_url(text: str) -> str:
    """``scheme://host[:port]`` of an S3 endpoint, without a default port."""
    parts = urlsplit(text.strip())
    try:
        port = parts.port
    except ValueError:
        port = -1
    if (
        parts.scheme not in _DEFAULT_PORTS
        or not parts.hostname
        or port == -1
        or parts.username is not None
        or parts.path not in ("", "/")
        or parts.query
        or parts.fragment
    ):
        raise argparse.ArgumentTypeError(
            f"{text!r} is not an http:// or https:// URL with a host and an optional port and "
            "nothing else (for example https://s3.eu-west-1.amazonaws.com)"
        )
    host = parts.hostname.lower()
    if ":" in host:
        host = f"[{host}]"
    if port is not None and port != _DEFAULT_PORTS[parts.scheme]:
        host = f"{host}:{port}"
    return f"{parts.scheme}://{host}"


def _value(text: str) -> str:
    if not text.strip() or re.search(r"[\s#'\"$]", text.strip()):
        raise argparse.ArgumentTypeError(f"{text!r} is empty or has spaces, quotes, # or $")
    return text.strip()


def parser() -> argparse.ArgumentParser:
    cli = argparse.ArgumentParser(
        prog="generate_env.py",
        description="Print a .env file with fresh random secrets for deploy/compose.",
    )
    tls = cli.add_argument_group(
        "TLS (the caddy profile)",
        "Caddy serves the UI and API on --domain and the store on --store-domain; both names "
        "must resolve to this host.",
    )
    tls.add_argument("--domain", type=_domain, help="host name of the UI and API")
    tls.add_argument(
        "--store-domain",
        type=_domain,
        help="host name of the object store (default: store.<domain>; none with --store-url)",
    )
    issuer = tls.add_mutually_exclusive_group()
    issuer.add_argument(
        "--acme-email",
        type=_email,
        help="certificates from Let's Encrypt (ZeroSSL as a fallback) for this ACME account; "
        "ports 80 and 443 must be reachable from the internet",
    )
    issuer.add_argument(
        "--internal-ca",
        action="store_true",
        help="certificates from Caddy's own CA (local and test installations: clients must "
        "trust its root, /data/caddy/pki/authorities/local/root.crt in the caddy container)",
    )
    tls.add_argument(
        "--https-port", type=_port, default=443, help="host port for HTTPS (default 443)"
    )
    tls.add_argument(
        "--http-port", type=_port, default=80, help="host port for HTTP, which redirects (80)"
    )
    store = cli.add_argument_group(
        "An external object store (with external-store.yml)",
        "The gateway calls the store at --store-url, workers reach it through the egress proxy, "
        "which allows that URL only, and the bundled RustFS is not started.",
    )
    store.add_argument("--store-url", type=_store_url, help="the store's S3 endpoint")
    store.add_argument(
        "--public-store-url",
        type=_store_url,
        help="where browsers and API clients reach the store (default: --store-url)",
    )
    store.add_argument("--store-bucket", type=_value, default="forklift", help="(forklift)")
    store.add_argument("--store-region", type=_value, default="us-east-1", help="(us-east-1)")
    return cli


def _check(cli: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.domain is None:
        for name in ("store_domain", "acme_email"):
            if getattr(args, name) is not None:
                cli.error(f"--{name.replace('_', '-')} needs --domain")
        if args.internal_ca or args.https_port != 443 or args.http_port != 80:
            cli.error("--internal-ca, --https-port and --http-port need --domain")
    elif not (args.acme_email or args.internal_ca):
        cli.error("--domain needs --acme-email (certificates from ACME) or --internal-ca")
    if args.store_url is None:
        if args.public_store_url is not None:
            cli.error("--public-store-url needs --store-url")
        if args.store_bucket != "forklift" or args.store_region != "us-east-1":
            cli.error("--store-bucket and --store-region need --store-url")
    elif args.store_domain is not None:
        cli.error(
            "--store-domain and --store-url exclude each other: an external store is not "
            "behind Caddy (browsers reach it at --public-store-url)"
        )
    if args.domain and args.store_domain == args.domain:
        cli.error("--store-domain must differ from --domain (the store needs a host of its own)")
    public_store = args.public_store_url or args.store_url
    if args.domain and public_store and public_store.startswith("http://"):
        cli.error(
            f"the UI is served over https, so browsers cannot upload to {public_store} (mixed "
            "content): give the store an https:// URL (--public-store-url)"
        )


def _https_url(host: str, port: int) -> str:
    return f"https://{host}" if port == 443 else f"https://{host}:{port}"


def render(args: argparse.Namespace) -> str:
    # Fernet keys are 32 random bytes, url-safe base64
    fernet_key = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()
    lines = [
        "# Generated by generate_env.py: random secrets for one installation; keep it private.",
        f"FORKLIFT_SECRET_KEY={token(48)}",
        f"FORKLIFT_SECRETS_KEYS={fernet_key}",
        f"FORKLIFT_DB_PASSWORD={token()}",
    ]
    if args.store_url:
        lines += [
            "# The store's credentials, an access key for the bucket: fill them in",
            "FORKLIFT_STORE_ACCESS_KEY=",
            "FORKLIFT_STORE_SECRET_KEY=",
        ]
    else:
        lines += [
            f"FORKLIFT_STORE_ACCESS_KEY=forklift-{secrets.token_hex(6)}",
            f"FORKLIFT_STORE_SECRET_KEY={token(32)}",
        ]
    lines += ["FORKLIFT_ADMIN_USERNAME=admin", f"FORKLIFT_ADMIN_PASSWORD={token(18)}", ""]

    public_url, public_store, hosts = "http://localhost:8080", "http://localhost:9000", []
    if args.domain:
        store_domain = None if args.store_url else (args.store_domain or f"store.{args.domain}")
        public_url = _https_url(args.domain, args.https_port)
        if store_domain:
            public_store = _https_url(store_domain, args.https_port)
        lines += [
            "# TLS: the caddy profile serves the UI and API (and the store) over https;",
            "# the names must resolve to this host",
            "COMPOSE_PROFILES=caddy",
            f"FORKLIFT_DOMAIN={args.domain}",
            f"FORKLIFT_STORE_DOMAIN={store_domain or ''}",
            f"FORKLIFT_TLS={'internal' if args.internal_ca else 'acme'}",
            f"FORKLIFT_ACME_EMAIL={args.acme_email or ''}",
            f"FORKLIFT_HTTPS_HOST_PORT={args.https_port}",
            f"FORKLIFT_HTTP_HOST_PORT={args.http_port}",
            "FORKLIFT_SECURE_COOKIES=true",
            "FORKLIFT_BEHIND_TLS_PROXY=true",
            "FORKLIFT_TRUSTED_PROXIES=1",
            "# Only Caddy is reachable from other hosts: the gateway's and the store's",
            "# ports are published on localhost",
            "FORKLIFT_BIND_ADDRESS=127.0.0.1",
            "",
        ]
        hosts = [args.domain]
    if args.store_url:
        public_store = args.public_store_url or args.store_url
        lines += [
            "# An object store outside the stack (start it with -f external-store.yml):",
            "# workers reach it through the egress-proxy, which allows FORKLIFT_STORE_URL only",
            f"FORKLIFT_STORE_URL={args.store_url}",
            f"FORKLIFT_STORE_HOST={urlsplit(args.store_url).netloc}",
            f"FORKLIFT_STORE_BUCKET={args.store_bucket}",
            f"FORKLIFT_STORE_REGION={args.store_region}",
            "",
        ]
    where = "" if args.domain or args.store_url else " (change both when it is not localhost)"
    lines += [
        f"# Where people reach the UI and the store{where}",
        f"FORKLIFT_PUBLIC_URL={public_url}",
        f"FORKLIFT_PUBLIC_STORE_URL={public_store}",
        f"FORKLIFT_ALLOWED_HOSTS={','.join([*hosts, 'localhost', '127.0.0.1', 'gateway'])}",
    ]
    return "\n".join(lines) + "\n"


def main(argv: Optional[Sequence[str]] = None) -> None:
    cli = parser()
    args = cli.parse_args(argv)
    _check(cli, args)
    sys.stdout.write(render(args))


if __name__ == "__main__":
    main()
