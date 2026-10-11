"""Check single sign-on against the OpenID Provider with this installation's settings."""

from __future__ import annotations

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.urls import reverse

from forklift_web.oidc import provider as providers
from forklift_web.oidc.provider import ProviderError


class Command(BaseCommand):
    help = (
        "Fetch the OpenID Provider's discovery document and key set as a sign-in would, and "
        "print what Forklift uses: the endpoints, the accepted ID token algorithms, the keys, "
        "the role mapping and the redirect URI to register at the provider. Exits with an "
        "error when single sign-on is not configured or the provider cannot be used."
    )

    def handle(self, *args, **options):
        conf = settings.FORKLIFT_OIDC
        if conf is None:
            raise CommandError(
                "Single sign-on is not configured: set FORKLIFT_OIDC_ISSUER and the other "
                "FORKLIFT_OIDC_* settings (docs/platform/oidc.md)."
            )
        provider = providers.get(conf)
        try:
            metadata = provider.metadata()
            keys, _ = provider.keys()
        except ProviderError as error:
            raise CommandError(error.detail) from None
        signing = [key for key in keys if key.get("use", "sig") == "sig"]
        if not signing:
            raise CommandError(f"The key set at {metadata.jwks_uri} has no signing keys.")
        base = settings.FORKLIFT_PUBLIC_URL or "https://<the gateway's address>"
        lines = [
            f"Issuer:                 {metadata.issuer}",
            f"Authorization endpoint: {metadata.authorization_endpoint}",
            f"Token endpoint:         {metadata.token_endpoint} ({conf.token_auth_method})",
            f"UserInfo endpoint:      {metadata.userinfo_endpoint or '(none)'}"
            + (" (used)" if conf.userinfo else " (not used)"),
            f"End-session endpoint:   {metadata.end_session_endpoint or '(none)'}"
            + (" (used)" if conf.provider_logout else " (not used)"),
            f"ID token algorithms:    {', '.join(metadata.algorithms)}",
            "Signing keys:           "
            + ", ".join(
                f"{key.get('kid', '(no kid)')} ({key.get('kty')}"
                + (f", {key['alg']}" if key.get("alg") else "")
                + ")"
                for key in signing
            ),
            f"Scopes:                 {' '.join(conf.scopes)}",
            f"Username claim:         {conf.username_claim}",
            f"Role claim:             {conf.role_claim} ("
            + (
                "; ".join(f"{value} -> {role}" for value, role in conf.role_values)
                or "no values mapped"
            )
            + f"; otherwise {conf.default_role or 'refused'})",
            f"Redirect URI:           {base}{reverse('forklift-oidc-callback')}",
        ]
        if conf.provider_logout:
            lines.append(f"Post-logout URI:        {base}{reverse('forklift-login')}")
        if not settings.FORKLIFT_PUBLIC_URL:
            lines.append(
                "FORKLIFT_PUBLIC_URL is not set: the redirect URI is built from each request's "
                "host; set it to the address people use, and register that URI."
            )
        self.stdout.write("\n".join(lines))
