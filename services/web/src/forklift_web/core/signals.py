"""Sign-ins and sign-outs go into the audit log, whichever view handled them."""

from __future__ import annotations

from django.contrib.auth.signals import user_logged_in, user_logged_out, user_login_failed
from django.dispatch import receiver

from forklift_web.policy import Actor
from forklift_web.services import audit


def _context(request) -> dict:
    return {
        "request_id": getattr(request, "request_id", ""),
        "ip": getattr(request, "client_ip", None),
    }


@receiver(user_logged_in)
def _logged_in(sender, request, user, **kwargs):
    # A single sign-on says how it went (method, issuer, subject); a password sign-in says
    # nothing more.
    details = getattr(request, "forklift_sign_in", None)
    audit.record(Actor.for_user(user, **_context(request)), "user.login", user, details)


@receiver(user_logged_out)
def _logged_out(sender, request, user, **kwargs):
    if user is not None:
        audit.record(Actor.for_user(user, **_context(request)), "user.logout", user)


@receiver(user_login_failed)
def _login_failed(sender, credentials, request=None, **kwargs):
    # Only the username: Django already masks the password in `credentials`, and it never
    # belongs in the log anyway.
    audit.record(
        Actor.anonymous(**_context(request)),
        "user.login_failed",
        details={"username": str(credentials.get("username", ""))[:150]},
    )
