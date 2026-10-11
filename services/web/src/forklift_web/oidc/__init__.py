"""Single sign-on through an OpenID Provider (OpenID Connect, the authorization code flow).

- ``config``: the ``FORKLIFT_OIDC_*`` settings, checked at start-up;
- ``provider``: the provider's discovery document, keys, token and UserInfo endpoints, over a
  client with TLS, deadlines, size limits and no redirects;
- ``id_token``: checking an ID token (signature, algorithm, issuer, audience, times, nonce).

What a sign-in does with a person's account (linking, creating, roles, audit) is
``forklift_web.services.oidc``; the pages are ``forklift_web.ui.views.oidc``.
"""
