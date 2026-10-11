# Single sign-on (OpenID Connect)

The gateway can sign people in through your organisation's OpenID Provider: Keycloak, Microsoft
Entra ID, Okta, Google, Dex, Authentik or any other provider that implements OpenID Connect.
The sign-in page then shows **Sign in with *your provider***; the provider checks the person's
password (and second factor), and Forklift creates or finds their account and gives it a role
from the groups the provider sends. Password sign-in can stay, for break-glass accounts, or be
turned off. API tokens work as before, whichever way their owner signs in.

Single sign-on is off until `FORKLIFT_OIDC_ISSUER` is set. Settings that cannot work together
stop the gateway at start-up with a message that names the variables.

## How a sign-in works

1. The person clicks **Sign in with …**. The gateway keeps a random `state`, a `nonce` and a
   PKCE verifier in their session (server side; the cookie holds only the session key; until
   the sign-in is finished the session lasts 10 minutes, and the retention sweeper deletes
   expired sessions) and sends the browser to the provider's authorization endpoint (the
   authorization code flow with PKCE `S256`).
2. The person signs in at the provider, which sends the browser back to
   `https://<forklift>/accounts/oidc/callback/` with a code and the state.
3. The gateway takes the state out of the session (each one works once, for 10 minutes, and
   only in the browser that started it), exchanges the code at the provider's token endpoint
   with the client secret and the PKCE verifier, and checks the ID token it gets:
   - the signature, with the provider's published keys (`jwks_uri`). Only asymmetric
     algorithms are accepted (RS256/384/512, PS256/384/512, ES256/384/512, EdDSA; never `none`,
     never HMAC), the key must suit the algorithm, RSA keys must have at least 2048 bits;
   - `iss` equals `FORKLIFT_OIDC_ISSUER`; `aud` contains the client id, and with several
     audiences `azp` must be the client id; `exp`, `nbf` and `iat` with 60 seconds of clock
     skew; the `nonce` is this sign-in's.
4. The account is found by the provider's issuer and the person's `sub` (never by username),
   or linked or created as described under [Accounts](#accounts-and-linking); the role is
   set from the claims; the person is signed in and lands where they were going.

The gateway fetches the provider's discovery document from
`<issuer>/.well-known/openid-configuration` (its `issuer` must equal `FORKLIFT_OIDC_ISSUER`
character for character, a trailing slash included) and its key set, and caches both for an
hour per process. A token signed with a key the cache does not hold makes the gateway fetch the
key set again, at most once a minute, so key rotation at the provider needs nothing from you.

Every call to the provider uses `https://` (`http://` only with `FORKLIFT_OIDC_ALLOW_HTTP`),
verifies the certificate against the host name, has 5 seconds to connect and 10 seconds in all,
reads at most 1 MiB, follows no redirects and uses no proxy: the gateway must reach the
provider directly.

## Set it up

1. **Register Forklift at the provider** as a confidential web application (it has a client
   secret) that uses the authorization code flow. Give it:
   - the redirect URI `https://<forklift>/accounts/oidc/callback/`, where `<forklift>` is
     `FORKLIFT_PUBLIC_URL` (the address people use);
   - with `FORKLIFT_OIDC_PROVIDER_LOGOUT`, the post-logout redirect URI
     `https://<forklift>/accounts/login/`;
   - the scopes `openid email profile`, plus whatever makes the provider send groups (often
     `groups`).
2. **Give the gateway the settings** below. Keep the client secret in a file
   (`FORKLIFT_OIDC_CLIENT_SECRET_FILE`, e.g. a mounted Kubernetes or Docker secret) rather than
   in the environment.
3. **Check it** from a gateway container: `forklift-web check_oidc` fetches the discovery
   document and the keys as a sign-in would and prints the endpoints, the accepted
   algorithms, the role mapping and the redirect URI to register; it fails with the reason
   when something does not fit.
4. **Sign in** with an account that is in one of the mapped groups.

The gateway must know its own address: set `FORKLIFT_PUBLIC_URL` (the redirect URI is built
from it), and behind a TLS proxy `FORKLIFT_BEHIND_TLS_PROXY=true` and
`FORKLIFT_SECURE_COOKIES=true` as for any installation.

## Settings

An empty variable counts as unset, so Compose and Helm can pass every name through. A
`FORKLIFT_OIDC_*` name the gateway does not know (a typo) stops it at start-up.

| Variable | Default | What it is |
|---|---|---|
| `FORKLIFT_OIDC_ISSUER` | (unset: no single sign-on) | The provider's issuer, exactly as its discovery document states it (`https://keycloak.example.org/realms/main`) |
| `FORKLIFT_OIDC_CLIENT_ID` | (required) | The client id the provider issued for Forklift |
| `FORKLIFT_OIDC_CLIENT_SECRET_FILE` | | A file that holds the client secret (white space around it is ignored) |
| `FORKLIFT_OIDC_CLIENT_SECRET` | | The client secret itself (one of the two is required, not both) |
| `FORKLIFT_OIDC_TOKEN_AUTH_METHOD` | `client_secret_basic` | How the gateway authenticates at the token endpoint: `client_secret_basic` (HTTP Basic) or `client_secret_post` (in the form) |
| `FORKLIFT_OIDC_SCOPES` | `openid email profile` | Scopes to ask for, separated by spaces or commas; must include `openid` |
| `FORKLIFT_OIDC_PROVIDER_NAME` | `SSO` | The name on the button: "Sign in with *name*" |
| `FORKLIFT_OIDC_USERNAME_CLAIM` | `preferred_username` | The claim new accounts are named after (`email` for Google) |
| `FORKLIFT_OIDC_ROLE_CLAIM` | `groups` | The claim whose values map to roles: a string or a list of strings; a dotted name reaches into objects (`realm_access.roles`) |
| `FORKLIFT_OIDC_ADMIN_GROUPS` | | Values of the role claim that make someone an Admin (comma-separated) |
| `FORKLIFT_OIDC_AUTHOR_GROUPS` | | … an Author |
| `FORKLIFT_OIDC_OPERATOR_GROUPS` | | … an Operator |
| `FORKLIFT_OIDC_VIEWER_GROUPS` | | … a Viewer |
| `FORKLIFT_OIDC_DEFAULT_ROLE` | `none` | The role of someone none of whose values is mapped: `viewer`, `operator`, `author`, or `none` (they are refused). Never `admin` |
| `FORKLIFT_OIDC_CREATE_USERS` | `true` | Create an account at a person's first sign-in |
| `FORKLIFT_OIDC_SYNC_ROLE` | `true` | Set the role from the claims at every sign-in (else only when the account is created; admins manage it afterwards) |
| `FORKLIFT_OIDC_LINK_VERIFIED_EMAIL` | `false` | Link a person's first sign-in to the existing account with their email address, when the provider says it is verified |
| `FORKLIFT_OIDC_USERINFO` | `false` | Also read the claims from the provider's UserInfo endpoint (for providers that put groups there) |
| `FORKLIFT_OIDC_PROVIDER_LOGOUT` | `false` | Signing out of Forklift signs out at the provider too (its `end_session_endpoint`) |
| `FORKLIFT_OIDC_ALGORITHMS` | all asymmetric ones | ID token signature algorithms to accept, from RS256, RS384, RS512, PS256, PS384, PS512, ES256, ES384, ES512, EdDSA (also limited to what the provider publishes) |
| `FORKLIFT_OIDC_CA_FILE` | (the system's CAs) | PEM CA certificates to verify the provider with, instead of the system's (a private CA) |
| `FORKLIFT_OIDC_ALLOW_HTTP` | `false` | Accept an `http://` issuer and endpoints. **Development and tests only**: codes, tokens and the client secret would cross the network in clear text |
| `FORKLIFT_PASSWORD_SIGN_IN` | `true` | Keep the password form on the sign-in page. `false` is accepted only while single sign-on is configured |

What stops the gateway at start-up: an issuer that is not an `https://` URL (or `http://`
without the switch) or has a query or a fragment; no client id; no client secret, or both
forms of it, or a secret file that is missing, unreadable or empty; scopes without `openid`;
an unknown token authentication method; a value mapped to two roles; `DEFAULT_ROLE` `admin`
or unknown; `DEFAULT_ROLE` `none` with no mapped values (nobody could sign in); `none` or an
HMAC algorithm in `ALGORITHMS`; a CA file that holds no certificate; a boolean that is not one;
any `FORKLIFT_OIDC_*` variable without `FORKLIFT_OIDC_ISSUER`; an unknown `FORKLIFT_OIDC_*`
name; `FORKLIFT_PASSWORD_SIGN_IN=false` without single sign-on.

## Roles

The role claim's values are compared exactly (letter case counts) with the four lists. A person
whose values map to several roles gets the most powerful one; a person with no mapped value gets
`FORKLIFT_OIDC_DEFAULT_ROLE`, or is refused when it is `none` (the default: for a provider
that serves your whole organisation, or Google, only the groups you list get in).

With `FORKLIFT_OIDC_SYNC_ROLE=true` (the default) the provider decides: every sign-in sets the
role again (audited as `user.update` with `source: oidc`), and someone who has left every
mapped group is refused at their next sign-in. This applies to admins too, even the last one:
taking someone out of the admin group at the provider takes their admin role in Forklift at
their next sign-in. The claims are read only when someone signs in: a session that is already
open keeps the account's role until it ends (`FORKLIFT_SESSION_SECONDS`, 8 hours by default);
deactivate the account in Forklift to stop someone at once. With `false`, the claims give a new account its first role, and admins change
it in Forklift afterwards.

"View raw rows" is never taken from the provider: admins grant it in Forklift.

## Accounts and linking

A sign-in finds its account by the pair (issuer, `sub`), which admins see on the user's page.
The username claim only names new accounts; it never finds or links one. Without a link:

1. With `FORKLIFT_OIDC_LINK_VERIFIED_EMAIL=true`, an `email` that the provider marks verified
   (`email_verified` is JSON `true`; providers that do not send it, like Entra ID, never link)
   links the sign-in to the one account with that address, in any letter case, and the sign-in
   is audited as `user.oidc_link`. Not when several accounts have the address, when the account
   is a service account, or when it is already linked to another sign-in at this provider:
   those are refused.
2. Else, with `FORKLIFT_OIDC_CREATE_USERS=true`, a new account is created, named after the
   username claim, with no password (it cannot sign in with one until an admin sets it). A
   username that another account already has, in any letter case, is refused: the provider's
   `alice` is not taken to be Forklift's `Alice`.
3. Else the sign-in is refused.

Deactivated accounts are refused, and so are service accounts (they use API tokens only). At
every sign-in the account's first and last name (`given_name`, `family_name`) are copied from
the claims, and so is its email address when the provider marks it verified.

**Moving existing local accounts to single sign-on:** give each account the email address the
provider has for the person, turn on `FORKLIFT_OIDC_LINK_VERIFIED_EMAIL` while people sign in
for the first time, and turn it off again. Linking trusts the provider's email verification for
every account it reaches, admins included.

**Unlinking:** Admin, Users, the user, **Single sign-on**, **Unlink**, or
`DELETE /api/v1/admin/users/{id}/identities/{identity_id}` (audited as `user.oidc_unlink`;
`GET /api/v1/admin/users/{id}/identities` lists them). The person's next sign-in starts over:
it creates an account (which is refused while the old one has the username), links one by email
or is refused, as configured.

## Password sign-in, and getting back in

`FORKLIFT_PASSWORD_SIGN_IN=false` removes the password form: a password sent anyway is refused
(403) without being checked. People without a password do not see "Password" in the menu.

Keep a way in for when the provider is down or the role mapping went wrong:

- **API tokens keep working**: an admin's token (or a service account's with admin scopes)
  reaches `/api/v1/admin` whatever happens to single sign-on; keep one in your secrets vault.
- **Password sign-in again**: set `FORKLIFT_PASSWORD_SIGN_IN=true` (or remove it), restart the
  gateway, and sign in with a local admin account, such as the one `bootstrap_admin` created at
  installation (Docker Compose: `admin` and `FORKLIFT_ADMIN_PASSWORD` from `.env`). Fix what is
  wrong, then turn password sign-in off again.
- **A new admin from the shell**, when no admin account is left: in a gateway container run
  `FORKLIFT_ADMIN_PASSWORD='…' forklift-web bootstrap_admin --username rescue-admin`, then sign
  in with it as above.

## Signing out

Signing out of Forklift ends the Forklift session. With `FORKLIFT_OIDC_PROVIDER_LOGOUT=true`
and a provider that publishes an `end_session_endpoint` (Keycloak, Entra ID, Okta and Authentik
do; Google and Dex do not), a session that signed in through the provider then continues to the
provider's sign-out (OpenID Connect RP-Initiated Logout), with `client_id`, the ID token as
`id_token_hint` and `post_logout_redirect_uri=https://<forklift>/accounts/login/`, which you
register at the provider. For that, the ID token is kept in the session, on the server, until
the session ends; without the setting it is not kept.

## Provider examples

In each example `https://forklift.example.org` is `FORKLIFT_PUBLIC_URL`. Put the client secret
into the file `FORKLIFT_OIDC_CLIENT_SECRET_FILE` names.

### Keycloak

1. In the realm: **Clients**, **Create client**: type *OpenID Connect*, client id `forklift`;
   **Client authentication** on, **Standard flow** on (the other flows off); **Valid redirect
   URIs** `https://forklift.example.org/accounts/oidc/callback/`, **Valid post logout redirect
   URIs** `https://forklift.example.org/accounts/login/`. Under **Advanced**, set **Proof Key
   for Code Exchange Code Challenge Method** to `S256`. Copy the secret from **Credentials**.
2. Groups in the ID token: the client's **Client scopes**, `forklift-dedicated`, **Configure a
   new mapper**, **Group Membership**: token claim name `groups`, **Full group path** off,
   **Add to ID token** on. (For realm roles instead, add the *realm roles* mapper to the ID
   token and use `FORKLIFT_OIDC_ROLE_CLAIM=realm_access.roles`.)

```bash
FORKLIFT_OIDC_ISSUER=https://keycloak.example.org/realms/main
FORKLIFT_OIDC_CLIENT_ID=forklift
FORKLIFT_OIDC_CLIENT_SECRET_FILE=/run/secrets/forklift-oidc
FORKLIFT_OIDC_PROVIDER_NAME=Keycloak
FORKLIFT_OIDC_ADMIN_GROUPS=forklift-admins
FORKLIFT_OIDC_AUTHOR_GROUPS=forklift-authors
FORKLIFT_OIDC_OPERATOR_GROUPS=forklift-operators
FORKLIFT_OIDC_VIEWER_GROUPS=forklift-viewers
FORKLIFT_OIDC_PROVIDER_LOGOUT=true
```

Keycloak sends `email_verified`, so email linking works with it for the addresses Keycloak has
verified.

### Microsoft Entra ID

1. **App registrations**, **New registration**: accounts in this organizational directory only;
   redirect URI, platform *Web*: `https://forklift.example.org/accounts/oidc/callback/`. Add
   `https://forklift.example.org/accounts/login/` as a second redirect URI if you use provider
   logout (Entra accepts only registered `post_logout_redirect_uri` values).
2. **Certificates & secrets**, **New client secret**. It expires: note the date and rotate it
   (put the new value into the secret file and restart the gateways).
3. Roles: either **App roles** (create `forklift.admin`, `forklift.author`, … for users and
   groups, assign them under **Enterprise applications**, **Users and groups**; the ID token's
   claim is `roles`), or **Token configuration**, **Add groups claim** (security groups, *Group
   ID* in the ID token; the values are group object ids). App roles are the better choice: a
   person in more than 200 groups gets no `groups` claim at all (the "groups overage"), and app
   roles have readable names.

```bash
FORKLIFT_OIDC_ISSUER=https://login.microsoftonline.com/<tenant id>/v2.0
FORKLIFT_OIDC_CLIENT_ID=<application (client) id>
FORKLIFT_OIDC_CLIENT_SECRET_FILE=/run/secrets/forklift-oidc
FORKLIFT_OIDC_PROVIDER_NAME=Microsoft
FORKLIFT_OIDC_ROLE_CLAIM=roles
FORKLIFT_OIDC_ADMIN_GROUPS=forklift.admin
FORKLIFT_OIDC_AUTHOR_GROUPS=forklift.author
FORKLIFT_OIDC_OPERATOR_GROUPS=forklift.operator
FORKLIFT_OIDC_VIEWER_GROUPS=forklift.viewer
```

Use the tenant's own issuer: the multi-tenant endpoints (`common`, `organizations`) publish an
issuer with a `{tenantid}` placeholder, which never equals the tokens' issuer. Entra does not
send `email_verified`, so `FORKLIFT_OIDC_LINK_VERIFIED_EMAIL` never links for it.
`preferred_username` is the person's sign-in name.

### Google

1. In the Google Cloud console, **Google Auth Platform**: audience **Internal** (only your
   Workspace's accounts; with *External* any Google account could reach the role mapping).
   **Clients**, **Create client**, *Web application*, authorized redirect URI
   `https://forklift.example.org/accounts/oidc/callback/`.
2. Google sends no groups. Map the Workspace domain (the `hd` claim) to a role and let admins
   raise people in Forklift, or map single addresses with the `email` claim.

```bash
FORKLIFT_OIDC_ISSUER=https://accounts.google.com
FORKLIFT_OIDC_CLIENT_ID=<client id>.apps.googleusercontent.com
FORKLIFT_OIDC_CLIENT_SECRET_FILE=/run/secrets/forklift-oidc
FORKLIFT_OIDC_PROVIDER_NAME=Google
FORKLIFT_OIDC_USERNAME_CLAIM=email
FORKLIFT_OIDC_ROLE_CLAIM=hd
FORKLIFT_OIDC_VIEWER_GROUPS=example.org
FORKLIFT_OIDC_SYNC_ROLE=false
```

Google has no end-session endpoint: provider logout does not apply.

### Dex

A static client in Dex's configuration, and the `groups` scope (Dex sends groups only when
asked):

```yaml
staticClients:
  - id: forklift
    name: Forklift
    secretEnv: FORKLIFT_OIDC_CLIENT_SECRET
    redirectURIs: ["https://forklift.example.org/accounts/oidc/callback/"]
```

```bash
FORKLIFT_OIDC_ISSUER=https://dex.example.org/dex
FORKLIFT_OIDC_CLIENT_ID=forklift
FORKLIFT_OIDC_CLIENT_SECRET_FILE=/run/secrets/forklift-oidc
FORKLIFT_OIDC_SCOPES=openid email profile groups
FORKLIFT_OIDC_PROVIDER_NAME=Dex
FORKLIFT_OIDC_AUTHOR_GROUPS=forklift-authors
```

The groups come from Dex's connectors (LDAP, GitHub teams, …). The Compose end-to-end test
runs Dex with static users (`deploy/compose/e2e/oidc.yml`).

### Others

Okta (`https://<org>.okta.com/oauth2/default`, a *groups* claim set up on the authorization
server), Authentik (`https://authentik.example.org/application/o/<slug>/`, with its trailing
slash) and other providers work the same way: register a confidential web client with the
redirect URI, make the provider put the groups into the ID token (or set
`FORKLIFT_OIDC_USERINFO=true` if it puts them only into UserInfo answers, which must be JSON,
not signed JWTs), and copy the issuer from the provider's discovery document.

## Audit log and logs

| Event | Audit log | Log (JSON) |
|---|---|---|
| A sign-in through the provider | `user.login` with `method: oidc`, the issuer and the subject (and `updated`: the fields the claims changed) | |
| An account created at the first sign-in | `user.create` by `system:oidc`, with `source: oidc`, the role, the issuer and the subject | |
| A role set from the claims | `user.update` by `system:oidc`, with `changed`, `source: oidc` and the claim | INFO `Role set from the provider's claims` |
| A sign-in linked to an account by email | `user.oidc_link` with the issuer, the subject and `by: verified_email` | |
| A refused sign-in | `user.oidc_login_refused` with a `reason` (below), the issuer, and the subject and username once known | WARNING `Single sign-on refused`, and the provider's own error detail |
| An admin unlinks a sign-in | `user.oidc_unlink` with the issuer and the subject | |
| A callback with an unknown or used state | nothing (anyone can make one up) | INFO `Single sign-on callback without a known state` |
| Signing out | `user.logout` | |

Neither ever holds a code, a token, a state, a nonce or the client secret. The callback's URL
carries the code and the state, so the image's access log (gunicorn, through
`forklift_web.access_log`) writes its query as `[redacted]`, the JSON log writes requests as
method and path without the query, and the callback's pages ask browsers not to send it on as a
`Referer`. The reasons:
`provider_error` (with the provider's standard error code), `no_code`,
`provider_unavailable`, `provider_misconfigured`, `token_exchange_failed`, `userinfo_failed`,
`malformed`, `algorithm_not_allowed`, `unknown_key`, `unusable_key`, `bad_signature`,
`expired`, `not_yet_valid`, `wrong_issuer`, `wrong_audience`, `missing_claim`,
`nonce_mismatch`, `userinfo_mismatch`, `no_role`, `no_account`, `no_username`,
`invalid_username`, `username_taken`, `email_ambiguous`, `email_already_linked`, `inactive`,
`service_account`.

## Troubleshooting

Run `forklift-web check_oidc` first; the gateway's log has the details of every failure.

| What people see | Likely cause |
|---|---|
| "… is not set up correctly: the provider's discovery document names another issuer" | `FORKLIFT_OIDC_ISSUER` differs from the document's `issuer`: copy it from `<issuer>/.well-known/openid-configuration`, trailing slash and all (Entra: the tenant's own issuer, not `common`) |
| "… is not possible right now: … presented a TLS certificate that could not be verified" | A private CA: set `FORKLIFT_OIDC_CA_FILE` to its certificates |
| "… did not answer in time", "… refused the connection", "… could not be reached" | The gateway cannot reach the provider directly (network policy, firewall, DNS; the gateway does not use a proxy) |
| The provider shows "invalid redirect URI" | Register `https://<forklift>/accounts/oidc/callback/` exactly; set `FORKLIFT_PUBLIC_URL` to the address people use |
| "… refused to complete the sign-in (invalid_client)" | Wrong client secret, or the provider expects the other `FORKLIFT_OIDC_TOKEN_AUTH_METHOD` |
| "… refused to complete the sign-in (invalid_grant)" | The code expired or was used (start again), or the redirect URI the provider registered differs |
| "This sign-in could not be finished: it was already used …" | Back button or a reload of the callback, more than 10 minutes at the provider, or the session cookie was not sent back (a different host name than `FORKLIFT_PUBLIC_URL`, or `FORKLIFT_SECURE_COOKIES=true` over plain http) |
| "The ID token … is signed with a key the provider does not publish" | Keys rotated less than a minute after the gateway fetched them: it retries after a minute |
| "The ID token … has expired" / "is not valid yet" | The gateway's or the provider's clock is more than a minute off |
| "Your … account has no role in Forklift" | None of the person's role-claim values is mapped: check the claim's name and values (letter case counts), that the provider puts it into the ID token (Keycloak: *Add to ID token*), and the scope that makes it send them (Dex, Okta: `groups`) |
| "A Forklift account named … already exists and is not linked" | A local account has the username; see [moving local accounts](#accounts-and-linking) |
| "… did not send the 'preferred_username' claim" | The provider has no such claim (Google): set `FORKLIFT_OIDC_USERNAME_CLAIM=email` |

## Docker Compose and Helm

Both pass the variables above to the gateway under the same names; keep the client secret in a
secret mounted as a file and point `FORKLIFT_OIDC_CLIENT_SECRET_FILE` at it. Only the gateway
(the `gateway` service or Deployment) needs them: the sweeper, the dispatcher and the publisher
never sign anyone in. `FORKLIFT_PUBLIC_URL` must be the address people use.
