# OIDC: single sign-on for the gateway (agent OIDC, round 4)

Status: **done**. OIDC sign-in with any OpenID Provider, configured from `FORKLIFT_OIDC_*`,
with accounts, roles from claims, linking rules, admin screens and API, audit, provider logout,
a fake-provider test suite (real TLS, real RSA/EC keys, real JWTs), a Chromium test, and a
real end-to-end run against Dex in Compose (reliable: six runs in a row, and the whole e2e suite
with `webhooks.yml` + `oidc.yml` together). Full gateway suite: see "Verification" (100% line
and branch).

**Heads-up for anyone running the gateway with the shared venvs:** the gateway now imports
PyJWT (a new dependency in `services/web/pyproject.toml`), and `$S/webvenv` / `$S/webvenv313`
do not have it, so the gateway (and its tests) fail there with `ModuleNotFoundError: jwt`
until `$S/webvenv/bin/pip install "PyJWT>=2.15.1,<3"` (same for `webvenv313`). I left the
shared venvs untouched (brief: own venvs for new packages); `$S/oidc-venv` has everything.
Images built from this tree get it automatically.

## What I built, and where

All under `services/web/` unless noted.

| Piece | Files |
|---|---|
| Settings (checked at start-up) | `src/forklift_web/oidc/config.py` (`load()`, `password_sign_in()`), wired in `settings.py` as `FORKLIFT_OIDC` / `FORKLIFT_PASSWORD_SIGN_IN` |
| Provider client: discovery (exact issuer match, cached 1 h), JWKS (cached 1 h, refetched at most once a minute on an unknown `kid`), token exchange (`client_secret_basic`/`client_secret_post`), UserInfo; https only (dev switch), TLS verified (system CAs or `FORKLIFT_OIDC_CA_FILE`), 5 s connect / 10 s whole exchange, 1 MiB answers, no redirects, no proxy | `src/forklift_web/oidc/provider.py` |
| ID token checks with PyJWT 2.15.1: alg allow-list (asymmetric only, ∩ provider's list; never `none`/HMAC), key by `kid` (key `alg`/`use`/type/curve, RSA ≥ 2048 via `enforce_minimum_key_length`), `iss`, `aud`, `azp` (required with several audiences), `exp`/`nbf`/`iat` with 60 s leeway, `sub`, `nonce` (constant-time) | `src/forklift_web/oidc/id_token.py` |
| The flow and accounts: PKCE S256, `state`/`nonce`/verifier in the server-side session, single-use, 10 min, ≤5 parallel flows; (issuer, sub) → `OidcIdentity`; verified-email linking only when enabled; account creation with unusable password; role mapping and sync; inactive/service accounts refused; audit; RP-initiated logout URL | `src/forklift_web/services/oidc.py` |
| Model + migration | `OidcIdentity` in `core/models/accounts.py`, `OidcLink` in `core/choices.py`, `core/migrations/0007_oidc_identities.py` |
| Pages | `ui/views/oidc.py` (`/accounts/oidc/login/`, `/accounts/oidc/callback/`, `SignOutView`), `ui/views/account.py` (button, password form switch, no-password page), `registration/login.html`, `registration/signed_out.html` (meta refresh to the provider's end-session endpoint), `ui/base.html` (`head` block; "Password" link only with a usable password), `urls.py` |
| Admin | user page section "Single sign-on" (identities, Unlink), `admin-user-identity-unlink`; API `GET /api/v1/admin/users/{id}/identities`, `DELETE /api/v1/admin/users/{id}/identities/{identity_id}`; `services/accounts.py` (`list_identities`, `unlink_identity`, audited `user.oidc_unlink`); `contracts/openapi.json` regenerated (`export_openapi`) |
| `forklift-web check_oidc` | `core/management/commands/check_oidc.py`: fetches discovery + keys as a sign-in would, prints endpoints, algorithms, keys, role mapping, the redirect URI to register |
| Log hygiene (found while testing, see Security) | `access_log.py` (gunicorn `--logger-class`, Dockerfile CMD), `logs.py` (requests in JSON logs without query), callback pages `Referrer-Policy: no-referrer` |
| Housekeeping | `services/retention.py`: the sweeper deletes expired sessions (`sessions` in the sweep report / `SweepOut`); `begin()` gives a visitor session 10 minutes, `login()` resets it to `FORKLIFT_SESSION_SECONDS` |
| Shared deadline socket | `deadline.py` (extracted from `webhook_client.py`, now used by both; also fixed: the wrapper closed the real socket while `http.client` was still reading a `Connection: close` body, which the webhook client never hit because it reads no bodies) |
| Dependency | `pyproject.toml`: `PyJWT>=2.15.1,<3` (cryptography was already there) |
| Docs | `docs/platform/oidc.md` (new: how it works, setup, every setting, Keycloak/Entra ID/Google/Dex examples, roles, linking, recovery, logout, audit, troubleshooting); `services/web/README.md` (new: the gateway README with the full settings table; there was no gateway README before) |
| Tests | `tests/oidc_support.py` (the fake provider), `test_oidc_config.py`, `test_oidc_provider.py`, `test_oidc_id_token.py`, `test_oidc_flow.py`, `test_oidc_admin.py`, `test_oidc_browser.py`, `test_access_log.py`, `test_deadline.py`; rows added to `test_role_matrix.py`, `test_ui_matrix.py`, `world.py` (`World.identity()`), `test_plumbing.py`; `test_webhook_client.py` patches the deadline module's clock too |
| Dex end to end | `deploy/compose/e2e/oidc.yml` (Dex `dexidp/dex:v2.45.1` with an inline `configs.content` config: a static client and two static password users, one in `forklift-authors`, one in an unmapped group; the gateway's OIDC env) and `deploy/compose/e2e/test_oidc.py` |

Not mine: `core/management/commands/create_worker_token.py` and
`tests/test_create_worker_token.py` are HELM's (already committed by the lead); I did not touch
them, and the full suite below includes them.

## Decisions, and why

- **Settings names** (Compose/Helm pass them through unchanged): `FORKLIFT_OIDC_ISSUER` (turns it
  on), `_CLIENT_ID`, `_CLIENT_SECRET` or `_CLIENT_SECRET_FILE`, `_TOKEN_AUTH_METHOD`, `_SCOPES`,
  `_PROVIDER_NAME`, `_USERNAME_CLAIM`, `_ROLE_CLAIM`, `_ADMIN_GROUPS`, `_AUTHOR_GROUPS`,
  `_OPERATOR_GROUPS`, `_VIEWER_GROUPS`, `_DEFAULT_ROLE`, `_CREATE_USERS`, `_SYNC_ROLE`,
  `_LINK_VERIFIED_EMAIL`, `_USERINFO`, `_PROVIDER_LOGOUT`, `_ALGORITHMS`, `_CA_FILE`,
  `_ALLOW_HTTP`, and `FORKLIFT_PASSWORD_SIGN_IN` (not under `OIDC_`: it is about the password
  form; `false` is refused without OIDC). Per-role `*_GROUPS` lists instead of one
  `value=role` map: simpler in Helm values and env files, and no escaping of `=`/`,`.
- **Empty = unset** for every OIDC variable (Compose's `${VAR:-}` passes empty strings; with the
  stock `env_bool` an empty `FORKLIFT_OIDC_CREATE_USERS` would have meant false). **Unknown
  `FORKLIFT_OIDC_*` names stop start-up** (typos like `_ADMIN_GROUP`), and so does any OIDC
  variable without the issuer.
- **`DEFAULT_ROLE` defaults to `none`** (refused), and `admin` is not accepted as a default:
  a provider usually serves the whole organisation (or, with Google External, everyone).
  `none` with no mapped values stops start-up ("nobody could sign in").
- **Role sync is authoritative, even for the last admin.** The "last active admin" guard of the
  admin UI is not applied to sync: if the provider took someone out of the admin group, keeping
  them admin would be a privilege the provider revoked. Recovery is documented (API token,
  password sign-in again with the bootstrap admin, `bootstrap_admin --username rescue-admin`).
- **Username collisions are refused** (case-insensitive), never suffixed or linked; migration of
  local accounts goes through verified-email linking (documented). Unverified emails are not
  copied onto accounts (a later provider change plus email linking could otherwise hand an
  account to whoever verifies that address); `email_verified` must be JSON `true`.
- **The button is a link (GET), not a form**, and **provider logout goes through a page with a
  meta refresh**: the gateway's CSP has `form-action 'self'`, and Chromium applies it to the
  redirects after a form submission, so a POST that redirects to the provider would be blocked.
  Login CSRF is prevented by the state being bound to the session.
- **The ID token is kept in the session only with `FORKLIFT_OIDC_PROVIDER_LOGOUT`** (for
  `id_token_hint`; Okta requires it, Keycloak asks for confirmation without it). Sessions are
  database-backed: it stays server side and goes with the session.
- **Refusals before a known state are logged, not audited** (anyone can call the callback with
  made-up values; auditing those would let anyone fill the audit log). From a known state on,
  every refusal is audited with a reason code, after the account transaction is rolled back (an
  internal `_Refusal` carries the details out of `@transaction.atomic`).
- **Login audit**: the existing `user_logged_in` signal records `user.login`; an OIDC sign-in
  adds details `{"method": "oidc", "issuer", "subject", "updated"?}` (set on the request before
  `login()`), so there is one login entry per sign-in, whatever the method. Creation and role
  changes reuse `user.create` / `user.update` with `source: oidc`, actor `system:oidc` (with the
  request id and IP). New actions: `user.oidc_link`, `user.oidc_unlink`,
  `user.oidc_login_refused`.
- **No proxy support for provider calls** (like the webhook client): the gateway must reach the
  provider directly. Documented as such.
- **Not linking via an admin action**: admins can see and unlink identities, but not link one by
  hand (not asked for; email linking covers migration). Easy to add later if wanted.

## Security: what I found and fixed on the way

1. **The authorization code and state were in the logs** (found with the real stack):
   gunicorn's access log writes the full request line, and Django logs every 4xx/5xx answer
   with the request, which `JsonFormatter` serialised as `<WSGIRequest: GET '/path?query'>`. Fixed:
   `forklift_web.access_log.AccessLog` (the image's gunicorn `--logger-class`) writes the
   callback's query (and a `Referer` naming the callback) as `[redacted]`; `JsonFormatter`
   writes any request as `METHOD /path` without the query (this also keeps search terms and
   other queries out of 4xx/5xx warnings everywhere); callback pages send
   `Referrer-Policy: no-referrer`. Verified against the real stack: `docker logs` of the gateway
   holds no `code=` after the Dex tests.
2. **`DeadlineSocket` closed the real socket under `http.client`** when an answer ended with the
   connection: reading a body afterwards failed with EBADF. Latent for webhooks (they read no
   body); fixed with reference counting like a real socket's `makefile`.
3. **Anonymous sessions**: starting a sign-in needs a session (state server side). Those
   sessions now last 10 minutes until the sign-in is finished, and the sweeper deletes expired
   sessions (Django never did: expired sessions accumulated before this change too).

Residual risks (for §8): whoever controls the provider (its admins, group memberships, email
verification when linking is on) controls who gets which role in Forklift; sync may demote the
last admin; an open session keeps its role until it ends (8 h default) unless the account is
deactivated (that takes effect on the next request); the discovery document's endpoints are
trusted as the provider states them (they may point at internal hosts); the DNS lookup of the
provider has the system resolver's timeout, not the 10 s deadline; the ID token used as
`id_token_hint` travels in the browser's URL to the provider (as RP-Initiated Logout specifies).

## What I could not do or verify

- **Keycloak, Entra ID, Okta, Google, Authentik were not run** (only Dex, really, and the fake
  provider). The provider sections in `docs/platform/oidc.md` follow those providers'
  documented behaviour (issuer formats, claims, end-session support, Entra's groups overage and
  registered post-logout URIs); the menu paths and claim details come from my knowledge of
  those providers' documentation, not from their live consoles: worth a check by someone with
  access to one. Dex was verified for real: static passwords with `groups` and
  `preferredUsername` (v2.45.1 supports both), PKCE S256, `client_secret_basic`.
- **Dex has no `end_session_endpoint`**, so provider logout was verified only against the fake
  provider (unit tests and Chromium), not against a real provider.
- The Dex e2e uses an `http://` issuer with `FORKLIFT_OIDC_ALLOW_HTTP` (TLS to the provider is
  covered by the unit tests' fake provider with a CA of the tests' own, and the failure without
  `FORKLIFT_OIDC_CA_FILE`).

## Exact text for the lead's shared files

### CHANGELOG.md

Under `### Added`, a new bullet (or a sub-bullet of this round's platform entry):

```markdown
- **Single sign-on through an OpenID Provider** (`docs/platform/oidc.md`). With
  `FORKLIFT_OIDC_ISSUER` set, the sign-in page offers "Sign in with *provider*" (Keycloak, Entra
  ID, Okta, Google, Dex, Authentik or any OpenID Connect provider): the authorization code flow
  with PKCE (S256), the `state`, `nonce` and verifier kept in the server-side session and used
  once, within 10 minutes, by the browser that started the sign-in; the discovery document
  (whose issuer must match exactly) and the key set are cached for an hour, and an unknown key
  makes the gateway fetch the key set again, at most once a minute. ID tokens are checked with
  PyJWT: asymmetric algorithms only (never `none` or HMAC), RSA keys of at least 2048 bits,
  `iss`, `aud`, `azp` when there are several audiences, `exp`/`nbf`/`iat` with 60 s of clock
  skew and the nonce; the UserInfo endpoint's claims can be added. Accounts are found by the
  provider's issuer and `sub` (the new `OidcIdentity`, migration `0007`), never by username;
  an existing account is linked by a verified email address only with
  `FORKLIFT_OIDC_LINK_VERIFIED_EMAIL`; new accounts (`FORKLIFT_OIDC_CREATE_USERS`, on by
  default) have no usable password. Roles come from a claim (`FORKLIFT_OIDC_ROLE_CLAIM`,
  default `groups`; a dotted name reaches into objects) through
  `FORKLIFT_OIDC_{ADMIN,AUTHOR,OPERATOR,VIEWER}_GROUPS`, with a default role or none (refused),
  and are set again at every sign-in unless `FORKLIFT_OIDC_SYNC_ROLE=false`. Deactivated
  accounts and service accounts are refused. `FORKLIFT_PASSWORD_SIGN_IN=false` removes the
  password form (API tokens keep working; the guide describes the ways back in), and
  `FORKLIFT_OIDC_PROVIDER_LOGOUT` signs people out at the provider too. Settings that cannot
  work together, a misspelt `FORKLIFT_OIDC_*` name included, stop the gateway at start-up; the
  client secret can be read from a file, a private CA from `FORKLIFT_OIDC_CA_FILE`, and an
  `http://` provider is accepted only with the development switch `FORKLIFT_OIDC_ALLOW_HTTP`.
  Sign-ins, accounts created, roles set from claims, email links and every refused sign-in
  (with a reason code) are audited, never with a code or a token. Admins see a user's linked
  sign-ins on the user page and through `GET /api/v1/admin/users/{id}/identities`, and unlink
  one there or with `DELETE /api/v1/admin/users/{id}/identities/{identity_id}` (audited).
  `forklift-web check_oidc` checks the settings against the provider and prints the redirect
  URI to register. Tested against a fake provider (a real TLS server whose real RSA and EC keys
  sign real JWTs, with every check failing in turn), in Chromium, and end to end against Dex
  (`deploy/compose/e2e/oidc.yml`). The gateway's settings are listed in `services/web/README.md`.
```

Under `### Security`:

```markdown
- **Single sign-on keeps its codes out of the logs.** The callback's URL carries the provider's
  authorization code and the state: the image's gunicorn access log writes that query (and a
  `Referer` naming the callback) as `[redacted]`, the callback's pages send
  `Referrer-Policy: no-referrer`, and the gateway's JSON log now writes the request Django
  attaches to every 4xx and 5xx warning as method and path, without the query string (which
  could also carry search terms).
```

Under `### Changed`:

```markdown
- The retention sweeper also deletes expired sessions (`sessions` in the sweep report); a
  single sign-on that is started but not finished keeps its visitor session for 10 minutes.
  The "Password" link and page are offered only to accounts that have a password.
```

### docs/design/platform.md

§1 "Decisions already taken", the Authentication row:

```markdown
| Authentication | Local accounts, single sign-on through an OpenID Provider (OIDC; password sign-in can stay or be turned off), and API tokens |
```

§5.1, replace the `User, Group` row and add `OidcIdentity` after it:

```markdown
| `User`, `Group` | Django auth | Accounts that sign in with a password or through the OpenID Provider (accounts it created have no usable password until an admin sets one) |
| `OidcIdentity` | user, issuer, subject (`sub`), linked_by (`created`, `email`), created_at, last_used_at | How a single sign-on finds its account: (issuer, subject) is unique; never by username, by a verified email only with `FORKLIFT_OIDC_LINK_VERIFIED_EMAIL`; admins unlink ([single sign-on](../platform/oidc.md)) |
```

§5.4, last paragraph: in "The admin endpoints (`/api/v1/admin/...`: users and roles, tokens,
...)" add "users' single sign-on identities" after "users and roles", and append:

```markdown
Single sign-on uses two pages of the public port, `/accounts/oidc/login/` (starts a sign-in at
the provider) and `/accounts/oidc/callback/` (the redirect URI registered there).
```

§5.7, last sentences: "It also aborts stale multipart output uploads, deletes webhook
deliveries with their jobs' records, and drops sign-in counters whose window and lock have
ended." → "... drops sign-in counters whose window and lock have ended, and deletes expired
sessions."

§8, the "Accounts, tokens" row: replace "; OIDC later" at its end with "; optional single sign-on
(next row)", and add a row after it:

```markdown
| Single sign-on (OIDC) | Forged, replayed or misdirected ID tokens; login CSRF and code injection; an account taken over through a matching username or email; codes in logs | Authorization code flow with PKCE (S256); `state`, `nonce` and verifier kept in the server-side session and used once by the browser that started the sign-in; ID tokens verified with the provider's published keys (asymmetric algorithms only, never `none` or HMAC; key type and size checked), `iss` exact, `aud`/`azp`, `exp`/`nbf`/`iat` with 60 s of skew, the nonce; provider calls `https` only with TLS verified, a 10 s deadline, a 1 MiB limit and no redirects; accounts found by (issuer, `sub`) only, email linking only for verified addresses and only when enabled; refusals audited; the callback's code and state redacted from the logs ([single sign-on](../platform/oidc.md)) |
```

§8 "Residual risks", append:

```markdown
With single sign-on, whoever controls the provider (its admins, group memberships, email
verification when linking is on) decides who gets which role, admins included, and role
synchronisation can demote the last admin (the way back in is documented); an open session keeps
its role until it ends unless an admin deactivates the account; the endpoints the provider's
discovery document names are trusted as stated.
```

§13, M5 "Built so far": add "OIDC".

### docs/DOCUMENTATION_INDEX.md

Under "## Platform (`platform/`)":

```markdown
- [oidc.md](platform/oidc.md): single sign-on through an OpenID Provider: setting it up (Keycloak, Entra ID, Google, Dex), every setting, roles from claims, linking accounts, signing out, the way back in, troubleshooting
```

If the index lists service READMEs (it does not today), `services/web/README.md`: "the gateway:
ports, running it, every setting".

### .github/workflows/test.yml (the `platform-e2e` job)

Run the stack with Dex too (the two overrides merge; verified locally: 17 passed with both, the
13 skips are the caddy and egress-proxy profiles). Replace the three
`-f deploy/compose/e2e/webhooks.yml` command lines (start, logs, down) with:

```yaml
        docker compose -f deploy/compose/docker-compose.yml -f deploy/compose/e2e/webhooks.yml
        -f deploy/compose/e2e/oidc.yml up -d --build --wait
```

(and the same `-f deploy/compose/e2e/oidc.yml` in "Show the stack's logs" and "Stop the
stack"), and add to "Run the end-to-end tests":

```yaml
        FORKLIFT_E2E_DEX_URL: http://localhost:15556
```

The `web` job needs nothing new: PyJWT comes with `pip install -e ".[test]"`, the browser test
uses the Chromium it already installs, and the fake provider needs no network. The OIDC tests
add about 30 s to the job. The Compose README's "Test it" section (EDGE's/the lead's) could add:
"The single sign-on test needs Dex: start the stack with `e2e/oidc.yml` as well and set
`FORKLIFT_E2E_DEX_URL=http://localhost:15556`; without it the test is skipped."

## Env names for Compose and Helm

The gateway service/Deployment only (sweeper, dispatcher, publisher never sign anyone in):
every `FORKLIFT_OIDC_*` name above and `FORKLIFT_PASSWORD_SIGN_IN`, plus `FORKLIFT_PUBLIC_URL`
(the redirect URI is built from it; it must be the address people use). Mount the client secret
as a file and set `FORKLIFT_OIDC_CLIENT_SECRET_FILE`. Empty values count as unset, so a
pass-through of every name with `${VAR:-}` is safe. HELM's chart (`gateway.oidc` in
`values.yaml`, `forklift.gatewayOidcEnv`) already uses exactly these names
(`/etc/forklift/oidc/client-secret` for the secret file, `/etc/forklift/oidc-ca/ca.crt` for the
CA); `gateway.oidc.settings` passes the rest by name. Its NetworkPolicy must let the gateway
reach the provider (HELM has `networkPolicies.oidc`). The image's CMD now adds
`--logger-class forklift_web.access_log.AccessLog`: a chart or Compose file that overrides the
gateway's command should keep that flag.

## Verification

- **Full gateway suite**, final code, my runner `$S/oidc/w_test.py` (database
  `forklift_web_oidc`, `COVERAGE_FILE=$S/oidc/coverage.data`):
  `python $S/oidc/w_test.py --cov=forklift_web --cov-branch --cov-report=term-missing --cov-fail-under=100`
  - Python 3.12 (`$S/oidc-venv`: forklift-web editable + test extras, PyJWT 2.15.1, Playwright
    1.56.0 for the installed Chromium): **2246 passed, 100.00% line and branch** (8385
    statements, 1712 branches), 4 min 45 s.
  - Python 3.13 (a venv I made for this and deleted): **2246 passed, 100.00%**, 4 min 42 s.
  - (2033 before this round; the rest are mine, ~190, plus HELM's `create_worker_token` tests.)
- **Lint**: `black --check --line-length 99`, `isort --check-only --profile black
  --line-length 99`, `flake8` (the tree's `.flake8`) on `services/web/src` and `services/web/tests`,
  and on `deploy/compose/e2e/test_oidc.py`: clean.
- `forklift-web makemigrations core --check --dry-run`: no changes; the checked-in
  `contracts/openapi.json` is current (`test_the_checked_in_openapi_document_is_current`).
- **Real Dex end to end** (`-p forklift-oidc`; host ports 38080 gateway, 38090 store, 38556 Dex,
  38099 receiver; images `forklift-web:oidc` built from this tree with
  `docker build -f services/web/Dockerfile .`, and the existing `forklift-worker:local`; runner
  `$S/oidc/run_e2e_all.py` sets `FORKLIFT_E2E_ENV_FILE`, `FORKLIFT_E2E_DEX_URL`,
  `FORKLIFT_E2E_RECEIVER_URL`):
  - `deploy/compose/e2e/test_oidc.py`: 3 passed, six times in a row (it is re-runnable: later
    runs find the account by its subject);
  - all of `deploy/compose/e2e` with `webhooks.yml` + `oidc.yml` together, on the final image:
    **17 passed, 13 skipped** (the skips are the caddy and egress-proxy stacks' tests);
  - `docker exec … forklift-web check_oidc` printed Dex's endpoints, RS256, its key and the
    redirect URI `http://localhost:38080/accounts/oidc/callback/`;
  - the gateway's `docker logs` hold no `code=` after the runs (they did before the log fix).
- **Browser**: `tests/test_oidc_browser.py` (Chromium): the button, the fake provider's own
  sign-in form, back on the page the person was going to, the role from the group, no
  "Password" link; sign-out through the provider's end-session endpoint back to the sign-in
  page, with `id_token_hint` and the post-logout URI; the page without the password form.
- **Cleaned up**: my stack is `down -v`; the probe Dex container, `forklift-web:oidc` and the
  Dex image are removed; the 3.13 venv, the bcrypt venv (used once to make the two test
  password hashes) and the Dex binary copy are deleted. Kept: `$S/oidc-venv` (the only gateway
  venv with PyJWT; `$S/webvenv` does not have it, so the shared runner `$S/w_test.py` with
  `$S/webvenv` fails on `import jwt` until `pip install "PyJWT>=2.15.1,<3"` there) and my
  runners `$S/oidc/w_test.py`, `$S/oidc/manage.py`, `$S/oidc/run_e2e.py`,
  `$S/oidc/run_e2e_all.py`, `$S/oidc/e2e.env`.
