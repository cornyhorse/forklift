# REVIEW-OIDC: independent security review of the gateway's OIDC sign-in (commit 8e23f3b)

Reviewer: agent REVIEW-OIDC. Scope: every changed line of `git show 8e23f3b` (services/web/**,
contracts/openapi.json, deploy/compose/e2e/oidc.yml + test_oidc.py, docs/platform/oidc.md,
services/web/README.md), read against the OIDC report (`round4/OIDC.md`). Nothing in the
repository was edited. Everything I wrote is under `$S/review-oidc/`.

**How to reproduce everything below** (22 tests, ~17 s; own database
`forklift_web_review_oidc`, own coverage file; uses the gateway's own fake provider
`tests/oidc_support.py` and conftest fixtures through `$S/review-oidc/tests/conftest.py`):

```bash
S=/tmp/claude-0/-home-user-forklift/32052cf1-7a9c-57b5-940d-29385d406628/scratchpad
cd /home/user/forklift/services/web
$S/webvenv/bin/python $S/review-oidc/w_test.py $S/review-oidc/tests/test_review_oidc.py -s -v
# -> 22 passed. Each finding's test asserts the CURRENT (defective) behaviour, so "passed"
#    means "reproduced". Full output: $S/review-oidc/run_final.log
```

The existing suites I re-ran in `$S/webvenv` (Python 3.12, PyJWT 2.15.1): `test_oidc_flow.py`
(58 passed), `test_oidc_provider/id_token/config.py`, `test_access_log.py`, `test_deadline.py`,
`test_webhook_client.py` with `-W error::ResourceWarning -W
error::pytest.PytestUnraisableExceptionWarning` (208 passed: no leaked sockets after the
deadline-socket refactor), `test_oidc_admin.py` + the OpenAPI/session/sweep tests (10 passed).
No Dex stack was started (the e2e is reviewed by reading; see "Dex e2e").

## Summary

| # | Severity | Finding |
|---|---|---|
| F1 | **blocking** | Email linking matches with PostgreSQL `UPPER()`: a provider-verified `admın@…` (dotless ı), `boſs@…` (long s) or `…@kıngsbury.example` (IDN look-alike) links to, and signs the attacker into, the local account `admin@…` / `boss@…` / `…@kingsbury.example` (admins included) |
| F2 | should-fix | `*_GROUPS` lists are split on white space as well as commas (docs and README say comma-separated): `ADMIN_GROUPS="authentik Admins"` makes everyone in any group called `Admins` (or `authentik`) an admin, and the real group never matches |
| F3 | should-fix | A visitor who starts single sign-on and then signs in with a **password** gets a 10-minute session (cookie max-age 600, the sweeper deletes it) instead of `FORKLIFT_SESSION_SECONDS`; typical in the documented break-glass path |
| F4 | should-fix | The "known state" gate does not stop anonymous audit/DB writes: `GET /accounts/oidc/login/` hands anyone a state, `?error=` then audits a refusal (2 requests per audit entry, no throttle); and every made-up callback creates an 8-hour session row |
| F5 | should-fix | No negative caching of discovery: while the provider is down or slow, every anonymous `GET /accounts/oidc/login/` makes a fresh outbound fetch (5 s connect + 10 s + unbounded DNS) in a gunicorn thread; 16 such requests occupy the whole gateway, including the API that the docs name as the way back in |
| F6 | should-fix | Role mapping ignores `email_verified`; the docs suggest mapping single addresses with the `email` claim: an unverified `email` grants admin |
| F7 | should-fix (docs) | Roles are synchronised only at a browser sign-in: an API token keeps the admin role (and the account stays active) after the provider removed the person from the admin group or deleted them; not documented as a residual risk |
| N1-N8 | nit | bad issuer port passes start-up; reload of a used callback shows an error to a signed-in person; NUL in `given_name` is a 500 (unaudited); extra untrusted `aud` values accepted; the ID-token problem the page promises "in the log" is not logged; DNS not bounded / docs say "10 seconds in all"; custom gunicorn log formats and the ingress log bypass the redaction; doc formatting |

Everything else I checked holds; see "Checked and sound" at the end.

---

## F1 (blocking): Unicode case-folding collision in email linking -> account takeover

**Where:** `services/web/src/forklift_web/services/oidc.py:401`
(`User.objects.select_for_update().filter(email__iexact=email)`), and `:519` (`_update` then
copies the attacker's address onto the victim's account). Docs: `docs/platform/oidc.md:134`
("links ... to the one account with that address, in any letter case").

**What happens.** Django's `iexact` on PostgreSQL is `UPPER(email) = UPPER(%s)`. With the
official image's `en_US.utf8` collation, `UPPER()` maps U+0131 (dotless ı) to `I` and U+017F
(long s) to `S`, so different addresses compare equal:

```
$ docker exec forklift-test-services-postgres-1 psql -U forklift_admin -d postgres -At \
    -c "select upper('admın@kıngsbury.example') = upper('admin@kingsbury.example'), upper('ſ'), upper('ı');"
t|S|I
```

With `FORKLIFT_OIDC_LINK_VERIFIED_EMAIL=true` (the documented migration path: "turn it on
while people sign in for the first time"), a person whose provider-verified address is
`admın@corp.example`, `boſs@corp.example` or `admin@kıngsbury.example` (an IDN domain the
attacker owns and receives the verification mail on) is linked to the local account
`admin@corp.example` / `boss@corp.example` / `admin@kingsbury.example` and signed in as it.
Any provider that lets a user hold a verified address of their choosing produces this
(self-registration with email verification, or a user-editable email that is re-verified:
Keycloak, Authentik, Auth0 database users, Okta self-service). The targets are exactly the
accounts that are not linked yet: local accounts, including the bootstrap admin and
break-glass admins (`email_already_linked` protects only accounts already linked).

**Reproduction** (`test_f1_unicode_email_links_to_another_account`, 3 parameter sets, and
`test_f1_with_role_sync_the_account_is_still_taken`):

```python
sso(LINK_VERIFIED_EMAIL="true", SYNC_ROLE="false")
boss = make_user(Role.ADMIN, username="boss", email="admin@corp.example")
mallory = idp.person("mallory", email="admın@corp.example", groups=["forklift-viewers"])
sign_in(client, idp, mallory)
assert me(client)["username"] == "boss" and me(client)["role"] == "admin"
```

Output:

```
signed in as: {'id': 1, 'username': 'boss', 'email': 'admın@corp.example', ..., 'role': 'admin', 'may_view_raw_rows': True, ...}
boss.email after sign-in: admın@corp.example
signed in as: {'id': 2, 'username': 'boss', 'email': 'boſs@corp.example', ..., 'role': 'admin', ...}
signed in as: {'id': 3, 'username': 'boss', 'email': 'admin@kıngsbury.example', ..., 'role': 'admin', ...}
eve signed in as: {'id': 4, 'username': 'maria', 'email': 'marıa@corp.example', 'role': 'viewer', 'can_view_raw_rows': True, ...}
victim now: viewer True marıa@corp.example
```

(Control in the second test: a Cyrillic `а` does not collide and gets its own account.)

**Why it matters.** Account takeover. With `SYNC_ROLE=false` (the docs' Google example, and
common during migration) the attacker gets the victim's admin role. With sync on, the attacker
still owns the victim's account: its jobs, datasets and webhooks, its `can_view_raw_rows`
(not touched by sync), its API-token page; the victim is demoted to the attacker's role, and
the victim's stored email is replaced by the attacker's, so the attacker also receives anything
mailed to the account and the real owner's later SSO sign-in is refused
(`email_already_linked`).

**Suggested fix.** Link only on an exact ASCII-case-insensitive match: refuse linking unless
`email.isascii()`, and compare in Python after the query (`[u for u in found if
u.email.isascii() and u.email.lower() == email.lower()]`); do not use `casefold()` (it maps
`ſ` to `s` too). Tests with `ı`, `ſ`, `K` (U+212A Kelvin) and an IDN domain. In the docs,
"in any letter case" -> "ignoring the case of ASCII letters; addresses with other characters
never link".

## F2 (should-fix): group lists are split on spaces; docs say comma-separated

**Where:** `services/web/src/forklift_web/oidc/config.py:121-123` (`_list` splits on
`[\s,]+`), used for every `*_GROUPS` at `:209`. Docs: `docs/platform/oidc.md:85`
("comma-separated"), `services/web/README.md:100` ("Comma-separated values of the role claim").

**Reproduction** (`test_f2_a_group_name_with_a_space_maps_its_words`,
`test_f2_a_comma_list_with_spaces_cannot_be_written`):

```
FORKLIFT_OIDC_ADMIN_GROUPS="authentik Admins"
role_values: (('authentik', 'admin'), ('Admins', 'admin'), ('forklift-authors', 'author'), ...)
mallory (groups ["Admins"]): {..., 'role': 'admin', ...}
alice (groups ["authentik Admins"], the real group): 403, not signed in

FORKLIFT_OIDC_ADMIN_GROUPS="Forklift Admins,Platform Team"
role_values: (('Forklift', 'admin'), ('Admins', 'admin'), ('Platform', 'admin'), ('Team', 'admin'))
```

**Why it matters.** Group and role names with spaces are normal in Okta, Authentik (its own
admin group is `authentik Admins`), Keycloak (group names, full paths) and LDAP-backed Dex.
An operator following the docs ("comma-separated") silently maps each *word* to the role:
everyone in some other application's `Admins` group, or in `All Staff` -> `All` and `Staff`,
gets the role, and the intended group never matches. That is a privilege escalation by
configuration, and nothing at start-up says so (the duplicate-value check does not catch it).

**Suggested fix.** Split `*_GROUPS` on commas only and strip each item (keep `_list` for
scopes, whose docs do say "spaces or commas"); a test with a value containing a space. If
white-space splitting is kept deliberately, document it and refuse at start-up any value that
the operator obviously meant as one (not possible to detect reliably, so prefer commas only).

## F3 (should-fix): password sign-in after a started SSO lasts 10 minutes

**Where:** `services/web/src/forklift_web/services/oidc.py:182`
(`request.session.set_expiry(FLOW_SECONDS)` on the visitor session); only the OIDC callback
resets it (`ui/views/oidc.py:70`). Django's `login()` keeps the session data on `cycle_key()`,
including `_session_expiry`, and `SignInView` (password) never resets it.

**Reproduction** (`test_f3_password_sign_in_after_a_started_sso_lasts_10_minutes`; control
`test_f3_control_without_a_started_sso` gets 28800):

```python
start(client)   # clicked "Sign in with ...", came back, used the password instead
client.post("/accounts/login/", {"username": user.username, "password": PASSWORD})
```

```
expiry age: 600 expected: 28800
expire_date - now: 0:09:59.993079
cookie max-age: 600
retention.sweep(now + 601 s).sessions == 1  ->  /api/v1/me answers 401 (signed out)
```

**Why it matters.** Authenticated sessions are cut to 10 minutes (from the last session
write, not from activity: `SESSION_SAVE_EVERY_REQUEST` is off), and the sweeper deletes them.
The realistic trigger is the docs' own recovery path: someone clicks single sign-on, the
provider is down or the mapping refuses them (`no_role`), and they fall back to a break-glass
password account, which then signs out after 10 minutes. The OIDC report says "`login()`
resets it to `FORKLIFT_SESSION_SECONDS`"; only the OIDC callback does.

**Suggested fix.** Reset the expiry for every sign-in: in the `user_logged_in` receiver
(`core/signals.py`) call `request.session.set_expiry(None)` and drop `FLOWS_KEY` (then the
callback's own two lines can go). Test: start SSO, then password sign-in -> SESSION_COOKIE_AGE.

## F4 (should-fix): anonymous clients write audit entries and session rows at will

**Where:** `services/web/src/forklift_web/services/oidc.py:211-220` (`_take_flow` always
assigns `request.session[FLOWS_KEY]`), `:252-279` (`provider_error` / `no_code` refusals are
audited), `docs/platform/oidc.md:316` and `round4/OIDC.md` ("Refusals before a known state are
logged, not audited (anyone can call the callback with made-up values; auditing those would let
anyone fill the audit log)").

**Reproduction** (`test_f4_anyone_can_write_refusals_into_the_audit_log`,
`test_f4_a_made_up_callback_creates_an_eight_hour_session_row`):

```python
anonymous = Client()
state = parse_qs(urlsplit(start(anonymous)).query)["state"]   # GET /accounts/oidc/login/, no redirect followed
anonymous.get("/accounts/oidc/callback/", {"state": state, "error": "access_denied", "error_description": "x" * 200})
```

```
audit entries written anonymously: 25 sessions: 25          (25 loops, 50 requests)
session rows from 30 made-up callbacks: 30 lifetime (h): {8}   (no cookie, no state, no audit)
```

**Why it matters.** The design's stated protection is illusory: a "known state" costs one
unauthenticated GET, so anyone can fill `user.oidc_login_refused` (plus a 200-character
attacker string in the JSON log) without the throttling that password failures get. Worse for
the database: `_take_flow` writes the session even when nothing was found, so each request to
`/accounts/oidc/callback/?state=anything` from a cookie-less client inserts a session row that
lives the default 8 hours (the 10-minute expiry is only set by `begin()`).

**Suggested fix.** In `_take_flow`, write the session only when it already has `FLOWS_KEY`
(or when a flow was popped), so a made-up callback creates nothing. Log, do not audit, the
refusals that happen before the code exchange (`provider_error`, `no_code`: nobody's identity
is involved yet either), or throttle them per client address with the existing sign-in
throttle. Update the docs' audit table.

## F5 (should-fix): every sign-in start retries a failing provider (no negative cache)

**Where:** `services/web/src/forklift_web/oidc/provider.py:208-216` (`metadata()` caches only
successes), called by `begin()` for every anonymous `GET /accounts/oidc/login/`
(`services/oidc.py:160`) and by sign-out (`provider_logout_url`); `provider.py:152`
(`connection.connect()`: per-address 5 s connect, DNS unbounded).

**Reproduction** (`test_f5_every_start_refetches_a_failing_discovery_document`,
`test_f5_a_slow_failing_provider_costs_every_start_the_full_wait`):

```
discovery fetches for 10 starts: 10          (provider answering 503)
4 anonymous starts: 8.6 s, 4 discovery fetches   (provider slow (2 s) and failing)
3 starts, discovery slow but up: 1 fetch     (control: successes are cached)
```

**Why it matters.** When the provider is unreachable (blackholed address: 5 s per connect
attempt, more with several A/AAAA records; slow DNS: no bound at all), each start holds a
gunicorn thread for the whole wait. The image runs 2 workers x 8 threads: about 3 anonymous
requests per second (or people repeatedly clicking the button, or a monitoring probe of the
sign-in page's link) keep all 16 threads busy, and `/api/v1` stops answering. The docs name
API tokens as the way back in "whatever happens to single sign-on".

**Suggested fix.** Remember a failed discovery for a short time (e.g. 30 s) and re-raise it
without a network call; let only one thread fetch while others fail fast (the key set already
has this pattern). Bound the DNS lookup like `webhook_client._lookup` does (thread + join on
the deadline). Optionally skip the provider fetch on sign-out when the cache is cold.

## F6 (should-fix): an unverified email grants a role

**Where:** `services/web/src/forklift_web/services/oidc.py:323-330` (`claim_values`) and
`oidc/config.py:106` (`role_for`): any claim, `email` included, is used as is.
Docs: `docs/platform/oidc.md:256-257` ("Map the Workspace domain (the `hd` claim) ... or map
single addresses with the `email` claim").

**Reproduction** (`test_f6_an_unverified_email_grants_a_role`):

```python
sso(ROLE_CLAIM="email", ADMIN_GROUPS="boss@example.org")
mallory = idp.person("mallory", email="boss@example.org", email_verified=False)
sign_in(client, idp, mallory)    # -> {'username': 'user-mallory', 'email': '', 'role': 'admin', ...}
```

**Why it matters.** The code itself refuses to copy or link an unverified address, but grants
roles from one. With Google Workspace (Internal) the address is always verified, so the
documented example is safe there; the advice reads as generic, and with any provider where
users set their own email (Keycloak, Authentik, Auth0, Okta self-service) it hands out admin.

**Suggested fix.** When the role claim is `email`, use it only when `email_verified` is JSON
`true` (or refuse `FORKLIFT_OIDC_ROLE_CLAIM=email` and offer an "admin emails" setting that
requires verification); at least warn in the docs that the role claim must be one the person
cannot set themselves (also true for user-editable attributes mapped into tokens).

## F7 (should-fix, docs): API tokens keep a role the provider took away

**Where:** design: roles are applied only in `_sign_in` (`services/oidc.py:383-385`);
`accounts.authenticate_api_token` checks only `is_active`. Docs: `docs/platform/oidc.md:116-123`
(mentions open sessions, not tokens); `round4/OIDC.md` "Residual risks" (same).

**Reproduction** (`test_f7_an_api_token_keeps_an_admin_role_the_provider_took_away`):
an SSO admin creates an `admin:read admin:write` token, signs out, is removed from
`forklift-admins` and then deleted at the provider; `GET /api/v1/admin/users` with the token
-> `200`; the account is still `admin` and active.

**Why it matters.** With single sign-on, off-boarding at the provider is what people expect to
cut access. Here it does nothing for API tokens (which by default never expire unless the
installation sets `token_max_days`) and for open sessions; an ex-admin keeps admin over the API
indefinitely, as long as they never sign in through the browser again.

**Suggested fix.** Document it in "Roles" and in the design's residual risks: removing someone
at the provider does not touch Forklift; deactivate the account (that stops tokens and
sessions at once) and consider `token_max_days` when single sign-on is on. Possible code
follow-up for the lead: an optional "an SSO account's tokens stop working N days after its last
SSO sign-in".

---

## Nits

**N1. An issuer with an invalid port passes start-up** (`oidc/config.py:141-165`, `_issuer`
never touches `parts.port`): `FORKLIFT_OIDC_ISSUER=https://idp.example:99999/realm` loads;
every sign-in then fails as `provider_misconfigured`. Fix: read `parts.port` in a `try` and
refuse. Repro: `test_nit_an_issuer_with_a_bad_port_passes_start_up`.

**N2. Reload / Back on the callback after a successful sign-in shows "This sign-in could not
be finished" (400) to a person who is signed in** (`ui/views/oidc.py:60-66`). Fix: when the
state is unknown and `request.user.is_authenticated`, redirect to `LOGIN_REDIRECT_URL`.
Repro: `test_nit_reload_after_success_shows_an_error_to_a_signed_in_person` (`reload: 400
signed in: {'username': 'user-alice', ...}`).

**N3. A NUL character in a copied claim is an unhandled 500** (`services/oidc.py:333-343`,
`_text` / `verified_email`): PostgreSQL refuses `\x00`, the callback answers 500 and nothing is
audited (`status: 500`, no `user.oidc_login_refused`). Users can often edit their own names at
the provider. Fix: strip control characters (or refuse with a reason). Repro:
`test_nit_a_nul_in_a_name_claim_is_a_500`.

**N4. Extra audiences are accepted** (`oidc/id_token.py:130-137`): `aud: [client_id,
"some-other-relying-party"]` with `azp == client_id` signs in. OpenID Connect Core §3.1.3.7
step 3 says to reject a token that "contains additional audiences not trusted by the Client".
Low risk (the token comes from the token endpoint over TLS), so either reject or note the
deviation. Repro: `test_nit_extra_untrusted_audiences_are_accepted`.

**N5. The ID-token problem is not logged although the page says it is**
(`services/oidc.py:202-208, 289-294`): the page says "an admin finds the details in the
gateway's log"; the log gets `{"reason": "unknown_key", "issuer": ...}` only, not the kid /
PyJWT message the person saw. Repro: `test_nit_the_id_token_problem_is_not_in_the_log` (log
records: `('Single sign-on refused', 'unknown_key')`). Fix: add `problem` to the log record
(it holds no token content).

**N6. DNS is not bounded, and the docs say "10 seconds in all"** (`oidc/provider.py:139-153`;
`docs/platform/oidc.md:41-42`): `HTTPSConnection.connect()` resolves with the system resolver
(no timeout) and tries each address for 5 s; the webhook client bounds DNS with a thread
(`webhook_client._lookup`). The OIDC report lists this as a residual risk; the operator docs do
not. Fix with F5, or reword the docs.

**N7. Redaction gaps outside the default format** (`access_log.py:27-38`): only atoms `r`, `q`,
`f` are rewritten. A custom `--access-logformat` with `%({referer}i)s`, `%({raw_uri}e)s` or
`%({location}o)s` (the login redirect's `Location` holds `state`, `nonce`) bypasses it, and
gunicorn's error log writes `Error handling request <uri>` with the query
(`gunicorn/workers/base.py:264-266`) if an exception escapes the WSGI app. The ingress in front
(ingress-nginx's default format logs `$request` with the query) will hold the code and state;
Caddy in `deploy/compose/caddy/Caddyfile` has no per-site `log`, so it does not. Fix: say in
`docs/platform/oidc.md:319` that "neither" means the gateway's own logs, and that a proxy's
access log should drop the callback's query.

**N8. Docs details**: `docs/platform/oidc.md:122` is one ~170-character line (the rest of the
file wraps at 99); `:316` "nothing (anyone can make one up)" is true only for unknown states
(see F4); `:134` "in any letter case" (see F1); `:85` / README `:100` "comma-separated" (see F2).

---

## Checked and sound

**Flow security**
- `state` (32 bytes), `nonce` (32 bytes), PKCE verifier (48 bytes -> 64 chars, S256 challenge
  computed correctly) from `secrets`; kept only server side (DB sessions); single use (popped
  before anything else), 10-minute flows, at most 5 per session.
- Login CSRF: a callback with another browser's state is refused before any token request
  (`test_sound_an_attackers_callback_does_not_log_the_victim_in`, also with flows of the
  victim's own in the session); a code injected with the victim's state fails PKCE at the
  provider (existing test).
- Session fixation: `login()` cycles the key for the visitor session; the old row is deleted
  (`test_sound_session_key_rotates_at_sign_in`). Signed-in people are redirected away from the
  start view, so `set_expiry(600)` never hits an authenticated session through that view (F3
  is the password path).
- `next`: validated at the start with `url_has_allowed_host_and_scheme` (host of the request,
  https required on https), stored in the flow, used as stored; the existing test covers
  `https://evil.example/`.
- Callback: provider `error` values shown only from the RFC list; `error_description` logged
  (truncated), never shown; missing code refused; duplicate params take Django's last value
  (no ambiguity in what is checked vs used). Concurrent callbacks with one state both proceed
  only to the code exchange, where the provider's single-use code stops the second.
- Mix-up: one provider only; `iss` in the authorization response is not needed.
- Logout: POST + CSRF (Django's `LogoutView`), `post_logout_redirect_uri` is fixed (the login
  page), `end_session_endpoint` must pass the same https/host check as every endpoint, no
  inline script (meta refresh + link), CSP unchanged, the page is `no-store`.

**Token validation (PyJWT 2.15.1 source read)**
- Algorithm allow-list: asymmetric only, intersected with the provider's list; `none`/HMAC
  refused before key lookup; `PyJWK(key, algorithm=alg)` binds the key type and curve
  (`ECAlgorithm._validate_curve`), and PyJWT 2.15 additionally refuses a header `alg` that
  differs from the PyJWK's; key `alg`/`use` checked; RSA >= 2048 enforced
  (`enforce_minimum_key_length`).
- `kid` must be a string (PyJWT) and match; no kid with several keys refused; `jku`/`x5u`/`jwk`
  headers never consulted (keys only from `jwks_uri`).
- `iss` exact (PyJWT >= 2.10.1 fixed the partial-match bug), `aud` via PyJWT (string or list of
  strings), `azp` required with several audiences and must equal the client id (N4 aside);
  `exp`, `iat` required, `nbf` checked, 60 s leeway; `sub` 1-255 chars; nonce required and
  compared in constant time. `at_hash` not checked: acceptable, the token comes from the back
  channel.
- JWKS: cached 1 h, refetched on an unknown kid at most once a minute, one fetch at a time;
  tokens only come from the token endpoint, so no outsider can force refetches. UserInfo `sub`
  must equal the token's; ID-token claims win over UserInfo's; signed UserInfo refused.
- Back channel: https only (http only with the switch, checked for every endpoint the discovery
  names), TLS verified with hostname (system CAs or only `FORKLIFT_OIDC_CA_FILE`), handshake
  bounded (CPython's handshake timeout is a deadline), 10 s deadline on the exchange after
  connect, 1 MiB (Content-Length and streamed), no redirects, no proxy. Discovery `issuer`
  compared exactly.
- `client_secret_basic`: id and secret `quote(..., safe="")`-encoded before Base64 (RFC 6749
  §2.3.1; `%20` instead of `+` decodes the same); `client_secret_post` in the form.

**Accounts and roles**
- `(issuer, subject)` unique constraint (migration 0007 matches the model); never found by
  username; username collisions refused case-insensitively, and `UPPER()` makes that refusal
  Unicode-aware too (`Admin`, `ADMIN`, `admın` all refused against `admin`:
  `test_sound_username_collision_is_refused_case_and_unicode`); created accounts have unusable
  passwords; inactive and service accounts refused; a refused link is rolled back (audited
  after the rollback); unverified emails never linked or copied; `email_verified` must be JSON
  `true`.
- Claim shapes: string, list of strings (non-strings dropped), dotted paths, missing -> no
  values; most powerful role wins; `DEFAULT_ROLE=admin` refused; sync is authoritative (the
  last-admin demotion is a documented decision with a documented way back in);
  `can_view_raw_rows` never touched by sync.
- Audit entries carry method/issuer/subject/reason, never a code, token, state, nonce or
  secret; role changes and links by `system:oidc`; admin unlink audited inside its
  transaction; the identities API/UI need `admin:read` / `admin:write`.

**Secrets in logs, pages, errors**
- Access log: the callback's query (with and without the trailing slash) and a `Referer`
  naming it are redacted in the default format, on both ports (one logger class per process);
  the JSON formatter writes any `HttpRequest` extra as method + path; callback pages send
  `Referrer-Policy: no-referrer`; `OidcConfig.client_secret` is `repr=False` (so settings
  dumps and Django's debug page do not show it); `ProviderError` details contain URLs and
  error codes only; `check_oidc` prints endpoints, algorithms, key ids/types, the mapping and
  the redirect URI, never the secret; no `ADMINS`, so no error mails with request data.

**Other changes**
- Deadline socket refactor: the webhook client still connects to the checked, pinned address,
  wraps TLS with `server_hostname=target.host`, bounds DNS with its own thread and the overall
  deadline; the only behavioural change (the real socket closes when the last file over it
  closes) leaks nothing (ResourceWarning-as-error run clean).
- Sweeper: deletes only `expire_date <= now` sessions; signed-in sessions stay (existing test).
- Settings validation fails closed: unknown `FORKLIFT_OIDC_*` names, any OIDC variable without
  the issuer, bad booleans, HMAC/`none` algorithms, missing/duplicate secrets, unreadable CA,
  `none` default role without mappings, `FORKLIFT_PASSWORD_SIGN_IN=false` without OIDC
  (N1 aside). `FORKLIFT_PASSWORD_SIGN_IN=false` really disables the only password path (the
  API has none); the password page refuses accounts without a password.
- CSP: unchanged; the button is a link; no inline script anywhere new.

**Docs vs code** (besides F1, F2, F6, F7, N6-N8): the settings tables in
`docs/platform/oidc.md` and `services/web/README.md` list every `FORKLIFT_*` name the gateway
reads (checked mechanically against `src/`), with matching defaults; the start-up refusal list
matches `config.py`.

**Dex e2e** (`deploy/compose/e2e/oidc.yml`, `test_oidc.py`, read, not run): a real
authorization-code flow through Dex's own login form with PKCE and `client_secret_basic`, the
`forklift-authors` group -> Author (role mapping proven for one role), an unmapped group
refused and audited, a callback that works once (also in a second browser), the identity
visible over the admin API. Not covered there (covered only by the fake provider): admin
mapping, role changes on a later sign-in, email linking, provider logout (Dex has no
end-session endpoint), https to the provider.

## Cleanup

No containers, images or venvs were created. Scratch files: `$S/review-oidc/` (runner, the
22 reproduction tests, a fixture bridge `conftest.py`, run logs). pytest-django dropped its
`test_forklift_web_review_oidc` database at the end of each run.
