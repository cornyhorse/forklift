# Sign-in throttling

The gateway limits password guessing on its sign-in page. Wrong passwords are counted per
username and per client address; when either reaches its limit, signing in with that username,
or from that address, is refused for a while. API tokens are not throttled: they are random
256-bit values, which nobody can guess.

## What is counted

| Counter | Counts | Default limit |
|---|---|---|
| Username | Wrong passwords for one username, at sign-in and in the old-password field of "Change your password" | 5 (`sign_in_max_failures`) |
| Client address | Failed sign-ins from one address, whatever the username | 30 (`sign_in_ip_max_failures`) |

- A username is counted in any letter case and after Unicode (NFKC) normalisation, so `Alice`,
  `ALICE` and ` alice ` are one counter. It is counted the same way whether or not an account
  has it: the responses never tell which usernames exist.
- An IPv6 address is counted by its /64 network, which is what one subscriber usually has; an
  IPv4-mapped IPv6 address counts as the IPv4 address.
- Failures are counted for `sign_in_window_seconds` (default 900) from the first one; after
  that, counting starts again.
- An attempt is counted **when it starts**, before its password is checked (which takes a
  while, on purpose), and a right password gives its attempt back. So many attempts sent at
  the same moment get no more password checks than the limit: the ones beyond it are refused
  without being checked.
- A correct password clears its username's count and gives the address back the attempt it
  took. It never ends a lock that another attempt started meanwhile.

The attempt that reaches a limit locks the username or the address for `sign_in_lock_seconds`
(default 900); if its own password turns out to be right, that lock is lifted again. While it is
locked, an attempt is refused **before its password is checked**, so a right password gets
exactly the same answer as a wrong one: HTTP 429 with a `Retry-After` header, and the sign-in
page saying when to try again ("Too many failed sign-in attempts. Try again in 15 minutes, after
2026-10-10 14:32 UTC."). The message is the same for a locked username and a locked address.
When the lock ends, counting starts from zero.

People who are already signed in stay signed in when their username is locked; a lock only
refuses new password checks.

## Settings

The four limits are installation settings (Admin, Settings, or `PATCH /api/v1/admin/settings`):

| Setting | Default | Allowed |
|---|---|---|
| `sign_in_max_failures` | 5 | 1 to 1000 |
| `sign_in_ip_max_failures` | 30 | 1 to 100000 |
| `sign_in_window_seconds` | 900 | 60 to 86400 |
| `sign_in_lock_seconds` | 900 | 60 to 86400 |

The counters are rows in PostgreSQL, so every gateway process and replica shares them, and
concurrent attempts are all counted.

## Behind a reverse proxy

The client address is the connection's address unless `FORKLIFT_TRUSTED_PROXIES` says how many
reverse proxies in front of the gateway add themselves to `X-Forwarded-For` (counted from the
right, so a client cannot forge it). Behind a TLS proxy or an ingress, set it (usually to `1`):
otherwise every sign-in seems to come from the proxy, and 30 failures from anyone lock everyone
out until the lock ends. A port the proxy writes with the address (`203.0.113.5:5555`,
`[2001:db8::1]:443`, as Azure Application Gateway does) is ignored.

Many people behind one NAT or office proxy share an address; raise `sign_in_ip_max_failures`
if they lock each other out.

## For admins

- **A user's page** (Admin, Users, the user) shows an active lock on their username: until
  when, and how many wrong passwords caused it. **Unlock** clears it and the count at once.
- **The overview** shows how many sign-in locks are active, linking to their audit entries.
- **The API** lists active locks (usernames and addresses) and clears one:

  ```bash
  curl -H "Authorization: Bearer $TOKEN" https://forklift.example.org/api/v1/admin/sign-in-locks
  curl -X DELETE -H "Authorization: Bearer $TOKEN" \
      https://forklift.example.org/api/v1/admin/sign-in-locks/42
  ```

  Listing needs `admin:read`, clearing `admin:write`.

Anyone who knows a username can lock it by failing on purpose; the address limit slows that
down, and an admin can unlock the account. A person locked out can also wait for the lock to
end.

## Audit log and logs

| Event | Audit log | Logs (JSON) |
|---|---|---|
| A wrong password | `user.login_failed` with the username (sign-ins only) | INFO `Sign-in failed`: normalised username, address, counts |
| A lock starts (recorded once the attempt that started it has failed) | `account.sign_in_locked`: kind (`user` or `ip`), username, failures, `locked_until` | WARNING `Sign-in locked` |
| An attempt refused while locked | nothing | INFO `Sign-in refused while locked` |
| An admin clears a lock | `account.sign_in_unlocked`: kind, failures, `locked_until` | |

Passwords are never logged or audited. The retention sweeper (`sweep_retention`, the `sweeper`
service in Docker Compose) deletes counters whose window and lock have both ended; it does not
audit that housekeeping.
