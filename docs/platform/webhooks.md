# Webhooks

A webhook tells another system when a job finishes: the gateway sends it a signed HTTPS `POST`
with the job's outcome, so a pipeline does not have to poll `GET /api/v1/jobs/{id}`. Webhooks
carry what happened to a job (its status, counts, error code and message, and links to the
API), never its data.

Every user can have webhooks (25 by default: the installation setting
`webhook_max_per_owner`), in the UI under **Webhooks** in the account area or
through `/api/v1/webhooks` (token scopes `webhooks:read` and `webhooks:write`, which every role
has). A webhook hears only about jobs its owner can see, and that is checked twice: when the
job finishes and again just before each delivery is sent.

## Which jobs a webhook hears about

| Scope | Jobs |
|---|---|
| `own_jobs` (the default) | The jobs its owner requested, in the UI or through their API tokens |
| `dataset` | Every job of one dataset (`dataset_id`), whoever started it, schedules included |
| `all_jobs` | Every job (admins only) |

It hears about the **events** it lists, any of `job.succeeded`, `job.failed` and
`job.cancelled`, for the **job kinds** it lists (`kinds`, by default only `run`; previews,
schema checks and schema generation are usually watched in the UI). A run of a dataset whose
outputs are published to an s3 destination finishes once publishing has: if publishing fails,
the webhook hears `job.failed` (`TARGET_WRITE_FAILED`), never `job.succeeded` first.

A delivery is queued in the same database transaction that finishes the job, so a job never
finishes without its deliveries being queued, and a delivery is never queued for a job that did
not finish. A delivery is not queued, or is *skipped* when it is due, if the webhook is
disabled, its owner was deactivated, or its owner may no longer see the job (for an `all_jobs`
webhook: is no longer an admin).

## What is sent

```http
POST /your/endpoint HTTP/1.1
Host: hooks.example.org
Content-Type: application/json
User-Agent: forklift-webhooks/0.1.0
Forklift-Event: job.failed
Forklift-Delivery: 5b0f8d1e-7c43-4f0e-9a39-2b6c1d9e0f11
Forklift-Signature: t=1760100000,v1=6f1c...e2a9
```

```json
{
  "id": "5b0f8d1e-7c43-4f0e-9a39-2b6c1d9e0f11",
  "event": "job.failed",
  "created_at": "2026-10-10T12:40:00.120000Z",
  "webhook": {"id": "c3e1...", "name": "pipeline alerts"},
  "job": {
    "id": "0a6d...",
    "kind": "run",
    "status": "failed",
    "dataset": {"id": "91f2...", "name": "people"},
    "schedule_id": null,
    "classification": "internal",
    "attempt": 1,
    "created_at": "2026-10-10T12:39:12.501000Z",
    "started_at": "2026-10-10T12:39:13.020000Z",
    "finished_at": "2026-10-10T12:39:59.874000Z",
    "counts": {"total_rows": 41, "valid_rows": 30, "invalid_rows": 11, "truncated_rows": 0},
    "error": {"code": "BAD_ROWS_THRESHOLD_EXCEEDED", "message": "11 of 41 rows were rejected ..."},
    "url": "https://forklift.example.org/api/v1/jobs/0a6d...",
    "artifacts_url": "https://forklift.example.org/api/v1/jobs/0a6d.../artifacts"
  }
}
```

- `id` is the delivery's id, also in `Forklift-Delivery`. It stays the same when a delivery is
  retried or sent again, so a receiver can ignore one it has already handled.
- `job.dataset` is null for jobs of uploads, `job.schedule_id` is set for runs a schedule
  started, `job.error` is null for successful jobs, and `job.counts` holds the counts the engine
  reported (`total_rows`, `valid_rows`, `invalid_rows`, `truncated_rows`, `rows_written` for
  table loads; empty when the job ended without a result). The error message is the engine's
  (at most 2000 characters), which never contains cell values.
- `url` and `artifacts_url` link to the API (the gateway's `FORKLIFT_PUBLIC_URL`; only the path
  where the deployment does not set it). Fetching them needs an API token, as always.
- A test event (`webhook.test`) has `"job": null`. **Send a test event** in the UI, or
  `POST /api/v1/webhooks/{id}/test` (which answers `202` with the pending delivery), queues one
  for the dispatcher: it is sent on its next pass, once (never retried), even while the webhook
  is disabled or backing off, and the delivery log shows how it went (the webhook's page follows
  it until it was sent). A webhook has at most one test event waiting at a time (`409
  test_pending` otherwise).

## Checking a delivery

Every delivery is signed with the webhook's **signing secret**, a random value starting with
`fkwh_` that is shown once, when the webhook is created or its secret is rotated (the gateway
keeps it encrypted). `Forklift-Signature` is `t=<unix seconds>,v1=<signature>`, where the
signature is the lower-case hex HMAC-SHA256, keyed with the secret (its UTF-8 bytes, prefix
included), of the timestamp, a `.` and the raw request body. Each attempt is signed when it is
sent, so `t` is the time of that attempt.

A receiver should check the signature against the raw body, before parsing it, with a
constant-time comparison, and refuse timestamps more than five minutes away from its own clock
(an old, recorded request cannot then be replayed):

```python
import hashlib
import hmac
import time

TOLERANCE_SECONDS = 300


def verify(secret: str, signature_header: str, body: bytes) -> bool:
    """Whether a delivery is genuine: ``body`` is the raw request body, unparsed."""
    try:
        fields = dict(part.split("=", 1) for part in signature_header.split(","))
        timestamp = int(fields["t"])
        signature = fields["v1"]
    except (KeyError, ValueError):
        return False
    if abs(time.time() - timestamp) > TOLERANCE_SECONDS:
        return False
    expected = hmac.new(
        secret.encode(), str(timestamp).encode() + b"." + body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)
```

Answer with any `2xx` status as soon as the delivery is checked, and do slow work afterwards:
the gateway waits at most 10 seconds for an answer. Rotating the secret takes effect at once for
every later delivery, retries included; to rotate without missing deliveries, let the receiver
accept both secrets until the new one is in place.

## Delivery, retries and disabling

The gateway never sends a webhook while it answers a request: the dispatcher
(`forklift-web dispatch`, the `dispatcher` service in Docker Compose) sends what is due on each
pass. A pass goes in rounds that take the oldest due delivery of each webhook in turn, so one
webhook's backlog or slow receiver cannot keep the others waiting, and it stops after 50
deliveries or 30 seconds. A delivery is **delivered** when the receiver answers `2xx`. Anything
else is a failed attempt: another status (a `3xx` too: redirects are not followed, so give the
webhook the URL it would redirect to), no answer within 10 seconds (looking the host up
included), a connection or TLS failure, or an address the gateway does not send to (below). The
receiver's answer is never stored; the delivery log keeps its status code and the gateway's own
description of what went wrong.

A failed attempt is retried after 1 minute, 5 minutes, 30 minutes, 2 hours, 6 hours and 12
hours; when the last retry fails too, the delivery has **failed**. Until the retry, nothing else
is sent to that webhook either (a circuit breaker): its other deliveries, and new ones, wait with
it. While the webhook's attempts keep failing, the waits grow with the number of failed attempts
in a row, whichever delivery is tried, so a receiver that is down costs one attempt per step
however many deliveries wait for it. The first attempt that gets through ends the wait (a test
event that gets through too), and everything that waited is sent.

Once `webhook_disable_after_failures` attempts in a row have failed (an installation setting, 10
by default, which with the growing waits is about two and a half days of a receiver that never
answers), the webhook is disabled and the audit log says so; the deliveries still waiting are
then skipped. Its owner can enable it again (which starts the count anew) and send deliveries
again from the delivery log (**Redeliver**, or
`POST /api/v1/webhooks/{id}/deliveries/{delivery_id}/redeliver`: queued at once, with the same
id and all its retries, and sent as soon as the webhook is not backing off). Test events do not
count towards backing off or disabling.

Admins see every webhook with its failure count (Admin, **Webhooks**, or
`GET /api/v1/admin/webhooks`, which leaves out query strings since they may hold a receiver's
credentials) and can disable any of them (`POST /api/v1/admin/webhooks/{id}/disable`).

## Where webhooks may point

The gateway sends webhooks from inside the installation's network, so it refuses URLs that
could reach what only the installation should reach:

- URLs must be `https://` with a host name (or a public IP address), without a user name or
  password and without a fragment. The form is checked when a webhook is created or changed.
- **Each time** a delivery is sent, the host is resolved (within the delivery's 10 seconds), and
  every address it resolves to must be publicly routable. Loopback, private (10/8, 172.16/12, 192.168/16, fc00::/7), link-local
  (169.254/16, which includes the cloud metadata address 169.254.169.254, and fe80::/10), shared
  (CGNAT, 100.64/10), multicast, reserved, documentation and unspecified addresses are refused,
  and so are IPv6 addresses that embed such an IPv4 address (IPv4-mapped `::ffff:a.b.c.d`,
  IPv4-compatible `::a.b.c.d`, NAT64 `64:ff9b::/96`).
- The gateway then connects to exactly an address it checked (there is no second lookup, so a
  DNS answer that changes between the check and the connection cannot redirect the request),
  verifies the TLS certificate against the host name (which it also sends as SNI and as the
  `Host` header) and does not follow redirects.

Two deployment settings (environment variables of the gateway and the dispatcher) relax this
for receivers on an internal network:

| Variable | Default | What it does |
|---|---|---|
| `FORKLIFT_WEBHOOK_ALLOWED_HOSTS` | (none) | Comma-separated host names (or IP addresses) that may resolve to any address, private ones included |
| `FORKLIFT_WEBHOOK_ALLOW_HTTP` | `false` | Also allow `http://` URLs (for every host); keep it off outside development |
| `FORKLIFT_PUBLIC_URL` | (none) | Where people reach the gateway, e.g. `https://forklift.example.org`; payloads link to the API under it |

Certificates are checked against the system's CA certificates; point `SSL_CERT_FILE` (or
`SSL_CERT_DIR`) at a bundle that includes a private CA for internal receivers. The gateway does
not use an HTTP proxy for webhooks: the dispatcher needs a direct route out (in Docker Compose it
is on the `public` network for that).

## Retention

Deliveries are kept as long as the job records they belong to: the sweeper deletes them once
the `job_records` lifetime of their job's dataset and classification has passed since they were
created (test deliveries follow the installation's `job_records`), and never while they are
still due to be sent. With no `job_records` lifetime set, they are kept. Deleting a webhook
deletes its delivery log.

## API

| Endpoint | What it does |
|---|---|
| `GET /api/v1/webhooks`, `POST /api/v1/webhooks` | Your webhooks; create one (the answer has its `secret`, the only time it is shown) |
| `GET`, `PATCH`, `DELETE /api/v1/webhooks/{id}` | Read, change (`active: false` disables it, `true` enables it again), delete |
| `POST /api/v1/webhooks/{id}/rotate-secret` | A new signing secret (shown in the answer only) |
| `POST /api/v1/webhooks/{id}/test` | Queue a `webhook.test` event for the dispatcher (`202`, with the pending delivery) |
| `GET /api/v1/webhooks/{id}/deliveries` | The delivery log, newest first (`?status=pending\|delivered\|failed\|skipped`) |
| `POST /api/v1/webhooks/{id}/deliveries/{delivery_id}/redeliver` | Send a delivery again |
| `GET /api/v1/admin/webhooks`, `POST /api/v1/admin/webhooks/{id}/disable` | Every user's webhooks (admins); disable one |

```bash
curl -s https://forklift.example.org/api/v1/webhooks \
  -H "Authorization: Bearer $FORKLIFT_TOKEN" -H "Content-Type: application/json" \
  -d '{"name": "pipeline alerts", "url": "https://hooks.example.org/forklift",
       "events": ["job.failed", "job.succeeded"], "scope": "dataset",
       "dataset_id": "91f2..."}'
```
