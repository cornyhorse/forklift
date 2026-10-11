"""The service layer: every behaviour of the gateway, for the API and the HTML views alike.

Each public function takes the acting :class:`forklift_web.policy.Actor` first, checks the
permission policy, does the work and writes the audit log; it raises the errors of
:mod:`forklift_web.errors`. Build an actor for a request with
``forklift_web.api.auth.actor_for_request(request)`` (session) or let the API do it (tokens).

==================  ===================================================================
``accounts``        users and roles (admin), API tokens (own and, for admins, anyone's)
``workers``         worker tokens and the workers seen leasing
``connections``     s3 / localfs / sql connections with write-only secrets, connection tests
``schemas``         schemas and immutable versions
``datasets``        datasets (source, schema version, destination, classification)
``schedules``       datasets that run on a timer (cron in a time zone), the dispatcher pass
``uploads``         presigned (multipart) uploads, completed by HEAD
``jobs``            enqueue with idempotency keys, validate_schema, events, cancel
``artifacts``       artifacts and audited presigned downloads
``queue``           the worker side: lease, heartbeat, presign, complete, lease expiry
``retention``       policies per installation / classification / dataset, the sweeper
``installation``    admin-set installation settings (stage_max_bytes, limits, ...)
``audit``           the audit log
==================  ===================================================================
"""
