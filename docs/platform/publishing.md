# Publishing to an s3 destination

A dataset can name an `s3` connection and a prefix as its destination. A successful run of it is
then published: its outputs are copied to `<connection prefix>/<destination prefix>/<job id>/`,
the data files first and `manifest.json` last, so a reader that trusts only complete manifests
never sees half a load. The copies are server-side, so the destination must be on the
installation's own store. Previews, schema checks and schema generation are never published,
and a table destination (`sql`) is written by the engine itself instead.

## The publishing status

When its worker completes a successful run, the job is not done yet: its result and files are
recorded and its status is **publishing** until a publisher has copied the outputs. Then it is
**succeeded**. So a client that sees `succeeded` can rely on the outputs being at the
destination, and a job never turns from succeeded to failed.

```
running ──complete──► publishing ──copied──► succeeded
                          │
                          └── store errors until publish_max_attempts, or another error ──► failed (TARGET_WRITE_FAILED)
```

- The job page says where it is publishing to and which attempt this is, and shows the store's
  last error while it is tried again. `GET /api/v1/jobs/{id}` has `publish_attempts` and
  `publish_error`.
- A publishing job cannot be cancelled (409): its run is done, and publishing ends on its own.
- Webhooks hear about the run once, when publishing has ended: `job.succeeded`, or `job.failed`
  with `TARGET_WRITE_FAILED`.
- A schedule treats a run that is still publishing as still in progress (the next slot is
  skipped), and retention does not delete the files of a publishing job.
- The files stay job artifacts either way, also when publishing failed.

## The publisher

`forklift-web publish` does the copying; in Docker Compose it is the `publisher` service. It is
a process of its own, not a step of the dispatcher, because one copy can take an hour and
schedules and webhook deliveries must not wait for it.

```bash
forklift-web publish                                    # one pass
forklift-web publish --every=5 --heartbeat=/tmp/publisher.alive
```

A pass publishes every run that is due, one after the other. It takes each with a **lease**:
the run is claimed with `SELECT ... FOR UPDATE SKIP LOCKED` and a token of the publisher's own,
for `publish_lease_seconds`, and the publisher renews the lease every third of that while it
copies. Several publishers can run; they never take the same run. Everything a publisher
changes on the run checks its token, so one that lost its lease (it was stalled for longer than
the lease, and another publisher took the run over) stops without changing the run.

- **A publisher that stops** (crash, restart, out of memory) leaves its lease to expire; the
  next pass resumes the run and copies everything again. Copies are idempotent: the same keys
  get the same content.
- **A store error** (the store or the destination did not answer, refused, throttled) releases
  the lease and tries again later: after 1 minute, then 5, then 30, then every 2 hours.
- **Any other error** fails the run at once with `TARGET_WRITE_FAILED`: for example the
  destination's secrets cannot be decrypted with `FORKLIFT_SECRETS_KEYS`, or the dataset no
  longer has an s3 destination. Messages never carry the text of unexpected errors.
- Every claim counts as an attempt, also the claim of a run whose publisher stopped. After
  `publish_max_attempts` attempts the run fails: "The run succeeded but publishing to '...' did
  not finish after 5 attempts: <the last reason>".

`--heartbeat` touches the file after each pass that did not fail and every few seconds while a
copy runs, so a health check on its age (Compose: under a minute) stays green during a long copy
and notices a publisher that keeps failing. A pass that fails is logged; the loop goes on, and a
single pass (no `--every`) exits with an error. SIGTERM stops the loop once the run being copied
is done: no further run is claimed, and the command exits (a stop that has to wait for a long
copy is cut short by the container's grace period, and the next publisher resumes that run once
its lease expires).

## Settings

| Installation setting | Default | What it does |
|---|---|---|
| `publish_lease_seconds` | 300 | How long a publisher holds a run without renewing its lease; a run whose publisher stopped is resumed this long after its last renewal |
| `publish_max_attempts` | 5 | Claims a run gets before it fails with `TARGET_WRITE_FAILED` |
