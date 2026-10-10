# forklift-worker

The worker half of the forklift service ([platform design](../../docs/design/platform.md) §6,
[ADR 0004](../../docs/design/adr/0004-trust-boundary.md), [ADR 0006](../../docs/design/adr/0006-streaming-large-inputs.md)).
A worker is two processes with different trust:

- the **supervisor** (this package, standard library only, no Django) leases jobs from the
  gateway's internal API, stages inputs, uploads artifacts and reports results. It holds the
  worker token and nothing else;
- the **engine** (`forklift run-job`, from `forklift-etl`) runs one job in a child process with
  no credentials, resource limits and, where the kernel allows, a Landlock sandbox and its own
  network namespace. It is started fresh for every job, so a crash, a leak or an exploit does
  not outlive its job.

Workers only make outbound calls: to the gateway's internal port and to the object store, through
presigned URLs. They have no inbound ports and no database access.

## How a job runs

1. **Lease.** `POST /internal/v1/leases` with the worker's lanes and the spec versions it runs
   (`[1]`). An empty answer (204) makes the supervisor wait, doubling the wait from
   `--idle-min-seconds` up to `--idle-max-seconds`.
2. **Check the spec.** The job id must match the lease, `s3` locations are refused (workers hold
   no storage credentials), `file` paths must stay inside scratch, presigned URLs must be http(s)
   on an allowed host (`--store-host`, or the URL's own host).
3. **Stage or stream** ([ADR 0006](../../docs/design/adr/0006-streaming-large-inputs.md)). An input
   up to `stage_max_bytes` (the lease's value, capped by `--stage-max-bytes`) is downloaded into
   the job's scratch directory through its presigned GET and its location rewritten to `file`.
   The download must match the lease: `Content-Length` and the byte count equal `size`, the
   `ETag` header equals `etag`, and for a single-part object the MD5 of the bytes equals the ETag.
   Transient failures restart the download. A larger input stays a `presigned_url`, and the
   engine gets `--allow-url-host` for that URL's host only, and `--input-url-requests`: when the
   URL is about to expire or the store refuses it, the engine writes `{"type": "input_url"}` on
   stdout, and the supervisor answers with one line on the engine's stdin, `{"url": ...}` from
   `POST /jobs/{id}/input-url` (only if it points at that same host) or `{"error": ...}`. It asks
   the gateway up to three times on connection errors and 5xx, at most 100 times per job, and
   answers within the engine's `--input-url-timeout` (three times `--http-timeout` plus room).
   A 409 answers with an error and stops the job as a lost lease; a 401 or 403 also stops the
   worker.
4. **Run the engine** in a private scratch directory (mode 0700) holding `spec.json` (mode 0600),
   `in/`, `out/` and `tmp/`: `python -I -m forklift run-job spec.json --base-dir <scratch>
   --result result.json --progress-jsonl [--allow-url-host HOST --input-url-requests
   --input-url-timeout SECONDS]`, inside the sandbox described below, with a wall-clock limit of
   `limits.max_seconds` (at most `--max-job-seconds`).
5. **Heartbeat** every third of the lease (or `--heartbeat-seconds`) for the whole job, with the
   engine's latest progress. A `cancel: true` answer sends the engine SIGTERM (it writes a
   cancelled result) and SIGKILL after `--kill-grace-seconds`; the job completes as `cancelled`
   without artifacts. A 409 (or 404), or no accepted heartbeat for a whole lease, means the lease
   is lost: the engine is stopped and nothing is uploaded or reported.
6. **Upload** the artifacts the result lists, data files first and the manifest last, through
   `POST /jobs/{id}/presign` and one presigned PUT each (with `Content-MD5`, so the store rejects
   a corrupted body), or, for files the gateway answers with a multipart upload (those above its
   `multipart_threshold_bytes`), part by part: each part is read from the file in chunks of at
   most 1 MiB (once for its `Content-MD5`, once as it is sent), retried on its own with a fresh
   URL (`POST /jobs/{id}/parts`) after a connection error, a 5xx or a 403 (an expired URL); a
   cancel, a lost lease or a shutdown stops the upload before the next part. URLs are
   used only in the first three quarters of their lifetime (`expires_in`): older part URLs are
   fetched again, older PUT URLs signed again with presign. Heartbeats carry `bytes_uploaded`.
   The worker never aborts a multipart upload; the gateway aborts what an attempt left pending
   when it ends. The supervisor treats what the engine leaves as untrusted: it opens
   `result.json` and every artifact one directory at a time without following symbolic links,
   accepts only regular files with no other hard links, computes sizes and sha256 itself, checks
   before uploading that the file is still the one it hashed, and reports a result reduced to
   the fields of the contract.
7. **Complete** with the JobResult and the uploaded artifacts. An engine that crashed, timed out,
   wrote no result or an unusable one, an input that could not be staged and an upload that
   failed all become a `failed` result with a specific code and message (crashes include the
   last, redacted, lines of the engine's stderr).
8. **Clean up.** Processes the engine left running are killed, even ones that left its process
   group (the supervisor is their subreaper), and the scratch directory is removed, whatever
   happened. Directories left by a worker that died are removed when the next worker starts on
   that scratch directory.

Gateway calls are retried with exponential backoff and jitter on connection errors, 408, 429
and 5xx; downloads three times, uploads (and each part) five times, presign and part URLs five
and complete eight.

## Running it

From a checkout of this repository (the worker needs the engine, `forklift-etl`, from its root):

```bash
pip install -e ".[excel,sql]" -e services/worker
forklift-worker --gateway http://gateway:8081 --token-file /run/secrets/worker-token \
    --lanes batch,interactive --scratch /var/lib/forklift/scratch --landlock required
```

The image (built from the repository root) runs as uid 10001, works with a read-only root file
system and writes only to `/scratch`:

```bash
docker build -f services/worker/Dockerfile -t forklift-worker .
docker run --read-only --tmpfs /scratch:uid=10001,gid=10001,mode=0700 \
    -v ./secrets/worker-token:/run/secrets/worker-token:ro \
    -e FORKLIFT_WORKER_GATEWAY=http://gateway:8081 -e FORKLIFT_WORKER_LANES=batch,interactive \
    --memory 4g --pids-limit 256 forklift-worker
```

Give each worker process its own scratch directory (a second worker on the same one refuses to
start), size it for the outputs plus staged inputs, and give the container a memory and a pids
limit (see what is not enforced, below).

## Settings

Every flag has an environment variable; a flag wins over its variable. Sizes take `512`, `512MiB`,
`2GiB` or `10G`; durations `30`, `2.5`, `90s`, `5m` or `1h`.

| Flag | Variable | Default | What it does |
|---|---|---|---|
| `--gateway` | `FORKLIFT_WORKER_GATEWAY` | (required) | URL of the gateway's internal port; /internal/v1 is appended unless it ends with that. |
| `--token-file` | `FORKLIFT_WORKER_TOKEN_FILE` | (required) | File holding the worker token. It is read again for every request, so it can be rotated, and it should live outside the engine's readable paths (for example /run/secrets). |
| `--lanes` | `FORKLIFT_WORKER_LANES` | `batch` | Comma-separated lanes to lease jobs from (batch, interactive, sql). |
| `--scratch` | `FORKLIFT_WORKER_SCRATCH` | (required) | Directory for the per-job scratch directories (created if missing, mode 0700). |
| `--isolation` | `FORKLIFT_WORKER_ISOLATION` | `standard` | Isolation profile: standard or no-network (see README.md for what each enforces). |
| `--landlock` | `FORKLIFT_WORKER_LANDLOCK` | `auto` | Landlock file-system (and TCP) sandbox for the engine: auto (use it when the kernel has it), required (refuse to start without it) or off. |
| `--concurrency` | `FORKLIFT_WORKER_CONCURRENCY` | `1` | Jobs run at the same time, each in its own engine process and scratch directory. |
| `--worker-id` | `FORKLIFT_WORKER_ID` | host name + random suffix | Name the gateway knows this worker by (with --concurrency above 1, each slot appends -1, -2, ...). |
| `--max-jobs` | `FORKLIFT_WORKER_MAX_JOBS` | `0` | Exit after this many jobs (0: never). --max-jobs 1 runs one job per process. |
| `--engine-command` | `FORKLIFT_WORKER_ENGINE_COMMAND` | `{python} -I -m forklift` | Command that starts the engine; `run-job SPEC --base-dir ... --result ...` is appended. {python} stands for the worker's own interpreter. |
| `--engine-env` | `FORKLIFT_WORKER_ENGINE_ENV` | (none) | Extra environment variables passed to the engine, by name (for example ODBCSYSINI). Credentials never are: AWS_*, FORKLIFT_* and names with TOKEN, SECRET, PASSW, CREDENTIAL or KEY are refused. |
| `--engine-read-path` | `FORKLIFT_WORKER_ENGINE_READ_PATHS` | (none) | Extra paths the engine may read under Landlock (the Python installation, /usr, /lib*, /etc, /opt, /proc and /sys are always readable). |
| `--store-host` | `FORKLIFT_WORKER_STORE_HOSTS` | (none) | Object store host (or host:port) that presigned URLs must point at; repeat for several. Without it, each URL's own host is the only one its engine may reach. |
| `--ca-file` | `FORKLIFT_WORKER_CA_FILE` | (none) | CA bundle for HTTPS to the gateway and the object store (default: the system's). |
| `--stage-max-bytes` | `FORKLIFT_WORKER_STAGE_MAX_BYTES` | (none) | Ceiling on the gateway's stage_max_bytes: larger inputs are streamed (standard profile) or refused (no-network). |
| `--max-job-seconds` | `FORKLIFT_WORKER_MAX_JOB_SECONDS` | `24h` | Wall-clock ceiling for one engine run (a job's limits.max_seconds applies when lower). |
| `--kill-grace-seconds` | `FORKLIFT_WORKER_KILL_GRACE_SECONDS` | `10` | Time a stopped engine gets between SIGTERM (it writes a cancelled result) and SIGKILL. |
| `--drain-seconds` | `FORKLIFT_WORKER_DRAIN_SECONDS` | `20` | After SIGTERM, how long running jobs may continue before their engines are stopped and the jobs handed back (their leases expire and the gateway queues them again). |
| `--idle-min-seconds` | `FORKLIFT_WORKER_IDLE_MIN_SECONDS` | `0.5` | First wait after an empty lease; it doubles up to --idle-max-seconds. |
| `--idle-max-seconds` | `FORKLIFT_WORKER_IDLE_MAX_SECONDS` | `10` | Longest wait between lease requests while there is nothing to do or the gateway is down. |
| `--heartbeat-seconds` | `FORKLIFT_WORKER_HEARTBEAT_SECONDS` | (none) | Time between heartbeats (default: a third of the lease the gateway grants). |
| `--http-timeout` | `FORKLIFT_WORKER_HTTP_TIMEOUT` | `60` | Socket timeout for requests to the gateway and the object store. |
| `--limit-address-space` | `FORKLIFT_WORKER_LIMIT_ADDRESS_SPACE` | `auto` | Engine RLIMIT_AS: a size, unlimited, or auto (the machine's physical memory). |
| `--limit-cpu-seconds` | `FORKLIFT_WORKER_LIMIT_CPU_SECONDS` | `auto` | Engine RLIMIT_CPU: a duration, unlimited, or auto (the job's wall-clock limit times the CPUs this worker may use). |
| `--limit-file-size` | `FORKLIFT_WORKER_LIMIT_FILE_SIZE` | `auto` | Engine RLIMIT_FSIZE (largest file it may write): a size, unlimited, or auto (the size of the scratch file system). |
| `--limit-open-files` | `FORKLIFT_WORKER_LIMIT_OPEN_FILES` | `1024` | Engine RLIMIT_NOFILE: a number or unlimited (the worker's own hard limit). |
| `--allow-root` | `FORKLIFT_WORKER_ALLOW_ROOT` | off | Run even as root (development only: the profiles expect a non-root worker). |
| `--log-level` | `FORKLIFT_WORKER_LOG_LEVEL` | `info` | debug, info, warning or error (debug includes the engine's own log lines). |
| `--log-format` | `FORKLIFT_WORKER_LOG_FORMAT` | `json` | json (one object per line) or text. |

RLIMIT_AS limits virtual memory, which runs well above resident memory (an idle engine maps
about 1.4 GiB for 120 MiB resident), so keep it far above what a job uses and let the container's
memory limit bound resident memory.

## Isolation profiles

Design §6.2 names three profiles; this worker implements `standard` and `no-network`, and
`sandboxed` is the same image under a sandboxed runtime (below). The worker logs what it enforces
when it starts (`isolation` in the `forklift-worker started` line) and refuses to start when the
profile asks for something the platform cannot do.

| Protection | standard | no-network | How |
|---|---|---|---|
| One process per job, killed with the supervisor | yes | yes | own session; `PR_SET_PDEATHSIG`; SIGTERM, then SIGKILL to the process group; leftovers killed (subreaper) |
| No credentials in the engine | yes | yes | environment allow-list (below); the token is never passed; the supervisor is non-dumpable |
| No privilege gain | yes | yes | `PR_SET_NO_NEW_PRIVS`; the worker refuses to run as root without `--allow-root` |
| Resource limits | yes | yes | RLIMIT_AS, RLIMIT_CPU (SIGXCPU, SIGKILL after the grace), RLIMIT_FSIZE, RLIMIT_NOFILE, RLIMIT_CORE 0; wall-clock limit |
| Writes only to its scratch directory | with Landlock | with Landlock | Landlock ABI 1+ (Linux 5.13) |
| Reads only system paths, the Python installation and its scratch | with Landlock | with Landlock | Landlock ABI 1+; `--engine-read-path` adds paths |
| Cannot ptrace or read the memory of other processes | with Landlock | with Landlock | Landlock domains (and the non-dumpable supervisor) |
| No TCP for staged inputs; the store's port only for streamed ones | with Landlock ABI 4 (Linux 6.7) | always (no network) | Landlock TCP connect/bind rules; SQL jobs are not TCP-restricted |
| No signals or abstract unix sockets outside its sandbox | with Landlock ABI 6 (Linux 6.12) | with Landlock ABI 6 | Landlock scopes |
| No network at all (TCP, UDP, DNS, abstract sockets) | no | yes | a new user and network namespace with only a loopback interface, which is down |
| Streamed inputs | yes | refused (`LIMIT_EXCEEDED`) | |
| SQL sources and targets (`sql` lane) | yes | refused (`SPEC_INVALID`; `--lanes sql` refused at start) | |

`--landlock auto` (the default) uses Landlock when the kernel has it and logs a warning when it
does not; `--landlock required` refuses to start without it, and is what production workers
should use. Docker's default seccomp profile allows the Landlock system calls (checked with
Docker 29: a read-only container as uid 10001 gets Landlock ABI 7 on Linux 6.18).

**The engine's environment** is built from an allow-list: `PATH`, `HOME` and `TMPDIR` (both in
its scratch directory), `LANG` (default `C.UTF-8`), `LC_ALL`, `LC_CTYPE`, `TZ`, `SSL_CERT_FILE`,
`SSL_CERT_DIR`, the names in `--engine-env`, and, for streamed inputs only, `HTTPS_PROXY`,
`HTTP_PROXY` and `NO_PROXY` when their URLs carry no user name or password. `AWS_*`,
`FORKLIFT_*` and names containing `TOKEN`, `SECRET`, `PASSW`, `CREDENTIAL` or `KEY` never reach
it; neither does the gateway's address.

**SQL jobs** carry their connection string in the leased spec. It exists in the supervisor's
memory and in the job's `spec.json` (mode 0600, inside the 0700 scratch directory, deleted with
it) and nowhere else: it is not logged, not passed on the command line, and every message, stderr
line and result the supervisor passes on is redacted (the job's connection strings and the
passwords in them, presigned URLs and their signatures, `Pwd=`/`Password=` pairs, URL user info).

### The no-network profile

The engine runs in a new user namespace (mapping only the worker's own uid and gid) and a new,
empty network namespace. This needs a platform that lets an unprivileged process create them;
the worker checks at start and exits with code 2 when it cannot:

- **Linux hosts**: unprivileged user namespaces enabled (`kernel.unprivileged_userns_clone=1` on
  Debian kernels; on Ubuntu 23.10 and later `kernel.apparmor_restrict_unprivileged_userns=0`, or an
  AppArmor profile that allows `userns` for the worker's interpreter).
- **Docker**: the default seccomp profile blocks `unshare(CLONE_NEWNET)` for processes without
  `CAP_SYS_ADMIN`. Run the worker with a seccomp profile that is the default one plus `unshare`
  and `clone` with namespace flags (`--security-opt seccomp=worker-seccomp.json`); with
  `--security-opt seccomp=unconfined` it works too, at the price of the supervisor's seccomp
  filter.
- **Kubernetes**: the same, as a `Localhost` seccomp profile (`RuntimeDefault` blocks it).

### What is not enforced

- **Without Landlock** the engine runs as the worker's user and can read whatever that user can,
  including the token file and, with `--concurrency` above 1, other jobs' scratch directories.
  Mount the token outside the engine's readable paths (the worker warns when it is inside one)
  and use `--landlock required`.
- **Container egress.** Landlock restricts TCP only: in the standard profile UDP (DNS lookups
  included) is not restricted, so limit the container's egress to the gateway's internal port
  and the store (Compose network, Kubernetes NetworkPolicy, or an allow-listing proxy; design
  §6.2 and §10). SQL jobs reach their database with TCP unrestricted.
- **Memory, disk and processes.** RLIMIT_AS bounds virtual memory, not resident memory;
  RLIMIT_FSIZE bounds one file, not the scratch directory; there is no RLIMIT_NPROC (it would
  count the supervisor's processes too). Set the container's memory and pids limits and size the
  scratch volume (`emptyDir.sizeLimit`).
- **The read-only root file system and the default seccomp profile** are the container
  runtime's to apply (`--read-only`, Pod Security "restricted"); the image works with both.
- **sandboxed** (design §6.2) is a deployment choice: run this image under a sandboxed runtime
  (`runtimeClassName: gvisor` or Kata), and with `--max-jobs 1` for one job per pod.

## Exit codes, signals, logs

| Exit code | When |
|---|---|
| 0 | stopped by SIGTERM or SIGINT, or after `--max-jobs` jobs |
| 1 | an internal error in the worker (logged with its traceback) |
| 2 | invalid settings, or an isolation profile or scratch directory this platform cannot provide |
| 3 | the gateway refused the worker: its token (401/403), or its internal API (404, 400, 422 or a malformed answer) |

The first SIGTERM (or SIGINT) stops leasing; running jobs may finish for `--drain-seconds`, then
their engines are stopped and the jobs handed back (nothing is uploaded or reported, their leases
expire and the gateway queues them again). A second signal stops them at once.

Logs go to stderr, one JSON object per line (`--log-format text` for development), with `job_id`,
`attempt`, `worker_id` and counts as keys: never cell values, connection strings, tokens or
presigned URLs (stores are named by host only). At `--log-level debug` the engine's own stderr
lines are included, redacted.

## What the worker expects from the gateway

The internal API of the platform brief, with these details:

- Every call: `Authorization: Bearer <worker token>`; 401 or 403 stops the worker (exit code 3).
- `POST /leases` sends `worker_id`, `lanes`, `spec_versions: [1]`, `engine_version` (the
  installed `forklift-etl` version) and `worker_version`; the answer needs `job_id`, `attempt`
  (from 1), `lease_seconds` (> 0), `stage_max_bytes` (>= 0) and `spec`, or 204.
- A store input is a `presigned_url` location with `size` (required: it decides staging or
  streaming) and `etag` (recommended: it is compared with the store's).
- `POST /jobs/{id}/heartbeat` sends `progress`: whole numbers only, at most 20 of them:
  `rows_read`, `rows_rejected` and `bytes_read` (0 until the engine reports), `bytes_staged`
  while an input is staged, `bytes_uploaded` once artifacts go up, and other counters the engine
  reports (`rows_written` for `sql_table` outputs). The answer may omit `lease_seconds` to keep
  the lease as it is.
- 409 on any job call means the lease is gone; 404 that the job is gone; the worker stops the job
  and reports nothing.
- `POST /jobs/{id}/presign` sends `files: [{name, bytes}]` and `multipart: true`; each answer
  entry needs `name` and `key`, and either `url` and `method: "PUT"` (one PUT; `headers`,
  optional, are sent as given) or a multipart upload: `upload_id`, `part_size` (every part but
  the last has exactly this size), `part_count` and `parts: [{part_number, url}]` (any number of
  them, even none). `expires_in` (optional) says how many seconds the URLs stay valid. The worker
  adds `Content-Length` and `Content-MD5` to every PUT, so a signed `Content-Type` is fine but a
  signed `Content-MD5` is not.
- `POST /jobs/{id}/parts` sends `name`, `upload_id` and `part_numbers` (at most 100); the answer
  needs `parts: [{part_number, url}]` for every one of them, and may give `expires_in`.
- `POST /jobs/{id}/complete` sends `result` (a JobResult that matches
  `contracts/jobresult.schema.json`, with the sizes and sha256 the supervisor computed, at most
  1 MiB of JSON: longer warning lists and messages are shortened) and `artifacts: [{kind, name,
  key, bytes, sha256, rows}]` (at most 100; a multipart artifact adds its `upload_id`,
  `part_count` and `parts_sha256`: the sha256 of its parts' ETags as the store answered their
  PUTs, without quotes, in part order, each followed by a newline, so the report does not grow
  with the parts), and is retried on 5xx, so it should be idempotent per attempt. The gateway
  completes the multipart uploads; a refusal (400) leaves the job unreported, so its lease
  expires.
- `POST /jobs/{id}/input-url` sends `attempt`; the answer needs `location`, a `presigned_url`
  location with `url` for the same object as the leased one (the engine checks that scheme,
  host, port and path are the same). Any other answer (400 for an input that is not streamed,
  410 for one that is gone) is passed to the engine as an error, and the job fails with
  `PERMISSION_DENIED` once the store refuses the URL it has.

## Development

```bash
python3.13 -m venv .venv && .venv/bin/pip install -e ".[excel,sql]" -e "services/worker[dev]"
cd services/worker
python -m pytest tests/unit --cov=forklift_worker --cov-branch --cov-fail-under=100
python -m pytest tests/integration                     # the real engine against a fake gateway
scripts/test-services.sh up                             # from the root: RustFS, PostgreSQL, ...
FORKLIFT_TEST_SERVICES=1 python -m pytest tests/integration
```

The unit tests run the worker against `tests/fake_gateway.py`, an in-process fake of the internal
API with a small object store, and `tests/fake_engine.py`, a stand-in for `forklift run-job` that
the spec drives (succeed, fail, crash, hang, ignore SIGTERM, write odd results, report what it can
reach). The Landlock and no-network tests skip on kernels that lack them. The integration tests run
the real engine, and with `FORKLIFT_TEST_SERVICES=1` presigned URLs signed by RustFS and an
sql-lane job against PostgreSQL with a restricted login.
