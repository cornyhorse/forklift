# ADR 0006: Large inputs are streamed to the engine through presigned URLs

- **Status**: Proposed
- **Date**: 2026-10-10
- **Context document**: [platform design](../platform.md), §6.1, §6.2 and §12

## Context

The platform has to handle single inputs larger than 10 GB. The first design copied every input
into the worker's scratch directory before the engine started, so that the engine process could run
without any network access ([ADR 0004](0004-trust-boundary.md)). For inputs of that size, copying
first needs scratch disk larger than the input plus its outputs, and delays the start of every run
by a full transfer.

Three ways to give the engine a large input were considered:

1. **Presigned URL, engine reads it.** The spec carries a presigned GET URL for one object; the
   engine reads it over HTTPS; its network is limited to the object store.
2. **Supervisor pipe.** The supervisor streams the object and feeds the engine through a local pipe,
   so the engine never touches the network.
3. **Large scratch only.** Keep copying everything, and size scratch for the largest input.

The engine reads parts of a file more than once: header detection reads the start, the fallback
reader for rows with too many or too few fields starts again from the beginning, and footer handling
writes a filtered copy. A pipe gives one forward-only pass, so option 2 would first require the
engine to become single-pass. Option 3 is the simplest but moves the cost to disk and start-up time.

## Decision

- Inputs up to `stageMaxBytes` (an admin setting; default 2 GiB) are staged into scratch as
  before. Larger inputs are **streamed**: the spec's input location is a `presigned_url` for that one
  object, valid for a limited time.
- The engine reads a streamed input with Arrow's streaming CSV reader over a forward-only HTTPS
  stream that resumes with range requests after a dropped connection, plus small range reads for
  header detection. When a URL is close to expiring, the engine asks the supervisor for a fresh one
  over its pipe; the supervisor gets it from the gateway's internal API.
- `presigned_url` locations are accepted only through `run_job`, and only for hosts the caller
  allows (the worker passes the object store's endpoint). Schema generation and the public API keep
  rejecting URLs supplied by users.
- The engine process's network is limited to the object store: by pod selector (in-cluster
  RustFS), by CIDR, or through an allow-listing egress proxy. The `no-network` isolation profile
  accepts staged inputs only.
- Outputs are still written to scratch and uploaded by the supervisor with presigned multipart PUTs:
  Parquet output is compressed and usually much smaller than the text input.

## Consequences

- No disk sized for the largest input, and runs start without waiting for a full copy.
- The engine process is no longer network-less for large inputs; it still holds no credentials, and
  a presigned URL opens only its own object for a limited time.
- Engine work is needed before streaming is usable: the streaming remote reader (also a speed-up for
  `s3://` inputs, which today go through Python's `csv` module row by row), footer detection
  without a full copy, and uniqueness checks whose memory does not grow without bound with the
  number of distinct keys.
- Option 2 stays open: if the engine becomes single-pass, the supervisor could feed large inputs
  through a pipe and every profile could be network-less.

## Implementation notes

**Fresh URLs** (as built). A presigned URL is signed for the job's `max_seconds` plus
`input_url_margin_seconds` (at most the 7-day SigV4 limit), but it stops working earlier when the
gateway signs with temporary credentials (an IAM role session lasts 1 to 12 hours). So:

- `run_job(..., refresh_input_url=callable)` lets `PresignedUrlSource` replace its URL. Before a
  request it replaces a SigV4 URL that expires within 5 minutes, or has used up half its lifetime
  if that is sooner (`X-Amz-Date` plus `X-Amz-Expires`; other URLs are not replaced in advance).
  A request refused with HTTP 403, or with HTTP 400 `ExpiredToken` (S3's answer once temporary
  credentials have ended), is sent once more with a fresh URL. A request gets at most one fresh
  URL and an input at most 100. A fresh URL must keep the scheme, host, port and path, and
  `If-Match` keeps the bytes the same; without a fresh URL the job fails with
  `PERMISSION_DENIED` ("The input URL expired and a fresh one could not be obtained: ...").
- `forklift run-job --input-url-requests` asks over its pipes: `{"type": "input_url"}` on
  stdout, one answer line on stdin (`{"url": ...}` or `{"error": ...}`) within
  `--input-url-timeout` seconds. A late or malformed answer ends the conversation.
- The supervisor starts the engine of a streamed input with that flag and answers each request
  from `POST /internal/v1/jobs/{id}/input-url` for its lease, passing on only URLs on the host
  the engine may reach. The gateway signs the input again, the same way and for as long as at
  lease time, answers only for inputs that can be streamed (CSV in a store; 400 otherwise) and
  answers 410 for an input that is gone (409 means a lost lease to workers).
