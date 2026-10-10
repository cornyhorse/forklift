# ADR 0005: S3-compatible object storage and local volumes; Parquet first, tables later

- **Status**: Proposed
- **Date**: 2026-10-09
- **Context document**: [platform design](../platform.md), §7

## Context

The platform should be cloud-agnostic but cloud-friendly. "S3" here means any store that speaks the
S3 API (RustFS, Ceph, Cloudflare R2, Backblaze B2, AWS S3, MinIO, ...). Some installations will
only have a disk. Writing to database tables is wanted eventually, but Parquet output has to be
dependable first.

## Decision

- The service uses one S3-compatible bucket with prefixes per purpose (`uploads/`, `jobs/`,
  `previews/`) and only features every such store has: presigned GET/PUT (including multipart),
  copy and delete. Store-specific features (STS, object lock, event notifications) are optional
  extras, never requirements.
- `localfs` connections name a mounted directory (Compose volume, Kubernetes PVC) with an
  admin-set root; paths are checked against the root before a job is queued and again by the worker.
- Destinations are Parquet on either kind of storage, published manifest-last (data files first,
  `manifest.json` last) from attempt-scoped staging prefixes.
- Database tables become a destination only after Parquet output meets these criteria: end-to-end
  tests against RustFS and a local volume, retries and crashes never mix attempts, manifest-last
  publishing verified, runs on inputs larger than 10 GB, and `bad_rows` kept when a run fails on
  its threshold.

## Consequences

- Compose ships RustFS (Apache-2.0), the S3-compatible store the integration tests already run
  against; any S3-compatible service works in production.
- No dependency on a particular cloud's IAM: large inputs are streamed through presigned GETs with
  range requests ([ADR 0006](0006-streaming-large-inputs.md)), which every S3-compatible store
  supports, rather than through short-lived scoped credentials, which many do not.
- Readers of a destination should trust only complete manifests.
