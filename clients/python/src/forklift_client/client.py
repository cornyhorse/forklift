"""The client of a forklift gateway's public API, ``/api/v1``.

One :class:`ForkliftClient` talks to one gateway with one API token. Every method sends one or a
few requests and returns the API's resources as frozen dataclasses (:mod:`forklift_client.models`);
errors are :class:`~forklift_client.errors.ForkliftError` subclasses. The token goes only to the
gateway: uploads and downloads go straight to the object store through presigned URLs, which
carry their own signature.
"""

from __future__ import annotations

import json
import logging
import os
import platform
import random
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import (
    IO,
    Any,
    Callable,
    ContextManager,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)
from urllib.parse import quote, urlencode, urlsplit, urlunsplit

from . import __version__, _downloads, _uploads
from ._model import Model, Page
from ._transport import RetryPolicy, Transport, retry_after, send_with_retries
from .errors import (
    ApiError,
    JobFailed,
    JobTimeout,
    TransportError,
    UnexpectedResponse,
    api_error,
)
from .models import (
    Artifact,
    Connection,
    Dataset,
    Download,
    Job,
    JobEvent,
    JobSummary,
    Me,
    PartUrl,
    Schedule,
    Schema,
    SchemaVersion,
    Upload,
    UploadTicket,
    Webhook,
)

log = logging.getLogger("forklift_client")

API_PREFIX = "/api/v1"
DEFAULT_TIMEOUT = 30.0
MAX_PAGE_SIZE = 500  # the gateway's NINJA_PAGINATION_MAX_LIMIT
MAX_IDEMPOTENCY_KEY = 255
TERMINAL_STATUSES = frozenset({"succeeded", "failed", "cancelled"})
ACTIVE_STATUSES = frozenset({"queued", "running", "publishing"})

M = TypeVar("M", bound=Model)
T = TypeVar("T")
Ref = Union[str, uuid.UUID, Model]  # an id, or the resource itself
PathLike = Union[str, "os.PathLike[str]"]


def _ref(value: Ref) -> str:
    """The id of ``value`` (a resource or an id) as a path segment."""
    text = str(getattr(value, "id")) if isinstance(value, Model) else str(value)
    if not text:
        raise ValueError("an id must not be empty")
    return quote(text, safe="")


def _json_default(value: Any) -> Any:
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"{type(value).__name__} values cannot be sent as JSON")


def _base_url(url: str) -> str:
    parts = urlsplit(url.strip())
    if (
        parts.scheme not in ("http", "https")
        or not parts.hostname
        or parts.username is not None
        or parts.query
        or parts.fragment
    ):
        raise ValueError(
            "base_url must be the gateway's http:// or https:// URL without credentials, query "
            "or fragment, such as https://forklift.example.org"
        )
    path = parts.path.rstrip("/")
    if path.endswith(API_PREFIX):
        path = path[: -len(API_PREFIX)]
    return urlunsplit((parts.scheme, parts.netloc, path, "", ""))


def _check_token(token: str) -> str:
    token = (token or "").strip()
    if not token:
        raise ValueError("token is required: an API token of the gateway (fkl_...)")
    if any(ch.isspace() or ord(ch) < 32 or ord(ch) > 126 for ch in token):
        raise ValueError("the token has whitespace or characters an HTTP header cannot carry")
    return token


def _check_key(key: str) -> str:
    if not key or len(key) > MAX_IDEMPOTENCY_KEY or any(not 32 < ord(ch) < 127 for ch in key):
        raise ValueError(
            f"an Idempotency-Key is 1 to {MAX_IDEMPOTENCY_KEY} printable ASCII characters "
            "without spaces"
        )
    return key


def _check_page(limit: int, offset: int) -> None:
    if not 1 <= limit <= MAX_PAGE_SIZE:
        raise ValueError(f"limit must be between 1 and {MAX_PAGE_SIZE} (the API's largest page)")
    if offset < 0:
        raise ValueError("offset must not be negative")


def _error_detail(body: bytes) -> Tuple[str, str]:
    """(code, detail) of an API error body; the detail of a 422 lists the fields it refused
    (pydantic's ``loc`` and ``msg``, never the ``input`` it echoes)."""
    try:
        document = json.loads(body)
    except ValueError:
        return "", "the answer is not the API's JSON; is base_url the gateway's public URL?"
    if not isinstance(document, dict):
        return "", ""
    detail = document.get("detail", "")
    if isinstance(detail, list):
        problems = []
        for item in detail:
            if isinstance(item, dict):
                where = ".".join(str(part) for part in item.get("loc", ()))
                problems.append(f"{where}: {item.get('msg', '')}" if where else item.get("msg"))
        detail = "; ".join(str(problem) for problem in problems)
    code = document.get("code", "")
    return (code if isinstance(code, str) else ""), str(detail)[:2000]


class ForkliftClient:
    """A client of one forklift gateway's ``/api/v1``, with one API token.

    ``base_url`` is the gateway's public URL (``https://forklift.example.org``; a trailing
    ``/api/v1`` is accepted too). ``timeout`` is the socket timeout of each request in seconds,
    ``ca_file`` a PEM bundle to verify the gateway's and the store's certificates with (default:
    the system's), ``retry`` the :class:`RetryPolicy` for requests that may be sent again.
    Instances are thread-safe and hold no open connections.
    """

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        timeout: float = DEFAULT_TIMEOUT,
        ca_file: Optional[PathLike] = None,
        retry: Optional[RetryPolicy] = None,
        user_agent: Optional[str] = None,
    ):
        self.base_url = _base_url(base_url)
        self._token = _check_token(token)
        if timeout <= 0:
            raise ValueError("timeout must be a positive number of seconds")
        self.retry = retry or RetryPolicy()
        agent = f"forklift-client/{__version__} Python/{platform.python_version()}"
        self._transport = Transport(
            timeout=timeout,
            user_agent=f"{user_agent} {agent}" if user_agent else agent,
            ca_file=os.fspath(ca_file) if ca_file is not None else None,
        )
        self._sleep: Callable[[float], None] = time.sleep
        self._clock: Callable[[], float] = time.monotonic
        self._rng = random.Random()

    @classmethod
    def from_env(cls, environ: Optional[Mapping[str, str]] = None, **options: Any):
        """A client from ``FORKLIFT_URL``, ``FORKLIFT_TOKEN`` and (optional)
        ``FORKLIFT_CA_FILE``; ``options`` go to the constructor."""
        environ = os.environ if environ is None else environ
        missing = [name for name in ("FORKLIFT_URL", "FORKLIFT_TOKEN") if not environ.get(name)]
        if missing:
            raise ValueError(f"set {' and '.join(missing)} (the gateway's URL and an API token)")
        if environ.get("FORKLIFT_CA_FILE"):
            options.setdefault("ca_file", environ["FORKLIFT_CA_FILE"])
        return cls(environ["FORKLIFT_URL"], environ["FORKLIFT_TOKEN"], **options)

    def __repr__(self) -> str:
        return f"ForkliftClient({self.base_url!r})"

    @property
    def timeout(self) -> float:
        return self._transport.timeout

    # ----------------------------------------------------------------------- requests

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        body: Any = None,
        headers: Optional[Mapping[str, str]] = None,
        replay_safe: Optional[bool] = None,
        expect: Sequence[int] = (200,),
    ) -> Tuple[int, Any]:
        """Send a request to the API; returns (status, parsed JSON or None)."""
        query = {
            key: (str(value).lower() if isinstance(value, bool) else str(value))
            for key, value in (params or {}).items()
            if value is not None
        }
        shown = API_PREFIX + path + (f"?{urlencode(query)}" if query else "")
        url = self.base_url + shown
        data = None if body is None else json.dumps(body, default=_json_default).encode()
        request_id = uuid.uuid4().hex
        sent = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._token}",
            "X-Request-ID": request_id,
            **({"Content-Type": "application/json"} if data is not None else {}),
            **(headers or {}),
        }
        if replay_safe is None:
            replay_safe = method in ("GET", "HEAD", "PUT", "DELETE")
        what = f"{method} {shown}"
        response = send_with_retries(
            lambda: self._transport.request(method, url, headers=sent, body=data, what=what),
            policy=self.retry,
            replay_safe=replay_safe,
            what=what,
            sleep=self._sleep,
            rng=self._rng,
        )
        if response.status not in expect:
            code, detail = _error_detail(response.body)
            raise api_error(
                method,
                shown,
                response.status,
                code=code,
                detail=detail,
                request_id=response.headers.get("x-request-id", request_id),
                retry_after=retry_after(response.headers),
            )
        if not response.body:
            return response.status, None
        try:
            return response.status, json.loads(response.body)
        except ValueError:
            kind = response.headers.get("content-type", "no content type")
            raise UnexpectedResponse(
                f"{what} answered HTTP {response.status} with a body that is not JSON ({kind}); "
                f"is {self.base_url} the gateway's public URL?"
            ) from None

    def _one(self, model: Type[M], method: str, path: str, **options: Any) -> M:
        _, document = self._request(method, path, **options)
        return _decode(model, document, f"{method} {API_PREFIX}{path}")

    def _list(self, model: Type[M], path: str, **options: Any) -> List[M]:
        _, document = self._request("GET", path, **options)
        where = f"GET {API_PREFIX}{path}"
        if not isinstance(document, list):
            raise UnexpectedResponse(f"{where} should answer a list")
        return [_decode(model, item, f"{where}[{i}]") for i, item in enumerate(document)]

    def _page(self, model: Type[M], path: str, limit: int, offset: int, **params: Any) -> Page[M]:
        _check_page(limit, offset)
        _, document = self._request(
            "GET", path, params={**params, "limit": limit, "offset": offset}
        )
        where = f"GET {API_PREFIX}{path}"
        if (
            not isinstance(document, dict)
            or not isinstance(document.get("items"), list)
            or not isinstance(document.get("count"), int)
        ):
            raise UnexpectedResponse(f"{where} should answer a page ({{items, count}})")
        items = tuple(
            _decode(model, item, f"{where} items[{i}]") for i, item in enumerate(document["items"])
        )
        return Page(items=items, count=document["count"], offset=offset, limit=limit)

    def paginate(
        self, list_method: Callable[..., Page[T]], *args: Any, page_size: int = 100, **kwargs: Any
    ) -> Iterator[T]:
        """Every item of a list, page by page: ``paginate(client.list_jobs, status="failed")``.

        ``list_method`` is one of the ``list_...`` methods that return a :class:`Page`; ``args``
        and ``kwargs`` are its arguments (not ``limit`` and ``offset``).
        """
        _check_page(page_size, 0)
        offset = 0
        while True:
            page = list_method(*args, limit=page_size, offset=offset, **kwargs)
            yield from page.items
            if not page.has_more:
                return
            offset = page.next_offset

    # ----------------------------------------------------------------------- me

    def me(self) -> Me:
        """The caller: their account, role, effective scopes and this request's token."""
        return self._one(Me, "GET", "/me")

    # ----------------------------------------------------------------------- schemas

    def list_schemas(self, *, limit: int = 50, offset: int = 0) -> Page[Schema]:
        """Schemas by name."""
        return self._page(Schema, "/schemas", limit, offset)

    def get_schema(self, schema: Ref) -> Schema:
        return self._one(Schema, "GET", f"/schemas/{_ref(schema)}")

    def create_schema(
        self, name: str, document: Mapping[str, Any], *, description: str = "", notes: str = ""
    ) -> Schema:
        """A new schema whose version 1 is ``document``; ``notes`` describe that version."""
        body = {"name": name, "document": document, "description": description, "notes": notes}
        return self._one(Schema, "POST", "/schemas", body=body, expect=(201,))

    def list_schema_versions(
        self, schema: Ref, *, limit: int = 50, offset: int = 0
    ) -> Page[SchemaVersion]:
        """Versions of a schema, newest first."""
        return self._page(SchemaVersion, f"/schemas/{_ref(schema)}/versions", limit, offset)

    def create_schema_version(
        self, schema: Ref, document: Mapping[str, Any], *, notes: str = ""
    ) -> SchemaVersion:
        """Save ``document`` as the schema's next version (versions are immutable)."""
        return self._one(
            SchemaVersion,
            "POST",
            f"/schemas/{_ref(schema)}/versions",
            body={"document": document, "notes": notes},
            expect=(201,),
        )

    def get_schema_version(self, schema: Ref, number: int) -> SchemaVersion:
        """Version ``number`` (1, 2, ...) of a schema."""
        return self._one(SchemaVersion, "GET", f"/schemas/{_ref(schema)}/versions/{int(number)}")

    def validate_schema(
        self,
        *,
        upload_id: Optional[Ref] = None,
        dataset_id: Optional[Ref] = None,
        schema: Optional[Mapping[str, Any]] = None,
        schema_version_id: Optional[Ref] = None,
        format: Optional[str] = None,
        input_options: Optional[Mapping[str, Any]] = None,
        options: Optional[Mapping[str, Any]] = None,
        wait_seconds: Optional[float] = None,
    ) -> Job:
        """Check a schema (``schema``, ``schema_version_id`` or the dataset's) against an upload
        or a dataset's source: a ``validate_schema`` job on the interactive lane.

        The gateway waits up to ``wait_seconds`` (at most its ``validate_wait_seconds``) for the
        result; the job comes back finished, or still queued or running (then
        :meth:`wait_for_job` it). Each call starts a new job.
        """
        body = {
            "upload_id": _opt_ref(upload_id),
            "dataset_id": _opt_ref(dataset_id),
            "schema": schema,
            "schema_version_id": _opt_ref(schema_version_id),
            "format": format,
            "input_options": dict(input_options or {}),
            "options": dict(options or {}),
            "wait_seconds": wait_seconds,
        }
        return self._one(Job, "POST", "/schemas/validate", body=body, expect=(200, 202))

    # ----------------------------------------------------------------------- connections

    def list_connections(
        self, *, kind: Optional[str] = None, limit: int = 50, offset: int = 0
    ) -> Page[Connection]:
        """Connections the caller may use (every one for admins), optionally of one ``kind``
        (``s3``, ``localfs``, ``sql``). Secrets are never returned."""
        return self._page(Connection, "/connections", limit, offset, kind=kind)

    def get_connection(self, connection: Ref) -> Connection:
        return self._one(Connection, "GET", f"/connections/{_ref(connection)}")

    # ----------------------------------------------------------------------- datasets

    def list_datasets(self, *, limit: int = 50, offset: int = 0) -> Page[Dataset]:
        """Datasets by name."""
        return self._page(Dataset, "/datasets", limit, offset)

    def get_dataset(self, dataset: Ref) -> Dataset:
        return self._one(Dataset, "GET", f"/datasets/{_ref(dataset)}")

    def run_dataset(
        self,
        dataset: Ref,
        *,
        upload_id: Optional[Ref] = None,
        idempotency_key: Optional[str] = None,
    ) -> Job:
        """Run a dataset (``upload_id``: the input, for datasets that read uploads).

        Like :meth:`submit_job`, the request carries an ``Idempotency-Key`` (a random one unless
        given), so sending it again cannot start a second run.
        """
        key = _check_key(idempotency_key) if idempotency_key else uuid.uuid4().hex
        body = {"upload_id": _opt_ref(upload_id)}
        return self._submitted(f"/datasets/{_ref(dataset)}/run", body, key)

    # ----------------------------------------------------------------------- uploads

    def create_upload(
        self,
        filename: str,
        size: int,
        *,
        content_type: str = "",
        sha256: str = "",
        classification: Optional[str] = None,
    ) -> UploadTicket:
        """Start an upload of ``size`` bytes: the ticket has one presigned PUT URL, or for a
        file above the gateway's ``multipart_threshold_bytes`` the part size and the first part
        URLs. :meth:`upload_file` does the whole upload."""
        body = {
            "filename": filename,
            "size": size,
            "content_type": content_type,
            "sha256": sha256,
            "classification": classification,
        }
        return self._one(UploadTicket, "POST", "/uploads", body=body, expect=(201,))

    def list_uploads(self, *, limit: int = 50, offset: int = 0) -> Page[Upload]:
        """The caller's uploads (everyone's for admins), newest first."""
        return self._page(Upload, "/uploads", limit, offset)

    def get_upload(self, upload: Ref) -> Upload:
        return self._one(Upload, "GET", f"/uploads/{_ref(upload)}")

    def upload_part_urls(self, upload: Ref, part_numbers: Iterable[int]) -> List[PartUrl]:
        """Fresh presigned URLs for parts (1 to 1000 at a time) of a pending multipart upload."""
        body = {"part_numbers": [int(number) for number in part_numbers]}
        _, document = self._request(
            "POST", f"/uploads/{_ref(upload)}/parts", body=body, replay_safe=True
        )
        where = f"POST {API_PREFIX}/uploads/.../parts"
        if not isinstance(document, list):
            raise UnexpectedResponse(f"{where} should answer a list")
        return [_decode(PartUrl, item, f"{where}[{i}]") for i, item in enumerate(document)]

    def complete_upload(
        self, upload: Ref, parts: Optional[Sequence[Mapping[str, Any]]] = None
    ) -> Upload:
        """Complete an upload once its bytes are in the store; a multipart upload needs every
        part's ``{"part_number", "etag"}``. Completing a complete upload returns it."""
        body = {"parts": [dict(part) for part in parts]} if parts else {}
        return self._one(
            Upload, "POST", f"/uploads/{_ref(upload)}/complete", body=body, replay_safe=True
        )

    def delete_upload(self, upload: Ref) -> None:
        """Delete an upload's object (a pending multipart upload is aborted); the record stays.
        An upload that is already deleted or expired is not an error."""
        self._request("DELETE", f"/uploads/{_ref(upload)}", expect=(204, 410))

    def upload_file(
        self,
        source: Union[PathLike, IO[bytes]],
        *,
        filename: Optional[str] = None,
        size: Optional[int] = None,
        content_type: Optional[str] = None,
        classification: Optional[str] = None,
        sha256: str = "",
    ) -> Upload:
        """Upload a file (a path, or a binary file object from its current position) straight
        to the store and complete it; returns the complete upload, the input of jobs.

        See :mod:`forklift_client._uploads` for how parts, retries and checks work.
        """
        return _uploads.upload(
            self,
            source,
            filename=filename,
            size=size,
            content_type=content_type,
            classification=classification,
            sha256=sha256,
        )

    # ----------------------------------------------------------------------- jobs

    def submit_job(
        self,
        kind: str,
        *,
        dataset_id: Optional[Ref] = None,
        upload_id: Optional[Ref] = None,
        format: Optional[str] = None,
        input_options: Optional[Mapping[str, Any]] = None,
        schema_version_id: Optional[Ref] = None,
        schema: Optional[Mapping[str, Any]] = None,
        compression: Optional[str] = None,
        options: Optional[Mapping[str, Any]] = None,
        limits: Optional[Mapping[str, Union[int, float]]] = None,
        classification: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> Job:
        """Enqueue a job (``kind``: ``run``, ``preview``, ``validate_schema``,
        ``generate_schema``) on a dataset (``dataset_id``) or an upload (``upload_id`` with a
        ``format``), with a stored schema version or an inline ``schema``.

        The request carries ``idempotency_key`` (a random one when not given) as its
        ``Idempotency-Key``: the gateway answers a key it has seen from this user with the job
        it created then, so a request sent again after a lost answer never creates a second
        job. Give your own key to make that hold across processes (a key used before for a
        different request is a :class:`~forklift_client.errors.Conflict`, code
        ``idempotency_key_reused``).
        """
        key = _check_key(idempotency_key) if idempotency_key else uuid.uuid4().hex
        body = {
            "kind": kind,
            "dataset_id": _opt_ref(dataset_id),
            "upload_id": _opt_ref(upload_id),
            "format": format,
            "input_options": dict(input_options or {}),
            "schema_version_id": _opt_ref(schema_version_id),
            "schema": schema,
            "compression": compression,
            "options": dict(options or {}),
            "limits": dict(limits or {}),
            "classification": classification,
        }
        return self._submitted("/jobs", body, key)

    def _submitted(self, path: str, body: Mapping[str, Any], key: str) -> Job:
        status, document = self._request(
            "POST",
            path,
            body=body,
            headers={"Idempotency-Key": key},
            replay_safe=True,
            expect=(200, 201),
        )
        job = _decode(Job, document, f"POST {API_PREFIX}{path}")
        if status == 200:
            log.info("Idempotency-Key %s was used before: job %s (%s)", key, job.id, job.status)
        return job

    def get_job(self, job: Ref) -> Job:
        return self._one(Job, "GET", f"/jobs/{_ref(job)}")

    def list_jobs(
        self,
        *,
        status: Optional[str] = None,
        kind: Optional[str] = None,
        dataset_id: Optional[Ref] = None,
        mine: bool = False,
        limit: int = 50,
        offset: int = 0,
    ) -> Page[JobSummary]:
        """Jobs, newest first; ``mine``: only those the caller requested."""
        return self._page(
            JobSummary,
            "/jobs",
            limit,
            offset,
            status=status,
            kind=kind,
            dataset_id=_opt_ref(dataset_id),
            mine=mine or None,
        )

    def list_job_events(
        self, job: Ref, *, after: Optional[int] = None, limit: int = 100
    ) -> Page[JobEvent]:
        """A job's events in order, after the event id ``after`` (a cursor: pass the last id
        you have to get only newer ones)."""
        return self._page(JobEvent, f"/jobs/{_ref(job)}/events", limit, 0, after=after)

    def iter_job_events(
        self, job: Ref, *, after: Optional[int] = None, page_size: int = 100
    ) -> Iterator[JobEvent]:
        """Every event of a job after ``after``, page by page."""
        while True:
            page = self.list_job_events(job, after=after, limit=page_size)
            yield from page.items
            if not page.has_more:
                return
            after = page.items[-1].id

    def cancel_job(self, job: Ref) -> Job:
        """Cancel a job: a queued one at once (it comes back ``cancelled``), a running one at
        its worker's next heartbeat (``cancel_requested``). A finished or publishing job cannot
        be cancelled (:class:`~forklift_client.errors.Conflict`)."""
        return self._one(Job, "POST", f"/jobs/{_ref(job)}/cancel")

    def wait_for_job(
        self,
        job: Ref,
        *,
        timeout: Optional[float] = 3600.0,
        poll_interval: float = 1.0,
        max_poll_interval: float = 30.0,
        raise_on_failure: bool = False,
        on_poll: Optional[Callable[[Job], None]] = None,
    ) -> Job:
        """Poll a job until it has ended (``succeeded``, ``failed`` or ``cancelled``; a job that
        is ``publishing`` has not) and return it.

        Polls start ``poll_interval`` seconds apart and grow by half each time up to
        ``max_poll_interval``. Transient errors (no answer, 429, 502-504) are logged and the
        polling goes on; :class:`~forklift_client.errors.JobTimeout` after ``timeout`` seconds
        (None: no limit). ``raise_on_failure`` raises
        :class:`~forklift_client.errors.JobFailed` for a job that did not succeed. ``on_poll``
        is called with the job each time it was fetched (to report progress).
        """
        if poll_interval <= 0 or max_poll_interval < poll_interval:
            raise ValueError("poll_interval must be positive and at most max_poll_interval")
        deadline = None if timeout is None else self._clock() + timeout
        interval = poll_interval
        while True:
            try:
                current = self.get_job(job)
            except (TransportError, ApiError) as error:
                if isinstance(error, ApiError) and not error.transient:
                    raise
                if deadline is not None and self._clock() >= deadline:
                    raise
                log.warning("Waiting for job %s: %s; polling on", _ref(job), error)
            else:
                if on_poll is not None:
                    on_poll(current)
                if current.status in TERMINAL_STATUSES:
                    if raise_on_failure and current.status != "succeeded":
                        raise JobFailed(current)
                    return current
                if deadline is not None and self._clock() >= deadline:
                    raise JobTimeout(current, timeout)  # type: ignore[arg-type]
            pause = interval if deadline is None else min(interval, deadline - self._clock())
            self._sleep(max(0.0, pause))
            interval = min(interval * 1.5, max_poll_interval)

    # ----------------------------------------------------------------------- artifacts

    def list_artifacts(self, job: Ref) -> List[Artifact]:
        """A job's files (every attempt's), by attempt and name."""
        return self._list(Artifact, f"/jobs/{_ref(job)}/artifacts")

    def get_artifact(self, artifact: Ref) -> Artifact:
        return self._one(Artifact, "GET", f"/artifacts/{_ref(artifact)}")

    def get_download_url(self, artifact: Ref) -> Download:
        """A presigned GET for an artifact, valid until ``expires_at`` (each call is audited)."""
        return self._one(Download, "GET", f"/artifacts/{_ref(artifact)}/download")

    def download_artifact(
        self, artifact: Union[Artifact, str, uuid.UUID], destination: PathLike
    ) -> Path:
        """Download an artifact to ``destination`` (a file, or a directory to put it in under
        its name) and return the file's path.

        The bytes are written to a temporary file next to it, checked against the artifact's
        size and sha256 (when the worker reported one), and only then renamed into place. A
        connection error or a store error worth retrying starts again with a fresh URL.
        """
        return _downloads.download(self, artifact, destination)

    def open_artifact(
        self, artifact: Union[Artifact, str, uuid.UUID]
    ) -> ContextManager[_downloads.ArtifactStream]:
        """The artifact's bytes as a stream: ``with client.open_artifact(a) as stream:``,
        then ``stream.read(n)``; the size and sha256 are checked when the end is read
        (:class:`~forklift_client.errors.VerificationError`). No retries once reading began."""
        return _downloads.open_stream(self, artifact)

    # ----------------------------------------------------------------------- schedules

    def list_schedules(
        self,
        *,
        enabled: Optional[bool] = None,
        dataset_id: Optional[Ref] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Page[Schedule]:
        """Schedules, the next to run first."""
        return self._page(
            Schedule, "/schedules", limit, offset, enabled=enabled, dataset_id=_opt_ref(dataset_id)
        )

    def get_schedule(self, schedule: Ref) -> Schedule:
        return self._one(Schedule, "GET", f"/schedules/{_ref(schedule)}")

    # ----------------------------------------------------------------------- webhooks

    def list_webhooks(self, *, limit: int = 50, offset: int = 0) -> Page[Webhook]:
        """The caller's webhooks (signing secrets are never shown again)."""
        return self._page(Webhook, "/webhooks", limit, offset)

    def get_webhook(self, webhook: Ref) -> Webhook:
        return self._one(Webhook, "GET", f"/webhooks/{_ref(webhook)}")


def _opt_ref(value: Optional[Ref]) -> Optional[str]:
    if value is None:
        return None
    return str(getattr(value, "id")) if isinstance(value, Model) else str(value)


def _decode(model: Type[M], document: Any, where: str) -> M:
    if not isinstance(document, dict):
        raise UnexpectedResponse(f"{where} should answer an object")
    return model._from_dict(document, model.__name__)
