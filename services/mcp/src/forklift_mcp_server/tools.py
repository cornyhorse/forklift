"""The tools of the remote MCP server: the tool set of design §9.1 over the gateway's /api/v1.

Every tool calls the gateway with the caller's own API token (the request's ``Authorization``
header, which :mod:`forklift_mcp_server.auth` has checked), so permissions, connections and the
audit log are the gateway's: a refusal comes back as a tool error that says what is missing (a
scope, the "view raw rows" permission, the owner of an upload). The names and meanings are the
local server's (``forklift.mcp``), with upload, dataset, schema and job ids where it takes paths.

- Jobs run on the service's workers. ``preview``, ``validate_schema``, ``generate_schema`` and
  ``run_job`` submit one (``POST /jobs`` with an ``Idempotency-Key``) and wait for it a bounded
  time; a job that has not ended by then is returned with its id and status, to follow with
  ``get_job`` (``publishing`` has not ended). ``dry_run`` checks what can be checked without
  submitting anything.
- Results are JSON objects with bounded sizes; ``caps`` in each says which bounds applied.
- Rejected rows are read from the job's ``bad_rows`` Parquet artifact with HTTP range requests of
  the presigned URL the gateway hands out (:mod:`forklift_mcp_server.parquet`), never whole.
"""

from __future__ import annotations

import io
import json
import logging
import time
import uuid
from typing import Annotated, Any, Callable, Dict, List, Literal, Mapping, Optional, Tuple, Union

import anyio
from forklift_client import (
    TERMINAL_STATUSES,
    ApiError,
    Artifact,
    Dataset,
    Forbidden,
    ForkliftClient,
    Job,
    Page,
    Schema,
    TransportError,
)
from mcp.server.mcpserver import Context
from pydantic import Field

from . import explain, views
from .auth import bearer_token
from .docs import URI_PREFIX, schema_pages
from .gateway import Gateway, ToolInputError, describe, tool_errors
from .parquet import MAX_READ_BYTES, REASON_COLUMN, BadRows, RangeFile, RangeReader, StoreReadError
from .values import (
    MAX_LIST_ITEMS,
    MAX_LISTED_COLUMNS,
    bounded,
    bounded_document,
    bounded_findings,
    bounded_list,
    bounded_warnings,
    split_finding,
    summarize_schema,
    text,
    top,
)

log = logging.getLogger("forklift_mcp_server")

MIN_BYTES = 1024
DEFAULT_BYTES = 65536
MAX_BYTES = 1048576
DEFAULT_PREVIEW_ROWS = 20
MAX_PREVIEW_ROWS = 500
DEFAULT_SAMPLE_ROWS = 1000
MAX_SAMPLE_ROWS = 100_000
DEFAULT_BAD_ROWS = 20
MAX_BAD_ROWS = 200
DEFAULT_EXPLAIN_ROWS = 10
MAX_EXPLAIN_ROWS = 50
#: Rejected rows of a validate_schema sample shown in its result
VALIDATE_REJECTED_ROWS = 10
#: Columns of one row view (bad rows)
MAX_COLUMNS = 100
#: Rejection reasons in a result (the most frequent)
MAX_REASONS = 20
#: Items of one list page in a result (the API's pages go up to 500)
MAX_PAGE = 100
DEFAULT_PAGE = 50
#: Records looked through to find one by name (datasets, schemas)
MAX_NAME_SCAN = 5000
#: Largest JSON artifact (a generated schema, a validation report, a preview) read
MAX_JSON_ARTIFACT_BYTES = 8 * 1024 * 1024
#: Largest text upload_csv accepts (bytes of UTF-8)
MAX_UPLOAD_BYTES = 1024 * 1024
#: Default waits of the tools that submit a job, in seconds (at most the server's maximum)
DEFAULT_WAIT_SECONDS = 30.0
JOB_SCOPE = "jobs:run"

_UPLOAD_ID = Field(
    description="The id of a complete upload of yours: the input file (upload_csv makes one "
    "from CSV text)"
)
_DATASET = Field(
    description="A dataset's id or name: its source, format, options and schema (give upload_id "
    "too for a dataset that reads uploads)"
)
_FORMAT = Field(
    description="The upload's format (default csv); a dataset has its own (leave it out)"
)
_INPUT_OPTIONS = Field(
    description="How to read the input, as the job contract's input.options: CSV encoding, "
    "delimiter, quote_char, escape_char, header_mode (present|absent|auto), header_search_rows, "
    "skip_blank_lines, comment_rows, footer_detection, excess_column_mode; Excel sheet, "
    "values_only, engine, date_system. For a dataset they are merged over its own"
)
_SCHEMA = Field(description="A forklift JSON schema as an object (or give stored_schema)")
_STORED_SCHEMA = Field(description="A stored schema's id or name, instead of schema")
_SCHEMA_VERSION = Field(ge=1, description="The stored schema's version number (default: latest)")
_DRY_RUN = Field(
    description="Check what can be checked (the input, the schema, the token's scopes) and say "
    "what would be submitted; submit nothing"
)
_JOB_ID = Field(description="The id of a job (from run_job, preview, validate_schema, ...)")
_PAGE_LIMIT = Field(ge=1, le=MAX_PAGE, description=f"Items to list, 1 to {MAX_PAGE}")
_OFFSET = Field(ge=0, description="Items to skip (0-based)")
_SAMPLE_ROWS = Field(
    ge=1, le=MAX_SAMPLE_ROWS, description=f"Rows of the input to analyse, 1 to {MAX_SAMPLE_ROWS}"
)


def _bytes(what: str) -> Any:
    return Field(
        ge=MIN_BYTES,
        le=MAX_BYTES,
        description=f"Largest {what} in the result in bytes (compact JSON), {MIN_BYTES} to "
        f"{MAX_BYTES}",
    )


def _wait(default: float) -> Any:
    return Field(
        ge=0,
        le=600,
        description=f"Seconds to wait for the job to end (default {default:g}; at most the "
        "server's FORKLIFT_MCP_MAX_WAIT_SECONDS). A job still queued or running then is "
        "returned with its id: follow it with get_job",
    )


Format = Optional[Literal["csv", "excel"]]


class Toolbox:
    """The tools, bound to one gateway; each call uses the token of its own request."""

    def __init__(
        self, gateway: Gateway, *, poll_interval: float = 0.5, max_poll_interval: float = 5.0
    ):
        self.gateway = gateway
        self.max_wait = gateway.settings.max_wait_seconds
        self.poll_interval = poll_interval
        self.max_poll_interval = max_poll_interval

    # ================================================================== catalog

    @tool_errors
    async def list_connections(
        self,
        ctx: Context,
        kind: Annotated[
            Optional[Literal["s3", "localfs", "sql"]], Field(description="Only this kind")
        ] = None,
        limit: Annotated[int, _PAGE_LIMIT] = DEFAULT_PAGE,
        offset: Annotated[int, _OFFSET] = 0,
    ) -> dict[str, Any]:
        """The connections (sources and destinations) the caller may use."""
        client = self._client(ctx)
        page = await self._call(client.list_connections, kind=kind, limit=limit, offset=offset)
        return {
            "connections": [views.connection_view(item) for item in page.items],
            **views.page_view(page),
            "caps": {"limit": limit, "max_limit": MAX_PAGE, "text_chars": 1000},
        }

    @tool_errors
    async def list_datasets(
        self,
        ctx: Context,
        limit: Annotated[int, _PAGE_LIMIT] = DEFAULT_PAGE,
        offset: Annotated[int, _OFFSET] = 0,
    ) -> dict[str, Any]:
        """The datasets the caller can see, by name."""
        client = self._client(ctx)
        page = await self._call(client.list_datasets, limit=limit, offset=offset)
        return {
            "datasets": [views.dataset_view(item) for item in page.items],
            **views.page_view(page),
            "caps": {"limit": limit, "max_limit": MAX_PAGE, "text_chars": 1000},
        }

    @tool_errors
    async def get_schema(
        self,
        ctx: Context,
        schema: Annotated[
            Optional[str], Field(description="A stored schema's id or name; leave out to list")
        ] = None,
        version: Annotated[
            Optional[int], Field(ge=1, description="Its version number (default: the latest)")
        ] = None,
        dataset: Annotated[
            Optional[str],
            Field(description="A dataset's id or name: the schema version it runs (or schema)"),
        ] = None,
        max_bytes: Annotated[int, _bytes("schema document")] = DEFAULT_BYTES,
        limit: Annotated[int, _PAGE_LIMIT] = DEFAULT_PAGE,
        offset: Annotated[int, _OFFSET] = 0,
    ) -> dict[str, Any]:
        """A stored schema version (summary and document), or the list of stored schemas."""
        client = self._client(ctx)
        if schema is not None and dataset is not None:
            raise ToolInputError("give schema or dataset, not both")
        if schema is None and dataset is None:
            if version is not None:
                raise ToolInputError("version needs schema (the stored schema it is a version of)")
            page = await self._call(client.list_schemas, limit=limit, offset=offset)
            return {
                "schemas": [views.schema_view(item) for item in page.items],
                **views.page_view(page),
                "next": "get_schema with schema=<name or id> shows one with its document",
                "caps": {"limit": limit, "max_limit": MAX_PAGE},
            }
        dataset_name = None
        if dataset is not None:
            if version is not None:
                raise ToolInputError("a dataset runs one schema version: leave version out")
            found = await self._dataset(client, dataset)
            dataset_name = found.name
            stored = await self._call(client.get_schema, found.schema_id)
            number = found.schema_version
        else:
            stored = await self._schema(client, schema)
            number = version if version is not None else _latest(stored)
        document_version = await self._call(client.get_schema_version, stored.id, number)
        document = document_version.document
        return {
            "schema": views.schema_view(stored),
            "version": {
                "id": document_version.id,
                "number": document_version.number,
                "sha256": document_version.sha256,
                "author_id": document_version.author_id,
                "created_at": document_version.created_at.isoformat(),
                "notes": text(document_version.notes),
            },
            "dataset": dataset_name,
            "summary": summarize_schema(document),
            **bounded_document(document, max_bytes),
            "caps": {"max_bytes": max_bytes, "listed_columns": MAX_LISTED_COLUMNS},
        }

    # ================================================================== interactive jobs

    @tool_errors
    async def generate_schema(
        self,
        ctx: Context,
        upload_id: Annotated[Optional[str], _UPLOAD_ID] = None,
        dataset: Annotated[Optional[str], _DATASET] = None,
        format: Annotated[Format, _FORMAT] = None,
        input_options: Annotated[Optional[Dict[str, Any]], _INPUT_OPTIONS] = None,
        sample_rows: Annotated[int, _SAMPLE_ROWS] = DEFAULT_SAMPLE_ROWS,
        infer_primary_key: Annotated[
            bool, Field(description="Propose x-primaryKey from columns unique in the sample")
        ] = False,
        wait_seconds: Annotated[float, _wait(DEFAULT_WAIT_SECONDS)] = DEFAULT_WAIT_SECONDS,
        dry_run: Annotated[bool, _DRY_RUN] = False,
        max_bytes: Annotated[int, _bytes("schema document")] = DEFAULT_BYTES,
    ) -> dict[str, Any]:
        """Infer a schema from the first rows of an upload or a dataset's source."""
        client = self._client(ctx)
        options = {"sample_rows": sample_rows, "infer_primary_key": infer_primary_key}
        request = await self._job_request(
            client, "generate_schema", upload_id, dataset, format, input_options, options
        )
        return await self._submit_and_view(
            client, request, dry_run, wait_seconds, max_bytes=max_bytes
        )

    @tool_errors
    async def validate_schema(
        self,
        ctx: Context,
        upload_id: Annotated[Optional[str], _UPLOAD_ID] = None,
        dataset: Annotated[Optional[str], _DATASET] = None,
        schema: Annotated[Optional[Dict[str, Any]], _SCHEMA] = None,
        stored_schema: Annotated[Optional[str], _STORED_SCHEMA] = None,
        schema_version: Annotated[Optional[int], _SCHEMA_VERSION] = None,
        format: Annotated[Format, _FORMAT] = None,
        input_options: Annotated[Optional[Dict[str, Any]], _INPUT_OPTIONS] = None,
        sample_rows: Annotated[int, _SAMPLE_ROWS] = DEFAULT_SAMPLE_ROWS,
        apply_schema_extensions: Annotated[
            bool,
            Field(
                description="Apply the schema's x-... extensions (false: only types, nulls and "
                "required)"
            ),
        ] = True,
        wait_seconds: Annotated[float, _wait(DEFAULT_WAIT_SECONDS)] = DEFAULT_WAIT_SECONDS,
        dry_run: Annotated[bool, _DRY_RUN] = False,
    ) -> dict[str, Any]:
        """Check a schema against an upload (CSV) or a dataset's source, as the import would."""
        client = self._client(ctx)
        options = {"sample_rows": sample_rows, "apply_schema_extensions": apply_schema_extensions}
        request = await self._job_request(
            client, "validate_schema", upload_id, dataset, format, input_options, options
        )
        await self._with_schema(client, request, schema, stored_schema, schema_version)
        return await self._submit_and_view(client, request, dry_run, wait_seconds)

    @tool_errors
    async def preview(
        self,
        ctx: Context,
        upload_id: Annotated[Optional[str], _UPLOAD_ID] = None,
        dataset: Annotated[Optional[str], _DATASET] = None,
        rows: Annotated[
            int,
            Field(ge=1, le=MAX_PREVIEW_ROWS, description=f"Rows to show, 1 to {MAX_PREVIEW_ROWS}"),
        ] = DEFAULT_PREVIEW_ROWS,
        format: Annotated[Format, _FORMAT] = None,
        input_options: Annotated[Optional[Dict[str, Any]], _INPUT_OPTIONS] = None,
        max_bytes: Annotated[int, _bytes("rows")] = DEFAULT_BYTES,
        wait_seconds: Annotated[float, _wait(DEFAULT_WAIT_SECONDS)] = DEFAULT_WAIT_SECONDS,
        dry_run: Annotated[bool, _DRY_RUN] = False,
    ) -> dict[str, Any]:
        """The first rows of an upload or a dataset's source, as text as the import reads them."""
        client = self._client(ctx)
        options = {"preview_rows": rows, "preview_max_bytes": max_bytes}
        request = await self._job_request(
            client, "preview", upload_id, dataset, format, input_options, options
        )
        return await self._submit_and_view(
            client, request, dry_run, wait_seconds, max_bytes=max_bytes
        )

    # ================================================================== runs

    @tool_errors
    async def run_job(
        self,
        ctx: Context,
        upload_id: Annotated[Optional[str], _UPLOAD_ID] = None,
        dataset: Annotated[Optional[str], _DATASET] = None,
        schema: Annotated[
            Optional[Dict[str, Any]],
            Field(
                description="A forklift JSON schema as an object (uploads only; a dataset runs "
                "its own schema version)"
            ),
        ] = None,
        stored_schema: Annotated[Optional[str], _STORED_SCHEMA] = None,
        schema_version: Annotated[Optional[int], _SCHEMA_VERSION] = None,
        format: Annotated[Format, _FORMAT] = None,
        input_options: Annotated[Optional[Dict[str, Any]], _INPUT_OPTIONS] = None,
        options: Annotated[
            Optional[Dict[str, Any]],
            Field(
                description="The job contract's options: apply_schema_extensions (true), "
                "batch_size (10000), include_value_statistics (false: the output metadata holds "
                "no cell values)"
            ),
        ] = None,
        limits: Annotated[
            Optional[Dict[str, Union[int, float]]],
            Field(
                description="max_rows, max_input_bytes, max_seconds (each optional; at most the "
                "lane's limits); exceeding one fails the run with LIMIT_EXCEEDED"
            ),
        ] = None,
        compression: Annotated[
            Optional[str],
            Field(description="Parquet compression (default snappy, or the dataset's)"),
        ] = None,
        classification: Annotated[
            Optional[Literal["public", "internal", "sensitive"]],
            Field(description="Uploads only: the job's classification (at least the upload's)"),
        ] = None,
        idempotency_key: Annotated[
            Optional[str],
            Field(
                pattern=r"^[!-~]{1,255}$",
                description="Give the key of an earlier call to get the job it started instead "
                "of starting another (for example after a timeout); 1 to 255 printable ASCII "
                "characters without spaces; default: a new key",
            ),
        ] = None,
        wait_seconds: Annotated[float, _wait(DEFAULT_WAIT_SECONDS)] = DEFAULT_WAIT_SECONDS,
        dry_run: Annotated[bool, _DRY_RUN] = False,
    ) -> dict[str, Any]:
        """Apply a schema: run the import of an upload or a dataset on the service's workers."""
        client = self._client(ctx)
        request = await self._job_request(
            client, "run", upload_id, dataset, format, input_options, options or {}
        )
        if dataset is not None and (schema is not None or stored_schema is not None):
            raise ToolInputError(
                f"a run of dataset {request.dataset.name!r} uses the dataset's schema version "
                "(and its format); to run another schema, run_job with upload_id, or "
                "validate_schema the draft against the dataset"
            )
        if dataset is not None and classification is not None:
            raise ToolInputError(
                f"jobs of dataset {request.dataset.name!r} have its classification "
                f"({request.dataset.classification}); leave classification out"
            )
        await self._with_schema(client, request, schema, stored_schema, schema_version)
        request.body.update(
            limits=dict(limits or {}), compression=compression, classification=classification
        )
        if idempotency_key is not None:
            request.key = idempotency_key
        return await self._submit_and_view(client, request, dry_run, wait_seconds)

    @tool_errors
    async def get_job(
        self,
        ctx: Context,
        job_id: Annotated[str, _JOB_ID],
        wait_seconds: Annotated[float, _wait(0)] = 0,
        max_bytes: Annotated[int, _bytes("rows or document")] = DEFAULT_BYTES,
    ) -> dict[str, Any]:
        """A job: its status, counts, findings, warnings and files; for a finished preview,
        validate_schema or generate_schema job also what the tool that submitted it returns."""
        client = self._client(ctx)
        job = await self._call(client.get_job, _job_ref(job_id))
        job = await self._wait_for(client, job, wait_seconds)
        return await self._job_result(client, job, wait_seconds, max_bytes)

    @tool_errors
    async def explain_failure(
        self,
        ctx: Context,
        job_id: Annotated[str, _JOB_ID],
        rows: Annotated[
            int,
            Field(
                ge=1,
                le=MAX_EXPLAIN_ROWS,
                description=f"Rejected rows to show, 1 to {MAX_EXPLAIN_ROWS}",
            ),
        ] = DEFAULT_EXPLAIN_ROWS,
        max_bytes: Annotated[int, _bytes("rows")] = DEFAULT_BYTES,
    ) -> dict[str, Any]:
        """Why a job failed (or rejected rows): its error with what it means and what to try,
        findings by rule, the reasons rows were rejected, the first rejected rows and the
        documentation pages that apply."""
        client = self._client(ctx)
        job = await self._call(client.get_job, _job_ref(job_id))
        result = job.result or {}
        caps = {
            "rows": rows,
            "max_rows": MAX_EXPLAIN_ROWS,
            "max_bytes": max_bytes,
            "columns": MAX_COLUMNS,
            "reasons": MAX_REASONS,
            "findings": 100,
            "warnings": MAX_LIST_ITEMS,
            "max_read_bytes": MAX_READ_BYTES,
        }
        if job.status not in TERMINAL_STATUSES:
            return {
                "job": views.job_view(job),
                "summary": f"Job {job.id} is still {job.status}: there is nothing to explain yet.",
                "next": _follow(job),
                "caps": caps,
            }
        pages = {page.name for page in schema_pages()}
        docs: List[str] = []
        artifacts = await self._call(client.list_artifacts, job.id)
        bad = _bad_rows_artifacts(artifacts)
        rejected = None
        if bad:
            rejected = await self._rejected(client, bad[0], rows, max_bytes)
            for reason in rejected.get("reasons", []):
                meaning = explain.REASON_MEANINGS.get(reason["rule"])
                if meaning:
                    reason["meaning"] = meaning
                docs.extend(explain.RULE_PAGES.get(reason["rule"], []))
            if len(bad) > 1:
                rejected["other_bad_rows"] = [views.artifact_view(item) for item in bad[1:10]]
        summary = result.get("validation_summary")
        findings = [
            dict(split_finding(key), key=key, count=count) for key, count in top(summary, 100)
        ]
        for finding in findings:
            docs.extend(explain.RULE_PAGES.get(finding["rule"], []))
        error = None
        if job.error is not None:
            meaning, steps, error_pages = explain.ERROR_HELP.get(
                job.error.code, explain.UNKNOWN_ERROR
            )
            error = {
                "code": job.error.code,
                "message": text(job.error.message, 8000),
                "retryable": (result.get("error") or {}).get("retryable"),
                "meaning": meaning,
                "next_steps": steps,
            }
            docs = list(error_pages) + docs
        return {
            "job": views.job_view(job),
            "summary": _failure_summary(job, result, rejected),
            "error": error,
            "counts": bounded(result.get("counts") or {}),
            "findings": findings,
            "findings_total": len(summary) if isinstance(summary, dict) else 0,
            "rejected": rejected,
            **bounded_warnings(result.get("warnings")),
            "docs": [URI_PREFIX + name for name in dict.fromkeys(docs) if name in pages],
            "caps": caps,
        }

    @tool_errors
    async def get_bad_rows(
        self,
        ctx: Context,
        job_id: Annotated[str, _JOB_ID],
        offset: Annotated[int, Field(ge=0, description="Index of the first row (0-based)")] = 0,
        limit: Annotated[
            int, Field(ge=1, le=MAX_BAD_ROWS, description=f"Rows to show, 1 to {MAX_BAD_ROWS}")
        ] = DEFAULT_BAD_ROWS,
        columns: Annotated[
            Optional[List[str]],
            Field(
                description=f"Columns to show (default: the first {MAX_COLUMNS}, with "
                "_rejection_reason); at most that many"
            ),
        ] = None,
        max_bytes: Annotated[int, _bytes("rows")] = DEFAULT_BYTES,
        artifact_id: Annotated[
            Optional[str],
            Field(
                description="Which bad_rows artifact, when the job has several (default: the "
                "first of its latest attempt)"
            ),
        ] = None,
    ) -> dict[str, Any]:
        """Rows a job rejected (its bad_rows artifact: the input's text plus _rejection_reason),
        from offset on, at most limit rows and max_bytes bytes; next_offset continues."""
        client = self._client(ctx)
        caps = {
            "limit": limit,
            "max_limit": MAX_BAD_ROWS,
            "max_bytes": max_bytes,
            "columns": MAX_COLUMNS,
            "max_cell_chars": 2000,
            "max_read_bytes": MAX_READ_BYTES,
        }
        if columns is not None and len(columns) > MAX_COLUMNS:
            raise ToolInputError(f"columns: at most {MAX_COLUMNS} columns can be selected")
        job = await self._call(client.get_job, _job_ref(job_id))
        artifacts = await self._call(client.list_artifacts, job.id)
        bad = _bad_rows_artifacts(artifacts)
        if artifact_id is not None:
            bad = [item for item in bad if item.id == artifact_id]
            if not bad:
                raise ToolInputError(
                    f"job {job.id} has no bad_rows artifact with id {artifact_id!r}"
                )
        if not bad:
            return {
                "job": views.job_view(job),
                "artifact": None,
                "total_rows": 0,
                "rows": [],
                "row_count": 0,
                "message": _no_bad_rows(job),
                "caps": caps,
            }

        def read(bad_rows: BadRows) -> Dict[str, Any]:
            if columns is not None:
                unknown = [name for name in columns if name not in bad_rows.columns]
                if unknown:
                    available = ", ".join(map(repr, bad_rows.columns[:MAX_LISTED_COLUMNS]))
                    raise ToolInputError(
                        f"columns {', '.join(map(repr, unknown[:10]))} are not in the bad rows "
                        f"(its columns: {available})"
                    )
                selected = list(dict.fromkeys(columns))
            else:
                selected = _default_columns(bad_rows.columns)
            return {
                "total_rows": bad_rows.total_rows,
                "offset": offset,
                "columns": selected,
                "columns_total": len(bad_rows.columns),
                "columns_truncated": len(selected) < len(bad_rows.columns),
                **bad_rows.rows(selected, offset, limit, max_bytes),
            }

        try:
            view = await self._read_bad_rows(client, bad[0], read)
        except StoreReadError as error:
            raise ToolInputError(str(error)) from None
        return {
            "job": views.job_view(job),
            "artifact": views.artifact_view(bad[0]),
            **view,
            "other_bad_rows": [views.artifact_view(item) for item in bad[1:10]],
            "caps": caps,
        }

    # ================================================================== uploads

    @tool_errors
    async def upload_csv(
        self,
        ctx: Context,
        filename: Annotated[
            str,
            Field(
                min_length=1, max_length=255, description="A name for the file, e.g. orders.csv"
            ),
        ],
        content: Annotated[
            str,
            Field(
                description=f"The file's text (CSV, or another delimited text), at most "
                f"{MAX_UPLOAD_BYTES} bytes as UTF-8"
            ),
        ],
        classification: Annotated[
            Optional[Literal["public", "internal", "sensitive"]],
            Field(description="How sensitive the data is (default: the installation's)"),
        ] = None,
        dry_run: Annotated[
            bool, Field(description="Check the size and the token's scopes; upload nothing")
        ] = False,
    ) -> dict[str, Any]:
        """Upload a small text file (inline CSV) as an upload of yours, for the other tools."""
        client = self._client(ctx)
        data = content.encode("utf-8")
        if len(data) > MAX_UPLOAD_BYTES:
            raise ToolInputError(
                f"content is {len(data)} bytes as UTF-8, more than the {MAX_UPLOAD_BYTES} "
                "upload_csv takes; upload larger files with the forklift UI, forklift-client "
                "(upload_file) or POST /api/v1/uploads"
            )
        caps = {"max_upload_bytes": MAX_UPLOAD_BYTES}
        if dry_run:
            return {
                "dry_run": True,
                "would_upload": {
                    "filename": filename,
                    "bytes": len(data),
                    "classification": classification,
                },
                **await self._scopes(client, "uploads:write"),
                "caps": caps,
            }
        upload = await self._call(
            _upload_bytes, client, data, filename=filename, classification=classification
        )
        return {
            "dry_run": False,
            "upload": views.upload_view(upload),
            "next": f"preview, generate_schema, validate_schema or run_job with upload_id "
            f"{upload.id!r} (format csv)",
            "caps": caps,
        }

    # ================================================================== helpers

    def _client(self, ctx: Context) -> ForkliftClient:
        token = bearer_token(ctx.headers)
        if token is None:  # the gate refuses such requests before any tool runs
            raise ToolInputError("this request carries no API token (Authorization: Bearer ...)")
        return self.gateway.client(token)

    async def _call(self, function: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        return await self.gateway.call(function, *args, **kwargs)

    async def _wait_for(self, client: ForkliftClient, job: Job, wait_seconds: float) -> Job:
        """Poll ``job`` until it has ended or ``wait_seconds`` (at most the server's maximum)
        have passed; transient gateway errors are polled through."""
        deadline = time.monotonic() + min(wait_seconds, self.max_wait)
        interval = self.poll_interval
        while job.status not in TERMINAL_STATUSES:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            await anyio.sleep(min(interval, remaining))
            interval = min(interval * 1.5, self.max_poll_interval)
            try:
                job = await self._call(client.get_job, job.id)
            except (TransportError, ApiError) as error:
                if isinstance(error, ApiError) and not error.transient:
                    raise
                log.info("Waiting for job %s: %s; polling on", job.id, error)
        return job

    async def _dataset(self, client: ForkliftClient, ref: str) -> Dataset:
        if _is_uuid(ref):
            return await self._call(client.get_dataset, ref.strip())
        return await self._by_name(client.list_datasets, ref, "dataset", "list_datasets")

    async def _schema(self, client: ForkliftClient, ref: str) -> Schema:
        if _is_uuid(ref):
            return await self._call(client.get_schema, ref.strip())
        return await self._by_name(client.list_schemas, ref, "schema", "get_schema")

    async def _by_name(self, lister: Callable[..., Page], name: str, what: str, tool: str):
        offset = 0
        while True:
            page = await self._call(lister, limit=500, offset=offset)
            for item in page.items:
                if item.name == name:
                    return item
            if not page.has_more:
                raise ToolInputError(
                    f"there is no {what} named {name!r} that this token can see; {tool} lists "
                    "them (or give its id)"
                )
            offset = page.next_offset
            if offset >= MAX_NAME_SCAN:
                raise ToolInputError(
                    f"no {what} named {name!r} among the first {MAX_NAME_SCAN}; give its id"
                )

    async def _job_request(
        self,
        client: ForkliftClient,
        kind: str,
        upload_id: Optional[str],
        dataset: Optional[str],
        format: Optional[str],
        input_options: Optional[Dict[str, Any]],
        options: Dict[str, Any],
    ) -> "_JobRequest":
        if upload_id is None and dataset is None:
            raise ToolInputError(
                "give upload_id (an upload of yours; upload_csv makes one from CSV text) or "
                "dataset (a dataset's id or name; list_datasets lists them)"
            )
        if upload_id is not None and not _is_uuid(upload_id):
            raise ToolInputError(f"upload_id must be an upload's id (a UUID), not {upload_id!r}")
        found = await self._dataset(client, dataset) if dataset is not None else None
        if found is not None:
            if format is not None and format != found.input_format:
                raise ToolInputError(
                    f"dataset {found.name!r} reads {found.input_format}; leave format out"
                )
            if found.source_connection_id is None and upload_id is None:
                raise ToolInputError(
                    f"dataset {found.name!r} reads uploads: give the upload_id of the file to use"
                )
            if found.source_connection_id is not None and upload_id is not None:
                raise ToolInputError(
                    f"dataset {found.name!r} reads its source connection; leave upload_id out"
                )
        body: Dict[str, Any] = {
            "dataset_id": found.id if found is not None else None,
            "upload_id": upload_id.strip() if upload_id is not None else None,
            "format": None if found is not None else (format or "csv"),
            "input_options": dict(input_options or {}),
            "options": dict(options),
        }
        return _JobRequest(kind, body, found)

    async def _with_schema(
        self,
        client: ForkliftClient,
        request: "_JobRequest",
        schema: Optional[Dict[str, Any]],
        stored_schema: Optional[str],
        schema_version: Optional[int],
    ) -> None:
        """Put the schema into the request: inline, a stored version, or (datasets) theirs."""
        if schema is not None and stored_schema is not None:
            raise ToolInputError("give the schema inline (schema) or stored_schema, not both")
        if schema_version is not None and stored_schema is None:
            raise ToolInputError("schema_version needs stored_schema (the schema it belongs to)")
        if schema is not None:
            request.body["schema"] = schema
            request.schema = {"inline": True, "columns": summarize_schema(schema)["column_count"]}
        elif stored_schema is not None:
            stored = await self._schema(client, stored_schema)
            number = schema_version if schema_version is not None else _latest(stored)
            version = await self._call(client.get_schema_version, stored.id, number)
            request.body["schema_version_id"] = version.id
            request.schema = {"name": stored.name, "version": number, "id": version.id}
        elif request.dataset is not None:
            request.schema = {"dataset": request.dataset.name}
        elif request.kind in ("run", "validate_schema"):
            raise ToolInputError(
                f"{'run_job' if request.kind == 'run' else 'validate_schema'} needs a schema: "
                "give schema (inline) or stored_schema (a stored schema's name or id)"
            )

    async def _scopes(self, client: ForkliftClient, scope: str) -> Dict[str, Any]:
        me = await self._call(client.me)
        return {"user": me.user.username, "scope": scope, "has_scope": scope in me.scopes}

    async def _submit_and_view(
        self,
        client: ForkliftClient,
        request: "_JobRequest",
        dry_run: bool,
        wait_seconds: float,
        *,
        max_bytes: int = DEFAULT_BYTES,
    ) -> Dict[str, Any]:
        if dry_run:
            if request.body["upload_id"] is not None:
                upload = await self._call(client.get_upload, request.body["upload_id"])
                if upload.status != "complete":
                    raise ToolInputError(
                        f"upload {upload.id} is {upload.status}; only complete uploads can be "
                        "job inputs"
                    )
            body = {key: value for key, value in request.body.items() if key != "schema"}
            return {
                "dry_run": True,
                "would_submit": {
                    "endpoint": "POST /api/v1/jobs",
                    "idempotency_key": request.key,
                    "kind": request.kind,
                    **body,
                    "schema": request.schema,
                },
                **await self._scopes(client, JOB_SCOPE),
                "checked": "the input (and the schema version) exist and are visible to this "
                "token; the gateway checks the rest (permissions, limits, the spec) on submit",
                "caps": {"max_wait_seconds": self.max_wait},
            }
        job = await self._call(
            client.submit_job, request.kind, idempotency_key=request.key, **request.body
        )
        job = await self._wait_for(client, job, wait_seconds)
        view = await self._job_result(client, job, wait_seconds, max_bytes)
        view.update(dry_run=False, idempotency_key=request.key, schema=request.schema)
        return view

    async def _job_result(
        self, client: ForkliftClient, job: Job, wait_seconds: float, max_bytes: int
    ) -> Dict[str, Any]:
        """A job as the tools return it: its state and, once it has ended, what it produced."""
        result = job.result or {}
        view: Dict[str, Any] = {
            "finished": job.status in TERMINAL_STATUSES,
            "job": views.job_view(job),
            "counts": bounded(result.get("counts") or {}),
            "schema_extensions": bounded_list(result.get("schema_extensions") or [], 100),
            **bounded_findings(result.get("validation_summary")),
            **bounded_warnings(result.get("warnings")),
        }
        caps = {
            "wait_seconds": min(wait_seconds, self.max_wait),
            "max_wait_seconds": self.max_wait,
            "findings": 100,
            "warnings": MAX_LIST_ITEMS,
            "artifacts": MAX_LIST_ITEMS,
        }
        if job.status not in TERMINAL_STATUSES:
            view.update(next=_follow(job), caps=caps)
            return view
        artifacts = await self._call(client.list_artifacts, job.id)
        view["artifacts"] = [views.artifact_view(item) for item in artifacts[:MAX_LIST_ITEMS]]
        view["artifacts_total"] = len(artifacts)
        if job.error is not None:
            meaning, steps, _ = explain.ERROR_HELP.get(job.error.code, explain.UNKNOWN_ERROR)
            view["error"] = {
                "code": job.error.code,
                "message": text(job.error.message, 8000),
                "meaning": meaning,
                "next_steps": steps,
            }
        else:
            view["error"] = None
        if job.kind == "generate_schema":
            view.update(await self._generated(client, artifacts, max_bytes))
            caps.update(max_bytes=max_bytes, listed_columns=MAX_LISTED_COLUMNS)
        elif job.kind == "validate_schema":
            view.update(await self._report(client, job, artifacts))
            caps.update(
                listed_columns=MAX_LISTED_COLUMNS,
                rejected_rows=VALIDATE_REJECTED_ROWS,
                max_bytes=DEFAULT_BYTES,
                columns=MAX_COLUMNS,
                reasons=MAX_REASONS,
                max_read_bytes=MAX_READ_BYTES,
            )
        elif job.kind == "preview":
            try:
                view.update(await self._preview_rows(client, artifacts))
            except Forbidden as error:  # someone else's preview of sensitive data
                view.update(mode="raw", rows=[], row_count=0, rows_unavailable=describe(error))
            caps.update(max_bytes=max_bytes, max_cell_chars=2000)
        view["next"] = _next_after(job, artifacts)
        view["caps"] = caps
        return view

    async def _json_artifact(
        self, client: ForkliftClient, artifacts: List[Artifact], kind: str
    ) -> Optional[Dict[str, Any]]:
        found = [item for item in artifacts if item.kind == kind and item.deleted_at is None]
        if not found:
            return None
        artifact = found[-1]  # the latest attempt's
        if artifact.bytes > MAX_JSON_ARTIFACT_BYTES:
            raise ToolInputError(
                f"the {kind} artifact {artifact.name!r} of job {artifact.job_id} is "
                f"{artifact.bytes} bytes, more than the {MAX_JSON_ARTIFACT_BYTES} this server "
                f"reads; download it (GET /api/v1/artifacts/{artifact.id}/download)"
            )

        def read() -> bytes:
            with client.open_artifact(artifact) as stream:
                return stream.read()

        data = await self._call(read)
        try:
            document = json.loads(data)
        except ValueError:
            raise ToolInputError(
                f"the {kind} artifact {artifact.name!r} of job {artifact.job_id} is not JSON"
            ) from None
        if not isinstance(document, dict):
            raise ToolInputError(
                f"the {kind} artifact {artifact.name!r} of job {artifact.job_id} is not a JSON "
                "object"
            )
        return document

    async def _generated(
        self, client: ForkliftClient, artifacts: List[Artifact], max_bytes: int
    ) -> Dict[str, Any]:
        schema = await self._json_artifact(client, artifacts, "schema")
        if schema is None:
            return {"summary": None, "document": None}
        return {
            "summary": summarize_schema(schema),
            **bounded_document(schema, max_bytes),
            "note": "The schema is not stored: pass it as schema to validate_schema, preview "
            "and run_job, or save it in the schema editor (POST /api/v1/schemas)",
        }

    async def _report(
        self, client: ForkliftClient, job: Job, artifacts: List[Artifact]
    ) -> Dict[str, Any]:
        report = await self._json_artifact(client, artifacts, "report") or {}
        bad = _bad_rows_artifacts(artifacts)
        rejected = None
        if bad:
            rejected = await self._rejected(client, bad[0], VALIDATE_REJECTED_ROWS, DEFAULT_BYTES)
        lists = {
            name: bounded_list(report.get(name))
            for name in (
                "columns",
                "schema_columns",
                "required_columns",
                "columns_not_in_schema",
                "schema_columns_not_in_input",
            )
        }
        return {
            "valid": bool(report.get("valid")) and job.status == "succeeded",
            **lists,
            "sample_rows": report.get("sample_rows"),
            "rejected_rows": rejected,
        }

    async def _preview_rows(
        self, client: ForkliftClient, artifacts: List[Artifact]
    ) -> Dict[str, Any]:
        preview = await self._json_artifact(client, artifacts, "preview") or {}
        view = {
            "mode": "raw",
            "columns": bounded_list(preview.get("columns", [])),
            "rows": preview.get("rows", []),
            "row_count": preview.get("row_count", 0),
            "truncated": preview.get("truncated", False),
            "limit": preview.get("limit"),
            "truncated_cells": preview.get("truncated_cells", 0),
        }
        if "sheet" in preview:
            view["sheet"] = preview["sheet"]
        return view

    async def _read_bad_rows(
        self, client: ForkliftClient, artifact: Artifact, read: Callable[[BadRows], Dict[str, Any]]
    ) -> Dict[str, Any]:
        """``read(bad_rows)`` on the artifact, in a worker thread, through range requests."""
        if artifact.deleted_at is not None:
            raise ToolInputError(
                f"the bad_rows artifact {artifact.name!r} of job {artifact.job_id} was deleted by "
                f"retention on {artifact.deleted_at:%Y-%m-%d}"
            )
        settings = self.gateway.settings

        def sample() -> Dict[str, Any]:
            reader = RangeReader(
                lambda: client.get_download_url(artifact).url,
                artifact.bytes,
                what=f"bad_rows artifact {artifact.name!r} of job {artifact.job_id}",
                timeout=settings.api_timeout,
                ca_file=settings.ca_file,
            )
            file = RangeFile(reader, MAX_READ_BYTES)
            view = read(BadRows(file, f"/api/v1/artifacts/{artifact.id}/download"))
            view.update(bytes_read=file.fetched, range_requests=reader.requests)
            return view

        return await self._call(sample)

    async def _rejected(
        self, client: ForkliftClient, artifact: Artifact, rows: int, max_bytes: int
    ) -> Dict[str, Any]:
        """The first rejected rows and the reasons, or why they cannot be shown."""

        def read(bad_rows: BadRows) -> Dict[str, Any]:
            selected = _default_columns(bad_rows.columns)
            first = bad_rows.rows(selected, 0, rows, max_bytes)
            first.pop("next_offset")
            reasons = bad_rows.reasons(MAX_REASONS)
            return {
                "total_rows": bad_rows.total_rows,
                "reasons": reasons["reasons"],
                "reasons_scanned_rows": reasons["scanned_rows"],
                "distinct_reasons": reasons["distinct"],
                "first_rows": {
                    "columns": selected,
                    "columns_total": len(bad_rows.columns),
                    **first,
                },
            }

        base = {"artifact": views.artifact_view(artifact)}
        try:
            return {**base, **await self._read_bad_rows(client, artifact, read)}
        except (Forbidden, StoreReadError) as error:
            # The rest of the explanation is still worth having: say why the rows are missing
            reason = describe(error) if isinstance(error, Forbidden) else str(error)
            return {**base, "total_rows": artifact.rows, "reasons": [], "rows_unavailable": reason}


class _JobRequest:
    """A job about to be submitted: the body of ``POST /jobs`` and how results name it."""

    def __init__(self, kind: str, body: Dict[str, Any], dataset: Optional[Dataset]):
        self.kind = kind
        self.body = body
        self.dataset = dataset
        self.schema: Optional[Dict[str, Any]] = None
        self.key = f"mcp-{uuid.uuid4().hex}"


def _upload_bytes(client: ForkliftClient, data: bytes, **options: Any):
    return client.upload_file(io.BytesIO(data), size=len(data), content_type="text/csv", **options)


def _is_uuid(value: str) -> bool:
    try:
        uuid.UUID(value.strip())
    except ValueError:
        return False
    return True


def _job_ref(job_id: str) -> str:
    if not _is_uuid(job_id):
        raise ToolInputError(f"job_id must be a job's id (a UUID), not {job_id!r}")
    return job_id.strip()


def _latest(schema: Schema) -> int:
    if schema.latest_version is None:
        raise ToolInputError(f"schema {schema.name!r} has no versions yet")
    return schema.latest_version


def _bad_rows_artifacts(artifacts: List[Artifact]) -> List[Artifact]:
    """The bad_rows artifacts of the job's latest attempt that has any, by name."""
    bad = [item for item in artifacts if item.kind == "bad_rows"]
    if not bad:
        return []
    latest = max(item.attempt for item in bad)
    return sorted((item for item in bad if item.attempt == latest), key=lambda item: item.name)


def _default_columns(available: List[str]) -> List[str]:
    selected = available[:MAX_COLUMNS]
    if REASON_COLUMN in available and REASON_COLUMN not in selected:
        selected[-1] = REASON_COLUMN
    return selected


def _follow(job: Job) -> str:
    still = "publishing its outputs" if job.status == "publishing" else job.status
    return (
        f"Job {job.id} is still {still}: call get_job with job_id {job.id!r} (and wait_seconds) "
        "until its status is succeeded, failed or cancelled; publishing has not ended yet"
    )


def _next_after(job: Job, artifacts: List[Artifact]) -> str:
    if job.status != "succeeded":
        return f"explain_failure with job_id {job.id!r} explains the error"
    if _bad_rows_artifacts(artifacts):
        return f"get_bad_rows with job_id {job.id!r} shows the rejected rows"
    return "The job succeeded"


def _no_bad_rows(job: Job) -> str:
    if job.status not in TERMINAL_STATUSES:
        return f"Job {job.id} is still {job.status}; its rejected rows are kept when it ends"
    return f"Job {job.id} ({job.status}) kept no bad_rows file: it rejected no rows"


def _failure_summary(
    job: Job, result: Mapping[str, Any], rejected: Optional[Dict[str, Any]]
) -> str:
    counts = result.get("counts") or {}
    read = counts.get("total_rows")
    rows = f" after reading {read} rows" if read is not None else ""
    if job.error is not None:
        text_ = f"The {job.kind} job {job.status} with {job.error.code}{rows}."
    else:
        text_ = (
            f"The {job.kind} job {job.status}: {counts.get('valid_rows', 0)} of {read or 0} rows "
            f"were written and {counts.get('invalid_rows', 0)} rejected."
        )
    if rejected is not None and rejected.get("reasons"):
        common = ", ".join(f"{r['reason']} ({r['rows']})" for r in rejected["reasons"][:3])
        kept = rejected["total_rows"]
        rows = f"{kept} rejected row is" if kept == 1 else f"{kept} rejected rows are"
        text_ += f" {rows} kept; most common reasons: {common}."
    return text_


def tool_names() -> Tuple[str, ...]:
    """The tools, in the order they are listed."""
    return (
        "list_connections",
        "list_datasets",
        "get_schema",
        "generate_schema",
        "validate_schema",
        "preview",
        "run_job",
        "get_job",
        "explain_failure",
        "get_bad_rows",
        "upload_csv",
    )
