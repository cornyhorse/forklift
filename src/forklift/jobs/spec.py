"""``JobSpec``: what a job reads, how it checks it and where the results go (contract v1).

A spec is plain data: build it in Python or load it with :meth:`JobSpec.from_dict` (which checks
every field and names the ones that are wrong), run it with :func:`forklift.jobs.run_job`, and
store it with :meth:`JobSpec.to_dict`. ``contracts/jobspec.schema.json`` is generated from these
classes (``python -m forklift.jobs.contract``).

Locations are relative to the ``base_dir`` given to ``run_job`` (``file``), or point at an S3
object with the caller's own credentials (``s3``, library use), a presigned URL for one object
(``presigned_url``, input only, accepted only for hosts the caller allows), or a database
(``sql`` input, ``sql_table`` output). Connection strings and presigned URLs are secrets: they are
hidden from ``repr()`` and never appear in results, logs or messages.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

from ._model import Model, Problem, contract_field

SPEC_VERSION = 1

KINDS = ("run", "preview", "validate_schema", "generate_schema")
FORMATS = ("csv", "excel", "fwf", "sql")
COMPRESSIONS = ("snappy", "gzip", "brotli", "zstd", "lz4", "none")
TABLE_MODES = ("create", "append", "replace", "upsert")
STAGING_MODES = ("table", "none")

#: Directory (inside ``base_dir``) the artifacts go to when the spec does not name one
DEFAULT_ARTIFACTS_PATH = "out/"

# A relative path: no leading '/', no drive letter, no backslash, no NUL, no '..' component
_RELATIVE_PATH = r"^(?![/\\])(?![A-Za-z]:)(?!.*\\)(?!.*\x00)(?!(?:.*/)?\.\.(?:/|$)).+$"
_PATH_MESSAGE = (
    "must be a path relative to the base directory: no leading '/', drive letter, backslash "
    "or '..' component"
)


@dataclass
class FileLocation(Model):
    """A file or directory inside the base directory given to run_job.

    Inputs name a file; outputs name a directory, which is created when missing.
    """

    location_type = "file"

    path: str = contract_field(
        "Path relative to the base directory, '/' separated; a directory for outputs",
        pattern=_RELATIVE_PATH,
        **{"x-pattern-message": _PATH_MESSAGE},
    )


@dataclass
class S3Location(Model):
    """An S3 object (input) or prefix (output), read and written with the caller's credentials.

    For library use: the service never hands a worker an s3 location.
    """

    location_type = "s3"

    uri: str = contract_field(
        "s3://bucket/key for an input, s3://bucket/prefix/ for an output",
        pattern=r"^s3://[^/\s]+(/.*)?$",
        **{"x-pattern-message": "must be an s3://bucket/key URI"},
    )


@dataclass
class PresignedUrlLocation(Model):
    """One object behind a presigned GET URL, read as a stream (CSV inputs only).

    run_job accepts it only when the URL's host is one of the allowed_url_hosts it was given.
    """

    location_type = "presigned_url"

    url: str = contract_field(
        "The presigned http(s) URL (a secret: never logged or reported)",
        repr=False,
        pattern=r"^https?://",
        **{"x-pattern-message": "must be an http:// or https:// URL"},
    )
    size: Optional[int] = contract_field(
        "Size of the object in bytes, if known (checked against limits.max_input_bytes)",
        default=None,
        minimum=0,
    )
    etag: Optional[str] = contract_field(
        "ETag of the object, if known; a resumed read is refused when the object changed",
        default=None,
    )


@dataclass
class SqlLocation(Model):
    """A database to read from; the tables come from the schema's x-sql section."""

    location_type = "sql"

    connection_string: str = contract_field(
        "ODBC connection string (a secret: never logged or reported)", repr=False, minLength=1
    )


@dataclass
class SqlTableLocation(Model):
    """A database table the validated rows are loaded into (forklift.outputs.sql.write_table).

    The data and bad_rows Parquet files are still written, to output.artifacts.
    """

    location_type = "sql_table"

    connection_string: str = contract_field(
        "ODBC connection string (a secret: never logged or reported)", repr=False, minLength=1
    )
    table: str = contract_field("Name of the target table", minLength=1)
    schema_name: Optional[str] = contract_field(
        "Database schema of the table (default: the login's default schema)",
        default=None,
        minLength=1,
    )
    mode: str = contract_field(
        "create: a new table; append: add rows; replace: swap the contents; upsert: update or "
        "insert by key_columns",
        default="append",
        enum=list(TABLE_MODES),
    )
    key_columns: Optional[List[str]] = contract_field(
        "Columns that identify a row (required for mode upsert)",
        default=None,
        minItems=1,
        items={"minLength": 1},
    )
    staging: str = contract_field(
        "table: load a staging table, then publish it in one transaction; none: load directly "
        "inside one transaction (for logins that may not create tables)",
        default="table",
        enum=list(STAGING_MODES),
    )

    __schema_rules__ = [
        {
            "if": {"properties": {"mode": {"const": "upsert"}}, "required": ["mode"]},
            "then": {
                "properties": {"key_columns": {"type": "array"}},
                "required": ["key_columns"],
            },
        }
    ]

    def _check(self, path: str, problems: List[Problem]) -> None:
        if self.mode == "upsert" and not self.key_columns:
            problems.append((f"{path}.key_columns", "is required for mode 'upsert'"))


InputLocation = Union[FileLocation, S3Location, PresignedUrlLocation, SqlLocation]
OutputLocation = Union[FileLocation, S3Location, SqlTableLocation]


@dataclass
class FooterDetection(Model):
    """Where the data of a CSV input ends (rows from the footer on are not read)."""

    stop_on_blank: bool = contract_field("Stop at the first blank row", default=False)
    column_index: Optional[int] = contract_field(
        "0-based column that patterns are matched against (requires patterns)",
        default=None,
        minimum=0,
    )
    patterns: Optional[List[str]] = contract_field(
        "Regular expressions; a row whose column_index cell matches one is the footer "
        "(requires column_index)",
        default=None,
        minItems=1,
    )

    __schema_rules__ = [
        {
            "if": {
                "properties": {"column_index": {"type": "integer"}},
                "required": ["column_index"],
            },
            "then": {"properties": {"patterns": {"type": "array"}}, "required": ["patterns"]},
        },
        {
            "if": {"properties": {"patterns": {"type": "array"}}, "required": ["patterns"]},
            "then": {
                "properties": {"column_index": {"type": "integer"}},
                "required": ["column_index"],
            },
        },
    ]

    def _check(self, path: str, problems: List[Problem]) -> None:
        if (self.column_index is None) != (self.patterns is None):
            problems.append((path, "column_index and patterns must be given together"))


_CSV = ("csv",)
_EXCEL = ("excel",)
_SQL = ("sql",)


@dataclass
class InputOptions(Model):
    """Options for reading the input; unset options keep the engine's defaults.

    An option that does not apply to the input format is ignored with a warning.
    """

    # CSV
    encoding: Optional[str] = contract_field(
        "CSV: text encoding (default utf-8; a UTF-8 byte order mark is ignored)",
        default=None,
        formats=_CSV,
        minLength=1,
    )
    delimiter: Optional[str] = contract_field(
        "CSV: field delimiter (default ',')",
        default=None,
        formats=_CSV,
        minLength=1,
        maxLength=1,
    )
    quote_char: Optional[str] = contract_field(
        "CSV: quote character (default '\"')",
        default=None,
        formats=_CSV,
        minLength=1,
        maxLength=1,
    )
    escape_char: Optional[str] = contract_field(
        "CSV: escape character (default: none)",
        default=None,
        formats=_CSV,
        minLength=1,
        maxLength=1,
    )
    header_mode: Optional[str] = contract_field(
        "CSV: present (the first non-comment row is the header), absent (the schema names the "
        "columns, or col_1..col_N), auto (detected); default present",
        default=None,
        formats=_CSV,
        enum=["present", "absent", "auto"],
    )
    header_search_rows: Optional[int] = contract_field(
        "CSV: rows searched for the header (default 10)", default=None, formats=_CSV, minimum=1
    )
    skip_blank_lines: Optional[bool] = contract_field(
        "CSV: skip blank rows above the header (default true)", default=None, formats=_CSV
    )
    comment_rows: Optional[List[str]] = contract_field(
        "CSV: regular expressions for comment rows above the header (default: a single '#' "
        "cell; [] turns comment detection off)",
        default=None,
        formats=_CSV,
    )
    footer_detection: Optional[FooterDetection] = contract_field(
        "CSV: where the data ends", default=None, formats=_CSV
    )
    excess_column_mode: Optional[str] = contract_field(
        "CSV: rows with more fields than the header: truncate (default), reject (to bad_rows) "
        "or passthrough (extra columns col_N)",
        default=None,
        formats=_CSV,
        enum=["truncate", "reject", "passthrough"],
    )
    # Excel
    sheet: Optional[Union[str, int]] = contract_field(
        "Excel: sheet name, or 0-based index (default: every sheet)",
        default=None,
        formats=_EXCEL,
    )
    values_only: Optional[bool] = contract_field(
        "Excel: read cached values instead of formulas (default true)",
        default=None,
        formats=_EXCEL,
    )
    engine: Optional[str] = contract_field(
        "Excel: reader (default: by file type)",
        default=None,
        formats=_EXCEL,
        enum=["openpyxl", "xlrd"],
    )
    date_system: Optional[str] = contract_field(
        "Excel: date system when the workbook does not say (default 1900)",
        default=None,
        formats=_EXCEL,
        enum=["1900", "1904"],
    )
    # SQL
    query_timeout: Optional[int] = contract_field(
        "SQL: query timeout in seconds (default 300)", default=None, formats=_SQL, minimum=1
    )
    connection_timeout: Optional[int] = contract_field(
        "SQL: connection timeout in seconds (default 30)", default=None, formats=_SQL, minimum=1
    )
    schema_name: Optional[str] = contract_field(
        "SQL: default database schema for x-sql tables without one",
        default=None,
        formats=_SQL,
        minLength=1,
    )
    null_values: Optional[List[str]] = contract_field(
        "SQL: values read as NULL", default=None, formats=_SQL
    )
    enable_streaming: Optional[bool] = contract_field(
        "SQL: fetch rows in batches instead of all at once (default true)",
        default=None,
        formats=_SQL,
    )


@dataclass
class InputSpec(Model):
    """What the job reads."""

    format: str = contract_field("Input format", enum=list(FORMATS))
    location: InputLocation = contract_field("Where the input is")
    options: InputOptions = contract_field(
        "How to read it", default_factory=InputOptions, required=False
    )


@dataclass
class OutputSpec(Model):
    """Where a run writes its results (and the other kinds their artifacts)."""

    location: OutputLocation = contract_field(
        "Directory (file), prefix (s3) or table (sql_table) for the output"
    )
    compression: str = contract_field(
        "Parquet compression", default="snappy", enum=list(COMPRESSIONS)
    )
    artifacts: Optional[FileLocation] = contract_field(
        "Directory for the data and bad_rows Parquet files of a sql_table output "
        f"(default {DEFAULT_ARTIFACTS_PATH}); only for sql_table outputs",
        default=None,
    )

    __schema_rules__ = [
        {
            "if": {"properties": {"location": {"properties": {"type": {"const": "sql_table"}}}}},
            "else": {
                "anyOf": [
                    {"not": {"required": ["artifacts"]}},
                    {"properties": {"artifacts": {"type": "null"}}},
                ]
            },
        }
    ]

    def _check(self, path: str, problems: List[Problem]) -> None:
        if self.artifacts is not None and not isinstance(self.location, SqlTableLocation):
            problems.append(
                (
                    f"{path}.artifacts",
                    "only applies to sql_table outputs; the files of a "
                    f"{self.location.location_type} output go to its location",
                )
            )


@dataclass
class JobOptions(Model):
    """How the job processes its input."""

    apply_schema_extensions: bool = contract_field(
        "Apply the schema's x-... extensions (CSV runs and validate_schema)", default=True
    )
    batch_size: int = contract_field(
        "Rows per batch (CSV and SQL inputs)", default=10000, minimum=1, maximum=1_000_000
    )
    include_value_statistics: bool = contract_field(
        "Let the output metadata and generated schemas hold statistics that copy cell values "
        "(top values, min/max, quantiles)",
        default=False,
    )
    preview_rows: int = contract_field(
        "preview: rows to return at most", default=100, minimum=1, maximum=10_000
    )
    preview_max_bytes: int = contract_field(
        "preview: size limit of preview.json in bytes",
        default=1_048_576,
        minimum=1024,
        maximum=67_108_864,
    )
    sample_rows: int = contract_field(
        "validate_schema and generate_schema: rows of the input to check or analyse",
        default=1000,
        minimum=1,
        maximum=1_000_000,
    )
    infer_primary_key: bool = contract_field(
        "generate_schema: propose x-primaryKey from columns that are unique in the sample",
        default=False,
    )


@dataclass
class Limits(Model):
    """Limits that fail the job with LIMIT_EXCEEDED (unset: no limit)."""

    max_input_bytes: Optional[int] = contract_field(
        "Largest input to read, in bytes", default=None, minimum=1
    )
    max_seconds: Optional[float] = contract_field(
        "Longest run time, in seconds (checked at batch boundaries)",
        default=None,
        exclusiveMinimum=0,
    )
    max_rows: Optional[int] = contract_field("Most input rows to read", default=None, minimum=1)


def _location_type(name: str, value: str) -> Dict[str, Any]:
    """JSON Schema: the ``location`` of ``input``/``output`` has ``type`` ``value``."""
    return {
        "properties": {
            name: {
                "properties": {"location": {"properties": {"type": {"const": value}}}},
                "required": ["location"],
            }
        },
        "required": [name],
    }


def _not_location_type(name: str, values: List[str]) -> Dict[str, Any]:
    return {
        "properties": {
            name: {"properties": {"location": {"properties": {"type": {"not": {"enum": values}}}}}}
        }
    }


def _format(value: Any) -> Dict[str, Any]:
    key = "enum" if isinstance(value, list) else "const"
    return {"properties": {"input": {"properties": {"format": {key: value}}}}}


def _kind(value: Any) -> Dict[str, Any]:
    key = "enum" if isinstance(value, list) else "const"
    return {"properties": {"kind": {key: value}}}


_OBJECT = {"type": "object"}
_INTERACTIVE = ["preview", "validate_schema", "generate_schema"]


@dataclass
class JobSpec(Model):
    """A job for the forklift engine (job contract v1).

    Run it with forklift.jobs.run_job or `forklift run-job`.
    """

    __contract_name__ = "job spec"

    job_id: str = contract_field(
        "Identifies the job (the gateway assigns one; any unique id for local runs); letters, "
        "digits, '.', '_' and '-'",
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$",
        **{
            "x-pattern-message": "must be 1-128 letters, digits, '.', '_' or '-', starting with "
            "a letter or digit"
        },
    )
    kind: str = contract_field(
        "run: a full import; preview: the first rows as text; validate_schema: check the schema "
        "against the input's header and a sample; generate_schema: infer a schema",
        enum=list(KINDS),
    )
    input: InputSpec = contract_field("What the job reads")
    schema: Optional[Dict[str, Any]] = contract_field(
        "Inline forklift JSON schema (never a path); required for validate_schema and for sql "
        "inputs",
        default=None,
    )
    output: Optional[OutputSpec] = contract_field(
        "Where the results go; required for run (the other kinds write their artifacts to a "
        f"file location, default {DEFAULT_ARTIFACTS_PATH})",
        default=None,
    )
    options: JobOptions = contract_field(
        "Processing options", default_factory=JobOptions, required=False
    )
    limits: Limits = contract_field(
        "Limits that fail the job", default_factory=Limits, required=False
    )
    spec_version: int = contract_field(
        "Version of the job contract",
        default=SPEC_VERSION,
        required=True,
        order=-1,
        const=SPEC_VERSION,
    )

    __schema_rules__ = [
        # run needs an output
        {"if": _kind("run"), "then": {"properties": {"output": _OBJECT}, "required": ["output"]}},
        # validate_schema and sql inputs need a schema
        {
            "if": _kind("validate_schema"),
            "then": {"properties": {"schema": _OBJECT}, "required": ["schema"]},
        },
        {
            "if": _format("sql"),
            "then": {"properties": {"schema": _OBJECT}, "required": ["schema"]},
        },
        # sql inputs come from sql locations, and only from those
        {"if": _format("sql"), "then": _location_type("input", "sql")},
        {"if": _location_type("input", "sql"), "then": _format("sql")},
        # a presigned URL is streamed, which only the CSV reader can do
        {"if": _location_type("input", "presigned_url"), "then": _format("csv")},
        # the interactive kinds write artifacts to a directory
        {"if": _kind(_INTERACTIVE), "then": _not_location_type("output", ["s3", "sql_table"])},
    ]

    def _check(self, path: str, problems: List[Problem]) -> None:
        location = self.input.location
        if self.kind == "run" and self.output is None:
            problems.append(("output", "is required for kind 'run'"))
        if self.schema is None and (self.kind == "validate_schema" or self.input.format == "sql"):
            reason = "kind 'validate_schema'" if self.kind == "validate_schema" else "sql inputs"
            problems.append(("schema", f"is required for {reason}"))
        if (self.input.format == "sql") != isinstance(location, SqlLocation):
            problems.append(
                (
                    "input.location.type",
                    "format 'sql' reads from a 'sql' location, and a 'sql' location only "
                    f"holds format 'sql' (got format {self.input.format!r} with location "
                    f"{location.location_type!r})",
                )
            )
        if isinstance(location, PresignedUrlLocation) and self.input.format != "csv":
            problems.append(
                (
                    "input.location.type",
                    f"a presigned_url input is read as a stream, which only format 'csv' "
                    f"supports; stage the {self.input.format} file and use a 'file' location",
                )
            )
        if (
            self.kind in _INTERACTIVE
            and self.output is not None
            and not isinstance(self.output.location, FileLocation)
        ):
            problems.append(
                (
                    "output.location.type",
                    f"kind {self.kind!r} writes its artifact to a 'file' directory, not to "
                    f"{self.output.location.location_type!r}",
                )
            )
