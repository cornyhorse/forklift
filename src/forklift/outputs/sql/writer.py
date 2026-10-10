"""Write Parquet or Arrow data to a database table, all or nothing (see the package docs)."""

from __future__ import annotations

import hashlib
import logging
import os
import re
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from itertools import chain
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from ...engine.exceptions import SPEC_INVALID
from ...engine.importers.redaction import redact_connection_string
from .columns import SourceColumn, source_columns, to_parameters
from .dialects import Dialect, ExistingColumn, dialect_for
from .errors import (
    DriverCodes,
    TableWriteCancelled,
    TableWriteError,
    describe_failure,
    driver_errors,
    failure_error_code,
)

logger = logging.getLogger(__name__)

MODES = ("create", "append", "replace", "upsert")
STAGING = ("table", "none")

# Login failures before the database is known: MySQL, Oracle and SQL Server codes
_LOGIN_CODES = DriverCodes(
    {
        1045: "access denied for the login",
        1017: "invalid user name or password",
        18456: "login failed",
    },
    privilege=frozenset({1045, 1017, 18456}),
)
_STAGING_TOKEN = re.compile(r"[^a-z0-9]+")


@dataclass
class TableWriteResult:
    """What :func:`write_table` wrote.

    Attributes:
        rows_written: Rows of the source written to the table (inserted or, for upsert,
            inserted or updated)
        table: The table as ``schema.table``, spelled as in the database
        mode: The write mode
        warnings: Things the caller should know (precision lost, time zones, leftovers); they
            hold names, never values
        database: ``postgresql``, ``mysql``, ``sqlserver`` or ``oracle``
        created: True when forklift created the table
        staging: ``table`` or ``none``
    """

    rows_written: int
    table: str
    mode: str
    warnings: List[str] = field(default_factory=list)
    database: str = ""
    created: bool = False
    staging: str = "table"

    def to_dict(self) -> Dict[str, object]:
        """The result as JSON-ready values."""
        return asdict(self)


def write_table(
    source,
    connection_string: str,
    table: str,
    *,
    schema_name: Optional[str] = None,
    mode: str = "append",
    key_columns: Optional[Sequence[str]] = None,
    batch_size: int = 10_000,
    staging: str = "table",
    job_id: Optional[str] = None,
    progress: Optional[Callable[[Dict[str, int]], None]] = None,
    cancel: Optional[Callable[[], bool]] = None,
    connect_timeout: int = 30,
) -> TableWriteResult:
    """Write ``source`` to a table in PostgreSQL, MySQL/MariaDB, SQL Server or Oracle.

    The rows are published all or nothing: with ``staging="table"`` they are loaded into a
    staging table first (named after ``job_id``) and moved into the table in one transaction;
    with ``staging="none"`` they are written straight into the table inside one transaction.
    A failure or a cancellation leaves the table as it was. What each database guarantees, and
    the privileges each mode needs, are described in the package documentation.

    Args:
        source: A Parquet file (path), a ``pyarrow.Table`` or a ``pyarrow.RecordBatchReader``
        connection_string: ODBC connection string (pyodbc); never repeated in errors or logs
        table: The table's name (created exactly as given, except that Oracle upper-cases
            plain lower-case names; an existing table is matched ignoring case when unique)
        schema_name: The table's schema (the database on MySQL); the login's default if None
        mode: ``create`` (the table must not exist), ``append`` (add the rows), ``replace``
            (the table's rows become the source's) or ``upsert`` (update rows whose key is in
            the source, insert the others). ``append``, ``replace`` and ``upsert`` create the
            table when it does not exist.
        key_columns: The key for ``upsert`` (required there), and the primary key of a table
            forklift creates
        batch_size: Rows read from the source and written per batch
        staging: ``table`` (default) or ``none`` (for logins that may not create tables)
        job_id: Names the staging table, so that a retry of the same job first drops what an
            interrupted attempt left; a random id when None
        progress: Called after each batch with ``{"rows_written": int}``
        cancel: Called before each batch and before publishing; True stops the write and
            leaves the table unchanged (:class:`TableWriteCancelled`)
        connect_timeout: Seconds to wait for the connection

    Returns:
        TableWriteResult: rows written, the table, the mode and warnings

    Raises:
        ValueError: An invalid mode, staging, batch size or key columns, or an empty table
            or schema name (other name problems are a TableWriteError with SPEC_INVALID)
        TypeError: ``source`` is not a path, Table or RecordBatchReader, or a callback is not
            callable
        ImportError: pyodbc is not installed
        TableWriteError: The table could not be written (the message says which step failed,
            the SQLSTATE and, when the database refused, the privilege the login lacks; it
            never holds cell values or the connection string)
        TableWriteCancelled: ``cancel()`` returned True
    """
    keys = _check_arguments(table, schema_name, mode, key_columns, batch_size, staging, job_id)
    for name, callback in (("progress", progress), ("cancel", cancel)):
        if callback is not None and not callable(callback):
            raise TypeError(f"{name} must be callable, got {type(callback).__name__}")
    arrow_schema, batches, close = _open_source(source, batch_size)
    try:
        columns = source_columns(arrow_schema, keys)
        writer = _TableWriter(
            connection_string,
            table,
            schema_name,
            mode,
            keys,
            staging,
            job_id or uuid.uuid4().hex,
            progress,
            cancel,
            connect_timeout,
            columns,
        )
        return writer.write(batches)
    finally:
        close()


def _check_arguments(table, schema_name, mode, key_columns, batch_size, staging, job_id):
    if mode not in MODES:
        raise ValueError(f"mode must be one of {', '.join(MODES)}; got {mode!r}")
    if staging not in STAGING:
        raise ValueError(f"staging must be 'table' or 'none'; got {staging!r}")
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
        raise ValueError(f"batch_size must be a positive integer; got {batch_size!r}")
    if not isinstance(table, str) or not table:
        raise ValueError("table must be a non-empty string")
    if schema_name is not None and (not isinstance(schema_name, str) or not schema_name):
        raise ValueError("schema_name must be a non-empty string or None")
    if job_id is not None and (not isinstance(job_id, str) or not job_id):
        raise ValueError("job_id must be a non-empty string or None")
    if isinstance(key_columns, str):
        raise ValueError("key_columns must be a list of column names, not a string")
    keys = list(key_columns or [])
    if any(not isinstance(key, str) or not key for key in keys):
        raise ValueError("key_columns must be non-empty column names")
    if len(set(keys)) != len(keys):
        raise ValueError(f"key_columns names a column twice: {keys}")
    if mode == "upsert" and not keys:
        raise ValueError("mode 'upsert' needs key_columns: the columns that identify a row")
    return keys


def _open_source(
    source, batch_size: int
) -> Tuple[pa.Schema, Iterator[pa.RecordBatch], Callable[[], None]]:
    """The source's schema, its record batches of at most ``batch_size`` rows, and a close."""
    if isinstance(source, (str, os.PathLike)):
        parquet = pq.ParquetFile(source)
        return parquet.schema_arrow, parquet.iter_batches(batch_size=batch_size), parquet.close
    if isinstance(source, pa.Table):
        return source.schema, iter(source.to_batches(max_chunksize=batch_size)), _nothing
    if isinstance(source, pa.RecordBatchReader):
        return source.schema, _rechunk(source, batch_size), _nothing
    raise TypeError(
        "source must be a Parquet file path, a pyarrow.Table or a pyarrow.RecordBatchReader; "
        f"got {type(source).__name__}"
    )


def _nothing() -> None:
    """Closing a Table or a caller's RecordBatchReader is the caller's business."""


def _rechunk(reader: pa.RecordBatchReader, batch_size: int) -> Iterator[pa.RecordBatch]:
    for batch in reader:
        for offset in range(0, batch.num_rows, batch_size):
            yield batch.slice(offset, batch_size)


def staging_table_name(job_id: str, schema: str, table: str) -> str:
    """The staging table of one job and table: the same for every attempt of the job.

    A readable part of the job id plus a hash of the job id and the table, so two jobs (or two
    tables of one job) never share a staging table. 50 characters, within every database's
    identifier limit.
    """
    token = _STAGING_TOKEN.sub("_", job_id.lower()).strip("_")[:24] or "job"
    digest = hashlib.sha256(f"{job_id}\0{schema}\0{table}".encode("utf-8")).hexdigest()[:12]
    return f"forklift_stg_{token}_{digest}"


def _match(names: Iterable[str], wanted: str, alternative: str) -> List[str]:
    """Catalog names that are ``wanted``: exact matches first, else equal ignoring case."""
    names = list(names)
    for candidate in (wanted, alternative):
        if candidate in names:
            return [candidate]
    folded = wanted.casefold()
    return [name for name in names if name.casefold() == folded]


def _last_per_key(rows: List[tuple], indexes: Sequence[int]) -> List[tuple]:
    """Keep the last row of each key (upsert without staging applies rows in order)."""
    latest: Dict[tuple, int] = {}
    for position, row in enumerate(rows):
        latest[tuple(row[i] for i in indexes)] = position
    if len(latest) == len(rows):
        return rows
    return [rows[position] for position in sorted(latest.values())]


class _Statements:
    """The SQL that writes rows to one table, built once per row count."""

    def __init__(self, dialect: Dialect, build, table: str, names, columns, upsert: bool):
        self.dialect, self.build, self.table = dialect, build, table
        self.names, self.columns, self.upsert = names, columns, upsert
        self.cache: Dict[int, str] = {}

    def rows(self, count: int) -> str:
        if count not in self.cache:
            self.cache[count] = self.build(self.table, self.names, self.columns, count)
        return self.cache[count]

    def single_row(self) -> str:
        """An INSERT of one row that binds values of any length (Oracle's long LOB values).

        Raises:
            TableWriteError: Upserts have no such statement (only the staging table's
                INSERT does)
        """
        if self.upsert:
            raise TableWriteError(
                "a CLOB or BLOB value is longer than 32,767 bytes, which Oracle's ODBC driver "
                "binds only in single-row INSERT statements; upsert with staging='table' "
                "(the default), which inserts into a staging table and merges from there"
            )
        if -1 not in self.cache:
            self.cache[-1] = self.dialect.single_row_insert_sql(
                self.table, self.names, self.columns
            )
        return self.cache[-1]


class _TableWriter:
    """One call of :func:`write_table`: its connection, plan and the steps it takes."""

    def __init__(
        self,
        connection_string: str,
        table: str,
        schema_name: Optional[str],
        mode: str,
        keys: List[str],
        staging: str,
        job_id: str,
        progress,
        cancel,
        connect_timeout: int,
        columns: List[SourceColumn],
    ):
        self.connection_string = connection_string
        self.table = table
        self.schema_name = schema_name
        self.mode = mode
        self.keys = keys
        self.staging = staging
        self.job_id = job_id
        self.progress = progress
        self.cancel = cancel
        self.connect_timeout = connect_timeout
        self.columns = columns
        self.display = table if schema_name is None else f"{schema_name}.{table}"
        self.warnings: List[str] = []
        self.dialect: Dialect = Dialect()
        self.connection = None
        self.schema = ""
        self.exists = False
        self.created = False
        self.staging_name = ""
        self.leftover = ""  # a table forklift created and must drop if the write fails

    # ------------------------------------------------------------------ running

    def write(self, batches: Iterator[pa.RecordBatch]) -> TableWriteResult:
        self._connect()
        try:
            self._prepare()
            logger.info(
                "Writing table %s on %s (mode %s, staging %s)",
                self.display,
                self.dialect.label,
                self.mode,
                self.staging,
            )
            try:
                if self.staging == "table":
                    rows = self._write_staged(batches)
                else:
                    rows = self._write_direct(batches)
            except BaseException as error:
                self._abandon(error)
                raise
        finally:
            self._disconnect()
        logger.info("Wrote %d row(s) to table %s", rows, self.display)
        return TableWriteResult(
            rows_written=rows,
            table=self.display,
            mode=self.mode,
            warnings=self.warnings,
            database=self.dialect.name,
            created=self.created,
            staging=self.staging,
        )

    def _connect(self) -> None:
        try:
            import pyodbc
        except ImportError:
            raise ImportError(
                "pyodbc is required to write database tables. "
                "Install it with: pip install forklift-etl[sql]"
            ) from None
        self.pyodbc = pyodbc
        self.errors = driver_errors(pyodbc)
        # Oracle's client converts text to the character set NLS_LANG names, US7ASCII when it
        # is unset (which replaces non-ASCII characters); other drivers ignore the variable
        os.environ.setdefault("NLS_LANG", ".AL32UTF8")
        pyodbc.pooling = False
        try:
            self.connection = pyodbc.connect(
                self.connection_string, autocommit=False, timeout=self.connect_timeout
            )
        except self.errors as error:
            failure = describe_failure(error, _LOGIN_CODES)
            raise TableWriteError(
                f"Could not connect to the database to write table {self.display} "
                f"({redact_connection_string(self.connection_string)}): {failure.describe()}",
                table=self.display,
                mode=self.mode,
                action="connect",
                sqlstate=failure.sqlstate,
                native_code=failure.native_code,
                retryable=failure.retryable,
                error_code=failure_error_code(failure),
            ) from None
        try:
            name = self.connection.getinfo(pyodbc.SQL_DBMS_NAME)
        except self.errors:
            name = ""
        dialect = dialect_for(name if isinstance(name, str) else "")
        if dialect is None:
            self._disconnect()
            raise TableWriteError(
                f"Cannot write table {self.display}: forklift writes tables to PostgreSQL, "
                f"MySQL/MariaDB, SQL Server and Oracle, and this database reports itself as "
                f"{name!r}",
                table=self.display,
                mode=self.mode,
            )
        if dialect.utf8_parameters:
            self.connection.setencoding(encoding="utf-8")
        self.dialect = dialect

    def _disconnect(self) -> None:
        connection, self.connection = self.connection, None
        try:
            connection.close()
        except self.errors:
            logger.warning("Could not close the database connection cleanly")

    @contextmanager
    def _step(self, action: str, privilege: Optional[str] = None, refused_hint: str = ""):
        """Run database calls as one step; a database error becomes a TableWriteError.

        A database error is any of ``self.errors``: pyodbc raises SystemError when it cannot
        even decode the driver's message (Oracle's driver sends undecodable bytes after some
        errors), and the ORA code in the raw message still says which error it was.
        """
        try:
            yield
        except self.errors as error:
            raise self._failed(error, action, privilege, refused_hint) from None

    def _failed(self, error, action, privilege, refused_hint) -> TableWriteError:
        failure = describe_failure(error, self.dialect.codes)
        refused = failure.privilege_refused
        message = (
            f"Writing table {self.display} (mode {self.mode!r}, staging {self.staging!r}) on "
            f"{self.dialect.label} failed: the database "
            f"{'refused to' if refused else 'could not'} {action}: {failure.describe()}."
        )
        if refused and privilege:
            message += f" The login needs {privilege}."
        if refused and refused_hint:
            message += f" {refused_hint}"
        if failure.retryable:
            message += " The error is temporary: trying again may succeed."
        return TableWriteError(
            message,
            table=self.display,
            mode=self.mode,
            action=action,
            sqlstate=failure.sqlstate,
            native_code=failure.native_code,
            privilege=privilege if refused else None,
            retryable=failure.retryable,
            error_code=failure_error_code(failure),
        )

    def _error(self, message: str, **kwargs) -> TableWriteError:
        return TableWriteError(
            f"Cannot write table {self.display} (mode {self.mode!r}): {message}",
            table=self.display,
            mode=self.mode,
            **kwargs,
        )

    def _query(self, sql: str, *params) -> List[tuple]:
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, *params)
            return [tuple(row) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def _execute(self, sql: str) -> None:
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql)
        finally:
            cursor.close()

    # ------------------------------------------------------------------ planning

    def _prepare(self) -> None:
        dialect = self.dialect
        self._check_names()
        with self._step("prepare the session"):
            for statement in dialect.session_statements:
                self._execute(statement)
            self.connection.commit()
        with self._step("read the database catalog (to find the table and its columns)"):
            self.schema = self._resolve_schema()
            found = self._find_table(self.table)
            existing = self._existing_columns(found) if found else None
        if found and self.mode == "create":
            raise self._error(
                "the table already exists, and mode 'create' only creates new tables "
                "(use append, replace or upsert to write to an existing table)"
            )
        self.exists = found is not None
        self.table = found or dialect.new_name(self.table)
        self.display = f"{self.schema}.{self.table}"
        if existing is not None:
            self._plan_existing(existing)
        else:
            for column in self.columns:
                column.sql_name = dialect.new_name(column.name)
                if column.kind == "timestamp_tz" and dialect.name == "mysql":
                    self.warnings.append(
                        f"Column {column.name!r} holds time zone-aware timestamps; MySQL has no "
                        "such type, so they are written in UTC to a DATETIME(6) column"
                    )
        for column in self.columns:
            column.key = column.key and (self.mode == "upsert" or not self.exists)
            column.ddl = column.load_type or self._column_type(column)
            column.placeholder, column.expression = dialect.bind(column)
        self.staging_name = dialect.new_name(
            staging_table_name(self.job_id, self.schema, self.table)
        )
        if self.staging == "none" and self.mode == "upsert" and self.exists:
            if dialect.upsert_needs_unique_index:
                self._require_unique_key()

    def _check_names(self) -> None:
        check = self.dialect.check_identifier
        try:
            check(self.table, "table")
            if self.schema_name is not None:
                check(self.schema_name, "schema")
            for column in self.columns:
                check(self.dialect.new_name(column.name), "column")
        except TableWriteError as error:
            raise self._error(str(error), error_code=SPEC_INVALID) from None

    def _column_type(self, column: SourceColumn) -> str:
        try:
            return self.dialect.column_type(column)
        except TableWriteError as error:
            raise self._error(f"column {column.name!r}: {error}") from None

    def _resolve_schema(self) -> str:
        dialect = self.dialect
        if self.schema_name is None:
            rows = self._query(dialect.current_schema_sql)
            name = rows[0][0] if rows else None
            if not name:
                raise self._error(
                    f"the login has no default schema on {dialect.label}; pass schema_name"
                )
            return str(name)
        names = [row[0] for row in self._query(dialect.schema_sql, self.schema_name)]
        matches = _match(names, self.schema_name, dialect.new_name(self.schema_name))
        if len(matches) != 1:
            reason = (
                f"matches several schemas ({', '.join(sorted(matches))}); give its exact name"
                if matches
                else "does not exist, or the login cannot see it"
            )
            raise self._error(f"schema {self.schema_name!r} {reason}", error_code=SPEC_INVALID)
        return matches[0]

    def _find_table(self, name: str) -> Optional[str]:
        names = [row[0] for row in self._query(self.dialect.tables_sql, self.schema, name)]
        matches = _match(names, name, self.dialect.new_name(name))
        if len(matches) > 1:
            raise self._error(
                f"the name matches several tables ignoring case ({', '.join(sorted(matches))}); "
                "give its exact name"
            )
        return matches[0] if matches else None

    def _existing_columns(self, table: str) -> List[ExistingColumn]:
        rows = self._query(self.dialect.columns_sql, self.schema, table)
        return [self.dialect.existing_column(row) for row in rows]

    def _plan_existing(self, existing: List[ExistingColumn]) -> None:
        """Match the source's columns to the table's and check that they fit."""
        names = [column.name for column in existing]
        by_name = {column.name: column for column in existing}
        missing, unfit, used = [], [], set()
        for column in self.columns:
            matches = _match(names, column.name, self.dialect.new_name(column.name))
            if len(matches) != 1:
                missing.append(column.name)
                continue
            target = by_name[matches[0]]
            used.add(target.name)
            column.sql_name = target.name
            column.load_type = target.load_type
            if not target.writable:
                unfit.append(f"{column.name!r} (the database computes {target.name})")
            elif column.kind not in target.accepts:
                unfit.append(
                    f"{column.name!r} ({column.kind} values; {target.name} is {target.type_name})"
                )
            else:
                self._warn_about_fit(column, target)
        required = [c.name for c in existing if c.required and c.name not in used]
        problems = []
        if missing:
            problems.append(
                f"the table has no column for {', '.join(repr(n) for n in missing)} "
                f"(its columns are {', '.join(names)})"
            )
        if unfit:
            problems.append(f"these columns cannot be written: {', '.join(unfit)}")
        if required:
            problems.append(
                f"the table's column(s) {', '.join(required)} need a value in every row "
                "(NOT NULL without a default) but are not in the source"
            )
        if problems:
            raise self._error("; ".join(problems))

    def _warn_about_fit(self, column: SourceColumn, target: ExistingColumn) -> None:
        if column.kind == "timestamp_tz" and target.kind == "timestamp":
            self.warnings.append(
                f"Column {column.name!r} holds time zone-aware timestamps; {target.name} "
                f"({target.type_name}) has no time zone, so they are written in UTC"
            )
        elif column.kind == "timestamp" and target.kind == "timestamp_tz":
            self.warnings.append(
                f"Column {column.name!r} holds timestamps without a time zone; they are "
                f"written to {target.name} ({target.type_name}) as UTC"
            )
        if self.dialect.name == "oracle" and target.type_name == "DATE":
            if column.kind in ("timestamp", "timestamp_tz"):
                self.warnings.append(
                    f"{target.name} is an Oracle DATE, which keeps whole seconds: the "
                    f"fractional seconds of column {column.name!r} are dropped"
                )
        if target.kind == "other":
            self.warnings.append(
                f"{target.name} has type {target.type_name}, which forklift does not check: "
                f"the database converts the text of column {column.name!r}"
            )

    def _require_unique_key(self) -> None:
        """Upsert without staging relies on the database's own upsert, which needs a key."""
        wanted = {c.sql_name for c in self.columns if c.key}
        with self._step("read the table's unique indexes"):
            rows = self._query(self.dialect.unique_keys_sql, self.schema, self.table)
        indexes: Dict[object, set] = {}
        for index, column in rows:
            indexes.setdefault(index, set()).add(column)
        if wanted not in indexes.values():
            raise self._error(
                f"upsert with staging='none' uses {self.dialect.label}'s own upsert, which "
                f"needs a primary key or unique index on exactly the key columns "
                f"({', '.join(sorted(wanted))}); add one, or use staging='table'"
            )

    # ------------------------------------------------------------------ loading

    @property
    def _target(self) -> str:
        return self.dialect.qualified(self.schema, self.table)

    @property
    def _staging_table(self) -> str:
        return self.dialect.qualified(self.schema, self.staging_name)

    @property
    def _primary_key(self) -> List[str]:
        return [c.sql_name for c in self.columns if c.key] if not self.exists else []

    def _names(self) -> List[str]:
        return [column.sql_name for column in self.columns]

    def _write_staged(self, batches) -> int:
        dialect = self.dialect
        staging = self._staging_table
        if self._find_staging():
            with self._step(
                f"drop the staging table {self.schema}.{self.staging_name} that an earlier "
                "attempt left",
                dialect.drop_privilege(self.schema),
            ):
                self._execute(dialect.drop_table_sql(staging))
                self.connection.commit()
            logger.info("Dropped the staging table an earlier attempt left")
        hint = (
            'Grant it, or pass staging="none" to write straight into the table inside one '
            "transaction (no staging table needed)."
            if self.exists
            else "The table does not exist yet, so it has to be created either way."
        )
        with self._step(
            f"create the staging table {self.schema}.{self.staging_name}",
            dialect.create_privilege(self.schema),
            hint,
        ):
            self._execute(
                dialect.create_table_sql(staging, self.columns, (), not_null=not self.exists)
            )
            self.connection.commit()
        self.leftover = staging
        statement = self._statements(dialect.insert_rows_sql, staging)
        rows = self._load(
            batches, statement, f"the staging table {self.schema}.{self.staging_name}", True
        )
        self._check_cancel(rows)
        if self.mode == "upsert" or self._primary_key:
            self._check_keys(staging)
        self._publish(staging)
        return rows

    def _find_staging(self) -> bool:
        with self._step("read the database catalog (to find a leftover staging table)"):
            return self._find_table(self.staging_name) is not None

    def _write_direct(self, batches) -> int:
        dialect = self.dialect
        target = self._target
        if not self.exists:
            with self._step(f"create table {self.display}", dialect.create_privilege(self.schema)):
                self._execute(dialect.create_table_sql(target, self.columns, self._primary_key))
            if not dialect.transactional_ddl:
                self.leftover = target  # the CREATE committed; drop it if the load fails
        elif self.mode == "replace":
            with self._step(
                f"delete the rows of table {self.display}", f"DELETE on table {self.display}"
            ):
                self._execute(f"DELETE FROM {target}")
        if self.mode == "upsert":
            statement = self._statements(self._upsert_rows, target, upsert=True)
        else:
            statement = self._statements(dialect.insert_rows_sql, target)
        rows = self._load(batches, statement, f"table {self.display}", False)
        self._check_cancel(rows)
        with self._step("commit the transaction"):
            self.connection.commit()
        self.leftover = ""
        self.created = not self.exists
        return rows

    def _upsert_rows(self, table, names, columns, rows) -> str:
        keys = [c.sql_name for c in self.columns if c.key]
        return self.dialect.upsert_rows_sql(table, names, columns, keys, rows)

    def _statements(self, build, table: str, upsert: bool = False) -> _Statements:
        return _Statements(self.dialect, build, table, self._names(), self.columns, upsert)

    def _load(self, batches, statement, label: str, commit_each: bool) -> int:
        """Write every batch; returns the number of source rows written."""
        dialect = self.dialect
        per_statement = 1
        if not dialect.fast_executemany:
            per_statement = max(
                1,
                min(dialect.rows_per_statement, dialect.max_parameters // len(self.columns)),
            )
        dedupe = []
        if self.staging == "none" and self.mode == "upsert" and dialect.upsert_needs_unique_rows:
            dedupe = [i for i, column in enumerate(self.columns) if column.key]
        privilege = (
            dialect.create_privilege(self.schema)
            if self.staging == "table"
            else self._write_privilege()
        )
        written = 0
        with self._step(f"open a cursor to write to {label}"):
            cursor = self.connection.cursor()
            cursor.fast_executemany = dialect.fast_executemany
        try:
            for batch in batches:
                self._check_cancel(written)
                rows = self._rows(batch)
                if dedupe:
                    rows = _last_per_key(rows, dedupe)
                first, last = written + 1, written + batch.num_rows
                with self._step(
                    f"write rows {first} to {last} of the source to {label}", privilege
                ):
                    self._execute_rows(cursor, statement, rows, per_statement)
                    if commit_each:
                        self.connection.commit()
                written = last
                if self.progress is not None:
                    self.progress({"rows_written": written})
        finally:
            try:
                cursor.close()
            except self.errors:
                pass  # the connection is gone; the error that ended the load says why
        return written

    def _write_privilege(self) -> str:
        if self.mode == "upsert":
            return f"SELECT, INSERT and UPDATE on table {self.display}"
        return f"INSERT on table {self.display}"

    def _rows(self, batch: pa.RecordBatch) -> List[tuple]:
        options = {
            "decimal_integers": self.dialect.decimal_integers,
            "timestamps_as_text": self.dialect.timestamps_as_text,
            "warn": self._warn_once,
        }
        values = [
            to_parameters(batch.column(i), column, **options)
            for i, column in enumerate(self.columns)
        ]
        return list(zip(*values))

    def _warn_once(self, warning: str) -> None:
        if warning not in self.warnings:
            self.warnings.append(warning)

    def _execute_rows(self, cursor, statements, rows: List[tuple], per_statement: int) -> None:
        if self.dialect.fast_executemany:
            # parameter arrays (the sources never yield empty batches)
            cursor.executemany(statements.rows(1), rows)
            return
        rows, single = self.dialect.split_long_values(self.columns, rows)
        full = len(rows) // per_statement * per_statement
        if full:
            cursor.executemany(
                statements.rows(per_statement),
                [
                    tuple(chain.from_iterable(rows[i : i + per_statement]))
                    for i in range(0, full, per_statement)
                ],
            )
        rest = rows[full:]
        if rest:
            cursor.execute(statements.rows(len(rest)), list(chain.from_iterable(rest)))
        if single:
            cursor.executemany(statements.single_row(), single)

    def _check_cancel(self, rows: int) -> None:
        if self.cancel is not None and self.cancel():
            where = "the staging table" if self.staging == "table" else "the open transaction"
            raise TableWriteCancelled(
                f"Writing table {self.display} (mode {self.mode!r}) was cancelled after {rows} "
                f"row(s) were written to {where}; nothing was published and the table is "
                "unchanged",
                table=self.display,
                mode=self.mode,
                action="write the rows",
            )

    def _check_keys(self, staging: str) -> None:
        keys = [c.sql_name for c in self.columns if c.key]
        with self._step("check the key columns of the staged rows"):
            nulls = self._query(self.dialect.null_keys_sql(staging, keys))[0][0]
            duplicates = self._query(self.dialect.duplicate_keys_sql(staging, keys))[0][0]
        if nulls:
            raise self._error(
                f"{int(nulls)} row(s) of the source have no value in key column(s) "
                f"{', '.join(keys)}; every row needs a key"
            )
        if duplicates:
            raise self._error(
                f"{int(duplicates)} key value(s) occur more than once in the source (key "
                f"column(s) {', '.join(keys)}); each key may occur once"
            )

    # ------------------------------------------------------------------ publishing

    def _publish(self, staging: str) -> None:
        """Move the staged rows into the table, in one transaction (or one rename)."""
        dialect = self.dialect
        target = self._target
        names = self._names()
        if not self.exists and not dialect.transactional_ddl:
            # MySQL and Oracle commit DDL at once: the staging table becomes the table in one
            # atomic rename instead
            if self._primary_key:
                with self._step("add the primary key to the staging table"):
                    self._execute(dialect.add_primary_key_sql(staging, self._primary_key))
            with self._step(
                f"rename the staging table {self.schema}.{self.staging_name} to {self.display}",
                dialect.rename_privilege(self.schema),
            ):
                self._execute(dialect.rename_table_sql(self.schema, self.staging_name, self.table))
                self.connection.commit()
            self.leftover = ""
            self.created = True
            return
        steps: List[Tuple[str, str, str]] = []
        if not self.exists:
            steps.append(
                (
                    f"create table {self.display}",
                    dialect.create_privilege(self.schema),
                    dialect.create_table_sql(target, self.columns, self._primary_key),
                )
            )
        elif self.mode == "replace":
            steps.append(
                (
                    f"delete the rows of table {self.display}",
                    f"DELETE on table {self.display}",
                    f"DELETE FROM {target}",
                )
            )
        if self.exists and self.mode == "upsert":
            pairs = [(name, name) for name in names]
            keys = [(c.sql_name, c.sql_name) for c in self.columns if c.key]
            for sql in dialect.upsert_from_staging_sql(target, staging, pairs, keys):
                steps.append(
                    (
                        f"upsert the staged rows into table {self.display}",
                        f"SELECT, INSERT and UPDATE on table {self.display}",
                        sql,
                    )
                )
        else:
            steps.append(
                (
                    f"insert the staged rows into table {self.display}",
                    f"INSERT on table {self.display}",
                    dialect.insert_select_sql(target, names, staging, names),
                )
            )
        for action, privilege, sql in steps:
            with self._step(action, privilege):
                self._execute(sql)
        with self._step("commit the transaction that publishes the rows"):
            self.connection.commit()
        self.created = not self.exists
        self._drop_staging(staging)

    def _drop_staging(self, staging: str) -> None:
        """Drop the staging table after a publish; a failure is only a warning."""
        self.leftover = ""
        try:
            self._execute(self.dialect.drop_table_sql(staging))
            self.connection.commit()
        except self.errors as error:
            failure = describe_failure(error, self.dialect.codes)
            self.warnings.append(
                f"The rows were published, but the staging table {self.schema}."
                f"{self.staging_name} could not be dropped ({failure.describe()}); drop it, or "
                "it is dropped by the next write with the same job_id"
            )

    def _abandon(self, error: BaseException) -> None:
        """After a failure: roll back and drop what this write created, so nothing is left."""
        try:
            self.connection.rollback()
        except self.errors:
            logger.warning("Could not roll back the transaction (the connection may be lost)")
        if not self.leftover:
            return
        leftover, self.leftover = self.leftover, ""
        try:
            self._execute(self.dialect.drop_table_sql(leftover))
            self.connection.commit()
        except self.errors as drop_error:
            failure = describe_failure(drop_error, self.dialect.codes)
            note = (
                f" The table {leftover} that this write created could not be dropped "
                f"({failure.describe()}); a retry with the same job_id drops a staging table, "
                "any other must be dropped by hand."
            )
            logger.warning(
                "Could not drop %s after a failed write: %s", leftover, failure.describe()
            )
            if isinstance(error, TableWriteError):
                error.args = (str(error) + note,)
