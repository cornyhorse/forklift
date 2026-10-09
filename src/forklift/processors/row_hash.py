"""Row hash processor for adding row-level hash columns and metadata.

Hash encoding versions
----------------------
``hash_version`` 2 (default) hashes an *injective* encoding of the row: for every hashed column
the column name, a type tag and the length-prefixed value bytes (NULL has its own marker), so
distinct rows can no longer collide through separators (``("a||b", "c")`` vs ``("a", "b||c")``),
the string ``"NULL"`` is not NULL, empty bytes are not NULL, and the integer ``1`` differs from the
string ``"1"``. ``null_value`` and ``separator`` are not used by this encoding.

``hash_version`` 1 (``legacy_encoding=True``) reproduces the original preimage byte for byte
(``separator``-joined ``str()`` of the values, ``null_value`` for NULL) so previously stored hashes
can still be verified. Hashes of the two versions are never comparable.

Use in a pipeline that drops rows
---------------------------------
The input hash (the hash of the row *as it entered the pipeline*) and the source row number
(the position of the row in the source) are both properties of the row *before* any stage dropped
or changed rows. When the processor runs as the last step of a per-batch pipeline in which rows
may have been dropped (type conversion, validation, uniqueness) and columns renamed or added,
the caller therefore computes them up front and carries them along:

* ``compute_input_hash(raw_batch)`` returns one hash per raw row. Compute it on the raw batch
  *before* any row is dropped, keep the array aligned with the surviving rows, and pass it as
  ``process_batch(batch, input_hash=...)`` (its length must equal ``len(batch)``).
* ``process_batch(batch, source_row_numbers=...)`` takes the 1-based position of each row among
  the data rows of the source (``int64``, length ``len(batch)``). It replaces the internal
  counter for the source row number column; ``source_row_offset`` is *not* added to it.

The processing sequence column (``_rownum``) always counts the rows this processor has received
since ``set_source_context``. ``output_columns()`` (and ``row_hash_output_columns`` in
``row_hash_factory``) list the columns the processor adds, in order.
"""

from __future__ import annotations

import hashlib
import json
import struct
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc

from .base import BaseProcessor, ValidationResult

HASH_VERSION_LEGACY = 1
HASH_VERSION_CURRENT = 2

#: Metadata keys stored on the hash column's Arrow field (and so in the Parquet schema).
HASH_VERSION_METADATA_KEY = b"forklift.row_hash.version"
HASH_ALGORITHM_METADATA_KEY = b"forklift.row_hash.algorithm"

_WEAK_ALGORITHMS = frozenset({"md5", "sha1"})
_SUPPORTED_ALGORITHMS = ["md5", "sha1", "sha256", "sha384", "sha512"]

_V2_MAGIC = b"forklift-row-hash\x00v2\x00"
_NULL_TAG = b"\x00"


@dataclass
class RowHashConfig:
    """Configuration for row hash column generation and metadata.

    Attributes:
        enabled: Whether to generate row hash column (default: False)
        column_name: Name of the hash column (default: "row_hash")
        algorithm: Hash algorithm to use (default: "sha256")
        include_columns: List of columns to include in hash (None = all columns)
        exclude_columns: List of columns to exclude from hash
        null_value: String to use for NULL values in hash calculation (default: "NULL");
            only used with ``legacy_encoding=True``
        separator: Separator between column values (default: "||"); only used with
            ``legacy_encoding=True``
        allow_weak_hash: Explicit opt-in required to use ``md5`` or ``sha1`` (default: False)
        legacy_encoding: Reproduce the original (hash_version 1) preimage so stored hashes can
            be verified (default: False = injective hash_version 2 encoding)
        input_hash_enabled: Whether to generate input row hash (default: False)
        input_hash_column_name: Name of the input hash column (default: "_input_hash")
        source_uri_enabled: Whether to add source URI column (default: False)
        source_uri_column_name: Name of the source URI column (default: "_source_uri")
        ingested_at_enabled: Whether to add ingestion timestamp (default: False)
        ingested_at_column_name: Name of the ingestion timestamp column
            (default: "_ingested_at_utc")
        row_number_enabled: Whether to add row numbers (default: False)
        source_row_number_column_name: Name of source row number column
            (default: "_rownum_in_source_file")
        processing_row_number_column_name: Name of processing row number column
            (default: "_rownum")
    """

    enabled: bool = False
    column_name: str = "row_hash"
    algorithm: str = "sha256"
    include_columns: Optional[List[str]] = None
    exclude_columns: Optional[List[str]] = None
    null_value: str = "NULL"
    separator: str = "||"
    allow_weak_hash: bool = False
    legacy_encoding: bool = False

    # New input hash options
    input_hash_enabled: bool = False
    input_hash_column_name: str = "_input_hash"

    # New metadata columns
    source_uri_enabled: bool = False
    source_uri_column_name: str = "_source_uri"
    ingested_at_enabled: bool = False
    ingested_at_column_name: str = "_ingested_at_utc"
    row_number_enabled: bool = False
    source_row_number_column_name: str = "_rownum_in_source_file"
    processing_row_number_column_name: str = "_rownum"

    def __post_init__(self):
        """Validate configuration after initialization."""
        if self.exclude_columns is None:
            self.exclude_columns = []

        # Validate algorithm
        if self.algorithm not in _SUPPORTED_ALGORITHMS:
            raise ValueError(
                f"Unsupported hash algorithm: {self.algorithm}. "
                f"Supported algorithms: {_SUPPORTED_ALGORITHMS}"
            )
        if self.algorithm in _WEAK_ALGORITHMS and not self.allow_weak_hash:
            raise ValueError(
                f"Hash algorithm '{self.algorithm}' is cryptographically weak and must be "
                f"enabled explicitly with allow_weak_hash=True (allowWeakHash in the schema)"
            )

    @property
    def hash_version(self) -> int:
        """Encoding version of the row hash: 2 (injective, default) or 1 (legacy)."""
        return HASH_VERSION_LEGACY if self.legacy_encoding else HASH_VERSION_CURRENT

    def output_column_names(self) -> List[str]:
        """Names of the columns the processor adds, in the order it adds them.

        Output hash, input hash, source URI, ingestion timestamp, then (with
        ``row_number_enabled``) the source row number and the processing row number.
        """
        names: List[str] = []
        if self.enabled:
            names.append(self.column_name)
        if self.input_hash_enabled:
            names.append(self.input_hash_column_name)
        if self.source_uri_enabled:
            names.append(self.source_uri_column_name)
        if self.ingested_at_enabled:
            names.append(self.ingested_at_column_name)
        if self.row_number_enabled:
            names.append(self.source_row_number_column_name)
            names.append(self.processing_row_number_column_name)
        return names


class RowHashProcessor(BaseProcessor):
    """Processor for adding row-level hash columns and metadata.

    This processor generates hash columns and metadata for each row including:
    - Output row hash (after transformations)
    - Input row hash (before transformations)
    - Source URI/file path
    - Ingestion timestamp
    - Row numbers (source file and processing sequence)

    The processor supports multiple hash algorithms and flexible column
    inclusion/exclusion rules. Failures raise; a batch is never returned without
    the requested hash/metadata columns.

    Meaning of the two row-number columns: the *source row number* is the 1-based position of
    the row among the data rows of the source (counted from ``source_row_offset`` + 1 over the
    rows this processor receives, unless ``process_batch(source_row_numbers=...)`` supplies the
    real positions - needed when rows were dropped before this processor); the *processing
    sequence number* is the 1-based count of the rows this processor has received since
    ``set_source_context``. See the module docstring for pipelines that drop rows.
    """

    def __init__(self, config: RowHashConfig):
        """Initialize the row hash processor."""
        super().__init__()
        self.config = config
        self.source_uri = None
        self.ingestion_timestamp = None
        self.source_row_offset = 0
        self.processing_row_counter = 0

    def set_source_context(self, source_uri: str, source_row_offset: int = 0):
        """Set source context for metadata generation.

        Starts a new source: the row counters restart, so each source is numbered from 1.
        """
        self.source_uri = source_uri
        self.source_row_offset = source_row_offset
        self.processing_row_counter = 0

        if self.config.ingested_at_enabled:
            from datetime import datetime, timezone

            self.ingestion_timestamp = datetime.now(timezone.utc).isoformat()

    def get_hash_info(self) -> Dict[str, Any]:
        """Describe how the hash column was produced (version, algorithm, encoding)."""
        return {
            "column_name": self.config.column_name,
            "algorithm": self.config.algorithm,
            "hash_version": self.config.hash_version,
            "legacy_encoding": self.config.legacy_encoding,
        }

    def compute_input_hash(self, input_batch: pa.RecordBatch) -> pa.Array:
        """Hash every row of ``input_batch`` the way the input-hash column is computed.

        Returns a string array with one hash per row: exactly the value ``process_batch`` stores
        in the input-hash column for that row when it is given the same batch as ``input_batch``
        (same algorithm, encoding version and column selection: *every* column of
        ``input_batch``, in its schema order, under its own name).

        Call this on the batch as it entered the pipeline, before any row is dropped, and pass
        the result (kept aligned with the surviving rows) as
        ``process_batch(..., input_hash=...)``.
        It does not depend on ``input_hash_enabled`` and does not change any processor state.
        """
        return self._compute_row_hashes(
            input_batch, self._get_input_hash_columns(input_batch.schema)
        )

    def process_batch(
        self,
        batch: pa.RecordBatch,
        input_batch: Optional[pa.RecordBatch] = None,
        *,
        input_hash: Optional[pa.Array] = None,
        source_row_numbers: Optional[pa.Array] = None,
    ) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process a batch by adding hash columns and metadata.

        Args:
            batch: The batch to hash and annotate.
            input_batch: The row as it entered the pipeline, used for the input-hash column. It
                must have exactly the rows of ``batch`` (same count, same order); if rows were
                dropped since, pass ``input_hash`` instead.
            input_hash: Precomputed input hashes (see ``compute_input_hash``): a string array
                with one non-null hash per row of ``batch``. Takes precedence over
                ``input_batch``, which may then be omitted or have a different length.
            source_row_numbers: ``int64`` array with one value per row of ``batch``: the 1-based
                position of the row among the data rows of the source. When given it is used for
                the source row number column instead of the internal counter (and
                ``source_row_offset`` is not added). The processing sequence column keeps
                counting the rows this processor receives.

        Raises:
            ValueError: If a metadata/hash column name already exists in the batch; if
                ``input_hash`` / ``source_row_numbers`` / ``input_batch`` do not match the rows
                of ``batch``; if the input hash is enabled but neither ``input_batch`` nor
                ``input_hash`` is given; if the source URI / ingestion timestamp column is
                enabled but ``set_source_context`` was not called; or if no column is left to
                hash. The processor never returns a batch without a requested column.
        """
        validation_results: List[ValidationResult] = []
        processed_batch = batch
        num_rows = batch.num_rows

        # Check every per-row input before anything is computed or any state changes
        input_hash_values = self._coerce_input_hash(input_hash, num_rows)
        source_row_values = self._coerce_source_row_numbers(source_row_numbers, num_rows)
        if self.config.input_hash_enabled and input_hash_values is None:
            if input_batch is None:
                raise ValueError(
                    f"Input hash is enabled (column '{self.config.input_hash_column_name}') but "
                    f"neither input_batch nor input_hash was given"
                )
            if input_batch.num_rows != num_rows:
                raise ValueError(
                    f"input_batch has {input_batch.num_rows} rows but the batch has {num_rows}; "
                    f"after rows were dropped pass input_hash=compute_input_hash(<raw batch>) "
                    f"aligned with the remaining rows instead"
                )
        if self.config.source_uri_enabled and self.source_uri is None:
            raise ValueError(
                f"Source URI is enabled (column '{self.config.source_uri_column_name}') but "
                f"set_source_context() was not called"
            )
        if self.config.ingested_at_enabled and self.ingestion_timestamp is None:
            raise ValueError(
                f"Ingestion timestamp is enabled (column '{self.config.ingested_at_column_name}')"
                f" but set_source_context() was not called"
            )

        # Add output row hash if enabled
        if self.config.enabled:
            hash_columns = self._get_hash_columns(batch.schema)
            if not hash_columns:
                raise ValueError(
                    f"Row hash column '{self.config.column_name}' is enabled but no column is "
                    f"left to hash (check includeColumns / excludeColumns)"
                )
            hash_values = self._compute_row_hashes(batch, hash_columns)
            processed_batch = self._add_column(
                processed_batch,
                self.config.column_name,
                hash_values,
                metadata=self._hash_field_metadata(),
            )

        # Add input row hash if enabled
        if self.config.input_hash_enabled:
            if input_hash_values is None:
                input_hash_values = self.compute_input_hash(input_batch)
            processed_batch = self._add_column(
                processed_batch,
                self.config.input_hash_column_name,
                input_hash_values,
                metadata=self._hash_field_metadata(),
            )

        # Add source URI if enabled
        if self.config.source_uri_enabled:
            source_uri_values = pa.array([self.source_uri] * num_rows, type=pa.string())
            processed_batch = self._add_column(
                processed_batch, self.config.source_uri_column_name, source_uri_values
            )

        # Add ingestion timestamp if enabled
        if self.config.ingested_at_enabled:
            timestamp_values = pa.array([self.ingestion_timestamp] * num_rows, type=pa.string())
            processed_batch = self._add_column(
                processed_batch, self.config.ingested_at_column_name, timestamp_values
            )

        # Add row numbers if enabled
        if self.config.row_number_enabled:
            # Position in the source: supplied by the caller, else counted from the rows seen
            if source_row_values is None:
                first = self.source_row_offset + self.processing_row_counter + 1
                source_row_values = pa.array(range(first, first + num_rows), type=pa.int64())
            processed_batch = self._add_column(
                processed_batch, self.config.source_row_number_column_name, source_row_values
            )

            # Processing sequence: rows this processor has emitted so far, 1-based
            first = self.processing_row_counter + 1
            processing_row_array = pa.array(range(first, first + num_rows), type=pa.int64())
            processed_batch = self._add_column(
                processed_batch,
                self.config.processing_row_number_column_name,
                processing_row_array,
            )

            # Update counter
            self.processing_row_counter += num_rows

        return processed_batch, validation_results

    @staticmethod
    def _as_array(values: Any, argument: str) -> pa.Array:
        """A single (combined) Arrow array from an Arrow array or chunked array."""
        if isinstance(values, pa.ChunkedArray):
            values = values.combine_chunks()
            if isinstance(values, pa.ChunkedArray):  # zero chunks on some pyarrow versions
                values = pa.array([], type=values.type)
        if not isinstance(values, pa.Array):
            raise TypeError(f"{argument} must be a pyarrow Array, got {type(values).__name__}")
        return values

    def _coerce_input_hash(self, input_hash: Optional[pa.Array], num_rows: int):
        """Validated string array of precomputed input hashes (``None`` if not supplied)."""
        if input_hash is None:
            return None
        values = self._as_array(input_hash, "input_hash")
        if len(values) != num_rows:
            raise ValueError(
                f"input_hash has {len(values)} values but the batch has {num_rows} rows"
            )
        if pa.types.is_null(values.type) or pa.types.is_large_string(values.type):
            values = values.cast(pa.string())
        if not pa.types.is_string(values.type):
            raise ValueError(f"input_hash must be a string array, got {values.type}")
        if values.null_count:
            raise ValueError("input_hash must not contain nulls")
        return values

    def _coerce_source_row_numbers(self, source_row_numbers: Optional[pa.Array], num_rows: int):
        """Validated int64 array of 1-based source positions (``None`` if not supplied)."""
        if source_row_numbers is None:
            return None
        values = self._as_array(source_row_numbers, "source_row_numbers")
        if len(values) != num_rows:
            raise ValueError(
                f"source_row_numbers has {len(values)} values but the batch has {num_rows} rows"
            )
        if not pa.types.is_integer(values.type):
            raise ValueError(f"source_row_numbers must be an integer array, got {values.type}")
        if values.null_count:
            raise ValueError("source_row_numbers must not contain nulls")
        values = values.cast(pa.int64())
        if num_rows and pc.min(values).as_py() < 1:
            raise ValueError("source_row_numbers are 1-based positions and must be >= 1")
        return values

    def output_columns(self) -> List[str]:
        """Names of the columns this processor adds to a batch, in order.

        The source URI and ingestion timestamp columns are only added once
        ``set_source_context`` was called (``process_batch`` raises otherwise).
        """
        return self.config.output_column_names()

    def _hash_field_metadata(self) -> Dict[bytes, bytes]:
        return {
            HASH_VERSION_METADATA_KEY: str(self.config.hash_version).encode("ascii"),
            HASH_ALGORITHM_METADATA_KEY: self.config.algorithm.encode("ascii"),
        }

    def _get_hash_columns(self, schema: pa.Schema) -> List[str]:
        """Determine which columns to include in hash calculation."""
        all_columns = [field.name for field in schema]

        if self.config.include_columns is not None:
            hash_columns = [col for col in self.config.include_columns if col in all_columns]
        else:
            hash_columns = [col for col in all_columns if col not in self.config.exclude_columns]

        # Don't include metadata columns if they already exist
        metadata_columns = [
            self.config.column_name,
            self.config.input_hash_column_name,
            self.config.source_uri_column_name,
            self.config.ingested_at_column_name,
            self.config.source_row_number_column_name,
            self.config.processing_row_number_column_name,
        ]

        hash_columns = [col for col in hash_columns if col not in metadata_columns]
        return hash_columns

    def _get_input_hash_columns(self, schema: pa.Schema) -> List[str]:
        """Get columns for input hash calculation (all columns of the input batch)."""
        return [field.name for field in schema]

    # ------------------------------------------------------------------ hashing

    def _compute_row_hashes(self, batch: pa.RecordBatch, hash_columns: List[str]) -> pa.Array:
        """Compute hash values for each row."""
        if self.config.legacy_encoding:
            preimages = self._legacy_preimages(batch, hash_columns)
        else:
            preimages = self._encoded_preimages(batch, hash_columns)
        return pa.array([self._compute_hash(preimage) for preimage in preimages], pa.string())

    def _legacy_preimages(self, batch: pa.RecordBatch, hash_columns: List[str]) -> List[bytes]:
        """hash_version 1 preimage: ``separator``-joined text, ``null_value`` for NULL."""
        preimages = []
        for row_idx in range(batch.num_rows):
            row_parts = []
            for col_name in hash_columns:
                column = batch.column(col_name)
                value = column[row_idx]

                if value.is_valid:
                    if pa.types.is_string(column.type) or pa.types.is_large_string(column.type):
                        row_parts.append(str(value.as_py()))
                    elif pa.types.is_binary(column.type):
                        row_parts.append(
                            value.as_py().hex() if value.as_py() else self.config.null_value
                        )
                    else:
                        row_parts.append(str(value.as_py()))
                else:
                    row_parts.append(self.config.null_value)

            preimages.append(self.config.separator.join(row_parts).encode("utf-8"))
        return preimages

    def _encoded_preimages(self, batch: pa.RecordBatch, hash_columns: List[str]) -> List[bytes]:
        """hash_version 2 preimage: injective, typed, length-prefixed, names included."""
        indices = self._resolve_columns(batch.schema, hash_columns)
        header = _V2_MAGIC + struct.pack(">Q", len(indices))
        # (length-prefixed name, per-row (tag, payload) cells) for each column
        columns = []
        for col_idx in indices:
            name = batch.schema.field(col_idx).name.encode("utf-8")
            columns.append((struct.pack(">Q", len(name)) + name, _cells(batch.column(col_idx))))

        preimages = []
        for row_idx in range(batch.num_rows):
            parts = [header]
            for name_part, cells in columns:
                parts.append(name_part)
                cell = cells[row_idx]
                if cell is None:
                    parts.append(_NULL_TAG)
                else:
                    tag, payload = cell
                    parts.append(tag + struct.pack(">Q", len(payload)) + payload)
            preimages.append(b"".join(parts))
        return preimages

    @staticmethod
    def _resolve_columns(schema: pa.Schema, names: List[str]) -> List[int]:
        """Checked name -> index lookup (raises on missing or duplicated names)."""
        indices = []
        for name in names:
            matches = [i for i, field in enumerate(schema) if field.name == name]
            if len(matches) != 1:
                raise ValueError(
                    f"Cannot hash column '{name}': it is "
                    f"{'missing' if not matches else 'present more than once'} in the batch"
                )
            indices.append(matches[0])
        return indices

    def _compute_hash(self, data) -> str:
        """Compute hash of the given bytes (or text, UTF-8 encoded)."""
        data_bytes = data.encode("utf-8") if isinstance(data, str) else data

        if self.config.algorithm in _SUPPORTED_ALGORITHMS:
            return hashlib.new(self.config.algorithm, data_bytes).hexdigest()
        raise ValueError(f"Unsupported hash algorithm: {self.config.algorithm}")

    def _add_column(
        self,
        batch: pa.RecordBatch,
        column_name: str,
        column_values: pa.Array,
        metadata: Optional[Dict[bytes, bytes]] = None,
    ) -> pa.RecordBatch:
        """Add a column to the batch (the name must not exist yet)."""
        if column_name in batch.schema.names:
            raise ValueError(
                f"Cannot add column '{column_name}': the batch already has a column with "
                f"that name (choose a different column name in the row hash configuration)"
            )

        new_fields = list(batch.schema)
        new_fields.append(pa.field(column_name, column_values.type, metadata=metadata))
        new_schema = pa.schema(new_fields, metadata=batch.schema.metadata)

        new_columns = list(batch.columns)
        new_columns.append(column_values)

        return pa.RecordBatch.from_arrays(new_columns, schema=new_schema)

    def _output_fields(self) -> List[pa.Field]:
        """The fields ``process_batch`` adds, in order (types and metadata are fixed)."""
        config = self.config
        hash_metadata = self._hash_field_metadata()
        fields: List[pa.Field] = []
        if config.enabled:
            fields.append(pa.field(config.column_name, pa.string(), metadata=hash_metadata))
        if config.input_hash_enabled:
            fields.append(
                pa.field(config.input_hash_column_name, pa.string(), metadata=hash_metadata)
            )
        if config.source_uri_enabled:
            fields.append(pa.field(config.source_uri_column_name, pa.string()))
        if config.ingested_at_enabled:
            fields.append(pa.field(config.ingested_at_column_name, pa.string()))
        if config.row_number_enabled:
            fields.append(pa.field(config.source_row_number_column_name, pa.int64()))
            fields.append(pa.field(config.processing_row_number_column_name, pa.int64()))
        return fields

    def get_output_schema(self, input_schema: pa.Schema) -> pa.Schema:
        """Get the output schema: the input schema plus every column the processor adds.

        Same columns, types and field metadata as ``process_batch`` produces (for a batch with
        rows as well as for an empty one).
        """
        new_fields = list(input_schema) + self._output_fields()
        return pa.schema(new_fields, metadata=input_schema.metadata)


def _cells(column: pa.Array) -> List[Optional[Tuple[bytes, bytes]]]:
    """Per-row ``(type tag, payload)`` for the version-2 encoding; ``None`` for NULL."""
    if isinstance(column, pa.ChunkedArray):
        column = column.combine_chunks()
    t = column.type

    if pa.types.is_dictionary(t):
        return _cells(column.dictionary_decode())

    def encode(values, tag: bytes, to_bytes) -> List[Optional[Tuple[bytes, bytes]]]:
        return [None if v is None else (tag, to_bytes(v)) for v in values]

    if pa.types.is_null(t):
        return [None] * len(column)
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return encode(column.to_pylist(), b"s", lambda v: v.encode("utf-8"))
    if pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_fixed_size_binary(t):
        return encode(column.to_pylist(), b"b", bytes)
    if pa.types.is_boolean(t):
        return encode(column.to_pylist(), b"o", lambda v: b"1" if v else b"0")
    if pa.types.is_integer(t):
        return encode(column.to_pylist(), b"i", lambda v: str(v).encode("ascii"))
    if pa.types.is_floating(t):
        return encode(column.to_pylist(), b"f", _float_bytes)
    if pa.types.is_decimal(t):
        return encode(column.to_pylist(), b"d", lambda v: format(v, "f").encode("ascii"))
    if pa.types.is_date32(t) or pa.types.is_time32(t):
        storage = column.cast(pa.int32()).to_pylist()
        return encode(storage, b"t", lambda v: f"{t}|{v}".encode("utf-8"))
    if (
        pa.types.is_date64(t)
        or pa.types.is_time64(t)
        or pa.types.is_timestamp(t)
        or pa.types.is_duration(t)
    ):
        storage = column.cast(pa.int64()).to_pylist()
        return encode(storage, b"t", lambda v: f"{t}|{v}".encode("utf-8"))

    # Nested and other types: canonical JSON of the Python value, qualified by the Arrow type
    def nested_bytes(value: Any) -> bytes:
        canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
        return f"{t}|{canonical}".encode("utf-8")

    return encode(column.to_pylist(), b"x", nested_bytes)


def _float_bytes(value: float) -> bytes:
    """IEEE-754 big-endian bytes with a single NaN and no negative zero."""
    if value != value:
        return struct.pack(">d", float("nan"))
    if value == 0:
        return struct.pack(">d", 0.0)
    return struct.pack(">d", value)
