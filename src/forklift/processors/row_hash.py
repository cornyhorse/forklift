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
"""

from __future__ import annotations

import hashlib
import json
import struct
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa

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

    def process_batch(
        self, batch: pa.RecordBatch, input_batch: Optional[pa.RecordBatch] = None
    ) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process a batch by adding hash columns and metadata.

        Raises:
            ValueError: If a metadata/hash column name already exists in the batch.
        """
        validation_results: List[ValidationResult] = []
        processed_batch = batch

        # Add output row hash if enabled
        if self.config.enabled:
            hash_columns = self._get_hash_columns(batch.schema)
            if hash_columns:
                hash_values = self._compute_row_hashes(batch, hash_columns)
                processed_batch = self._add_column(
                    processed_batch,
                    self.config.column_name,
                    hash_values,
                    metadata=self._hash_field_metadata(),
                )

        # Add input row hash if enabled and input batch provided
        if self.config.input_hash_enabled and input_batch is not None:
            input_hash_columns = self._get_input_hash_columns(input_batch.schema)
            if input_hash_columns:
                input_hash_values = self._compute_row_hashes(input_batch, input_hash_columns)
                processed_batch = self._add_column(
                    processed_batch,
                    self.config.input_hash_column_name,
                    input_hash_values,
                    metadata=self._hash_field_metadata(),
                )

        # Add source URI if enabled
        if self.config.source_uri_enabled and self.source_uri:
            source_uri_values = pa.array([self.source_uri] * batch.num_rows, type=pa.string())
            processed_batch = self._add_column(
                processed_batch, self.config.source_uri_column_name, source_uri_values
            )

        # Add ingestion timestamp if enabled
        if self.config.ingested_at_enabled and self.ingestion_timestamp:
            timestamp_values = pa.array(
                [self.ingestion_timestamp] * batch.num_rows, type=pa.string()
            )
            processed_batch = self._add_column(
                processed_batch, self.config.ingested_at_column_name, timestamp_values
            )

        # Add row numbers if enabled
        if self.config.row_number_enabled:
            # Source file row numbers
            source_row_numbers = list(
                range(
                    self.source_row_offset + self.processing_row_counter + 1,
                    self.source_row_offset + self.processing_row_counter + batch.num_rows + 1,
                )
            )
            source_row_array = pa.array(source_row_numbers, type=pa.int64())
            processed_batch = self._add_column(
                processed_batch, self.config.source_row_number_column_name, source_row_array
            )

            # Processing sequence row numbers
            processing_row_numbers = list(
                range(
                    self.processing_row_counter + 1,
                    self.processing_row_counter + batch.num_rows + 1,
                )
            )
            processing_row_array = pa.array(processing_row_numbers, type=pa.int64())
            processed_batch = self._add_column(
                processed_batch,
                self.config.processing_row_number_column_name,
                processing_row_array,
            )

            # Update counter
            self.processing_row_counter += batch.num_rows

        return processed_batch, validation_results

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
        """Get columns for input hash calculation (all original columns)."""
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
        new_schema = pa.schema(new_fields)

        new_columns = list(batch.columns)
        new_columns.append(column_values)

        return pa.RecordBatch.from_arrays(new_columns, schema=new_schema)

    def get_output_schema(self, input_schema: pa.Schema) -> pa.Schema:
        """Get the output schema with hash column added."""
        if not self.config.enabled:
            return input_schema

        new_fields = list(input_schema)
        new_fields.append(
            pa.field(self.config.column_name, pa.string(), metadata=self._hash_field_metadata())
        )
        return pa.schema(new_fields)


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
