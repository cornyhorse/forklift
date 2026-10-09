"""Schema extensions applied while importing a CSV file.

``import_csv`` reads a JSON schema whose ``x-...`` extensions describe processing that goes beyond
column types. :class:`ExtensionPipeline` turns those extensions into the processors in
``forklift.processors`` and runs them on every batch.

Per batch the stages run in this order::

    reader (raw text)
      PRE   hidden row id / input hash columns, ``x-transformations``    (header names)
      engine: schema types, ``x-csv.nulls``, ``required``                 (existing behaviour)
      POST  ``x-columnMapping``       renames columns; from here on columns have their output names
            ``x-calculatedColumns``   appends columns
            ``x-dataQuality``         reports only
            ``x-validation``          rejects rows
            ``x-primaryKey`` / ``x-uniqueConstraints`` / per-property constraints /
            ``x-constraintHandling``  rejects rows
            ``x-rowHash``             appends hash and metadata columns

Names: ``properties``, ``required``, ``x-csv`` and ``x-transformations`` use the column names of the
file header; every extension after the mapping step uses the output names (a header name that was
renamed is accepted there too and resolved to its output name).

Rows rejected by the validation and constraint stages are returned to the caller, which writes them
to ``bad_rows.parquet`` (in the shape of the input columns, with a reason per row).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence

import pyarrow as pa

from ...processors.base import ValidationResult

logger = logging.getLogger(__name__)

#: Columns the pipeline adds temporarily; they never reach an output file.
HIDDEN_PREFIX = "__forklift_"
ROW_ID_COLUMN = f"{HIDDEN_PREFIX}row_id"
INPUT_HASH_COLUMN = f"{HIDDEN_PREFIX}input_hash"
POSITION_COLUMN = f"{HIDDEN_PREFIX}pos"

#: Name of the column that explains why a row is in bad_rows.parquet (see ``rejects_rows``).
REASON_COLUMN = "_rejection_reason"

#: At most this many distinct "CODE:column" keys are counted in ``summary``; the rest is "OTHER".
_MAX_SUMMARY_KEYS = 200
#: Reasons are truncated to this length.
_MAX_REASON_LENGTH = 200


def strip_hidden_columns(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Remove the pipeline's temporary columns from ``batch``."""
    keep = [i for i, name in enumerate(batch.schema.names) if not name.startswith(HIDDEN_PREFIX)]
    if len(keep) == batch.num_columns:
        return batch
    return pa.RecordBatch.from_arrays(
        [batch.column(i) for i in keep], schema=pa.schema([batch.schema.field(i) for i in keep])
    )


def _with_column(batch: pa.RecordBatch, name: str, array: pa.Array) -> pa.RecordBatch:
    return pa.RecordBatch.from_arrays(
        list(batch.columns) + [array],
        schema=pa.schema(list(batch.schema) + [pa.field(name, array.type)]),
    )


def _without_column(batch: pa.RecordBatch, name: str) -> pa.RecordBatch:
    position = batch.schema.get_field_index(name)
    if position < 0:
        return batch
    keep = [i for i in range(batch.num_columns) if i != position]
    return pa.RecordBatch.from_arrays(
        [batch.column(i) for i in keep], schema=pa.schema([batch.schema.field(i) for i in keep])
    )


def _result_label(result: ValidationResult) -> str:
    """``CODE`` or ``CODE:column`` - never a message, which could quote a cell value."""
    code = result.error_code or "VALIDATION_ERROR"
    return f"{code}:{result.column_name}" if result.column_name else code


@dataclass
class PostStageResult:
    """Outcome of the post-conversion stage for one batch.

    Attributes:
        kept: Rows that continue to the data file, in their final shape
        rejected: Rows rejected by validation/constraints, in the shape they had when they
            entered the post stage (typed values, header column names); None if there were none
        reasons: One reason per rejected row, in the same order
    """

    kept: pa.RecordBatch
    rejected: Optional[pa.RecordBatch] = None
    reasons: List[str] = field(default_factory=list)


class ExtensionPipeline:
    """Runs the schema extensions on the batches of one import.

    Instances are stateful (uniqueness bookkeeping, row counters) and meant for one import; build
    one with :meth:`from_schema`.

    Attributes:
        warnings: Notes for the user (unsupported schema content, ...), set when building
        summary: Count of non-valid :class:`ValidationResult` s per ``CODE`` / ``CODE:column``
            over all batches so far
        applied: Names of the extensions that are active
    """

    def __init__(
        self,
        *,
        transformer: Any = None,
        mapper: Any = None,
        calculated: Any = None,
        quality: Any = None,
        validator: Any = None,
        constraints: Any = None,
        row_hash: Any = None,
        source_uri: Optional[str] = None,
        warnings: Optional[List[str]] = None,
    ):
        self.transformer = transformer
        self.mapper = mapper
        self.calculated = calculated
        self.quality = quality
        self.validator = validator
        self.constraints = constraints
        self.row_hash = row_hash
        self.warnings: List[str] = list(warnings or [])
        self.summary: Dict[str, int] = {}
        self._rows_seen = 0

        config = getattr(row_hash, "config", None)
        self._needs_input_hash = bool(getattr(config, "input_hash_enabled", False))
        self._needs_row_ids = bool(getattr(config, "row_number_enabled", False))

        if row_hash is not None and source_uri is not None:
            row_hash.set_source_context(source_uri)

        self.applied: List[str] = [
            name
            for name, stage in (
                ("x-transformations", transformer),
                ("x-columnMapping", mapper),
                ("x-calculatedColumns", calculated),
                ("x-dataQuality", quality),
                ("x-validation", validator),
                ("x-primaryKey/x-uniqueConstraints/constraints", constraints),
                ("x-rowHash", row_hash),
            )
            if stage is not None
        ]

    # ------------------------------------------------------------------------------ queries

    @property
    def has_pre_stage(self) -> bool:
        """Whether raw batches need to pass through :meth:`pre_convert`."""
        return bool(self.transformer is not None or self._needs_input_hash or self._needs_row_ids)

    @property
    def rejects_rows(self) -> bool:
        """Whether rows can be sent to bad_rows by the validation/constraint stages.

        When True, bad_rows.parquet has a :data:`REASON_COLUMN` for every row it contains.
        """
        return self.validator is not None or self.constraints is not None

    @property
    def is_active(self) -> bool:
        return bool(self.applied)

    # ----------------------------------------------------------------------------- recording

    def record(self, results: Sequence[ValidationResult]) -> None:
        """Count the invalid results (by code and column) in :attr:`summary`."""
        for result in results:
            if result.is_valid:
                continue
            key = _result_label(result)
            if key not in self.summary and len(self.summary) >= _MAX_SUMMARY_KEYS:
                key = "OTHER"
            self.summary[key] = self.summary.get(key, 0) + 1

    # ---------------------------------------------------------------------------------- PRE

    def pre_convert(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Run the pre-conversion stage on a raw (text) batch.

        Adds the hidden row id / input hash columns that follow each row through type conversion,
        then applies ``x-transformations`` to the text columns.
        """
        count = batch.num_rows
        hidden: List[pa.Array] = []
        names: List[str] = []

        if self._needs_input_hash:
            # The hash is taken before any transformation: it identifies the row as it was read
            hidden.append(self.row_hash.compute_input_hash(batch))
            names.append(INPUT_HASH_COLUMN)
        if self._needs_row_ids:
            first = self._rows_seen + 1
            hidden.append(pa.array(range(first, first + count), type=pa.int64()))
            names.append(ROW_ID_COLUMN)
        self._rows_seen += count

        if self.transformer is not None:
            batch, results = self.transformer.process_batch(batch)
            self.record(results)

        for name, array in zip(names, hidden):
            batch = _with_column(batch, name, array)
        return batch

    # --------------------------------------------------------------------------------- POST

    def post_convert(self, batch: pa.RecordBatch) -> PostStageResult:
        """Run the post-conversion stage on a typed batch that passed the ``required`` check."""
        row_ids = self._hidden(batch, ROW_ID_COLUMN)
        input_hash = self._hidden(batch, INPUT_HASH_COLUMN)
        data = strip_hidden_columns(batch)
        total = data.num_rows

        current = data
        if self.mapper is not None:
            current, results = self.mapper.process_batch(current)
            self.record(results)
        if self.calculated is not None:
            current, results = self.calculated.process_batch(current)
            self.record(results)

        kept_positions = pa.array(range(total), type=pa.int64())
        reasons: Dict[int, List[str]] = {}

        if self.quality is not None:
            current, results = self.quality.process_batch(current)
            self.record(results)

        if self.validator is not None or self.constraints is not None:
            current = _with_column(current, POSITION_COLUMN, kept_positions)
            for stage_name, stage in (
                ("x-validation", self.validator),
                ("constraints", self.constraints),
            ):
                if stage is None:
                    continue
                before = current.column(POSITION_COLUMN).to_pylist()
                current, results = stage.process_batch(current)
                self.record(results)
                after = set(current.column(POSITION_COLUMN).to_pylist())
                self._attribute_reasons(reasons, before, after, results, stage_name)
            kept_positions = current.column(POSITION_COLUMN)
            current = _without_column(current, POSITION_COLUMN)

        rejected_batch: Optional[pa.RecordBatch] = None
        reason_list: List[str] = []
        if len(current) != total:
            kept = set(kept_positions.to_pylist())
            rejected_positions = [p for p in range(total) if p not in kept]
            rejected_batch = data.take(pa.array(rejected_positions, type=pa.int64()))
            reason_list = [
                "; ".join(dict.fromkeys(reasons.get(p, ["REJECTED"])))[:_MAX_REASON_LENGTH]
                for p in rejected_positions
            ]

        if self.row_hash is not None:
            kwargs: Dict[str, Any] = {}
            if input_hash is not None:
                kwargs["input_hash"] = input_hash.take(kept_positions)
            if row_ids is not None:
                kwargs["source_row_numbers"] = row_ids.take(kept_positions)
            current, results = self.row_hash.process_batch(current, **kwargs)
            self.record(results)

        return PostStageResult(kept=current, rejected=rejected_batch, reasons=reason_list)

    @staticmethod
    def _hidden(batch: pa.RecordBatch, name: str) -> Optional[pa.Array]:
        position = batch.schema.get_field_index(name)
        return batch.column(position) if position >= 0 else None

    @staticmethod
    def _attribute_reasons(
        reasons: Dict[int, List[str]],
        before: List[int],
        after: set,
        results: Sequence[ValidationResult],
        stage_name: str,
    ) -> None:
        """Record why each row that disappeared in one stage was rejected.

        ``before`` holds the original position of every row the stage received; a result's
        ``row_index`` points into that list.
        """
        dropped = {pos for pos in before if pos not in after}
        explained = set()
        for result in results:
            index = result.row_index
            if result.is_valid or index is None or not 0 <= index < len(before):
                continue
            position = before[index]
            if position in dropped:
                reasons.setdefault(position, []).append(_result_label(result))
                explained.add(position)
        for position in dropped - explained:
            reasons.setdefault(position, []).append(stage_name.upper())

    # --------------------------------------------------------------------------- bookkeeping

    def output_schema(self, input_schema: pa.Schema) -> pa.Schema:
        """Schema of the data file for rows that enter the post stage with ``input_schema``."""
        fields = [f for f in input_schema if not f.name.startswith(HIDDEN_PREFIX)]
        if self._needs_input_hash:
            fields.append(pa.field(INPUT_HASH_COLUMN, pa.string()))
        if self._needs_row_ids:
            fields.append(pa.field(ROW_ID_COLUMN, pa.int64()))
        arrays = [pa.array([], type=f.type) for f in fields]
        empty = pa.RecordBatch.from_arrays(arrays, schema=pa.schema(fields))
        return self.post_convert(empty).kept.schema

    def finalize(self) -> None:
        """Call after the last batch: stages that can only judge the whole input report now.

        Raises:
            ValueError: ``errorMode: fail_complete`` and constraints were violated
        """
        if self.constraints is not None:
            self.constraints.finalize()

    def describe(self) -> Dict[str, Any]:
        """Summary for the processing metadata file."""
        return {
            "applied": list(self.applied),
            "warnings": list(self.warnings),
            "validation_summary": dict(self.summary),
        }


# --------------------------------------------------------------------------------- building


def build_extension_pipeline(
    schema: Dict[str, Any],
    header_names: Sequence[str],
    *,
    source_uri: Optional[str] = None,
    log: Callable[[str], None] = logger.warning,
) -> Optional[ExtensionPipeline]:
    """Create the pipeline for ``schema``, or None if the schema asks for nothing.

    Args:
        schema: The loaded JSON schema
        header_names: Column names of the input file (as the reader will deliver them)
        source_uri: Where the data comes from (recorded by ``x-rowHash`` source columns)
        log: Called once per warning

    Raises:
        ValueError: An extension is configured incorrectly, refers to a column that does not
            exist, or would create a column that already exists
    """
    # Imported here: the processors package pulls in modules the plain engine does not need
    from ...processors.calculated_columns_factory import (
        create_calculated_columns_processor_from_schema,
    )
    from ...processors.row_hash_factory import (
        create_row_hash_processor_from_schema,
        row_hash_output_columns,
    )
    from ...processors.schema_extensions import (
        build_column_mapper,
        build_constraint_validator,
        build_data_validator,
        build_quality_processor,
        referenced_columns,
        unsupported_extension_keys,
    )
    from ...processors.transformations import SchemaBasedTransformer

    header = list(header_names)
    warnings: List[str] = list(unsupported_extension_keys(schema))

    # --- renames first: every later stage refers to the output names
    mapper = build_column_mapper(schema)
    mapping = mapper.output_names(header) if mapper is not None else {n: n for n in header}
    output_names = [name for name in mapping.values() if name is not None]

    def resolve(name: str) -> str:
        target = mapping.get(name)
        return target if target is not None else name

    # --- pre stage
    transformer = SchemaBasedTransformer(schema)
    if not transformer.column_transformations:
        transformer = None  # nothing configured (also: no x-special-type columns)
    if transformer is not None:
        for column in transformer.column_transformations:
            if column not in header:
                warnings.append(
                    f"x-transformations refers to column '{column}', which is not in the input"
                )

    # --- post stage
    calculated = None
    if schema.get("x-calculatedColumns"):
        calculated = create_calculated_columns_processor_from_schema(schema["x-calculatedColumns"])
    calculated_names = [c.name for c in calculated.config.columns] if calculated else []
    clashes = sorted(set(calculated_names) & set(output_names))
    if clashes:
        raise ValueError(
            f"x-calculatedColumns would overwrite existing column(s) {clashes}; "
            "rename the calculated column(s) or the input column(s)"
        )
    available = output_names + calculated_names

    quality = build_quality_processor(schema, resolve_column=resolve)
    validator = build_data_validator(schema, resolve_column=resolve)
    constraints = build_constraint_validator(schema, resolve_column=resolve)

    row_hash = None
    hash_columns: List[str] = []
    if schema.get("x-rowHash"):
        row_hash = create_row_hash_processor_from_schema(schema["x-rowHash"])
        if row_hash is not None:
            hash_columns = row_hash_output_columns(schema["x-rowHash"])
            clashes = sorted(set(hash_columns) & set(available))
            if clashes:
                raise ValueError(
                    f"x-rowHash would overwrite existing column(s) {clashes}; "
                    "change its column names (columnName, ...)"
                )

    warnings.extend(_check_references(referenced_columns(schema), available, resolve=resolve))

    if any(name.startswith(HIDDEN_PREFIX) for name in header) and (
        row_hash is not None or transformer is not None
    ):
        raise ValueError(f"Column names starting with '{HIDDEN_PREFIX}' are reserved")

    pipeline = ExtensionPipeline(
        transformer=transformer,
        mapper=mapper,
        calculated=calculated,
        quality=quality,
        validator=validator,
        constraints=constraints,
        row_hash=row_hash,
        source_uri=source_uri,
        warnings=warnings,
    )
    for message in warnings:
        log(message)
    return pipeline if (pipeline.is_active or warnings) else None


def _check_references(
    references: Dict[str, List[str]], available: Sequence[str], *, resolve: Callable[[str], str]
) -> List[str]:
    """Fail early when an extension refers to a column that will not exist.

    A schema may describe more columns than a given file has, so constraints declared on plain
    ``properties`` only produce a warning; the explicit key/validation extensions raise.

    Returns:
        Warnings for the lenient cases
    """
    known = set(available)
    warnings: List[str] = []
    for extension, columns in references.items():
        missing = sorted({c for c in columns if resolve(c) not in known})
        if not missing:
            continue
        shown = ", ".join(repr(c) for c in missing)
        if extension == "properties":
            warnings.append(
                f"constraints declared on column(s) {shown} are not checked: "
                "the columns are not in the input"
            )
            continue
        raise ValueError(
            f"{extension} refers to column(s) {shown} that are not in the input "
            f"(columns after mapping: {', '.join(sorted(known))})"
        )
    return warnings
