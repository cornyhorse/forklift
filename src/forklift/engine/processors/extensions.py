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

Names: ``properties``, ``required``, ``x-csv`` and ``x-transformations`` use the column names of
the file header; every extension after the mapping step uses the output names (a header name that
was renamed is accepted there too and resolved to its output name).

Rows rejected by the validation and constraint stages are returned to the caller, which writes them
to ``bad_rows.parquet`` (in the shape of the input columns, with a reason per row).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

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

#: Extensions whose rules for a column the input lacks (but ``properties`` declares) only warn;
#: a column missing from the input is always an error for the key extensions.
LENIENT_EXTENSIONS = frozenset({"properties", "x-validation", "x-dataQuality"})

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
        mark_nulls: Optional[Callable[[pa.RecordBatch], pa.RecordBatch]] = None,
    ):
        self.mark_nulls = mark_nulls
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
            if self.mark_nulls is not None:
                # x-csv.nulls describe the text of the file, so they apply before it is rewritten
                batch = self.mark_nulls(batch)
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
    mark_nulls: Optional[Callable[[pa.RecordBatch], pa.RecordBatch]] = None,
    log: Callable[[str], None] = logger.warning,
) -> Optional[ExtensionPipeline]:
    """Create the pipeline for ``schema``, or None if the schema asks for nothing.

    Args:
        schema: The loaded JSON schema
        header_names: Column names of the input file (as the reader will deliver them)
        source_uri: Where the data comes from (recorded by ``x-rowHash`` source columns)
        mark_nulls: Applies the schema's null markers to a raw batch (see
            ``ColumnConverter.mark_nulls``); run before ``x-transformations``
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
        skipped = [c for c in transformer.column_transformations if c not in header]
        if skipped:
            warnings.append(
                "x-transformations (and x-special-type) steps for column(s) "
                f"{', '.join(repr(c) for c in skipped)} are not applied: "
                "the columns are not in the input"
            )

    # --- post stage
    properties = schema.get("properties")
    declared_names = set(properties) if isinstance(properties, dict) else set()
    calculated = None
    if schema.get("x-calculatedColumns"):
        renamed = {src: dst for src, dst in mapping.items() if dst is not None and dst != src}
        calculated_config, skipped = _calculated_columns_for_input(
            schema["x-calculatedColumns"], output_names, declared_names, resolve, renamed
        )
        warnings.extend(skipped)
        calculated = create_calculated_columns_processor_from_schema(calculated_config)
    calculated_names = [c.name for c in calculated.config.columns] if calculated else []
    clashes = sorted(set(calculated_names) & set(output_names))
    if clashes:
        raise ValueError(
            f"x-calculatedColumns would overwrite existing column(s) {clashes}; "
            "rename the calculated column(s) or the input column(s)"
        )
    available = output_names + calculated_names

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

    declared = declared_names
    warnings.extend(_renamed_onto_property_warnings(mapping, declared))
    reference_warnings, absent = _check_references(
        referenced_columns(schema), available, declared=declared, resolve=resolve
    )
    warnings.extend(reference_warnings)

    # Rules for columns this input lacks are left out (they were reported above)
    rules_schema = _without_columns(schema, absent)
    quality = build_quality_processor(rules_schema, resolve_column=resolve)
    validator = build_data_validator(rules_schema, resolve_column=resolve)
    constraints = build_constraint_validator(rules_schema, resolve_column=resolve)

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
        mark_nulls=mark_nulls,
    )
    for message in warnings:
        log(message)
    return pipeline if (pipeline.is_active or warnings) else None


def _calculated_columns_for_input(
    config: Any,
    output_names: Sequence[str],
    declared: set,
    resolve: Callable[[str], str],
    renamed: Dict[str, str],
) -> Tuple[Any, List[str]]:
    """``x-calculatedColumns`` checked against the columns of this input.

    Every name and function an expression uses is checked here, before anything is written, so a
    typo is reported with the column it is in and a suggestion instead of failing on the first
    row. A column that uses (or lists in ``dependencies``) a column the input lacks is left out
    with a warning when ``properties`` declares that column (a standard describing more columns
    than the file); the same goes for a column that depends on one that was left out.

    Args:
        config: The ``x-calculatedColumns`` object
        output_names: Columns of the data after ``x-columnMapping``
        declared: Names the schema declares in ``properties``
        resolve: Maps a header name to its output name
        renamed: Header name -> new name, for the columns ``x-columnMapping`` renames

    Returns:
        ``(config, warnings)``; ``config`` is a copy when something was left out

    Raises:
        ValueError: An expression calls an unknown function or uses a name that is neither a
            column, a constant, nor declared in ``properties``
    """
    from ...processors.calculated_columns.functions import get_available_functions, get_constants
    from ...processors.calculated_columns.limits import ExpressionError
    from ...processors.calculated_columns.safe_eval import (
        compile_expression,
        unknown_function_message,
        unknown_name_message,
    )

    if not isinstance(config, dict):
        return config, []

    def entries(key: str) -> List[Any]:
        value = config.get(key)
        return [e for e in value if isinstance(e, dict)] if isinstance(value, list) else []

    constants = {e.get("name") for e in entries("constants")}
    pending = entries("expressions") + entries("calculated")
    present = set(output_names) | constants
    functions = set(get_available_functions())
    builtin = set(get_constants())

    # name -> names the expression uses as values (empty when it does not compile: the
    # processor reports that, with the expression's own message)
    uses: Dict[int, List[str]] = {}
    for entry in pending:
        source = entry.get("expression", entry.get("function", ""))
        try:
            compiled = compile_expression(source) if isinstance(source, str) else None
        except ExpressionError:
            compiled = None
        # ``names`` also lists the targets of calls; those are checked as functions below
        called = set(compiled.function_names) if compiled else set()
        uses[id(entry)] = [n for n in compiled.names if n not in called] if compiled else []
        for function in compiled.function_names if compiled else ():
            if function not in functions:
                raise ValueError(
                    f"x-calculatedColumns column '{entry.get('name')}': "
                    f"{unknown_function_message(function, functions)}"
                )

    dropped: Dict[str, List[str]] = {}
    changed = True
    while changed:
        changed = False
        for entry in list(pending):
            kept_names = {e.get("name") for e in pending}
            satisfied = present | kept_names

            listed = entry.get("dependencies")
            missing = [
                d
                for d in (listed if isinstance(listed, list) else [])
                if isinstance(d, str) and d not in satisfied and resolve(d) not in satisfied
            ]
            for name in uses[id(entry)]:
                if name in satisfied or name in builtin:
                    continue
                if name in renamed and renamed[name] in satisfied:
                    raise ValueError(
                        f"x-calculatedColumns column '{entry.get('name')}' uses '{name}', "
                        f"which x-columnMapping renames to '{renamed[name]}'; write "
                        f"'{renamed[name]}' in the expression"
                    )
                if name not in missing:
                    missing.append(name)
            if not missing:
                continue

            unknown = [d for d in missing if d not in declared and d not in dropped]
            if unknown:
                first = unknown[0]
                raise ValueError(
                    f"x-calculatedColumns column '{entry.get('name')}': "
                    + unknown_name_message(first, sorted(satisfied - {entry.get("name")}))
                    + (
                        f" (also not found: {', '.join(map(repr, unknown[1:]))})"
                        if unknown[1:]
                        else ""
                    )
                    + " If the column exists only in some files, declare it under 'properties' "
                    "so that this calculated column is skipped (with a warning) for files "
                    "that lack it."
                )
            dropped[entry.get("name")] = missing
            pending.remove(entry)
            changed = True

    if not dropped:
        return config, []
    kept = {id(e) for e in pending}
    pruned = dict(config)
    for key in ("expressions", "calculated"):
        if isinstance(config.get(key), list):
            pruned[key] = [e for e in config[key] if not isinstance(e, dict) or id(e) in kept]
    notes = [
        f"x-calculatedColumns column '{name}' is not added: "
        f"{', '.join(repr(d) for d in missing)} not in the input"
        for name, missing in dropped.items()
    ]
    return pruned, notes


def _renamed_onto_property_warnings(mapping: Dict[str, Optional[str]], declared: set) -> List[str]:
    """Warn for a column renamed to the name of a property that is meant for another column.

    ``properties`` (types, ``required``, constraints, ``x-csv.nulls``) are matched by the names
    in the file, before ``x-columnMapping`` renames anything. A property declared under the new
    name therefore does not apply to the renamed column.
    """
    warnings: List[str] = []
    for source, target in mapping.items():
        if (
            target is not None
            and target != source
            and target in declared
            and source not in declared
        ):
            warnings.append(
                f"column '{source}' is renamed to '{target}' by x-columnMapping, but the "
                f"schema properties are matched by the names in the file: the definition of "
                f"'{target}' is not applied to it (declare the property as '{source}')"
            )
    return warnings


def _check_references(
    references: Dict[str, List[str]],
    available: Sequence[str],
    *,
    declared: set,
    resolve: Callable[[str], str],
) -> Tuple[List[str], Dict[str, List[str]]]:
    """Fail early when an extension refers to a column that cannot exist.

    A schema may describe more columns than a given file has, so a name that is declared in
    ``properties`` but missing from the file only produces a warning. A name that is neither in
    the file nor declared is most likely a typo, and so is an error: otherwise the rule that
    mentions it would silently check nothing.

    Returns:
        ``(warnings, absent)``: the warnings for the lenient cases and, per extension, the
        declared names the input lacks (as written in the schema)

    Raises:
        ValueError: An extension names a column that is in neither the input nor ``properties``
            (or, for the key extensions, in the input)
    """
    known = set(available)
    warnings: List[str] = []
    absent: Dict[str, List[str]] = {}
    for extension, columns in references.items():
        missing = sorted({c for c in columns if resolve(c) not in known})
        if not missing:
            continue
        lenient = extension in LENIENT_EXTENSIONS
        typos = [c for c in missing if not (lenient and c in declared)]
        if typos:
            shown = ", ".join(repr(c) for c in typos)
            raise ValueError(
                f"{extension} refers to column(s) {shown} that are not in the input "
                f"(columns after mapping: {', '.join(sorted(known))})"
            )
        shown = ", ".join(repr(c) for c in missing)
        warnings.append(
            f"{extension} rules for column(s) {shown} are not checked: "
            "the columns are not in the input"
        )
        absent[extension] = missing
    return warnings, absent


def _without_columns(schema: Dict[str, Any], absent: Dict[str, List[str]]) -> Dict[str, Any]:
    """A copy of ``schema`` without the rules the input has no column for.

    Only the sections the rule loaders read are copied; the original is not modified.
    """
    if not absent:
        return schema
    view = dict(schema)

    def pruned(section: Any, key: Optional[str], names: Sequence[str]) -> Any:
        if not isinstance(section, dict):
            return section
        if key is None:
            return {k: v for k, v in section.items() if k not in names}
        copy = dict(section)
        if isinstance(copy.get(key), dict):
            copy[key] = {k: v for k, v in copy[key].items() if k not in names}
        return copy

    if "x-validation" in absent:
        view["x-validation"] = pruned(
            schema.get("x-validation"), "fieldValidations", absent["x-validation"]
        )
    if "x-dataQuality" in absent:
        quality = pruned(
            schema.get("x-dataQuality"), "fieldSpecificRules", absent["x-dataQuality"]
        )
        view["x-dataQuality"] = pruned(quality, "fieldQualityRules", absent["x-dataQuality"])
    if "properties" in absent:
        view["properties"] = pruned(schema.get("properties"), None, absent["properties"])
    return view
