"""Build processors from the ``x-*`` extensions of a schema dictionary.

``x-transformations``, ``x-calculatedColumns`` and ``x-rowHash`` have their own factories
(``transformations``, ``calculated_columns_factory``, ``row_hash_factory``). This module adds the
loaders for the other extensions, so that an engine can build every processor from the schema
dictionary:

=======================  ======================================  ===========================
extension                loader                                  processor
=======================  ======================================  ===========================
``x-columnMapping``      :func:`build_column_mapper`             ``ColumnMapper``
``x-primaryKey``,        :func:`build_constraint_validator`      ``ConstraintValidator``
``x-uniqueConstraints``,
``x-constraintHandling``
and the per-property
``minimum``, ``maximum``,
``enum``, ``pattern``,
``minLength``,
``maxLength``, ``x-unique``
``x-validation``         :func:`build_data_validator`            ``DataValidationProcessor``
``x-dataQuality``        :func:`build_quality_processor`         ``DataQualityProcessor``
=======================  ======================================  ===========================

Every loader returns ``None`` when the extension is absent or has nothing to apply, and raises
``ValueError("x-<extension>...: <what is wrong>")`` for an invalid configuration (wrong type,
impossible value, invalid regular expression...). Keys that no processor reads are **not** errors
- the shipped standard files contain such keys - they are listed by
:func:`unsupported_extension_keys`. :func:`referenced_columns` lists the columns the extensions
refer to, so that a caller can report columns that are not in the input.

Naming: the schema ``properties`` use the file header names, the processors run on the *output*
names (after the column mapping). The loaders therefore take ``resolve_column(name) -> output
name`` (default: identity); every column name in the extension is passed through it. Names that
already are output names must be returned unchanged by ``resolve_column``.

Supported formats
-----------------
``x-columnMapping`` (the shape of ``schema-standards/20250826-csv.json``)::

    {"explicitMappings": {"FirstName": "first_name"},
     "namingConvention": "snake_case",   # snake_case|camelCase|PascalCase|lowercase|UPPERCASE
     "caseSensitive": true, "allowUnmapped": true, "dropUnmapped": false}

``allowUnmapped: false`` has the same effect as ``dropUnmapped: true`` (columns without an
explicit mapping are dropped).

``x-primaryKey``::

    {"columns": ["id"], "type": "single",  # optional, "single"|"composite", must match columns
     "enforceUniqueness": true,            # default true: the key must be unique (tuple if >1)
     "allowNulls": false}                  # default false: the key columns must not be NULL

``x-uniqueConstraints``: ``[{"name": "...", "columns": ["a", "b"], "description": "..."}]``;
``x-constraintHandling``: ``{"errorMode": "bad_rows" | "fail_fast" | "fail_complete"}`` (applies
to the primary key, the unique constraints and the per-property constraints). Rows with a NULL in
a unique key are not compared (SQL semantics); the same key defined twice is checked once.

``x-validation``::

    {"badRowsHandling": {"maxBadRowsPercent": 10.0, "failOnExceedThreshold": true},
     "uniquenessHandling": {"strategy": "first_wins"},  # |last_wins|fail_on_duplicate|
                                                        # mark_all_duplicates
     "fieldValidations": {"age": {
         "required": false, "unique": false,
         "range": {"min": 0, "max": 150, "inclusive": true},
         "stringValidation": {"minLength": 1, "maxLength": 9, "pattern": "^a", "allowEmpty": true},
         "enumValidation": {"allowedValues": ["A", "B"], "caseSensitive": true},
         "dateValidation": {"minDate": "1900-01-01", "maxDate": "2100-12-31",
                            "format": ["%Y-%m-%d"]}}}}

The returned ``DataValidationProcessor`` never writes files and does not keep the rejected rows
(it only counts them, so the ``maxBadRowsPercent`` threshold keeps working): the caller writes
the rejected rows itself, using the ``row_index``/``column_name`` of the results.

``x-dataQuality`` (report only, nothing is dropped): per column either the standard's
``fieldSpecificRules: {col: {"min": 0, "max": 150, "pattern": "..."}}`` or the documentation's
``fieldQualityRules: {col: {"parameters": {"min_length", "max_length", "pattern", "min_value",
"max_value"}}}``.
"""

from __future__ import annotations

import math
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

from ._regex import compile_pattern
from ._values import as_datetime, parse_temporal, to_decimal
from .column_mapper import NAMING_CONVENTIONS, ColumnMapper, ColumnMappingConfig
from .constraint_validator import (
    ConstraintConfig,
    ConstraintValidator,
    create_constraint_config_from_schema,
)
from .data_validation import (
    BadRowsConfig,
    DataValidationProcessor,
    DateValidation,
    EnumValidation,
    FieldValidationRule,
    RangeValidation,
    StringValidation,
    ValidationConfig,
    ValidationRules,
)
from .quality import DataQualityProcessor

__all__ = [
    "ResolveColumn",
    "build_column_mapper",
    "build_constraint_validator",
    "build_data_validator",
    "build_quality_processor",
    "referenced_columns",
    "unsupported_extension_keys",
]

#: Header/source name -> output name; names that already are output names pass through.
ResolveColumn = Callable[[str], str]

#: Violations kept in memory by the constraint validator (the counts and results are complete).
_MAX_STORED_VIOLATIONS = 1000

_DOC_KEYS = frozenset({"description", "description_detail"})
_UNIQUENESS_STRATEGIES = ("first_wins", "last_wins", "fail_on_duplicate", "mark_all_duplicates")
_PRIMARY_KEY_TYPES = ("single", "composite")
_PROPERTY_CONSTRAINT_KEYS = (
    "minimum",
    "maximum",
    "enum",
    "pattern",
    "minLength",
    "maxLength",
    "x-unique",
)


def _identity(name: str) -> str:
    return name


# ------------------------------------------------------------------------------ validation helpers


def _type_name(value: Any) -> str:
    return type(value).__name__


def _require_schema(schema: Any) -> Dict[str, Any]:
    if not isinstance(schema, dict):
        raise ValueError(f"schema must be a dictionary, got {_type_name(schema)}")
    return schema


def _checked_resolver(resolve_column: Optional[ResolveColumn]) -> ResolveColumn:
    """``resolve_column`` that insists on a non-empty string result."""
    if resolve_column is None or resolve_column is _identity:
        return _identity

    def resolve(name: str) -> str:
        resolved = resolve_column(name)
        if not isinstance(resolved, str) or not resolved:
            raise ValueError(f"resolve_column({name!r}) must return a non-empty string")
        return resolved

    return resolve


def _dict(value: Any, where: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{where}: must be an object, got {_type_name(value)}")
    return value


def _list(value: Any, where: str) -> list:
    if not isinstance(value, list):
        raise ValueError(f"{where}: must be a list, got {_type_name(value)}")
    return value


def _bool(section: Dict[str, Any], key: str, default: bool, where: str) -> bool:
    value = section.get(key)
    if value is None:
        return default
    if not isinstance(value, bool):
        raise ValueError(f"{where}.{key}: must be true or false, got {_type_name(value)}")
    return value


def _text(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{where}: must be a non-empty string, got {_type_name(value)}")
    return value


def _columns(value: Any, where: str) -> List[str]:
    """A non-empty list of distinct, non-empty column names."""
    names = _list(value, where)
    if not names:
        raise ValueError(f"{where}: must not be empty")
    seen: Set[str] = set()
    for i, name in enumerate(names):
        _text(name, f"{where}[{i}]")
        if name in seen:
            raise ValueError(f"{where}: column '{name}' is listed more than once")
        seen.add(name)
    return list(names)


def _int(value: Any, where: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{where}: must be an integer, got {_type_name(value)}")
    if value < minimum:
        raise ValueError(f"{where}: must be >= {minimum}")
    return value


def _number(value: Any, where: str) -> Union[int, float]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{where}: must be a number, got {_type_name(value)}")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{where}: must be a finite number")
    return value


def _ordered(low: Any, high: Any, where: str, low_key: str, high_key: str) -> None:
    if low is not None and high is not None and low > high:
        raise ValueError(f"{where}: {low_key} ({low}) is greater than {high_key} ({high})")


def _bound(value: Any, where: str) -> Any:
    """A range bound: a finite number, a numeric string or a date (``date`` or ISO string)."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{where}: must be a number or a date, got bool")
    if isinstance(value, (date, datetime)):
        return value
    if isinstance(value, (int, float, Decimal)):
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError(f"{where}: must be a finite number")
        return value
    if isinstance(value, str):
        if parse_temporal(value) is not None:
            return value
        try:
            number = to_decimal(value)
        except (InvalidOperation, ValueError, TypeError):
            raise ValueError(f"{where}: must be a number or an ISO date") from None
        if not number.is_finite():
            raise ValueError(f"{where}: must be a finite number")
        return value
    raise ValueError(f"{where}: must be a number or a date, got {_type_name(value)}")


def _check_bounds_order(low: Any, high: Any, where: str, inclusive: bool = True) -> None:
    """Raise if ``low > high`` (or ``low == high`` for an exclusive range)."""
    if low is None or high is None:
        return
    low_date, high_date = parse_temporal(low), parse_temporal(high)
    try:
        if low_date is not None and high_date is not None:
            first, second = as_datetime(low_date), as_datetime(high_date)
        elif low_date is None and high_date is None:
            first, second = to_decimal(low), to_decimal(high)
        else:
            raise ValueError(f"{where}: min and max must both be numbers or both be dates")
        order = (first > second) - (first < second)
    except (TypeError, InvalidOperation):
        raise ValueError(f"{where}: min and max cannot be compared") from None
    if order > 0:
        raise ValueError(f"{where}: min is greater than max")
    if order == 0 and not inclusive:
        raise ValueError(f"{where}: min equals max but the range is not inclusive")


def _compile(pattern: Any, where: str) -> str:
    """The pattern, after checking it is a valid, safe regular expression."""
    if not isinstance(pattern, str):
        raise ValueError(f"{where}: must be a string, got {_type_name(pattern)}")
    try:
        compile_pattern(pattern)
    except ValueError as exc:
        raise ValueError(f"{where}: {exc}") from None
    return pattern


def _is_set(value: Any) -> bool:
    """Whether a configuration block says anything (not ``None``, ``{}`` or ``[]``)."""
    return value is not None and not (isinstance(value, (dict, list, str)) and not value)


def _has_property_constraint(definition: Any) -> bool:
    if not isinstance(definition, dict):
        return False
    return any(
        definition.get(key) is not None and definition.get(key) is not False
        for key in _PROPERTY_CONSTRAINT_KEYS
    )


# ------------------------------------------------------------------------------- x-columnMapping


def build_column_mapper(schema: Dict[str, Any]) -> Optional[ColumnMapper]:
    """Build a :class:`ColumnMapper` from ``x-columnMapping``.

    Returns:
        The mapper, or ``None`` when there is no ``x-columnMapping`` or it renames nothing and
        drops nothing.

    Raises:
        ValueError: For an invalid configuration (see the module docstring for the format).
    """
    section = _require_schema(schema).get("x-columnMapping")
    if section is None:
        return None
    where = "x-columnMapping"
    section = _dict(section, where)

    case_sensitive = _bool(section, "caseSensitive", True, where)
    allow_unmapped = _bool(section, "allowUnmapped", True, where)
    drop_unmapped = _bool(section, "dropUnmapped", False, where) or not allow_unmapped

    explicit: Dict[str, str] = {}
    raw = section.get("explicitMappings")
    if raw is not None:
        for source, target in _dict(raw, f"{where}.explicitMappings").items():
            _text(source, f"{where}.explicitMappings key")
            _text(target, f"{where}.explicitMappings.{source}")
            explicit[source] = target
        if not case_sensitive:
            by_lower: Dict[str, str] = {}
            for source, target in explicit.items():
                other = by_lower.setdefault(source.lower(), source)
                if other != source and explicit[other] != target:
                    raise ValueError(
                        f"{where}.explicitMappings: '{other}' and '{source}' differ only in case "
                        f"but map to different names (caseSensitive is false)"
                    )

    convention = section.get("namingConvention")
    if convention is not None and convention not in NAMING_CONVENTIONS:
        raise ValueError(
            f"{where}.namingConvention: must be one of {list(NAMING_CONVENTIONS)}, "
            f"got {convention!r}"
        )

    if not explicit and convention is None and not drop_unmapped:
        return None

    return ColumnMapper(
        ColumnMappingConfig(
            explicit_mappings=explicit,
            naming_convention=convention,
            case_sensitive=case_sensitive,
            allow_unmapped=allow_unmapped,
            drop_unmapped=drop_unmapped,
        )
    )


# ----------------------------------------------------------------------- constraints (x-primaryKey
# x-uniqueConstraints, x-constraintHandling, per-property constraints)


def _check_property_constraints(properties: Any) -> None:
    """Validate the constraint keywords of the schema properties."""
    if properties is None:
        return
    for name, definition in _dict(properties, "properties").items():
        if not isinstance(definition, dict):
            continue  # e.g. a boolean schema
        where = f"properties.{name}"

        minimum, maximum = definition.get("minimum"), definition.get("maximum")
        _bound(minimum, f"{where}.minimum")
        _bound(maximum, f"{where}.maximum")
        if minimum is not None and maximum is not None:
            _check_bounds_order(minimum, maximum, f"{where} (minimum/maximum)")

        if definition.get("enum") is not None:
            if not _list(definition["enum"], f"{where}.enum"):
                raise ValueError(f"{where}.enum: must not be empty")

        if definition.get("pattern") is not None:
            _compile(definition["pattern"], f"{where}.pattern")

        lengths = {}
        for key in ("minLength", "maxLength"):
            if definition.get(key) is not None:
                lengths[key] = _int(definition[key], f"{where}.{key}")
        _ordered(
            lengths.get("minLength"), lengths.get("maxLength"), where, "minLength", "maxLength"
        )

        unique = definition.get("x-unique")
        if unique is not None and not isinstance(unique, bool):
            raise ValueError(f"{where}.x-unique: must be true or false, got {_type_name(unique)}")


class _PrimaryKey:
    def __init__(self, columns: List[str], enforce_uniqueness: bool, allow_nulls: bool):
        self.columns = columns
        self.enforce_uniqueness = enforce_uniqueness
        self.allow_nulls = allow_nulls


def _parse_primary_key(schema: Dict[str, Any]) -> Optional[_PrimaryKey]:
    section = schema.get("x-primaryKey")
    if section is None:
        return None
    where = "x-primaryKey"
    section = _dict(section, where)

    if section.get("columns") is None:
        raise ValueError(f"{where}.columns: is required (a list of column names)")
    columns = _columns(section["columns"], f"{where}.columns")

    key_type = section.get("type")
    if key_type is not None:
        if key_type not in _PRIMARY_KEY_TYPES:
            raise ValueError(f"{where}.type: must be one of {list(_PRIMARY_KEY_TYPES)}")
        if key_type == "single" and len(columns) != 1:
            raise ValueError(
                f"{where}.type: 'single' needs exactly one column, got {len(columns)}"
            )
        if key_type == "composite" and len(columns) < 2:
            raise ValueError(f"{where}.type: 'composite' needs at least two columns")

    return _PrimaryKey(
        columns,
        enforce_uniqueness=_bool(section, "enforceUniqueness", True, where),
        allow_nulls=_bool(section, "allowNulls", False, where),
    )


def _parse_unique_constraints(schema: Dict[str, Any]) -> List[Tuple[str, List[str]]]:
    """``[(where, columns)]`` of ``x-uniqueConstraints``."""
    section = schema.get("x-uniqueConstraints")
    if section is None:
        return []
    items = _list(section, "x-uniqueConstraints")

    parsed: List[Tuple[str, List[str]]] = []
    names: Set[str] = set()
    for i, item in enumerate(items):
        where = f"x-uniqueConstraints[{i}]"
        item = _dict(item, where)
        if item.get("columns") is None:
            raise ValueError(f"{where}.columns: is required (a list of column names)")
        columns = _columns(item["columns"], f"{where}.columns")
        if item.get("name") is not None:
            name = _text(item["name"], f"{where}.name")
            if name in names:
                raise ValueError(f"{where}.name: '{name}' is used by more than one constraint")
            names.add(name)
        for key in ("ignoreNulls", "caseSensitive"):
            _bool(item, key, True, where)
        parsed.append((where, columns))
    return parsed


def build_constraint_validator(
    schema: Dict[str, Any], *, resolve_column: ResolveColumn = _identity
) -> Optional[ConstraintValidator]:
    """Build a :class:`ConstraintValidator` from the constraint extensions of the schema.

    Reads ``x-primaryKey``, ``x-uniqueConstraints``, ``x-constraintHandling.errorMode`` and the
    per-property ``minimum``/``maximum``/``enum``/``pattern``/``minLength``/``maxLength`` and
    ``x-unique``. The primary key columns must be unique (a tuple for a composite key) unless
    ``enforceUniqueness`` is false, and not NULL unless ``allowNulls`` is true. A key that is
    defined more than once is checked once.

    The validator keeps at most 1000 violations in memory (``violation_count`` is the total).

    Returns:
        The validator, or ``None`` when nothing has to be checked.

    Raises:
        ValueError: For an invalid configuration, including an unknown ``errorMode``.
    """
    _require_schema(schema)
    resolve = _checked_resolver(resolve_column)

    _check_property_constraints(schema.get("properties"))
    base = create_constraint_config_from_schema(schema, resolve_column=resolve)
    primary_key = _parse_primary_key(schema)
    unique_defs = _parse_unique_constraints(schema)

    check_constraints = dict(base.check_constraints)
    unique: List[Union[str, Tuple[str, ...]]] = []
    seen: Set[frozenset] = set()

    def add_unique(where: str, columns: List[str]) -> None:
        if len(set(columns)) != len(columns):
            raise ValueError(f"{where}: two columns refer to the same output column")
        marker = frozenset(columns)
        if marker not in seen:
            seen.add(marker)
            unique.append(columns[0] if len(columns) == 1 else tuple(columns))

    if primary_key is not None:
        columns = [resolve(name) for name in primary_key.columns]
        if not primary_key.allow_nulls:
            for column in columns:
                check_constraints[f"primary_key_{column}_not_null"] = {
                    "column": column,
                    "nullable": False,
                }
        if primary_key.enforce_uniqueness:
            add_unique("x-primaryKey.columns", columns)
    for where, names in unique_defs:
        add_unique(f"{where}.columns", [resolve(name) for name in names])
    for column in base.unique_constraints:  # per-property x-unique
        add_unique("x-unique", [column])

    if not check_constraints and not unique:
        return None

    return ConstraintValidator(
        ConstraintConfig(
            error_mode=base.error_mode,
            check_constraints=check_constraints,
            unique_constraints=unique,
        ),
        max_stored_violations=_MAX_STORED_VIOLATIONS,
    )


# ----------------------------------------------------------------------------------- x-validation


def _parse_range(raw: Any, where: str) -> Optional[RangeValidation]:
    raw = _dict(raw, where)
    low, high = _bound(raw.get("min"), f"{where}.min"), _bound(raw.get("max"), f"{where}.max")
    inclusive = _bool(raw, "inclusive", True, where)
    if low is None and high is None:
        return None
    _check_bounds_order(low, high, where, inclusive)
    rule = RangeValidation(min_value=low, max_value=high, inclusive=inclusive)
    try:
        ValidationRules.check_range_config(rule)
    except ValueError as exc:
        raise ValueError(f"{where}: {exc}") from None
    return rule


def _parse_string_validation(raw: Any, where: str) -> Optional[StringValidation]:
    raw = _dict(raw, where)
    min_length = max_length = pattern = None
    if raw.get("minLength") is not None:
        min_length = _int(raw["minLength"], f"{where}.minLength")
    if raw.get("maxLength") is not None:
        max_length = _int(raw["maxLength"], f"{where}.maxLength")
    _ordered(min_length, max_length, where, "minLength", "maxLength")
    if raw.get("pattern") is not None:
        pattern = _compile(raw["pattern"], f"{where}.pattern")
    allow_empty = _bool(raw, "allowEmpty", True, where)
    if min_length is None and max_length is None and pattern is None and allow_empty:
        return None
    return StringValidation(
        min_length=min_length, max_length=max_length, pattern=pattern, allow_empty=allow_empty
    )


def _parse_enum_validation(raw: Any, where: str) -> EnumValidation:
    raw = _dict(raw, where)
    if raw.get("allowedValues") is None:
        raise ValueError(f"{where}.allowedValues: is required (a list of allowed values)")
    allowed = _list(raw["allowedValues"], f"{where}.allowedValues")
    if not allowed:
        raise ValueError(f"{where}.allowedValues: must not be empty")
    return EnumValidation(
        allowed_values=list(allowed), case_sensitive=_bool(raw, "caseSensitive", True, where)
    )


def _parse_date_validation(raw: Any, where: str) -> DateValidation:
    raw = _dict(raw, where)

    formats: Optional[List[str]] = None
    if raw.get("format") is not None:
        value = raw["format"]
        formats = [value] if isinstance(value, str) else list(_list(value, f"{where}.format"))
        if not formats:
            raise ValueError(f"{where}.format: must not be empty")
        for i, fmt in enumerate(formats):
            _text(fmt, f"{where}.format" if isinstance(value, str) else f"{where}.format[{i}]")

    bounds: Dict[str, Any] = {}
    for key in ("minDate", "maxDate"):
        if raw.get(key) is None:
            continue
        text = raw[key]
        if not isinstance(text, str):
            raise ValueError(f"{where}.{key}: must be a date string, got {_type_name(text)}")
        parsed = parse_temporal(text, formats or [])
        if parsed is None:
            raise ValueError(f"{where}.{key}: is not a valid date")
        bounds[key] = parsed.date() if isinstance(parsed, datetime) else parsed
    if len(bounds) == 2 and bounds["minDate"] > bounds["maxDate"]:
        raise ValueError(f"{where}: minDate is after maxDate")

    return DateValidation(
        min_date=raw.get("minDate"), max_date=raw.get("maxDate"), formats=formats or None
    )


def _parse_field_rule(
    name: str, raw: Any, resolve: ResolveColumn
) -> Optional[FieldValidationRule]:
    """The rule for one ``fieldValidations`` entry, or ``None`` if it checks nothing."""
    where = f"x-validation.fieldValidations.{name}"
    raw = _dict(raw, where)
    _text(name, "x-validation.fieldValidations key")

    required = _bool(raw, "required", False, where)
    unique = _bool(raw, "unique", False, where)
    range_validation = string_validation = enum_validation = date_validation = None
    if _is_set(raw.get("range")):
        range_validation = _parse_range(raw["range"], f"{where}.range")
    if _is_set(raw.get("stringValidation")):
        string_validation = _parse_string_validation(
            raw["stringValidation"], f"{where}.stringValidation"
        )
    if _is_set(raw.get("enumValidation")):
        enum_validation = _parse_enum_validation(raw["enumValidation"], f"{where}.enumValidation")
    if _is_set(raw.get("dateValidation")):
        date_validation = _parse_date_validation(raw["dateValidation"], f"{where}.dateValidation")
    if raw.get("onViolation") is not None:
        _dict(raw["onViolation"], f"{where}.onViolation")

    if not (
        required
        or unique
        or range_validation
        or string_validation
        or enum_validation
        or date_validation
    ):
        return None
    return FieldValidationRule(
        field_name=resolve(name),
        required=required,
        unique=unique,
        range_validation=range_validation,
        string_validation=string_validation,
        enum_validation=enum_validation,
        date_validation=date_validation,
    )


def _parse_field_validations(raw: Any, resolve: ResolveColumn) -> List[FieldValidationRule]:
    if raw is None:
        return []
    rules: List[FieldValidationRule] = []
    sources: Dict[str, str] = {}
    for name, rule_raw in _dict(raw, "x-validation.fieldValidations").items():
        rule = _parse_field_rule(name, rule_raw, resolve)
        if rule is None:
            continue
        if rule.field_name in sources:
            raise ValueError(
                f"x-validation.fieldValidations: '{sources[rule.field_name]}' and '{name}' "
                f"refer to the same column '{rule.field_name}'"
            )
        sources[rule.field_name] = name
        rules.append(rule)
    return rules


def build_data_validator(
    schema: Dict[str, Any], *, resolve_column: ResolveColumn = _identity
) -> Optional[DataValidationProcessor]:
    """Build a :class:`DataValidationProcessor` from ``x-validation``.

    Reads ``fieldValidations`` (``required``, ``unique``, ``range``, ``stringValidation``,
    ``enumValidation``, ``dateValidation``), ``uniquenessHandling.strategy`` and
    ``badRowsHandling.maxBadRowsPercent``/``failOnExceedThreshold``.

    The processor drops the rejected rows from the batch and reports them as ``ValidationResult``
    (``row_index``, ``error_code`` and ``column_name``); it does not write files and does not
    keep the rejected rows (``BadRowsConfig(enabled=False)``: they are only counted, so the
    percentage threshold keeps working with constant memory).

    Returns:
        The processor, or ``None`` when ``fieldValidations`` checks nothing.

    Raises:
        ValueError: For an invalid configuration.
    """
    section = _require_schema(schema).get("x-validation")
    if section is None:
        return None
    where = "x-validation"
    section = _dict(section, where)
    resolve = _checked_resolver(resolve_column)

    bad_rows = section.get("badRowsHandling")
    max_percent, fail_on_exceed = 10.0, True
    if bad_rows is not None:
        bad_rows = _dict(bad_rows, f"{where}.badRowsHandling")
        _bool(bad_rows, "enabled", True, f"{where}.badRowsHandling")
        if bad_rows.get("maxBadRowsPercent") is not None:
            max_percent = _number(
                bad_rows["maxBadRowsPercent"], f"{where}.badRowsHandling.maxBadRowsPercent"
            )
            if not 0 <= max_percent <= 100:
                raise ValueError(
                    f"{where}.badRowsHandling.maxBadRowsPercent: must be between 0 and 100"
                )
        fail_on_exceed = _bool(bad_rows, "failOnExceedThreshold", True, f"{where}.badRowsHandling")

    strategy = "first_wins"
    uniqueness = section.get("uniquenessHandling")
    if uniqueness is not None:
        uniqueness = _dict(uniqueness, f"{where}.uniquenessHandling")
        if uniqueness.get("strategy") is not None:
            strategy = uniqueness["strategy"]
            if strategy not in _UNIQUENESS_STRATEGIES:
                raise ValueError(
                    f"{where}.uniquenessHandling.strategy: must be one of "
                    f"{list(_UNIQUENESS_STRATEGIES)}, got {strategy!r}"
                )

    rules = _parse_field_validations(section.get("fieldValidations"), resolve)
    if section.get("crossFieldValidations") is not None:
        _list(section["crossFieldValidations"], f"{where}.crossFieldValidations")
    if section.get("globalValidations") is not None:
        _dict(section["globalValidations"], f"{where}.globalValidations")

    if not rules:
        return None

    return DataValidationProcessor(
        ValidationConfig(
            field_validations=rules,
            bad_rows_config=BadRowsConfig(
                enabled=False,
                include_original_row=False,
                include_validation_errors=False,
                max_bad_rows_percent=float(max_percent),
                fail_on_exceed_threshold=fail_on_exceed,
            ),
            uniqueness_strategy=strategy,
        )
    )


# ---------------------------------------------------------------------------------- x-dataQuality

#: standard (``fieldSpecificRules``) key -> ``DataQualityProcessor`` rule
_QUALITY_SPECIFIC_KEYS = {"min": "min_value", "max": "max_value", "pattern": "pattern"}
#: documentation (``fieldQualityRules[col].parameters``) keys, used as they are
_QUALITY_PARAMETER_KEYS = ("min_length", "max_length", "pattern", "min_value", "max_value")


def _quality_rule(source: Dict[str, Any], mapping: Dict[str, str], where: str) -> Dict[str, Any]:
    """``DataQualityProcessor`` rule from the keys of ``source`` listed in ``mapping``."""
    rule: Dict[str, Any] = {}
    for key, target in mapping.items():
        value = source.get(key)
        if value is None:
            continue
        if target in ("min_length", "max_length"):
            rule[target] = _int(value, f"{where}.{key}")
        elif target == "pattern":
            rule[target] = _compile(value, f"{where}.{key}")
        else:
            rule[target] = _number(value, f"{where}.{key}")
    _ordered(rule.get("min_length"), rule.get("max_length"), where, "min_length", "max_length")
    _ordered(rule.get("min_value"), rule.get("max_value"), where, "min_value", "max_value")
    return rule


def _quality_column_rules(
    section: Dict[str, Any], resolve: ResolveColumn
) -> Dict[str, Dict[str, Any]]:
    """``{output column: DataQualityProcessor rule}`` from both documented shapes."""
    column_rules: Dict[str, Dict[str, Any]] = {}
    sources: Dict[str, str] = {}

    def add(name: str, rule: Dict[str, Any], where: str) -> None:
        if not rule:
            return
        column = resolve(name)
        merged = column_rules.setdefault(column, {})
        if column in sources and sources[column] != where:
            for key, value in rule.items():
                if key in merged and merged[key] != value:
                    raise ValueError(
                        f"{where}: '{key}' of column '{column}' conflicts with "
                        f"{sources[column]}"
                    )
        sources.setdefault(column, where)
        merged.update(rule)

    specific = section.get("fieldSpecificRules")
    if specific is not None:
        for name, raw in _dict(specific, "x-dataQuality.fieldSpecificRules").items():
            where = f"x-dataQuality.fieldSpecificRules.{name}"
            _text(name, "x-dataQuality.fieldSpecificRules key")
            add(name, _quality_rule(_dict(raw, where), _QUALITY_SPECIFIC_KEYS, where), where)

    documented = section.get("fieldQualityRules")
    if documented is not None:
        for name, raw in _dict(documented, "x-dataQuality.fieldQualityRules").items():
            where = f"x-dataQuality.fieldQualityRules.{name}"
            _text(name, "x-dataQuality.fieldQualityRules key")
            raw = _dict(raw, where)
            if raw.get("parameters") is None:
                continue
            parameters = _dict(raw["parameters"], f"{where}.parameters")
            mapping = {key: key for key in _QUALITY_PARAMETER_KEYS}
            add(name, _quality_rule(parameters, mapping, f"{where}.parameters"), where)

    return column_rules


def build_quality_processor(
    schema: Dict[str, Any], *, resolve_column: ResolveColumn = _identity
) -> Optional[DataQualityProcessor]:
    """Build a :class:`DataQualityProcessor` from ``x-dataQuality``.

    Two shapes are read: the standard's ``fieldSpecificRules`` (``min``, ``max``, ``pattern``)
    and the documentation's ``fieldQualityRules[col].parameters`` (``min_length``,
    ``max_length``, ``pattern``, ``min_value``, ``max_value``). The processor only reports (it
    returns the batch unchanged) and its messages do not contain cell values. ``enabled: false``
    switches the whole extension off.

    Returns:
        The processor, or ``None`` when no column rule can be applied.

    Raises:
        ValueError: For an invalid configuration.
    """
    section = _require_schema(schema).get("x-dataQuality")
    if section is None:
        return None
    where = "x-dataQuality"
    section = _dict(section, where)
    if not _bool(section, "enabled", True, where):
        return None

    column_rules = _quality_column_rules(section, _checked_resolver(resolve_column))
    if not column_rules:
        return None
    return DataQualityProcessor({"column_rules": column_rules}, include_values=False)


# -------------------------------------------------------------------------------- referenced columns


def _names(value: Any) -> List[str]:
    return [name for name in value if isinstance(name, str)] if isinstance(value, list) else []


def _add_unique(target: List[str], names: Sequence[str]) -> None:
    for name in names:
        if name not in target:
            target.append(name)


def referenced_columns(schema: Dict[str, Any]) -> Dict[str, List[str]]:
    """Columns the extensions refer to, as written in the schema (not resolved).

    Keys are ``"x-primaryKey"``, ``"x-uniqueConstraints"``, ``"x-validation"`` (the fields with
    an applied rule), ``"x-dataQuality"`` (the columns with an applied rule) and
    ``"properties"`` (the properties that carry a constraint: ``minimum``, ``maximum``,
    ``enum``, ``pattern``, ``minLength``, ``maxLength`` or ``x-unique``). Only extensions that
    refer to at least one column are present; names are listed once, in schema order. Malformed
    entries are skipped (the loaders report them).
    """
    schema = _require_schema(schema)
    found: Dict[str, List[str]] = {}

    key = schema.get("x-primaryKey")
    if isinstance(key, dict) and _names(key.get("columns")):
        found["x-primaryKey"] = _names(key["columns"])

    unique: List[str] = []
    if isinstance(schema.get("x-uniqueConstraints"), list):
        for item in schema["x-uniqueConstraints"]:
            if isinstance(item, dict):
                _add_unique(unique, _names(item.get("columns")))
    if unique:
        found["x-uniqueConstraints"] = unique

    validation = schema.get("x-validation")
    if isinstance(validation, dict) and isinstance(validation.get("fieldValidations"), dict):
        fields: List[str] = []
        for name, raw in validation["fieldValidations"].items():
            try:
                active = _parse_field_rule(name, raw, _identity) is not None
            except ValueError:
                active = True  # invalid: the loader raises; refer to the column anyway
            if active:
                _add_unique(fields, [name])
        if fields:
            found["x-validation"] = fields

    quality = schema.get("x-dataQuality")
    if isinstance(quality, dict) and quality.get("enabled") is not False:
        try:
            quality_columns = list(_quality_column_rules(quality, _identity))
        except ValueError:
            quality_columns = [
                name
                for block in ("fieldSpecificRules", "fieldQualityRules")
                if isinstance(quality.get(block), dict)
                for name in quality[block]
            ]
        if quality_columns:
            found["x-dataQuality"] = quality_columns

    properties = schema.get("properties")
    if isinstance(properties, dict):
        constrained = [name for name, d in properties.items() if _has_property_constraint(d)]
        if constrained:
            found["properties"] = constrained

    return found


# ------------------------------------------------------------------------------ unsupported keys

#: x-transformations key -> the ``column_transformations`` step that does the same
_TRANSFORMATION_STEPS = {
    "stringCleaning": "string_cleaning",
    "caseTransformation": "string_cleaning",
    "numericCleaning": "numeric_cleaning",
    "moneyType": "money_conversion",
    "dateTimeParsing": "datetime",
    "htmlXmlCleaning": "html_xml_cleaning",
    "stringPadding": "string_padding",
    "regexReplacements": "regex_replace",
    "stringReplacements": "string_replace",
}
_CALCULATED_COLUMNS_KEYS = frozenset(
    {
        "constants",
        "expressions",
        "calculated",
        "failOnError",
        "addMetadata",
        "validateDependencies",
    }
)
_COLUMN_MAPPING_KEYS = frozenset(
    {"explicitMappings", "namingConvention", "caseSensitive", "allowUnmapped", "dropUnmapped"}
)
_PRIMARY_KEY_KEYS = frozenset({"columns", "type", "enforceUniqueness", "allowNulls"})
_UNIQUE_ITEM_KEYS = frozenset({"name", "columns", "ignoreNulls", "caseSensitive", "condition"})
_VALIDATION_KEYS = frozenset(
    {
        "badRowsHandling",
        "uniquenessHandling",
        "fieldValidations",
        "crossFieldValidations",
        "globalValidations",
    }
)
_BAD_ROWS_IGNORED = ("outputPath", "fileFormat", "includeOriginalRow", "includeValidationErrors")
_BAD_ROWS_KEYS = frozenset({"enabled", "maxBadRowsPercent", "failOnExceedThreshold"})
_FIELD_RULE_KEYS = frozenset(
    {
        "required",
        "unique",
        "range",
        "stringValidation",
        "enumValidation",
        "dateValidation",
        "onViolation",
    }
)
_FIELD_BLOCK_KEYS = {
    "range": frozenset({"min", "max", "inclusive"}),
    "stringValidation": frozenset({"minLength", "maxLength", "pattern", "allowEmpty"}),
    "enumValidation": frozenset({"allowedValues", "caseSensitive"}),
    "dateValidation": frozenset({"minDate", "maxDate", "format"}),
}
_QUALITY_TOP_KEYS = frozenset({"enabled", "fieldSpecificRules", "fieldQualityRules"})
#: quality rule keys that do nothing when false
_QUALITY_FALSE_IS_NOOP = frozenset({"required", "standardizeFormat"})


def _block_is_active(value: Any) -> bool:
    """A configuration block that is set and not switched off with ``enabled: false``."""
    if not _is_set(value):
        return False
    return not (isinstance(value, dict) and value.get("enabled") is False)


def _unknown_keys(
    section: Any, path: str, known: frozenset, warnings: List[str], suffix: str = ""
) -> None:
    """Warn about the keys of ``section`` that are neither ``known`` nor documentation."""
    if not isinstance(section, dict):
        return
    for key, value in section.items():
        if key in known or key in _DOC_KEYS or not _is_set(value):
            continue
        warnings.append(f"{path}.{key} is not supported and is ignored{suffix}")


def unsupported_extension_keys(schema: Dict[str, Any]) -> List[str]:
    """Human-readable warnings about extension content that no processor applies.

    One entry per unsupported item, in a stable order (extension order, then schema order).
    Items that are empty, switched off (``enabled: false``) or equal to what happens anyway
    (``onViolation: bad_rows``) are not reported. Never raises for malformed content - the
    loaders do - and returns ``[]`` for a schema without extensions.
    """
    if not isinstance(schema, dict):
        return []
    warnings: List[str] = []

    if schema.get("x-pii") is not None:
        warnings.append("x-pii is documentation only: no masking is applied")
    _transformation_warnings(schema.get("x-transformations"), warnings)
    _calculated_columns_warnings(schema.get("x-calculatedColumns"), warnings)
    _unknown_keys(schema.get("x-columnMapping"), "x-columnMapping", _COLUMN_MAPPING_KEYS, warnings)
    _unknown_keys(schema.get("x-primaryKey"), "x-primaryKey", _PRIMARY_KEY_KEYS, warnings)
    _unique_constraint_warnings(schema.get("x-uniqueConstraints"), warnings)
    _constraint_handling_warnings(schema.get("x-constraintHandling"), warnings)
    _validation_warnings(schema.get("x-validation"), warnings)
    _quality_warnings(schema.get("x-dataQuality"), warnings)
    return warnings


def _transformation_warnings(section: Any, warnings: List[str]) -> None:
    if not isinstance(section, dict):
        return
    for key, value in section.items():
        if key == "column_transformations" or key in _DOC_KEYS or not _block_is_active(value):
            continue
        step = _TRANSFORMATION_STEPS.get(key, "<transformation>")
        warnings.append(
            f"x-transformations.{key} is not read "
            f"(use x-transformations.column_transformations.<column>.{step})"
        )


def _calculated_columns_warnings(section: Any, warnings: List[str]) -> None:
    if not isinstance(section, dict):
        return
    for key, value in section.items():
        if key in _CALCULATED_COLUMNS_KEYS or key in _DOC_KEYS or not _is_set(value):
            continue
        if key == "partitionColumns":
            warnings.append(
                "x-calculatedColumns.partitionColumns is recorded only: the output is not "
                "partitioned"
            )
        elif key == "options":
            warnings.append(
                "x-calculatedColumns.options is not read (use the top-level failOnError, "
                "addMetadata and validateDependencies keys)"
            )
        else:
            warnings.append(f"x-calculatedColumns.{key} is not read and is ignored")


def _unique_constraint_warnings(section: Any, warnings: List[str]) -> None:
    if not isinstance(section, list):
        return
    for i, item in enumerate(section):
        if not isinstance(item, dict):
            continue
        path = f"x-uniqueConstraints[{i}]"
        for key, value in item.items():
            if key in _DOC_KEYS or key in ("name", "columns") or not _is_set(value):
                continue
            if key == "condition":
                warnings.append(
                    f"{path}.condition is not supported and is ignored "
                    f"(the constraint applies to every row)"
                )
            elif key == "caseSensitive":
                if value is False:
                    warnings.append(
                        f"{path}.caseSensitive=false is not supported and is ignored "
                        f"(values are compared case-sensitively)"
                    )
            elif key == "ignoreNulls":
                if value is False:
                    warnings.append(
                        f"{path}.ignoreNulls=false is not supported and is ignored "
                        f"(NULL keys are never compared)"
                    )
            else:
                warnings.append(f"{path}.{key} is not supported and is ignored")


def _leaf_values(value: Any) -> List[Any]:
    if isinstance(value, dict):
        return [leaf for item in value.values() for leaf in _leaf_values(item)]
    return [value]


def _constraint_handling_warnings(section: Any, warnings: List[str]) -> None:
    if not isinstance(section, dict):
        return
    mode = section.get("errorMode") if isinstance(section.get("errorMode"), str) else "bad_rows"
    suffix = " (errorMode applies to all constraints)"
    for key, value in section.items():
        if key == "errorMode" or key in _DOC_KEYS or not _is_set(value):
            continue
        if key in ("primaryKeyViolations", "uniqueConstraintViolations", "notNullViolations"):
            if all(leaf == mode for leaf in _leaf_values(value)):
                continue  # asks for what errorMode does anyway
        warnings.append(f"x-constraintHandling.{key} is not supported and is ignored{suffix}")


def _validation_warnings(section: Any, warnings: List[str]) -> None:
    if not isinstance(section, dict):
        return
    where = "x-validation"
    _unknown_keys(section, where, _VALIDATION_KEYS, warnings)

    bad_rows = section.get("badRowsHandling")
    if isinstance(bad_rows, dict):
        for key in _BAD_ROWS_IGNORED:
            if bad_rows.get(key) is not None:
                warnings.append(
                    f"{where}.badRowsHandling.{key} is not supported and is ignored "
                    f"(rejected rows are written by the import, not by the validator)"
                )
        if bad_rows.get("enabled") is False:
            warnings.append(
                f"{where}.badRowsHandling.enabled=false is not supported and is ignored "
                f"(rejected rows are always removed and reported)"
            )
        for key, value in bad_rows.items():
            if (
                key not in _BAD_ROWS_KEYS
                and key not in _BAD_ROWS_IGNORED
                and key not in _DOC_KEYS
                and _is_set(value)
            ):
                warnings.append(f"{where}.badRowsHandling.{key} is not supported and is ignored")

    uniqueness = section.get("uniquenessHandling")
    if isinstance(uniqueness, dict):
        for key, value in uniqueness.items():
            if key not in ("strategy", "options") and key not in _DOC_KEYS and _is_set(value):
                warnings.append(
                    f"{where}.uniquenessHandling.{key} is not supported and is ignored"
                )

    fields = section.get("fieldValidations")
    if isinstance(fields, dict):
        for name, rule in fields.items():
            if isinstance(rule, dict):
                _field_rule_warnings(f"{where}.fieldValidations.{name}", rule, warnings)

    for key in ("crossFieldValidations", "globalValidations"):
        if _is_set(section.get(key)):
            warnings.append(f"{where}.{key} is not supported and is ignored")


def _field_rule_warnings(path: str, rule: Dict[str, Any], warnings: List[str]) -> None:
    for key, value in rule.items():
        if key in _DOC_KEYS or not _is_set(value):
            continue
        if key == "onViolation":
            if isinstance(value, dict) and any(leaf != "bad_rows" for leaf in _leaf_values(value)):
                warnings.append(
                    f"{path}.onViolation is not supported and is ignored "
                    f"(a violation always rejects the row)"
                )
        elif key in _FIELD_BLOCK_KEYS:
            _unknown_keys(value, f"{path}.{key}", _FIELD_BLOCK_KEYS[key], warnings)
        elif key not in _FIELD_RULE_KEYS:
            warnings.append(f"{path}.{key} is not supported and is ignored")


def _quality_warnings(section: Any, warnings: List[str]) -> None:
    if not isinstance(section, dict) or section.get("enabled") is False:
        return
    where = "x-dataQuality"
    for key, value in section.items():
        if key in _QUALITY_TOP_KEYS or key in _DOC_KEYS or not _block_is_active(value):
            continue
        warnings.append(f"{where}.{key} is not supported and is ignored")

    specific = section.get("fieldSpecificRules")
    if isinstance(specific, dict):
        for name, rule in specific.items():
            if not isinstance(rule, dict):
                continue
            for key, value in rule.items():
                if key in _QUALITY_SPECIFIC_KEYS or key in _DOC_KEYS or not _is_set(value):
                    continue
                if key in _QUALITY_FALSE_IS_NOOP and value is False:
                    continue
                warnings.append(
                    f"{where}.fieldSpecificRules.{name}.{key} is not supported and is ignored"
                )

    documented = section.get("fieldQualityRules")
    if isinstance(documented, dict):
        applied = (
            "only the parameters min_length, max_length, pattern, min_value and max_value "
            "are applied"
        )
        for name, rule in documented.items():
            if not isinstance(rule, dict):
                continue
            path = f"{where}.fieldQualityRules.{name}"
            for key, value in rule.items():
                if key == "parameters" or key in _DOC_KEYS or not _is_set(value):
                    continue
                warnings.append(f"{path}.{key} is not supported and is ignored ({applied})")
            parameters = rule.get("parameters")
            if isinstance(parameters, dict):
                for key, value in parameters.items():
                    if key not in _QUALITY_PARAMETER_KEYS and _is_set(value):
                        warnings.append(
                            f"{path}.parameters.{key} is not supported and is ignored ({applied})"
                        )
