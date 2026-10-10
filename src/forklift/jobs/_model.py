"""One definition per contract field: the dataclass, its validation and its JSON Schema.

The job contract (:mod:`forklift.jobs.spec`, :mod:`forklift.jobs.result`) is written as plain
dataclasses whose fields are declared with :func:`contract_field`. From that single definition:

* ``Model.from_dict()`` checks a document and builds the dataclasses, reporting every problem
  with the path of the field it is about (``input.location.path: required field is missing``);
* ``Model.to_dict()`` turns them back into plain JSON values;
* :func:`json_schema` writes the JSON Schema published in ``contracts/``.

Supported field types: ``str``, ``int``, ``float``, ``bool``, ``Any`` (any JSON value),
``Optional[...]``, ``List[...]``, ``Dict[str, ...]``, other models, unions of models that are
told apart by their ``type`` key (locations), and unions of ``str`` and ``int``. Constraints are
JSON Schema keywords given to :func:`contract_field` (``enum``, ``const``, ``minimum``,
``maximum``, ``exclusiveMinimum``, ``minLength``, ``maxLength``, ``pattern``, ``minItems``,
``items``); both the validation here and the published schema use them. Rules that involve
several fields are written twice, as ``_check`` (Python) and ``__schema_rules__`` (JSON Schema);
the contract tests make sure the two agree.
"""

from __future__ import annotations

import dataclasses
import difflib
import re
import typing
from typing import Any, ClassVar, Dict, List, Optional, Tuple, Type, TypeVar, Union

_MISSING = dataclasses.MISSING
_NONE_TYPE = type(None)

Problem = Tuple[str, str]
M = TypeVar("M", bound="Model")


def contract_field(
    description: str,
    *,
    default: Any = _MISSING,
    default_factory: Any = _MISSING,
    repr: bool = True,
    required: Optional[bool] = None,
    order: int = 0,
    formats: Optional[Tuple[str, ...]] = None,
    **schema: Any,
) -> Any:
    """Declare a field of a contract model.

    Args:
        description: What the field means (published in the JSON Schema)
        default: Default value; a field without one is required
        default_factory: Builds the default (for lists, dicts and nested models)
        repr: False hides the value from ``repr()`` (connection strings, presigned URLs)
        required: Override whether a document must contain the field (default: required
            exactly when there is no default)
        order: Fields are written in ascending ``order``, then in declaration order
        formats: For input options, the input formats the option applies to
        **schema: JSON Schema constraint keywords for the value
    """
    kwargs: Dict[str, Any] = {}
    if default is not _MISSING:
        kwargs["default"] = default
    if default_factory is not _MISSING:
        kwargs["default_factory"] = default_factory
    metadata = {
        "description": description,
        "schema": schema,
        "required": required,
        "order": order,
        "formats": formats,
    }
    return dataclasses.field(repr=repr, metadata=metadata, **kwargs)


class ContractError(ValueError):
    """A document does not match the job contract.

    Attributes:
        problems: ``(path, message)`` for every problem found, in document order
    """

    def __init__(self, what: str, problems: List[Problem]):
        self.problems = list(problems)
        lines = [f"{path}: {message}" if path else message for path, message in self.problems]
        count = len(lines)
        super().__init__(
            f"Invalid {what} ({count} problem{'s' if count != 1 else ''}):\n- "
            + "\n- ".join(lines)
        )


class Model:
    """Base class of the contract's dataclasses."""

    #: Value of the ``type`` key for models that are one member of a tagged union
    location_type: ClassVar[Optional[str]] = None
    #: Extra JSON Schema (``allOf`` members) for rules that involve several fields
    __schema_rules__: ClassVar[List[Dict[str, Any]]] = []
    #: Name used in error messages (``Invalid job spec (...)``)
    __contract_name__: ClassVar[str] = "document"

    @classmethod
    def from_dict(cls: Type[M], data: Any) -> M:
        """Check ``data`` against the contract and build the model from it.

        Raises:
            ContractError: Listing every problem, each with the path of its field
        """
        problems: List[Problem] = []
        value = _parse(cls, data, "", problems)
        if problems:
            raise ContractError(cls.__contract_name__, problems)
        return value

    def to_dict(self) -> Dict[str, Any]:
        """The model as plain JSON values; optional fields that are None are left out."""
        out: Dict[str, Any] = {}
        if self.location_type is not None:
            out["type"] = self.location_type
        for item in _fields(type(self)):
            value = getattr(self, item.name)
            if value is None and not _is_required(item):
                continue
            out[item.name] = _dump(value)
        return out

    def _check(self, path: str, problems: List[Problem]) -> None:
        """Rules that involve several fields (the Python side of ``__schema_rules__``)."""


# --------------------------------------------------------------------------------- helpers


def _fields(cls: type) -> List[dataclasses.Field]:
    """The dataclass fields of ``cls`` in output order."""
    items = list(dataclasses.fields(cls))
    return sorted(items, key=lambda f: (f.metadata.get("order", 0), items.index(f)))


def _has_default(item: dataclasses.Field) -> bool:
    return item.default is not _MISSING or item.default_factory is not _MISSING


def _is_required(item: dataclasses.Field) -> bool:
    required = item.metadata.get("required")
    return (not _has_default(item)) if required is None else required


def _hints(cls: type) -> Dict[str, Any]:
    return typing.get_type_hints(cls)


def _join(path: str, name: str) -> str:
    return f"{path}.{name}" if path else name


def _dump(value: Any) -> Any:
    if isinstance(value, Model):
        return value.to_dict()
    if isinstance(value, list):
        return [_dump(v) for v in value]
    if isinstance(value, dict):
        return {k: _dump(v) for k, v in value.items()}
    return value


def _is_model(tp: Any) -> bool:
    return isinstance(tp, type) and issubclass(tp, Model)


def _union_members(tp: Any) -> Tuple[Any, ...]:
    return typing.get_args(tp) if typing.get_origin(tp) is Union else ()


def _describe(value: Any) -> str:
    """A short description of a value for messages (types only for containers)."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, str):
        return repr(value if len(value) <= 60 else value[:57] + "...")
    return {dict: "an object", list: "a list"}.get(type(value), type(value).__name__)


# --------------------------------------------------------------------------------- parsing


_PRIMITIVES = {
    str: ("a string", lambda v: isinstance(v, str)),
    bool: ("true or false", lambda v: isinstance(v, bool)),
    int: ("an integer", lambda v: isinstance(v, int) and not isinstance(v, bool)),
    float: (
        "a number",
        lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    ),
}


def _parse(tp: Any, value: Any, path: str, problems: List[Problem], schema=None) -> Any:
    """Check ``value`` against type ``tp`` (and constraints ``schema``); return it parsed."""
    schema = schema or {}
    members = _union_members(tp)
    if members and _NONE_TYPE in members:
        if value is None:
            return None
        rest = tuple(m for m in members if m is not _NONE_TYPE)
        tp = rest[0] if len(rest) == 1 else Union[rest]  # type: ignore[valid-type]
        members = _union_members(tp)

    if members and all(_is_model(m) for m in members):
        return _parse_tagged(members, value, path, problems)
    if members:  # str or int (an Excel sheet name or index)
        if not any(_PRIMITIVES[m][1](value) for m in members):
            names = " or ".join(_PRIMITIVES[m][0] for m in members)
            problems.append((path, f"must be {names}, got {_describe(value)}"))
            return None
        _check_constraints(value, schema, path, problems)
        return value
    if _is_model(tp):
        if tp.location_type is not None:  # a location of one allowed type: still has "type"
            return _parse_tagged((tp,), value, path, problems)
        return _parse_model(tp, value, path, problems)
    if tp is Any:
        return value

    origin = typing.get_origin(tp)
    if origin is list:
        if not isinstance(value, list):
            problems.append((path, f"must be a list, got {_describe(value)}"))
            return None
        _check_constraints(value, schema, path, problems)
        (item_type,) = typing.get_args(tp)
        return [
            _parse(item_type, item, f"{path}[{i}]", problems, schema.get("items"))
            for i, item in enumerate(value)
        ]
    if origin is dict:
        if not isinstance(value, dict):
            problems.append((path, f"must be an object, got {_describe(value)}"))
            return None
        _, item_type = typing.get_args(tp)
        out = {}
        for key, item in value.items():
            out[key] = _parse(
                item_type,
                item,
                _join(path, str(key)),
                problems,
                schema.get("additionalProperties"),
            )
        return out

    name, accepts = _PRIMITIVES[tp]
    if not accepts(value):
        problems.append((path, f"must be {name}, got {_describe(value)}"))
        return None
    _check_constraints(value, schema, path, problems)
    return value


def _parse_tagged(members: Tuple[Any, ...], value: Any, path: str, problems: List[Problem]):
    """A member of a union of models, chosen by the document's ``type`` key."""
    by_type = {m.location_type: m for m in members}
    allowed = ", ".join(repr(t) for t in by_type)
    if not isinstance(value, dict):
        problems.append(
            (path, f"must be an object with a 'type' ({allowed}), got {_describe(value)}")
        )
        return None
    if "type" not in value:
        problems.append((_join(path, "type"), f"required field is missing (one of {allowed})"))
        return None
    chosen = by_type.get(value["type"]) if isinstance(value["type"], str) else None
    if chosen is None:
        problems.append(
            (
                _join(path, "type"),
                f"{_describe(value['type'])} is not allowed here; expected one of {allowed}",
            )
        )
        return None
    return _parse_model(chosen, {k: v for k, v in value.items() if k != "type"}, path, problems)


def _parse_model(cls: Any, value: Any, path: str, problems: List[Problem]) -> Any:
    if not isinstance(value, dict):
        problems.append((path, f"must be an object, got {_describe(value)}"))
        return None
    hints = _hints(cls)
    items = {item.name: item for item in _fields(cls)}
    before = len(problems)
    for key in value:
        if key not in items:
            known = sorted(items) + (["type"] if cls.location_type else [])
            close = difflib.get_close_matches(str(key), known, n=1)
            hint = f" (did you mean {close[0]!r}?)" if close else ""
            problems.append(
                (_join(path, str(key)), f"unknown field{hint}; allowed: {', '.join(known)}")
            )
    kwargs = {}
    for name, item in items.items():
        where = _join(path, name)
        if name not in value:
            if _is_required(item):
                problems.append(
                    (where, f"required field is missing ({item.metadata['description']})")
                )
            continue
        kwargs[name] = _parse(hints[name], value[name], where, problems, item.metadata["schema"])
    if len(problems) > before:
        return None
    model = cls(**kwargs)
    model._check(path, problems)
    return model


def _check_constraints(value: Any, schema: Dict[str, Any], path: str, problems: List[Problem]):
    """The JSON Schema constraint keywords the contract uses."""
    if "const" in schema and value != schema["const"]:
        problems.append((path, f"must be {_describe(schema['const'])}, got {_describe(value)}"))
    if "enum" in schema and value not in schema["enum"]:
        allowed = ", ".join(_describe(v) for v in schema["enum"])
        problems.append((path, f"{_describe(value)} is not one of {allowed}"))
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            problems.append((path, f"must be at least {schema['minimum']}, got {value!r}"))
        if "maximum" in schema and value > schema["maximum"]:
            problems.append((path, f"must be at most {schema['maximum']}, got {value!r}"))
        if "exclusiveMinimum" in schema and value <= schema["exclusiveMinimum"]:
            problems.append(
                (path, f"must be greater than {schema['exclusiveMinimum']}, got {value!r}")
            )
    if isinstance(value, str):
        if "minLength" in schema and len(value) < schema["minLength"]:
            problems.append((path, f"must have at least {schema['minLength']} character(s)"))
        if "maxLength" in schema and len(value) > schema["maxLength"]:
            problems.append((path, f"must have at most {schema['maxLength']} character(s)"))
        if "pattern" in schema and not re.search(schema["pattern"], value):
            problems.append((path, schema.get("x-pattern-message", "has an invalid format")))
    if isinstance(value, list):
        if "minItems" in schema and len(value) < schema["minItems"]:
            problems.append((path, f"must have at least {schema['minItems']} item(s)"))


# --------------------------------------------------------------------------------- schema


_JSON_TYPES = {str: "string", int: "integer", float: "number", bool: "boolean"}


def json_schema(root: Type[Model], *, schema_id: str, title: str) -> Dict[str, Any]:
    """The JSON Schema (draft 2020-12) of ``root`` and every model it uses."""
    defs: Dict[str, Any] = {}
    _model_schema(root, defs)
    top = dict(defs.pop(root.__name__))
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": schema_id,
        "title": title,
        **top,
        "$defs": dict(sorted(defs.items())),
    }


def _model_schema(cls: Any, defs: Dict[str, Any]) -> Dict[str, Any]:
    if cls.__name__ not in defs:
        defs[cls.__name__] = {}  # placeholder: models may refer to each other
        hints = _hints(cls)
        properties: Dict[str, Any] = {}
        required: List[str] = []
        if cls.location_type is not None:
            properties["type"] = {"const": cls.location_type}
            required.append("type")
        for item in _fields(cls):
            prop = _type_schema(hints[item.name], item.metadata["schema"], defs)
            prop = {"description": item.metadata["description"], **prop}
            if not _is_required(item) and item.default is not _MISSING:
                prop["default"] = item.default
            elif item.default_factory in (list, dict):
                prop["default"] = item.default_factory()
            properties[item.name] = prop
            if _is_required(item):
                required.append(item.name)
        body: Dict[str, Any] = {
            "type": "object",
            "description": " ".join((cls.__doc__ or "").split("\n\n")[0].split()),
            "properties": properties,
            "required": required,
            "additionalProperties": False,
        }
        if cls.__schema_rules__:
            body["allOf"] = list(cls.__schema_rules__)
        defs[cls.__name__] = body
    return {"$ref": f"#/$defs/{cls.__name__}"}


def _type_schema(tp: Any, constraints: Dict[str, Any], defs: Dict[str, Any]) -> Dict[str, Any]:
    constraints = {k: v for k, v in constraints.items() if not k.startswith("x-")}
    members = _union_members(tp)
    if members and _NONE_TYPE in members:
        rest = tuple(m for m in members if m is not _NONE_TYPE)
        inner = _type_schema(rest[0] if len(rest) == 1 else Union[rest], constraints, defs)
        if "enum" in inner:
            return {**inner, "enum": inner["enum"] + [None]}
        if isinstance(inner.get("type"), str):
            return {**inner, "type": [inner["type"], "null"]}
        if isinstance(inner.get("type"), list):
            return {**inner, "type": inner["type"] + ["null"]}
        return {"anyOf": [inner, {"type": "null"}]}
    if members and all(_is_model(m) for m in members):
        # Told apart by "type": if/then per member (rather than oneOf) lets a validator point
        # at the field that is wrong instead of reporting that no member matched
        return {
            "type": "object",
            "required": ["type"],
            "properties": {"type": {"enum": [m.location_type for m in members]}},
            "allOf": [
                {
                    "if": {
                        "properties": {"type": {"const": m.location_type}},
                        "required": ["type"],
                    },
                    "then": _model_schema(m, defs),
                }
                for m in members
            ],
        }
    if members:
        return {"type": [_JSON_TYPES[m] for m in members], **constraints}
    if _is_model(tp):
        return _model_schema(tp, defs)
    if tp is Any:
        return dict(constraints)
    origin = typing.get_origin(tp)
    if origin is list:
        (item_type,) = typing.get_args(tp)
        items = _type_schema(item_type, constraints.get("items", {}), defs)
        rest = {k: v for k, v in constraints.items() if k != "items"}
        return {"type": "array", "items": items, **rest}
    if origin is dict:
        _, item_type = typing.get_args(tp)
        extra = _type_schema(item_type, constraints.get("additionalProperties", {}), defs)
        return {"type": "object", "additionalProperties": extra} if extra else {"type": "object"}
    return {"type": _JSON_TYPES[tp], **constraints}
