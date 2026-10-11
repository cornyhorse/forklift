"""Building the frozen dataclasses of :mod:`forklift_client.models` from the API's JSON, and back.

Decoding follows each field's type annotation: ``datetime`` fields are parsed from ISO 8601
(``Z`` or an offset; without one, UTC), nested resources become their dataclass, arrays become
tuples, objects stay dicts (``Dict[str, Any]``: a job's ``result``, a schema's ``document``).
Fields the API sends that a model does not know are ignored, so a newer gateway does not break
an older client; a required field it leaves out is an :class:`UnexpectedResponse`.
"""

from __future__ import annotations

import dataclasses
import re
from datetime import datetime, timedelta, timezone
from typing import (
    Any,
    Dict,
    Generic,
    Mapping,
    Tuple,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

from .errors import UnexpectedResponse

M = TypeVar("M", bound="Model")
T = TypeVar("T")

_DATETIME = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?"
    r"(Z|[+-]\d{2}:?\d{2})?$",
    re.IGNORECASE,
)
_HINTS: Dict[type, Dict[str, Any]] = {}


def parse_datetime(text: str) -> datetime:
    """An aware datetime from ISO 8601 as the API writes it (Python 3.9's ``fromisoformat``
    reads neither ``Z`` nor every fraction length)."""
    match = _DATETIME.match(text)
    if match is None:
        raise ValueError(f"{text!r} is not an ISO 8601 date and time")
    year, month, day, hour, minute, second, fraction, offset = match.groups()
    micro = int((fraction or "0")[:6].ljust(6, "0"))
    zone = timezone.utc
    if offset and offset.upper() != "Z":
        digits = offset[1:].replace(":", "")
        delta = timedelta(hours=int(digits[:2]), minutes=int(digits[2:]))
        zone = timezone(delta if offset[0] == "+" else -delta)
    return datetime(
        int(year), int(month), int(day), int(hour), int(minute), int(second or 0), micro, zone
    )


def _hints(cls: type) -> Dict[str, Any]:
    hints = _HINTS.get(cls)
    if hints is None:
        hints = _HINTS[cls] = get_type_hints(cls)
    return hints


def _decode(hint: Any, value: Any, where: str) -> Any:
    origin = get_origin(hint)
    if origin is Union:  # Optional[X] is the only union the models use
        if value is None:
            return None
        (inner,) = [arg for arg in get_args(hint) if arg is not type(None)]
        return _decode(inner, value, where)
    if hint is Any:
        return value
    if origin is dict:
        if not isinstance(value, dict):
            raise UnexpectedResponse(f"{where} should be an object, not {type(value).__name__}")
        return value
    if origin is tuple:
        if not isinstance(value, list):
            raise UnexpectedResponse(f"{where} should be a list, not {type(value).__name__}")
        (item,) = [arg for arg in get_args(hint) if arg is not Ellipsis]
        return tuple(_decode(item, entry, f"{where}[{i}]") for i, entry in enumerate(value))
    if hint is datetime:
        try:
            return parse_datetime(value)
        except (TypeError, ValueError):
            raise UnexpectedResponse(f"{where} should be a date and time: {value!r}") from None
    if isinstance(hint, type) and issubclass(hint, Model):
        if not isinstance(value, dict):
            raise UnexpectedResponse(f"{where} should be an object, not {type(value).__name__}")
        return hint._from_dict(value, where)
    expected = str if origin is not None else hint  # Literal[...] values are strings
    if expected is float and isinstance(value, int) and not isinstance(value, bool):
        return float(value)
    if not isinstance(value, expected) or (expected is int and isinstance(value, bool)):
        raise UnexpectedResponse(
            f"{where} should be {expected.__name__}, not {type(value).__name__}"
        )
    return value


def _encode(value: Any) -> Any:
    if isinstance(value, Model):
        return value.to_dict()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, tuple):
        return [_encode(item) for item in value]
    return value


class Model:
    """Base class of the API's resources (frozen dataclasses)."""

    @classmethod
    def from_dict(cls: Type[M], data: Mapping[str, Any]) -> M:
        """Build the resource from the API's JSON document."""
        return cls._from_dict(data, cls.__name__)

    @classmethod
    def _from_dict(cls: Type[M], data: Mapping[str, Any], where: str) -> M:
        hints = _hints(cls)
        values = {}
        for field in dataclasses.fields(cls):  # type: ignore[arg-type]
            key = field.metadata.get("json", field.name)
            if key in data:
                values[field.name] = _decode(hints[field.name], data[key], f"{where}.{key}")
            elif field.default is dataclasses.MISSING:
                raise UnexpectedResponse(
                    f"{where} has no {key!r}; is the gateway of a version this client does not "
                    "know?"
                )
        return cls(**values)

    def to_dict(self) -> Dict[str, Any]:
        """The resource as JSON-ready data (datetimes in ISO 8601), as the API sent it."""
        return {
            field.metadata.get("json", field.name): _encode(getattr(self, field.name))
            for field in dataclasses.fields(self)  # type: ignore[arg-type]
        }


@dataclasses.dataclass(frozen=True)
class Page(Generic[T]):
    """One page of a list: ``items`` from ``offset`` on, of ``count`` in all.

    Lists are ordered by the API (jobs newest first, schemas by name ...). Offsets move when
    items are added or removed between pages, so a list read page by page while it changes can
    repeat or miss items.
    """

    items: Tuple[T, ...]
    count: int
    offset: int
    limit: int

    @property
    def has_more(self) -> bool:
        """Whether items follow this page."""
        return bool(self.items) and self.offset + len(self.items) < self.count

    @property
    def next_offset(self) -> int:
        """The offset of the next page."""
        return self.offset + len(self.items)

    def __iter__(self):
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)
