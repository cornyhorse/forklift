from __future__ import annotations
from .forklift_core import Engine, ReaderFunction
from typing import Any, Optional
from importlib import import_module

# Single shared engine instance
_engine = Engine()


def _register_reader(
    reader_kind: str,
    reader_function: Optional[ReaderFunction] = None,
    override: bool = False,
) -> None:
    """Internal: register a reader function (lazy-importing builtin module if needed)."""
    normalized = reader_kind.lower()
    fn = reader_function
    if fn is None:
        try:
            module = import_module(f"forklift.inputs.{normalized}")
        except ModuleNotFoundError as e:  # pragma: no cover
            raise ValueError(
                f"Could not auto-import builtin module 'forklift.inputs.{normalized}' for reader registration"
            ) from e
        fn = getattr(module, "read", None)
        if fn is None:  # pragma: no cover
            raise ValueError(
                f"Module 'forklift.inputs.{normalized}' does not expose a 'read' callable"
            )
    _engine.register_reader(normalized, fn, override=override)


def _read(reader_kind: str, source_path: str, **options: Any):
    normalized = reader_kind.lower()
    if normalized not in _engine._registered_readers:  # type: ignore[attr-defined]
        _register_reader(normalized)
    return _engine.read(normalized, source_path, **options)


def read_csv(source_path: str, **options: Any):
    return _read("csv", source_path, **options)

__all__ = [
    "Engine",
    "ReaderFunction",
    "read_csv",
]
