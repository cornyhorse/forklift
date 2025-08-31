from __future__ import annotations
from .forklift_core import Engine, ReaderFunction, WriterFunction
from typing import Any, Optional
from importlib import import_module
import polars as pl

# Single shared engine instance
_engine = Engine()

# ----------------- Reader registration & use (private helpers) -----------------


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


def read_csv(
    source_path: str,
    *,
    schema_mode: str = "accept",
    header_comment_detection_mode: str = "off",
    processing_mode: str = "atomic",
    chunk_size: int = 50000,
    **options: Any,
):
    """Read a CSV file with optional schema + header comment detection placeholders.

    Parameters
    ----------
    source_path : str
        Path to CSV file.
    schema_mode : {"accept", "infer", "enforce"}, default "accept"
        Placeholder schema behavior mode.
    header_comment_detection_mode : {"header", "firstcol", "regex", "off"}, default "off"
        Strategy for detecting header comment lines (placeholder, active only in enforce mode).
    processing_mode : {"atomic", "chunk"}, default "atomic"
        Execution mode. ``atomic`` reads/processes the entire file at once. ``chunk`` (placeholder)
        would process the file in segments of ``chunk_size`` rows.
    chunk_size : int, default 50000
        Target number of rows per chunk in chunk processing mode (ignored for atomic).
    **options : Any
        Additional options forwarded to the CSV reader implementation. Use Polars' native
        ``skip_rows`` if you need to drop a fixed number of initial lines.
    """
    options["schema_mode"] = schema_mode
    options["header_comment_detection_mode"] = header_comment_detection_mode
    options["processing_mode"] = processing_mode
    options["chunk_size"] = chunk_size
    return _read("csv", source_path, **options)

# ----------------- Writer registration & use (private helpers) -----------------


def _register_writer(
    writer_kind: str,
    writer_function: Optional[WriterFunction] = None,
    override: bool = False,
) -> None:
    """Internal: register a writer function (lazy-importing builtin module if needed)."""
    normalized = writer_kind.lower()
    fn = writer_function
    if fn is None:
        try:
            module = import_module(f"forklift.outputs.{normalized}_output")
        except ModuleNotFoundError:
            # Try without suffix for symmetry (parquet vs parquet_output)
            try:
                module = import_module(f"forklift.outputs.{normalized}")
            except ModuleNotFoundError as e:  # pragma: no cover
                raise ValueError(
                    f"Could not auto-import builtin module 'forklift.outputs.{normalized}_output' or '.{normalized}' for writer registration"
                ) from e
        fn = getattr(module, "write", None)
        if fn is None:  # pragma: no cover
            raise ValueError(
                f"Module 'forklift.outputs.{normalized}' does not expose a 'write' callable"
            )
    _engine.register_writer(normalized, fn, override=override)


def _write(writer_kind: str, destination_path: str, df: pl.DataFrame, **options: Any):
    normalized = writer_kind.lower()
    if normalized not in _engine._registered_writers:  # type: ignore[attr-defined]
        _register_writer(normalized)
    return _engine.write(normalized, destination_path, df, **options)


def write_parquet(destination_path: str, df: pl.DataFrame, **options: Any):
    return _write("parquet", destination_path, df, **options)

__all__ = [
    "Engine",
    "ReaderFunction",
    "WriterFunction",
    "read_csv",
    "write_parquet",
]
