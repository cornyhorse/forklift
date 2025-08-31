"""CSV input adapter.

Thin wrapper around :func:`polars.read_csv` with placeholders for future:
- schema handling modes (accept/infer/enforce)
- header/footer comment detection strategies
- delimiter alias passthrough (``delimiter`` -> ``separator``)
"""
from __future__ import annotations
from typing import Dict, Any, Literal
import polars as pl

SchemaMode = Literal["accept", "infer", "enforce"]
HeaderDetectionMode = Literal["header", "nrows", "firstcol", "regex", "off"]
FooterDetectionMode = Literal["nrows", "regex", "word", "column", "off"]

# --- Placeholder detection strategy hooks ------------------------------------

def _detect_header_comments(mode: HeaderDetectionMode, rows: int, path: str, options: Dict[str, Any]) -> None:  # pragma: no cover - placeholder
    """Placeholder header comment detection.

    Parameters
    ----------
    mode : HeaderDetectionMode
    rows : int
        Number of initial rows to inspect.
    path : str
        CSV path (raw file scanning not yet implemented).
    options : Dict[str, Any]
        Reader options (may be mutated in future to inject schema/skip rows).
    """
    return None


def _detect_footer_comments(mode: FooterDetectionMode, rows: int, path: str, options: Dict[str, Any]) -> None:  # pragma: no cover - placeholder
    """Placeholder footer comment detection (unimplemented)."""
    return None

# --- Schema mode handlers ----------------------------------------------------

def _schema_accept(source_path: str, options: Dict[str, Any]) -> pl.DataFrame:
    return pl.read_csv(source_path, **options)


def _schema_infer(source_path: str, options: Dict[str, Any]) -> pl.DataFrame:  # pragma: no cover - placeholder
    raise NotImplementedError("schema_mode='infer' not implemented yet")


def _schema_enforce(source_path: str, options: Dict[str, Any]) -> pl.DataFrame:  # pragma: no cover - placeholder
    raise NotImplementedError("schema_mode='enforce' not implemented yet")

# --- Public read entry -------------------------------------------------------

def read(source_path: str, options: Dict[str, Any]) -> pl.DataFrame:
    """Read a CSV file into a Polars DataFrame.

    Recognized special option keys (all optional):
    - schema_mode: SchemaMode (default "accept")
    - header_comment_detection_mode: HeaderDetectionMode (default "off")
    - footer_comment_detection_mode: FooterDetectionMode (default "off")
    - header_detection_rows: int (default 100)
    - footer_detection_rows: int (default 100)
    - delimiter: alias for Polars ``separator`` (only used if ``separator`` not provided)

    Other keys are forwarded directly to polars.read_csv.
    """
    # delimiter alias -> separator (non-destructive if separator already set)
    delimiter = options.pop("delimiter", None)
    if delimiter is not None and "separator" not in options:
        options["separator"] = delimiter

    schema_mode: SchemaMode = options.pop("schema_mode", "accept")  # type: ignore[assignment]
    header_mode: HeaderDetectionMode = options.pop("header_comment_detection_mode", "off")  # type: ignore[assignment]
    footer_mode: FooterDetectionMode = options.pop("footer_comment_detection_mode", "off")  # type: ignore[assignment]
    header_rows: int = int(options.pop("header_detection_rows", 100))
    footer_rows: int = int(options.pop("footer_detection_rows", 100))

    # Execute (no-op) detection placeholders
    if header_mode != "off":
        _detect_header_comments(header_mode, header_rows, source_path, options)
    if footer_mode != "off":
        _detect_footer_comments(footer_mode, footer_rows, source_path, options)

    if schema_mode == "accept":
        return _schema_accept(source_path, options)
    if schema_mode == "infer":
        return _schema_infer(source_path, options)
    if schema_mode == "enforce":
        return _schema_enforce(source_path, options)

    raise ValueError(
        f"Unknown schema_mode '{schema_mode}'. Expected one of: accept, infer, enforce"
    )

__all__ = ["read"]
