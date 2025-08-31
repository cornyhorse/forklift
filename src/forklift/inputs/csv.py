"""CSV input adapter.

Thin wrapper around :func:`polars.read_csv` with placeholders for future:
- schema handling modes (accept/infer/enforce)
- header comment detection strategies only (footer removed)
- delimiter alias passthrough (``delimiter`` -> ``separator``)

Class-based design groups related placeholder behaviors.
"""
from __future__ import annotations
from typing import Dict, Any, Literal
import polars as pl

SchemaMode = Literal["accept", "infer", "enforce"]
HeaderDetectionMode = Literal["header", "nrows", "firstcol", "regex", "off"]


class CsvReader:
    """CSV reader with pluggable (placeholder) schema & header comment detection.

    Parameters
    ----------
    default_has_header : bool, default True
        Default value used if caller does not supply ``has_header``.
    default_encoding : str, default "utf-8-sig"
        Default text encoding passed to Polars if none provided.

    Notes
    -----
    If ``has_header`` resolves False, header comment detection is skipped even
    if a non-"off" header_comment_detection_mode is requested.
    """

    def __init__(self, default_has_header: bool = True, default_encoding: str = "utf-8-sig") -> None:
        self.default_has_header = default_has_header
        self.default_encoding = default_encoding

    # --- Public API -----------------------------------------------------
    def read(self, source_path: str, options: Dict[str, Any]) -> pl.DataFrame:
        """Read a CSV file into a Polars DataFrame.

        Recognized special option keys (all optional):
        - schema_mode: SchemaMode (default "accept")
        - header_comment_detection_mode: HeaderDetectionMode (default "off")
        - header_detection_rows: int (default 100)
        - delimiter: alias for Polars ``separator`` (only used if ``separator`` not provided)
        - has_header: bool (default set by instance, default True)
        - encoding: str (default set by instance, default "utf-8-sig")

        Other keys are forwarded directly to polars.read_csv.
        """
        # delimiter alias -> separator (non-destructive if separator already set)
        delimiter = options.pop("delimiter", None)
        if delimiter is not None and "separator" not in options:
            options["separator"] = delimiter

        # Extract / apply defaults for polars core options
        has_header = options.pop("has_header", self.default_has_header)
        encoding = options.pop("encoding", self.default_encoding)
        options["has_header"] = has_header
        options["encoding"] = encoding

        schema_mode: SchemaMode = options.pop("schema_mode", "accept")  # type: ignore[assignment]
        header_mode: HeaderDetectionMode = options.pop("header_comment_detection_mode", "off")  # type: ignore[assignment]
        header_rows: int = int(options.pop("header_detection_rows", 100))

        # Execute (no-op) detection placeholders; header detection only if file has header
        if has_header and header_mode != "off":
            self._detect_header_comments(header_mode, header_rows, source_path, options)

        if schema_mode == "accept":
            return self._schema_accept(source_path, options)
        if schema_mode == "infer":
            return self._schema_infer(source_path, options)
        if schema_mode == "enforce":
            return self._schema_enforce(source_path, options)

        raise ValueError(
            f"Unknown schema_mode '{schema_mode}'. Expected one of: accept, infer, enforce"
        )

    # --- Placeholder detection strategy hooks ---------------------------
    def _detect_header_comments(self, mode: HeaderDetectionMode, rows: int, path: str, options: Dict[str, Any]) -> None:  # pragma: no cover - placeholder
        return None

    # --- Schema mode handlers -------------------------------------------
    def _schema_accept(self, source_path: str, options: Dict[str, Any]) -> pl.DataFrame:
        # Infer schema is set to false to so that when dtypes are enforced, we can make a 'bad rows' file.
        return pl.read_csv(source_path, **options)

    def _schema_infer(self, source_path: str, options: Dict[str, Any]) -> pl.DataFrame:  # pragma: no cover - placeholder
        raise NotImplementedError("schema_mode='infer' not implemented yet")


    def _schema_enforce(self, source_path: str,
                        options: Dict[str, Any]) -> pl.DataFrame:  # pragma: no cover - placeholder
        # Infer schema is set to false to so that when dtypes are enforced, we can make a 'bad rows' file.
        return pl.read_csv(source_path, infer_schema=False, **options)


# Singleton instance used by engine dynamic registration
_csv_reader = CsvReader()


def read(source_path: str, options: Dict[str, Any]) -> pl.DataFrame:
    """Module-level wrapper retained for engine compatibility."""
    return _csv_reader.read(source_path, options)


__all__ = ["read", "CsvReader"]
