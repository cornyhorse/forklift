"""CSV input adapter.

Thin wrapper around :func:`polars.read_csv` with placeholders for future:
- schema handling modes (accept/infer/enforce)
- header detection strategies (only active in enforce mode) for: header, firstcol, regex
- processing modes: atomic (implemented) and chunk (placeholder)
- delimiter alias passthrough (``delimiter`` -> ``separator``)
"""
from __future__ import annotations
from typing import Dict, Any, Literal
import polars as pl

SchemaMode = Literal["accept", "infer", "enforce"]
HeaderDetectionMode = Literal["header", "firstcol", "regex", "off"]


class CsvReader:
    """CSV reader with pluggable (placeholder) schema & header detection.

    Header detection is ONLY applied when ``schema_mode='enforce'``. If any
    other schema_mode is used with a header detection mode != 'off', an error
    is raised to prevent ambiguous behavior.
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
        - processing_mode: {"atomic", "chunk"} (default "atomic")
        - chunk_size: int (default 50_000) used only when processing_mode="chunk"
        - delimiter: alias for Polars ``separator`` (only used if ``separator`` not provided)
        - has_header: bool (default set by instance, default True)
        - encoding: str (default set by instance, default "utf-8-sig")

        For removing a fixed number of initial lines, provide Polars' native ``skip_rows`` directly.
        """
        # delimiter alias -> separator (non-destructive if separator already set)
        delimiter = options.pop("delimiter", None)
        if delimiter and "separator" not in options:
            options["separator"] = delimiter

        # Core defaults
        has_header = options.pop("has_header", self.default_has_header)
        encoding = options.pop("encoding", self.default_encoding)
        options["has_header"] = has_header
        options["encoding"] = encoding

        # New processing controls
        processing_mode = options.pop("processing_mode", "atomic")
        chunk_size = int(options.pop("chunk_size", 50_000))
        if processing_mode not in ("atomic", "chunk"):
            raise ValueError("processing_mode must be 'atomic' or 'chunk'")

        schema_mode: SchemaMode = options.pop("schema_mode", "accept")  # type: ignore[assignment]
        header_mode: HeaderDetectionMode = options.pop("header_comment_detection_mode", "off")  # type: ignore[assignment]

        if schema_mode in ("accept", "infer") and header_mode != "off":
            raise ValueError(
                "header_comment_detection_mode only supported with schema_mode='enforce'"
            )

        # Placeholder chunk processing behavior
        if processing_mode == "chunk":  # pragma: no cover - placeholder
            raise NotImplementedError(
                "processing_mode='chunk' not implemented yet (will iterate file in chunks of chunk_size)"
            )

        # Atomic flow
        if schema_mode == "accept":
            return self._schema_accept(source_path, options)
        if schema_mode == "infer":
            return self._schema_infer(source_path, options)
        if schema_mode == "enforce":
            return self._schema_enforce(
                source_path, options, has_header=has_header, header_mode=header_mode
            )
        raise ValueError(
            f"Unknown schema_mode '{schema_mode}'. Expected one of: accept, infer, enforce"
        )

    # --- Schema mode handlers -------------------------------------------
    def _schema_accept(self, source_path: str, options: Dict[str, Any]) -> pl.DataFrame:
        # Strictly no header detection here.
        return pl.read_csv(source_path, infer_schema=False, **options)

    def _schema_infer(self, source_path: str, options: Dict[str, Any]) -> pl.DataFrame:  # pragma: no cover - placeholder
        # No header detection allowed in infer mode (validated earlier).
        return pl.read_csv(source_path, infer_schema=True, **options)

    def _schema_enforce(
        self,
        source_path: str,
        options: Dict[str, Any],
        *,
        has_header: bool,
        header_mode: HeaderDetectionMode,
    ) -> pl.DataFrame:
        """Enforce schema (placeholder) with optional header detection.

        Detection Modes (current placeholder behavior):
          header  : placeholder – no offset / change yet.
          firstcol: placeholder – future logic to find header by first column name.
          regex   : placeholder – future logic using pattern on first column.
          off     : no detection, use provided options as-is (treated as 'header' internally).
        """
        if not has_header and header_mode != "off":
            raise ValueError("Cannot perform header detection when has_header is False")

        if header_mode == "off":
            header_mode = "header"  # unify path

        if header_mode in ("header", "firstcol", "regex"):
            # Placeholder: real detection logic to be implemented.
            pass
        else:  # defensive
            raise ValueError(
                f"Unknown header detection mode '{header_mode}'. Expected one of: header, firstcol, regex, off"
            )

        # Enforce mode uses infer_schema=False to allow downstream validation of dtypes.
        return pl.read_csv(source_path, infer_schema=False, **options)

    # --- Deprecated placeholder retained for potential future factoring ---
    def _detect_header_comments(self, mode: HeaderDetectionMode, path: str, options: Dict[str, Any]) -> None:  # pragma: no cover
        return None


# Singleton instance used by engine dynamic registration
_csv_reader = CsvReader()


def read(source_path: str, options: Dict[str, Any]) -> pl.DataFrame:
    """Module-level wrapper retained for engine compatibility."""
    return _csv_reader.read(source_path, options)


__all__ = ["read", "CsvReader"]
