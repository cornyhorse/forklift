"""CSV input adapter.

Thin wrapper around :func:`polars.read_csv` with placeholders for future:
- schema handling modes (accept/infer/enforce)
- header comment detection strategies (only active in enforce mode)
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
        - header_detection_rows: int (default 100)  (used for modes 'nrows', future heuristics)
        - delimiter: alias for Polars ``separator`` (only used if ``separator`` not provided)
        - has_header: bool (default set by instance, default True)
        - encoding: str (default set by instance, default "utf-8-sig")

        Other keys are forwarded directly to polars.read_csv.
        """
        # delimiter alias -> separator (non-destructive if separator already set)
        delimiter = options.pop("delimiter", None)
        if delimiter and "separator" not in options:
            options["separator"] = delimiter

        # Extract / apply defaults for polars core options
        has_header = options.pop("has_header", self.default_has_header)
        encoding = options.pop("encoding", self.default_encoding)
        options["has_header"] = has_header
        options["encoding"] = encoding

        schema_mode: SchemaMode = options.pop("schema_mode", "accept")  # type: ignore[assignment]
        header_mode: HeaderDetectionMode = options.pop("header_comment_detection_mode", "off")  # type: ignore[assignment]
        header_rows: int = int(options.pop("header_detection_rows", 100))

        # Validate header detection usage for non-enforce modes
        if schema_mode in ("accept", "infer") and header_mode != "off":
            raise ValueError(
                "header_comment_detection_mode only supported with schema_mode='enforce'"
            )

        if schema_mode == "accept":
            return self._schema_accept(source_path, options)
        if schema_mode == "infer":
            return self._schema_infer(source_path, options)
        if schema_mode == "enforce":
            return self._schema_enforce(
                source_path, options, has_header=has_header, header_mode=header_mode, header_rows=header_rows
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
        header_rows: int,
    ) -> pl.DataFrame:
        """Enforce schema (placeholder) with optional header detection.

        Detection Modes (current placeholder behavior):
          header  : (default if 'off' specified) placeholder – no offset applied yet.
          nrows   : skip the first `header_rows` lines.
          firstcol: placeholder – future logic to find header by first column name.
          regex   : placeholder – future logic using pattern on first column.
          off     : no detection, use provided options as-is.
        """
        if not has_header and header_mode != "off":  # detection meaningless
            raise ValueError("Cannot perform header detection when has_header is False")

        # Default to 'header' if user requested 'off' but still in enforce context (per spec)
        if header_mode == "off":
            header_mode = "header"

        # Apply minimal handling for 'nrows'
        if header_mode == "nrows":
            # Polars supports skip_rows: adjust only if not already provided
            if "skip_rows" in options:
                raise ValueError("Conflicting skip_rows provided alongside header_mode='nrows'")
            options["skip_rows"] = header_rows
        elif header_mode in ("header", "firstcol", "regex"):
            # Placeholder: real detection logic to be implemented.
            pass
        else:  # safety net though types restrict this
            raise ValueError(
                f"Unknown header detection mode '{header_mode}'. Expected one of: header, nrows, firstcol, regex, off"
            )

        # Enforce mode uses infer_schema=False to allow downstream validation of dtypes.
        return pl.read_csv(source_path, infer_schema=False, **options)

    # --- Placeholder detection strategy hook (retained for potential future factoring) ---
    def _detect_header_comments(self, mode: HeaderDetectionMode, rows: int, path: str, options: Dict[str, Any]) -> None:  # pragma: no cover - deprecated placeholder
        return None

    # --- Legacy placeholders kept for API stability (not directly invoked now) ---
    def _schema_enforce_legacy(self, source_path: str, options: Dict[str, Any]) -> pl.DataFrame:  # pragma: no cover - legacy placeholder
        return pl.read_csv(source_path, infer_schema=False, **options)


# Singleton instance used by engine dynamic registration
_csv_reader = CsvReader()


def read(source_path: str, options: Dict[str, Any]) -> pl.DataFrame:
    """Module-level wrapper retained for engine compatibility."""
    return _csv_reader.read(source_path, options)


__all__ = ["read", "CsvReader"]
