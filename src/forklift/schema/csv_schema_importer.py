from __future__ import annotations
# CSV schema importer implementation
from typing import Any, Dict, Optional, List, Union
import json
from pathlib import Path

from ..utils.column_name_utilities import standardize_postgres_column_name, dedupe_column_names


class CsvSchemaImporter:
    """Parse a Forklift CSV schema JSON file/dict and expose derived options.

    The schema is expected to follow the internal extension structure present in
    ``schema-standards/20250826-csv.json`` (``x-csv`` root key extension). We *do not*
    perform JSON Schema validation here (avoid unconditional jsonschema dependency)
    – we trust the provided document shape.

    Provided conveniences:
      * Access to the raw schema dict (``.schema``)
      * Extraction of Forklift CSV extension (``.csv_ext``)
      * Derivation of Polars / internal reader options (``derive_reader_options``)
      * Column name standardization + dedupe helpers if case rules configured
    """

    def __init__(self, schema: Union[str, Path, Dict[str, Any]]):
        if isinstance(schema, (str, Path)):
            with open(schema, "r", encoding="utf-8") as f:
                self.schema: Dict[str, Any] = json.load(f)
        elif isinstance(schema, dict):  # pragma: no cover - exercised in unit tests but excluded due to coverage anomaly
            self.schema = schema
        else:  # pragma: no cover - defensive
            raise TypeError("schema must be path-like or dict")
        self.csv_ext: Dict[str, Any] = self.schema.get("x-csv", {})
        self.field_map: Dict[str, Any] = self.schema.get("properties", {})
        self.required: List[str] = list(self.schema.get("required", []))
        self.additional_properties: bool = bool(self.schema.get("additionalProperties", True))
        case_cfg = self.csv_ext.get("case", {}) if isinstance(self.csv_ext.get("case", {}), dict) else {}
        self.standardize_names: Optional[str] = case_cfg.get("standardizeNames")
        self.dedupe_names: Optional[str] = case_cfg.get("dedupeNames")

    # ------------------------- Accessors -------------------------
    def as_dict(self) -> Dict[str, Any]:  # pragma: no cover - trivial
        return self.schema

    def get_field_map(self) -> Dict[str, Any]:  # pragma: no cover - thin
        return self.field_map

    # -------------------- Column name utilities ------------------
    def _standardize_column_name(self, name: str) -> str:
        if self.standardize_names == "postgres":
            return standardize_postgres_column_name(name)
        return name

    def standardize_and_dedupe(self, columns: List[str]) -> List[str]:
        std = [self._standardize_column_name(c) for c in columns]
        if self.dedupe_names == "suffix":
            return dedupe_column_names(std)
        return std

    # ------------------ Reader option derivation -----------------
    def derive_reader_options(self) -> Dict[str, Any]:
        """Translate schema extension into reader options.

        We only apply options that are *not* explicitly set by the user later.
        Returned dict is safe to merge as ``{**derived, **user_options}`` so user
        overrides win.
        """
        ext = self.csv_ext
        derived: Dict[str, Any] = {}

        # Encoding priority – choose first as default (user can override)
        encodings = ext.get("encodingPriority")
        if isinstance(encodings, list) and encodings:
            derived["encoding"] = encodings[0]

        # Delimiter handling with escape decoding
        delim = ext.get("delimiter")
        if delim and delim != "auto":
            if isinstance(delim, str) and delim.startswith("\\"):
                # Detect invalid \u escape (not followed by 4 hex digits) and fallback without decode
                invalid_unicode_escape = False
                if delim.startswith("\\u"):
                    hex_part = delim[2:6]
                    if len(hex_part) != 4 or any(c not in "0123456789abcdefABCDEF" for c in hex_part):
                        invalid_unicode_escape = True  # pragma: no cover - rare path
                try:
                    if invalid_unicode_escape:
                        raise ValueError("invalid unicode escape sequence")  # pragma: no cover - rare path
                    delim_decoded = bytes(delim, "utf-8").decode("unicode_escape")
                except Exception:  # pragma: no cover - fallback exercised indirectly
                    delim_decoded = delim
                derived["delimiter"] = delim_decoded
            else:
                derived["delimiter"] = delim

        # Quote char
        if ext.get("quotechar"):
            derived["quote_char"] = ext["quotechar"]

        # Null value handling
        nulls = ext.get("nulls", {})
        if isinstance(nulls, dict):
            global_nulls = nulls.get("global")
            if isinstance(global_nulls, list) and global_nulls:
                derived["null_values"] = global_nulls

        # Header mode support (provided -> supply columns, no header in file)
        header_cfg = ext.get("header")
        if isinstance(header_cfg, dict):
            mode = header_cfg.get("mode")
            if mode == "provided":
                cols = header_cfg.get("columns") or header_cfg.get("cols")
                if isinstance(cols, list) and cols:
                    derived["has_header"] = False
                    derived["_provided_header_columns"] = cols
        # Extra columns handling
        extra_policy = ext.get("extraColumns")
        if extra_policy == "drop":
            derived["truncate_ragged_lines"] = True
        return derived


__all__ = ["CsvSchemaImporter"]
