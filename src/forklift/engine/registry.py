from __future__ import annotations
from typing import Type, Dict, Any
from ..inputs.base import BaseInput
from ..outputs.base import BaseOutput

def get_input_cls(kind: str) -> Type[BaseInput]:
    """
    Return the input class for the given kind.
    Supported kinds: "csv" (CSVInput)
    Note: Input classes now support explicit header handling via the 'header_mode' option ("present", "absent", "auto").
    """
    if kind == "csv":
        from ..inputs.csv_input import CSVInput
        return CSVInput
    if kind == "excel":
        from ..inputs.excel_input import ExcelInput
        return ExcelInput
    raise KeyError(f"Unknown input kind: {kind}")

def get_output_cls(kind: str) -> Type[BaseOutput]:
    if kind == "parquet":
        from ..outputs.parquet_output import PQOutput
        return PQOutput
    raise KeyError(f"Unknown output kind: {kind}")

def _extract_types_and_nulls(schema: Dict[str, Any] | None):
    if not schema:
        return {}, {}
    props = schema.get("properties", {})
    types: Dict[str, str] = {}
    for k, spec in props.items():
        t = spec.get("type")
        if t in ("number", "integer"):
            types[k] = "number"
        elif t == "string" and spec.get("format") == "date":
            types[k] = "date"
    nulls: Dict[str, list[str]] = {}
    xcsv = (schema.get("x-csv") or {})
    null_cfg = (xcsv.get("nulls") or {})
    percol = (null_cfg.get("perColumn") or {})
    for k, vals in percol.items():
        nulls[k] = list(vals)
    global_nulls = list(null_cfg.get("global") or [])
    for k in types:
        nulls.setdefault(k, [])
        nulls[k].extend(global_nulls)
    return types, nulls

def get_preprocessors(names, schema: Dict[str, Any] | None = None):
    if not names:
        return []
    mapping = {}
    try:
        from ..preprocessors.type_coercion import TypeCoercion
        mapping["type_coercion"] = TypeCoercion
    except Exception:
        pass
    try:
        from ..preprocessors.footer_filter import FooterFilter
        mapping["footer_filter"] = FooterFilter
    except Exception:
        pass

    types, nulls = _extract_types_and_nulls(schema)
    out = []
    for n in names:
        if n == "type_coercion" and "type_coercion" in mapping:
            out.append(mapping[n](types=types, nulls=nulls))
        elif n in mapping:
            out.append(mapping[n]())
    return out