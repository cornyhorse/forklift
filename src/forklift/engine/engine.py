# src/forklift/engine/engine.py
from __future__ import annotations
from typing import Iterable, Dict, Any, Optional, List
from ..types import Row, RowResult
from .registry import get_input_cls, get_output_cls, get_preprocessors
from ..schema.jsonschema_validator import Validator


class Engine:
    def __init__(
            self,
            input_kind: str,  # "csv" | "fwf" | "excel"
            output_kind: str = "parquet",  # for now: fixed to parquet
            schema: Optional[Dict[str, Any]] = None,
            preprocessors: Optional[List[str]] = None,
            **kwargs: Any,  # CLI/API args (delimiter, encoding, etc.)
    ):
        self.input_kind = input_kind
        self.output_kind = output_kind
        self.input_opts = kwargs
        self.schema = schema or {}
        self.validator = Validator(self.schema) if schema else None
        self.preprocessors = get_preprocessors(preprocessors)

        self.Input = get_input_cls(input_kind)
        self.Output = get_output_cls(output_kind)

    def run(self, source: str, dest: str) -> None:
        inp = self.Input(source, **self.input_opts)
        out = self.Output(dest, schema=self.schema, **self.input_opts)

        out.open()
        try:
            for rr in self._process(inp.iter_rows()):
                if rr.error:
                    out.quarantine(rr)  # DLQ
                else:
                    out.write(rr.row)
        finally:
            out.close()

    def _process(self, rows: Iterable[Row]) -> Iterable[RowResult]:
        for row in rows:
            # preprocess chain (composition)
            for p in self.preprocessors:
                row = p.apply(row)  # must return modified row or raise
            # jsonschema validation (value semantics)
            if self.validator:
                err = self.validator.validate_row(row)
                if err:
                    yield RowResult(row=None, error=err)
                    continue
            yield RowResult(row=row, error=None)
