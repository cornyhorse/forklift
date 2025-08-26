from __future__ import annotations
from typing import Type, Dict, List, Optional
from ..inputs.base import BaseInput
from ..outputs.base import BaseOutput
from ..preprocessors.base import Preprocessor
from ..inputs.csv_input import CSVInput
from ..inputs.fwf_input import FWFInput
from ..inputs.excel_input import ExcelInput
from ..outputs.parquet_output import PQOutput
from ..preprocessors.type_coercion import TypeCoercion
from ..preprocessors.footer_filter import FooterFilter

_INPUTS: Dict[str, Type[BaseInput]] = {
    "csv": CSVInput,
    "fwf": FWFInput,
    "excel": ExcelInput,
}
_OUTPUTS: Dict[str, Type[BaseOutput]] = {
    "parquet": PQOutput,
}
_PREPROCS: Dict[str, Type[Preprocessor]] = {
    "type_coercion": TypeCoercion,
    "footer_filter": FooterFilter,
    # …
}


def get_input_cls(kind: str) -> Type[BaseInput]:
    return _INPUTS[kind]


def get_output_cls(kind: str) -> Type[BaseOutput]:
    return _OUTPUTS[kind]


def get_preprocessors(names: Optional[List[str]]) -> List[Preprocessor]:
    if not names:
        return []
    return [_PREPROCS[n]() for n in names]
