"""row_validation module removed.

Former functionality (schema row/type validation) has been consolidated into the
TypeCoercion preprocessor. This stub remains only to give a clear error if
legacy imports/functions are still referenced.

Update your pipeline to include the 'type_coercion' preprocessor for type and
format enforcement.
"""
from __future__ import annotations

__all__ = []  # nothing exported

REMOVAL_MESSAGE = (
    "row_validation utilities have been removed. Use the 'type_coercion' preprocessor "
    "for schema-based coercion/validation."
)

def validate_row_against_schema(*_, **__):  # type: ignore
    raise RuntimeError(REMOVAL_MESSAGE)

def validate_dataframe_against_schema(*_, **__):  # type: ignore
    raise RuntimeError(REMOVAL_MESSAGE)
