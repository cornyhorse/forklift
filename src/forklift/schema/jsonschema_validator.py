from __future__ import annotations
from typing import Dict, Any, Optional
from jsonschema import Draft202012Validator, FormatChecker, ValidationError


class Validator:
    def __init__(self, schema: Dict[str, Any]):
        self.fc = FormatChecker()
        self._register_formats()
        self._validator = Draft202012Validator(schema, format_checker=self.fc)

    def _register_formats(self) -> None:
        @self.fc.checks("ssn", raises=Exception)
        def _is_ssn(value: str) -> bool:
            # naive example
            import re
            return bool(re.fullmatch(r"\d{3}-\d{2}-\d{4}", value))

    def validate_row(self, row: Dict[str, Any]) -> Optional[ValidationError]:
        try:
            self._validator.validate(row)
            return None
        except ValidationError as e:
            return e
