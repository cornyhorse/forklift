"""Checks of requests and answers against the gateway's OpenAPI document (tests only).

:class:`Contract` finds the operation of a method and concrete path (``/api/v1/jobs/<id>``),
checks that every query parameter it is sent is one the operation declares and that a JSON body
has the request schema's shape (strictly: no property the schema does not have), and that an
answer has the response schema of its status. The fake gateway runs every request through it,
so neither the client nor the fake can drift from contracts/openapi.json unnoticed.
"""

from __future__ import annotations

import json
import re
import uuid
from pathlib import Path
from typing import Any, Dict, List, Tuple

OPENAPI = Path(__file__).resolve().parents[3] / "contracts" / "openapi.json"
_DATETIME = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|[+-]\d{2}:\d{2})$")


class ContractViolation(AssertionError):
    pass


class Contract:
    def __init__(self, path: Path = OPENAPI):
        self.document = json.loads(path.read_text(encoding="utf-8"))
        self.schemas = self.document["components"]["schemas"]
        self._routes: List[Tuple[re.Pattern, str]] = []
        # literal paths (/schemas/validate) before templated ones (/schemas/{schema_id})
        for template in sorted(self.document["paths"], key=lambda t: (t.count("{"), t)):
            pattern = re.sub(r"\{[^}]+\}", "[^/]+", template)
            self._routes.append((re.compile(f"^{pattern}$"), template))

    def operation(self, method: str, path: str) -> Tuple[str, Dict[str, Any]]:
        for pattern, template in self._routes:
            if pattern.match(path) and method.lower() in self.document["paths"][template]:
                return template, self.document["paths"][template][method.lower()]
        raise ContractViolation(f"{method} {path} is not an operation of the API")

    def check_request(self, method: str, path: str, query: Dict[str, str], body: Any) -> None:
        template, operation = self.operation(method, path)
        declared = {p["name"] for p in operation.get("parameters", []) if p["in"] == "query"}
        unknown = set(query) - declared
        if unknown:
            raise ContractViolation(f"{method} {template} has no query parameters {unknown}")
        schema = (
            operation.get("requestBody", {})
            .get("content", {})
            .get("application/json", {})
            .get("schema")
        )
        if body is None:
            if schema is not None and operation["requestBody"].get("required"):
                raise ContractViolation(f"{method} {template} needs a body")
            return
        if schema is None:
            raise ContractViolation(f"{method} {template} takes no body")
        self.validate(schema, body, f"{method} {template} body", strict=True)

    def check_response(self, method: str, path: str, status: int, body: Any) -> None:
        template, operation = self.operation(method, path)
        response = operation["responses"].get(str(status))
        if response is None:
            raise ContractViolation(f"{method} {template} does not answer {status}")
        schema = response.get("content", {}).get("application/json", {}).get("schema")
        if schema is not None:
            self.validate(schema, body, f"{method} {template} -> {status}")

    def validate(self, schema: Dict[str, Any], value: Any, where: str, strict=False) -> None:
        if "$ref" in schema:
            schema = self.schemas[schema["$ref"].rsplit("/", 1)[-1]]
        if "anyOf" in schema:
            for option in schema["anyOf"]:
                try:
                    self.validate(option, value, where, strict)
                    return
                except ContractViolation:
                    continue
            raise ContractViolation(f"{where}: {value!r} matches none of {schema['anyOf']}")
        if "const" in schema and value != schema["const"]:
            raise ContractViolation(f"{where}: {value!r} is not {schema['const']!r}")
        if "enum" in schema and value not in schema["enum"]:
            raise ContractViolation(f"{where}: {value!r} is not one of {schema['enum']}")
        kind = schema.get("type")
        if kind is None:
            return
        checks = {
            "null": lambda v: v is None,
            "string": lambda v: isinstance(v, str),
            "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
            "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
            "boolean": lambda v: isinstance(v, bool),
            "array": lambda v: isinstance(v, list),
            "object": lambda v: isinstance(v, dict),
        }
        if not checks[kind](value):
            raise ContractViolation(f"{where}: {value!r} is not of type {kind}")
        if kind == "string" and schema.get("format") == "date-time":
            if not _DATETIME.match(value):
                raise ContractViolation(f"{where}: {value!r} is not a date-time")
        if kind == "string" and schema.get("format") == "uuid":
            uuid.UUID(value)
        if kind == "array":
            for index, item in enumerate(value):
                self.validate(schema["items"], item, f"{where}[{index}]", strict)
        if kind == "object":
            properties = schema.get("properties", {})
            missing = [key for key in schema.get("required", []) if key not in value]
            if missing:
                raise ContractViolation(f"{where}: missing {missing}")
            extra = schema.get("additionalProperties")
            for key, item in value.items():
                if key in properties:
                    self.validate(properties[key], item, f"{where}.{key}", strict)
                elif isinstance(extra, dict):
                    self.validate(extra, item, f"{where}.{key}", strict)
                elif strict and properties and extra is not True:
                    raise ContractViolation(f"{where}: {key!r} is not a property of the schema")
