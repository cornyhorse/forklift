"""The client against contracts/openapi.json: the models are generated from it, every call the
client makes is an operation it lists and returns the model of that operation's answer, and the
methods take the fields of the request bodies and the filters of the lists.

(Every request and answer of the other tests also goes through the fake gateway's contract
checks: tests/openapi_check.py.)
"""

from __future__ import annotations

import ast
import importlib.util
import inspect
import json
import re
from pathlib import Path

import pytest
from openapi_check import OPENAPI, Contract, ContractViolation

import forklift_client
from forklift_client import ForkliftClient
from forklift_client import client as client_module

ROOT = Path(__file__).resolve().parents[2]
DOCUMENT = json.loads(OPENAPI.read_text(encoding="utf-8"))


def _generator():
    spec = importlib.util.spec_from_file_location(
        "generate_models", ROOT / "tools" / "generate_models.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


GENERATOR = _generator()
MODEL_OF = {component: name for component, (name, _) in GENERATOR.MODELS.items()}


def test_the_models_are_generated_from_the_openapi_document():
    models = Path(forklift_client.models.__file__).read_text(encoding="utf-8")
    assert GENERATOR.render() == models, (
        "src/forklift_client/models.py is stale: run python "
        "clients/python/tools/generate_models.py and look at what changed in the API"
    )


def test_the_generator_check_mode(capsys):
    assert GENERATOR.main(["--check"]) == 0


def _path(node: ast.AST) -> str:
    if isinstance(node, ast.Constant):
        return node.value
    assert isinstance(node, ast.JoinedStr), ast.dump(node)
    return "".join(part.value if isinstance(part, ast.Constant) else "{}" for part in node.values)


def _calls():
    """(function, model or None, method, path with {} for parameters) of every API call."""
    tree = ast.parse(Path(client_module.__file__).read_text(encoding="utf-8"))
    found = []
    for function in ast.walk(tree):
        if not isinstance(function, ast.FunctionDef) or function.name.startswith("_"):
            continue
        for node in ast.walk(function):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "self"
            ):
                continue
            args, name = node.args, node.func.attr
            if name == "_one":
                found.append((function.name, args[0].id, args[1].value, _path(args[2])))
            elif name in ("_page", "_list"):
                found.append((function.name, args[0].id, "GET", _path(args[1])))
            elif name == "_submitted":
                found.append((function.name, "Job", "POST", _path(args[0])))
            elif name == "_request":
                found.append((function.name, None, args[0].value, _path(args[1])))
    return found


def _operation(method: str, path: str):
    for template, operations in DOCUMENT["paths"].items():
        if re.sub(r"\{[^}]+\}", "{}", template) == "/api/v1" + path:
            if method.lower() in operations:
                return template, operations[method.lower()]
    raise AssertionError(f"{method} /api/v1{path} is not an operation of contracts/openapi.json")


def _component(schema: dict) -> str:
    return schema["$ref"].rsplit("/", 1)[-1]


CALLS = _calls()


def test_every_api_method_was_found():
    methods = {name for name, _ in inspect.getmembers(ForkliftClient, inspect.isfunction)}
    calling = {function for function, *_ in CALLS}
    helpers = {"paginate", "iter_job_events", "wait_for_job", "upload_file", "from_env"}
    helpers |= {"download_artifact", "open_artifact"}
    public = {name for name in methods if not name.startswith("_")}
    assert public - helpers == calling


@pytest.mark.parametrize("function, model, method, path", CALLS, ids=[c[0] for c in CALLS])
def test_calls_are_operations_and_return_their_answers_model(function, model, method, path):
    template, operation = _operation(method, path)
    answers = {
        status: response["content"]["application/json"]["schema"]
        for status, response in operation["responses"].items()
        if status.startswith("2") and "content" in response
    }
    if model is None:
        return
    assert answers, f"{method} {template} documents no answer"
    for status, schema in answers.items():
        if schema.get("type") == "array":
            schema = schema["items"]
        component = _component(schema)
        if component.startswith("Paged"):
            component = _component(
                DOCUMENT["components"]["schemas"][component]["properties"]["items"]["items"]
            )
        assert (
            MODEL_OF.get(component) == model
        ), f"{function}: {status} of {template} is {component}"


BODIES = {
    "submit_job": ("JobIn", {"idempotency_key"}),
    "validate_schema": ("ValidateIn", set()),
    "create_upload": ("UploadIn", set()),
    "create_schema": ("SchemaIn", set()),
    "create_schema_version": ("SchemaVersionIn", {"schema"}),
    "run_dataset": ("DatasetRunIn", {"dataset", "idempotency_key"}),
}


@pytest.mark.parametrize("function", sorted(BODIES))
def test_methods_take_the_fields_of_their_request_body(function):
    component, others = BODIES[function]
    properties = set(DOCUMENT["components"]["schemas"][component]["properties"])
    parameters = set(inspect.signature(getattr(ForkliftClient, function)).parameters) - {"self"}
    assert parameters - others == properties


LISTS = sorted(function for function, model, method, path in CALLS if function.startswith("list_"))


@pytest.mark.parametrize("function", LISTS)
def test_list_methods_take_the_filters_of_their_operation(function):
    (path,) = [path for name, _, _, path in CALLS if name == function]
    _, operation = _operation("GET", path)
    declared = {p["name"] for p in operation.get("parameters", []) if p["in"] == "query"}
    signature = inspect.signature(getattr(ForkliftClient, function))
    keywords = {n for n, p in signature.parameters.items() if p.kind is p.KEYWORD_ONLY}
    assert keywords <= declared, f"{function} sends parameters the API does not declare"
    assert declared - {"limit", "offset"} <= keywords, f"{function} misses filters of the API"


class TestTheChecker:
    """The fake's contract checks themselves refuse what they should."""

    contract = Contract()

    def test_unknown_operations_and_parameters(self):
        with pytest.raises(ContractViolation, match="is not an operation"):
            self.contract.check_request("PUT", "/api/v1/me", {}, None)
        with pytest.raises(ContractViolation, match="has no query parameters"):
            self.contract.check_request("GET", "/api/v1/jobs", {"colour": "red"}, None)

    def test_bodies(self):
        with pytest.raises(ContractViolation, match="needs a body"):
            self.contract.check_request("POST", "/api/v1/jobs", {}, None)
        with pytest.raises(ContractViolation, match="takes no body"):
            self.contract.check_request("GET", "/api/v1/me", {}, {"a": 1})
        with pytest.raises(ContractViolation, match="'schema_' is not a property"):
            self.contract.check_request("POST", "/api/v1/jobs", {}, {"kind": "run", "schema_": {}})
        with pytest.raises(ContractViolation, match="missing \\['kind'\\]"):
            self.contract.check_request("POST", "/api/v1/jobs", {}, {})
        with pytest.raises(ContractViolation, match="is not one of"):
            self.contract.check_request("POST", "/api/v1/jobs", {}, {"kind": "explode"})

    def test_answers(self):
        with pytest.raises(ContractViolation, match="does not answer 299"):
            self.contract.check_response("GET", "/api/v1/me", 299, {})
        with pytest.raises(ContractViolation, match="is not a date-time"):
            self.contract.validate({"type": "string", "format": "date-time"}, "today", "x")
        with pytest.raises(ContractViolation, match="matches none"):
            self.contract.validate({"anyOf": [{"type": "integer"}, {"type": "null"}]}, "1", "x")
        with pytest.raises(ContractViolation, match="is not 'PUT'"):
            self.contract.validate({"const": "PUT"}, "GET", "x")
        with pytest.raises(ValueError):
            self.contract.validate({"type": "string", "format": "uuid"}, "not-a-uuid", "x")


class TestTheGenerator:
    """The generator refuses what it cannot map, so a change of the API is noticed."""

    def _render(self, tmp_path, schemas):
        document = {"components": {"schemas": schemas}}
        path = tmp_path / "openapi.json"
        path.write_text(json.dumps(document))
        return GENERATOR.render(path)

    def _all(self, **override):
        schemas = {
            name: {"properties": {"a": {"type": "string"}}, "required": ["a"]}
            for name in GENERATOR.MODELS
        }
        schemas.update(override)
        return schemas

    def test_a_minimal_document(self, tmp_path):
        text = self._render(tmp_path, self._all())
        assert "class Job(Model):" in text and "    a: str" in text

    @pytest.mark.parametrize(
        "schema, message",
        [
            ({"properties": {"a": {"$ref": "#/components/schemas/Nope"}}}, "MODELS does not map"),
            ({"properties": {"a": {"enum": ["x", "y"]}}}, "name the enumeration"),
            (
                {"properties": {"a": {"anyOf": [{"type": "string"}, {"type": "integer"}]}}},
                "unions",
            ),
            ({"properties": {"a": {"type": "frobnicate"}}}, "no mapping"),
            ({"properties": {"class": {"type": "string"}}, "required": ["class"]}, "identifier"),
            ({"properties": {"a": {"type": "string"}}}, "only optional fields"),
            (
                {"properties": {"a" * 120: {"type": "string"}}, "required": ["a" * 120]},
                "longer than",
            ),
        ],
    )
    def test_what_it_refuses(self, tmp_path, schema, message):
        with pytest.raises(GENERATOR.GenerationError, match=message):
            self._render(tmp_path, self._all(JobOut=schema))

    def test_a_component_that_is_gone(self, tmp_path):
        schemas = self._all()
        del schemas["JobOut"]
        with pytest.raises(GENERATOR.GenerationError, match="not in the OpenAPI document"):
            self._render(tmp_path, schemas)


def test_the_version_is_the_packages():
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert (
        re.search(r'^version = "([^"]+)"$', pyproject, re.M).group(1)
        == forklift_client.__version__
    )
