"""The gateway never imports the engine or pyarrow, and never reads object contents (ADR 0004).

Two checks each: what the source says (every import and every call on a store client in
src/forklift_web) and what a running process does (a fresh interpreter imports every module of
the package and loads both URL configurations, then lists what got imported).
"""

from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[1] / "src" / "forklift_web"
FORBIDDEN_MODULES = ("pyarrow", "forklift")
# S3 operations that read or download object contents
FORBIDDEN_CALLS = {
    "get_object",
    "download_file",
    "download_fileobj",
    "select_object_content",
    "get_object_torrent",
    "restore_object",
}
FORBIDDEN_PRESIGNS = {"select_object_content"}


def _sources():
    return sorted(PACKAGE.rglob("*.py"))


def _forbidden(module: str) -> bool:
    return any(module == name or module.startswith(name + ".") for name in FORBIDDEN_MODULES)


def test_no_source_file_imports_the_engine_or_pyarrow():
    offenders = []
    for path in _sources():
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                names = [node.module or ""]
            else:
                continue
            offenders += [f"{path.name}: {name}" for name in names if _forbidden(name)]
    assert offenders == []


def test_no_source_file_reads_objects():
    offenders = []
    for path in _sources():
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if not isinstance(node, ast.Call):
                continue
            if isinstance(node.func, ast.Attribute) and node.func.attr in FORBIDDEN_CALLS:
                offenders.append(f"{path.name}:{node.lineno} calls {node.func.attr}")
            if isinstance(node.func, ast.Attribute) and node.func.attr == "generate_presigned_url":
                operation = node.args[0].value if node.args else None
                if operation in FORBIDDEN_PRESIGNS:
                    offenders.append(f"{path.name}:{node.lineno} presigns {operation}")
    assert offenders == []


_PROBE = """
import importlib, json, pkgutil, sys
import django
django.setup()
import forklift_web
for module in pkgutil.walk_packages(forklift_web.__path__, "forklift_web."):
    if module.name.endswith("__main__"):
        continue
    importlib.import_module(module.name)
from django.urls import get_resolver
get_resolver("forklift_web.urls").url_patterns
get_resolver("forklift_web.urls_internal").url_patterns
from forklift_web.api import api
from forklift_web.internal import internal_api
api.get_openapi_schema(); internal_api.get_openapi_schema(path_prefix="/internal/v1/")
print(json.dumps(sorted(sys.modules)))
"""


def test_a_running_gateway_has_not_imported_the_engine_or_pyarrow():
    web = PACKAGE.parents[1]
    env = {
        **os.environ,
        "DJANGO_SETTINGS_MODULE": "django_settings",
        "PYTHONPATH": os.pathsep.join([str(web / "src"), str(web / "tests")]),
    }
    completed = subprocess.run(
        [sys.executable, "-c", _PROBE], env=env, capture_output=True, text=True, check=True
    )
    modules = json.loads(completed.stdout.splitlines()[-1])
    assert "forklift_web.services.queue" in modules  # the probe really imported the package
    assert [name for name in modules if _forbidden(name)] == []
