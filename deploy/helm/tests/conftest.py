"""Rendering the chart for the tests: `helm template` with each values file in ci/.

helm comes from $FORKLIFT_HELM (a command line, e.g. "helm" or "docker run ... alpine/helm"),
else the `helm` on PATH, else the pinned alpine/helm image through Docker.
"""

from __future__ import annotations

import base64
import os
import shlex
import shutil
import subprocess
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pytest
import yaml

CHART = Path(__file__).resolve().parents[1] / "forklift"
REPOSITORY = CHART.parents[2]
CI_VALUES = sorted((CHART / "ci").glob("*-values.yaml"))
HELM_IMAGE = (
    "alpine/helm:4.3.0@sha256:a6cf54599ccb99d90cf0712b30f03fdb3cab062e6b94e0418cc4db7e8a1464b2"
)
RELEASE = "fk"
NAMESPACE = "forklift"


def helm_command() -> List[str]:
    configured = os.environ.get("FORKLIFT_HELM")
    if configured:
        return shlex.split(configured)
    if shutil.which("helm"):
        return ["helm"]
    return [
        "docker", "run", "--rm", "-i", "-u", f"{os.getuid()}:{os.getgid()}",
        "-v", f"{REPOSITORY}:{REPOSITORY}:ro", "-e", "HOME=/tmp", HELM_IMAGE,
    ]  # fmt: skip


class Rendered:
    """The documents of one `helm template`, with lookups by kind and name."""

    def __init__(self, text: str):
        self.text = text
        self.documents = [doc for doc in yaml.safe_load_all(text) if doc]

    def kind(self, kind: str) -> List[dict]:
        return [doc for doc in self.documents if doc["kind"] == kind]

    def get(self, kind: str, name: str) -> dict:
        found = [doc for doc in self.kind(kind) if doc["metadata"]["name"] == name]
        assert found, f"no {kind} {name}; have {[d['metadata']['name'] for d in self.kind(kind)]}"
        return found[0]

    def workloads(self) -> List[dict]:
        return [d for d in self.documents if d["kind"] in ("Deployment", "StatefulSet", "Job")]

    def pod_specs(self) -> Iterable[tuple]:
        for workload in self.workloads():
            template = workload["spec"]["template"]
            yield workload, template["metadata"].get("labels", {}), template["spec"]

    def secret_values(self) -> List[str]:
        values = []
        for secret in self.kind("Secret"):
            for value in (secret.get("data") or {}).values():
                values.append(base64.b64decode(value).decode())
            values.extend((secret.get("stringData") or {}).values())
        return values


def render(values: Optional[Path] = None, *sets: str, extra: Optional[Dict] = None) -> Rendered:
    """`helm template` of the chart; raises RenderError with helm's message when it fails."""
    command = helm_command() + ["template", RELEASE, str(CHART), "--namespace", NAMESPACE]
    if values is not None:
        command += ["--values", str(values)]
    for item in sets:
        command += ["--set", item]
    stdin = None
    if extra is not None:
        command += ["--values", "-"]
        stdin = yaml.safe_dump(extra)
    done = subprocess.run(command, input=stdin, capture_output=True, text=True, timeout=120)
    if done.returncode != 0:
        raise RenderError(done.stderr.strip())
    return Rendered(done.stdout)


class RenderError(Exception):
    pass


_cache: Dict[str, Rendered] = {}


def rendered_ci(path: Path) -> Rendered:
    if path.name not in _cache:
        _cache[path.name] = render(path)
    return _cache[path.name]


@pytest.fixture(params=CI_VALUES, ids=[p.name for p in CI_VALUES])
def ci(request) -> Rendered:
    """The chart rendered with each ci/*-values.yaml in turn."""
    return rendered_ci(request.param)


@pytest.fixture
def ci_file():
    """``ci_file(name)``: the chart rendered with ci/<name>-values.yaml."""
    return lambda name: rendered_ci(CHART / "ci" / f"{name}-values.yaml")
