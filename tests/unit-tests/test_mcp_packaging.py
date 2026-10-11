"""forklift-mcp packaging: the ``mcp`` extra, the console script and the documentation pages.

A wheel must carry ``docs/schemas/*.md`` where :mod:`forklift.mcp.docs` looks for them; building
one and installing it into a clean environment is checked by hand (see docs/platform/mcp.md).
"""

from __future__ import annotations

from pathlib import Path

import pytest

import forklift
from forklift.mcp import docs, main

REPO_ROOT = Path(__file__).resolve().parents[2]
REQUIREMENT = "mcp>=2.3,<3; python_version >= '3.10'"


@pytest.fixture(scope="module")
def pyproject():
    tomllib = pytest.importorskip("tomllib")
    with open(REPO_ROOT / "pyproject.toml", "rb") as handle:
        return tomllib.load(handle)


def test_the_mcp_extra_and_all(pyproject):
    extras = pyproject["project"]["optional-dependencies"]
    assert extras["mcp"] == [REQUIREMENT]
    assert REQUIREMENT in extras["all"]


def test_the_console_script(pyproject):
    assert pyproject["project"]["scripts"]["forklift-mcp"] == "forklift.mcp:main"
    assert callable(main)


def test_the_wheel_carries_the_schema_pages_where_the_server_looks(pyproject):
    wheel = pyproject["tool"]["hatch"]["build"]["targets"]["wheel"]
    assert wheel["force-include"] == {"docs/schemas": "forklift/mcp/docs/schemas"}
    package = Path(forklift.__file__).resolve().parent
    assert docs.PACKAGED_DOCS == package / "mcp" / "docs" / "schemas"
    assert docs.CHECKOUT_DOCS == REPO_ROOT / "docs" / "schemas"
    # A wheel built from the sdist needs the pages in the sdist too
    assert "/docs" in pyproject["tool"]["hatch"]["build"]["targets"]["sdist"]["include"]
    assert sorted(REPO_ROOT.joinpath("docs", "schemas").glob("*.md"))
