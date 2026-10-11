"""The schema documentation pages the server offers as resources.

A wheel (and so the image) carries ``docs/schemas/*.md`` inside the package
(``forklift_mcp_server/docs/schemas``, see ``[tool.hatch.build.targets.wheel.force-include]`` in
``pyproject.toml``); an editable install or a checkout reads ``docs/schemas`` of the repository.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

#: Where a wheel installs the pages
PACKAGED_DOCS = Path(__file__).resolve().parent / "docs" / "schemas"
#: Where they are in a checkout (services/mcp/src/forklift_mcp_server/docs.py -> the root)
CHECKOUT_DOCS = Path(__file__).resolve().parents[4] / "docs" / "schemas"

URI_PREFIX = "forklift://docs/schemas/"


@dataclass(frozen=True)
class SchemaPage:
    """One page: its file, its resource URI and the title from its first heading."""

    name: str
    path: Path
    title: str

    @property
    def uri(self) -> str:
        return URI_PREFIX + self.name


def docs_directory() -> Optional[Path]:
    """The directory with the pages, or None when neither location has them."""
    for candidate in (PACKAGED_DOCS, CHECKOUT_DOCS):
        if candidate.is_dir():
            return candidate
    return None


def schema_pages(directory: Optional[Path] = None) -> List[SchemaPage]:
    """Every ``*.md`` page, sorted by file name (README.md first)."""
    directory = docs_directory() if directory is None else directory
    if directory is None:
        return []
    pages = []
    for path in sorted(directory.glob("*.md"), key=lambda p: (p.name != "README.md", p.name)):
        if path.is_file():
            pages.append(SchemaPage(path.name, path, _title(path)))
    return pages


def _title(path: Path) -> str:
    """The text of the first ``# `` heading (the file name when there is none)."""
    with open(path, encoding="utf-8-sig", errors="replace") as handle:
        for number, line in enumerate(handle):
            if line.startswith("# "):
                return line[2:].strip()
            if number >= 20:
                break
    return path.stem
