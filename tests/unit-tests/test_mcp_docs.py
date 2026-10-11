"""forklift-mcp: finding the schema documentation pages (wheel data or the checkout)."""

from __future__ import annotations

from pathlib import Path

from forklift.mcp import docs
from forklift.mcp.docs import SchemaPage, docs_directory, schema_pages

REPO_DOCS = Path(__file__).resolve().parents[2] / "docs" / "schemas"


def test_a_checkout_reads_docs_schemas_of_the_repository():
    # An editable install has no copy inside the package: the checkout's pages are served
    assert not docs.PACKAGED_DOCS.is_dir()
    assert docs_directory() == REPO_DOCS
    pages = schema_pages()
    assert [page.name for page in pages] == ["README.md"] + sorted(
        path.name for path in REPO_DOCS.glob("*.md") if path.name != "README.md"
    )
    by_name = {page.name: page for page in pages}
    assert by_name["X_VALIDATION_DOCUMENTATION.md"].title == "x-validation Documentation"
    assert by_name["README.md"].uri == "forklift://docs/schemas/README.md"


def test_the_packaged_copy_wins(tmp_path, monkeypatch):
    packaged = tmp_path / "docs" / "schemas"
    packaged.mkdir(parents=True)
    (packaged / "B.md").write_text("# Bee\n")
    (packaged / "README.md").write_text("﻿# Overview\n")
    (packaged / "A.md").write_text("no heading here\n")
    (packaged / "Z.md").write_text("\n" * 25 + "# Too late\n")
    (packaged / "notes.txt").write_text("# not a page\n")
    (packaged / "dir.md").mkdir()
    monkeypatch.setattr(docs, "PACKAGED_DOCS", packaged)
    assert docs_directory() == packaged
    assert schema_pages() == [
        SchemaPage("README.md", packaged / "README.md", "Overview"),
        SchemaPage("A.md", packaged / "A.md", "A"),
        SchemaPage("B.md", packaged / "B.md", "Bee"),
        SchemaPage("Z.md", packaged / "Z.md", "Z"),
    ]


def test_no_pages_anywhere(tmp_path, monkeypatch):
    monkeypatch.setattr(docs, "PACKAGED_DOCS", tmp_path / "a")
    monkeypatch.setattr(docs, "CHECKOUT_DOCS", tmp_path / "b")
    assert docs_directory() is None
    assert schema_pages() == []


def test_an_explicit_directory(tmp_path):
    (tmp_path / "X.md").write_text("# X page\n")
    assert schema_pages(tmp_path) == [SchemaPage("X.md", tmp_path / "X.md", "X page")]
