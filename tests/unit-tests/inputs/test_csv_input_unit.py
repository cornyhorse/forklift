from __future__ import annotations
from pathlib import Path
import io
import os

import pytest

from forklift.inputs.csv_input import CSVInput


def write(p: Path, text: str, encoding: str = "utf-8") -> None:
    p.write_text(text, encoding=encoding, newline="\n")


def rows_from(path: Path, **opts):
    inp = CSVInput(str(path), **opts)
    return list(inp.iter_rows())


def test_prologue_header_detection_footer_and_normalization(tmp_path: Path):
    f = tmp_path / "customers.csv"
    write(
        f,
        "# vendor: acme\n# exported: 2024-01-01\n"
        "ID, Name Weird, Amount USD\n"
        "1, Amy, 10.00\n"
        "2, Ben, 20.50\n"
        "TOTAL, 2, 30.50\n"  # should be skipped
    )
    rs = rows_from(f, delimiter=",", encoding_priority=["utf-8"])
    assert len(rs) == 2
    # keys are normalized to PG-safe + deduped
    assert list(rs[0].keys()) == ["id", "name_weird", "amount_usd"]
    assert rs[0]["id"] == "1"
    assert rs[1]["name_weird"] == "Ben"


def test_header_override_headerless_file(tmp_path: Path):
    f = tmp_path / "headerless.csv"
    write(
        f,
        "1\tAmy\n"
        "2\tBen\n"
        "2\tBen\n"  # consecutive duplicate → skipped
    )
    rs = rows_from(
        f,
        delimiter="\t",
        encoding_priority=["utf-8"],
        header_override=["id", "name"],
        has_header=False
    )
    # third row is a consecutive duplicate of the second, so we only keep 2
    assert len(rs) == 2
    assert list(rs[0].keys()) == ["id", "name"]
    assert rs[0]["name"] == "Amy"
    assert rs[1]["id"] == "2"


def test_utf8_sig_bom_and_empty_row_skip(tmp_path: Path):
    f = tmp_path / "bom.csv"
    write(
        f,
        "A,B\n"
        "x,y\n"
        "\n"  # empty → skipped
        "p,q\n",
        encoding="utf-8-sig",
    )
    rs = rows_from(f, delimiter=",", encoding_priority=["utf-8-sig", "utf-8"])
    assert len(rs) == 2
    assert rs[0]["a"] == "x" and rs[0]["b"] == "y"
    assert rs[1]["a"] == "p" and rs[1]["b"] == "q"


def test_summary_footer_is_skipped(tmp_path: Path):
    f = tmp_path / "with_summary.csv"
    write(
        f,
        "col1,col2\n"
        "a,1\n"
        "b,2\n"
        "SUMMARY,3\n"
    )
    rs = rows_from(f, delimiter=",", encoding_priority=["utf-8"])
    assert len(rs) == 2
    assert rs[1]["col1"] == "b"


def test_tab_delimiter_with_normalization_and_dedupe_headers(tmp_path: Path):
    f = tmp_path / "tab.tsv"
    # Two identical header names to trigger _dedupe → col and col_1
    write(
        f,
        "Col\tCol\tOther Val\n"
        "a\tb\tc\n"
    )
    rs = rows_from(f, delimiter="\t", encoding_priority=["utf-8"])
    assert len(rs) == 1
    assert list(rs[0].keys()) == ["col", "col_1", "other_val"]
    assert rs[0]["col_1"] == "b"


def test_empty_file_yields_no_rows(tmp_path: Path):
    f = tmp_path / "empty.csv"
    write(f, "")
    rs = rows_from(f, delimiter=",", encoding_priority=["utf-8"])
    assert rs == []
