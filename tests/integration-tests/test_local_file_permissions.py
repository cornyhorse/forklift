"""Imports on a local file system where the user lacks read or write permission.

A file the process may not read, or a directory it may not write, must stop the import with
the operating system's ``PermissionError`` and leave nothing half-written behind. The tests
skip when run as root, which bypasses file permissions (CI runs them as an ordinary user).
"""

from __future__ import annotations

import json
import os
import stat

import openpyxl
import pytest

from forklift import import_csv, import_excel

pytestmark = pytest.mark.skipif(
    not hasattr(os, "geteuid") or os.geteuid() == 0,
    reason="needs a non-root POSIX user (root ignores file permissions)",
)

PEOPLE = "id,name\n1,Ana\n2,Bo\n"


def _raised_permission_error(error: BaseException) -> bool:
    while error is not None:
        if isinstance(error, PermissionError):
            return True
        error = error.__cause__ or error.__context__
    return False


@pytest.fixture
def restore_permissions():
    """chmod paths for a test and give them back their permissions afterwards."""
    changed = []

    def chmod(path, mode):
        changed.append((path, stat.S_IMODE(path.stat().st_mode)))
        path.chmod(mode)

    yield chmod
    for path, mode in reversed(changed):
        path.chmod(mode)


class TestUnreadableInputs:
    def test_unreadable_csv_stops_the_import_with_permission_error(
        self, tmp_path, restore_permissions
    ):
        source = tmp_path / "people.csv"
        source.write_text(PEOPLE)
        restore_permissions(source, 0o000)

        with pytest.raises(Exception) as raised:
            import_csv(source, tmp_path / "out")

        assert _raised_permission_error(raised.value)
        assert not (tmp_path / "out" / "data.parquet").exists()

    def test_unreadable_schema_file_stops_the_import(self, tmp_path, restore_permissions):
        source = tmp_path / "people.csv"
        source.write_text(PEOPLE)
        schema = tmp_path / "schema.json"
        schema.write_text(json.dumps({"properties": {"id": {"type": "integer"}}}))
        restore_permissions(schema, 0o000)

        with pytest.raises(Exception) as raised:
            import_csv(source, tmp_path / "out", schema_file=schema)

        assert _raised_permission_error(raised.value)
        assert not (tmp_path / "out" / "data.parquet").exists()

    def test_unreadable_workbook_stops_the_import(self, tmp_path, restore_permissions):
        workbook = openpyxl.Workbook()
        workbook.active.append(["id"])
        workbook.active.append([1])
        source = tmp_path / "book.xlsx"
        workbook.save(source)
        restore_permissions(source, 0o000)

        with pytest.raises(Exception) as raised:
            import_excel(source, tmp_path / "out")

        assert _raised_permission_error(raised.value)
        assert not any((tmp_path / "out").glob("*.parquet"))


class TestUnwritableOutputs:
    def test_read_only_output_directory_stops_the_import_and_keeps_its_files(
        self, tmp_path, restore_permissions
    ):
        source = tmp_path / "people.csv"
        source.write_text(PEOPLE)
        out = tmp_path / "out"
        out.mkdir()
        (out / "keep.txt").write_text("from an earlier run")
        restore_permissions(out, 0o555)

        with pytest.raises(Exception) as raised:
            import_csv(source, out)

        assert _raised_permission_error(raised.value)
        assert sorted(p.name for p in out.iterdir()) == ["keep.txt"]
        assert (out / "keep.txt").read_text() == "from an earlier run"

    def test_output_directory_that_cannot_be_created_stops_the_import(
        self, tmp_path, restore_permissions
    ):
        source = tmp_path / "people.csv"
        source.write_text(PEOPLE)
        parent = tmp_path / "locked"
        parent.mkdir()
        restore_permissions(parent, 0o555)

        with pytest.raises(Exception) as raised:
            import_csv(source, parent / "out")

        assert _raised_permission_error(raised.value)
        assert not (parent / "out").exists()

    def test_input_is_never_modified_by_a_failed_import(self, tmp_path, restore_permissions):
        source = tmp_path / "people.csv"
        source.write_text(PEOPLE)
        restore_permissions(source, 0o444)
        out = tmp_path / "out"
        out.mkdir()
        restore_permissions(out, 0o555)

        with pytest.raises(Exception):
            import_csv(source, out)

        assert source.read_text() == PEOPLE
