"""SqlSchemaManager against a fake ODBC catalog: table specs, column filtering and quoting."""

from __future__ import annotations

import sys
import types
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from forklift.inputs.config import SqlInputConfig
from forklift.inputs.sql import SqlConnectionManager, SqlSchemaManager

TABLES = [("dbo", "orders"), ("sales", "items"), ("archive", "items")]


def _column(schema, table, name, type_name):
    return SimpleNamespace(
        table_schem=schema,
        table_name=table,
        column_name=name,
        type_name=type_name,
        column_size=None,
        decimal_digits=None,
        nullable=True,
    )


COLUMNS = [
    _column("sales", "items", "id", "INTEGER"),
    _column("archive", "items", "legacy_code", "VARCHAR"),  # same name, other schema
    _column("sales", "items", "name", "VARCHAR"),
]


@pytest.fixture
def connection():
    cursor = MagicMock(name="cursor")
    cursor.tables.side_effect = lambda **kwargs: [
        SimpleNamespace(table_schem=schema, table_name=table, table_type="TABLE")
        for schema, table in TABLES
        if kwargs.get("table") in (None, table)
    ]
    cursor.columns.side_effect = lambda **kwargs: list(COLUMNS)
    connection = MagicMock(name="connection")
    connection.cursor.return_value = cursor
    return connection


@pytest.fixture
def manager(connection):
    config = SqlInputConfig(connection_string="DSN=x")
    connection_manager = SqlConnectionManager(config)
    connection_manager.connection = connection
    return SqlSchemaManager(config, connection_manager)


@pytest.fixture
def fake_pyodbc():
    module = types.ModuleType("pyodbc")
    module.SQL_IDENTIFIER_QUOTE_CHAR = 29
    with patch.dict(sys.modules, {"pyodbc": module}):
        yield module


class TestSpecifiedTables:
    def test_specifications_resolve_to_catalog_entries_and_unknown_ones_are_skipped(self, manager):
        result = manager.get_specified_tables(["orders", "sales.items", "missing"])
        assert result == [("dbo", "orders"), ("sales", "items")]

    def test_bare_name_found_in_two_schemas_is_ambiguous(self, manager):
        with pytest.raises(ValueError, match="found in schemas archive, sales"):
            manager.get_specified_tables(["items"])


class TestCatalogColumns:
    def test_columns_of_a_same_named_table_in_another_schema_are_ignored(
        self, manager, connection
    ):
        schema = manager.get_table_schema("sales", "items")

        assert schema.names == ["id", "name"]
        assert schema.field("id").type == pa.int32()
        connection.cursor.return_value.columns.assert_called_once_with(
            table="items", schema="sales"
        )


class TestIdentifierQuoting:
    def test_driver_quote_character_is_used(self, manager, connection, fake_pyodbc):
        connection.getinfo.return_value = "`"
        assert manager.qualified_table_name("sales", "it`ems") == "`sales`.`it``ems`"
        connection.getinfo.assert_called_with(fake_pyodbc.SQL_IDENTIFIER_QUOTE_CHAR)

    def test_driver_that_cannot_report_a_quote_character_gets_double_quotes(
        self, manager, connection, fake_pyodbc
    ):
        connection.getinfo.side_effect = RuntimeError("SQLGetInfo not supported")
        assert manager.qualified_table_name("sales", 'it"ems') == '"sales"."it""ems"'
