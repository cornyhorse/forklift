"""SqlConnectionManager as a context manager, with pyodbc replaced by a fake module."""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock, patch

import pytest

from forklift.inputs.config import SqlInputConfig
from forklift.inputs.sql import SqlConnectionManager


@pytest.fixture
def fake_pyodbc():
    module = types.ModuleType("pyodbc")
    module.connect = MagicMock(name="connect", return_value=MagicMock(name="connection"))
    with patch.dict(sys.modules, {"pyodbc": module}):
        yield module


class TestConnectionLifecycle:
    def test_new_manager_is_not_connected(self):
        manager = SqlConnectionManager(SqlInputConfig(connection_string="DSN=x"))
        assert manager.is_connected() is False

    def test_context_manager_connects_on_entry_and_closes_on_exit(self, fake_pyodbc):
        config = SqlInputConfig(connection_string="DSN=warehouse", query_timeout=12)

        with SqlConnectionManager(config) as manager:
            connection = fake_pyodbc.connect.return_value
            assert manager.is_connected() is True
            assert manager.get_connection() is connection
            assert connection.timeout == 12

        fake_pyodbc.connect.assert_called_once_with("DSN=warehouse", timeout=30, readonly=True)
        connection.close.assert_called_once_with()
        assert manager.is_connected() is False

    def test_context_manager_disconnects_when_the_body_raises(self, fake_pyodbc):
        config = SqlInputConfig(connection_string="DSN=warehouse")

        with pytest.raises(KeyError):
            with SqlConnectionManager(config) as manager:
                raise KeyError("boom")

        fake_pyodbc.connect.return_value.close.assert_called_once_with()
        assert manager.is_connected() is False
