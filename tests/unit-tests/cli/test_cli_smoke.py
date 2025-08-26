import pytest
from forklift import cli
import sys


def test_cli_help(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["forklift", "--help"])
    with pytest.raises(SystemExit) as e:
        cli.main()
    assert e.value.code == 0
