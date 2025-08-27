import sys
import pytest
import json
import tempfile
import os
from forklift.cli import main

class DummyEngine:
    def __init__(self, input_kind, output_kind, schema=None, preprocessors=None, **opts):
        self.called = False
    def run(self, source, dest):
        self.called = True

@pytest.fixture(autouse=True)
def patch_engine(monkeypatch):
    monkeypatch.setattr('forklift.cli.Engine', DummyEngine)

@pytest.mark.parametrize('args', [
    ['ingest', 'source.csv', '--dest', 'out.parquet', '--input-kind', 'csv'],
    ['ingest', 'source.csv', '--dest', 'out.parquet', '--input-kind', 'fwf'],
    ['ingest', 'source.xlsx', '--dest', 'out.parquet', '--input-kind', 'excel'],
])
def test_cli_ingest(monkeypatch, args):
    monkeypatch.setattr(sys, 'argv', ['forklift'] + args)
    main()

@pytest.mark.parametrize('args', [
    [],
    ['ingest'],
    ['ingest', 'source.csv'],
    ['ingest', 'source.csv', '--dest', 'out.parquet'],
])
def test_cli_missing_args(monkeypatch, args):
    monkeypatch.setattr(sys, 'argv', ['forklift'] + args)
    with pytest.raises(SystemExit):
        main()

def test_cli_help(monkeypatch, capsys):
    monkeypatch.setattr(sys, 'argv', ['forklift', '--help'])
    with pytest.raises(SystemExit):
        main()
    out = capsys.readouterr().out
    assert 'usage:' in out
    assert 'ingest' in out

def test_cli_invalid_schema_file(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['forklift', 'ingest', 'source.csv', '--dest', 'out.parquet', '--input-kind', 'csv', '--schema', 'nonexistent.json'])
    with pytest.raises(FileNotFoundError):
        main()

def test_cli_invalid_schema_json(monkeypatch):
    with tempfile.NamedTemporaryFile('w', delete=False) as tf:
        tf.write('{invalid json}')
        tf.flush()
        monkeypatch.setattr(sys, 'argv', ['forklift', 'ingest', 'source.csv', '--dest', 'out.parquet', '--input-kind', 'csv', '--schema', tf.name])
        with pytest.raises(json.JSONDecodeError):
            main()
    os.unlink(tf.name)

def test_cli_invalid_fwf_spec_file(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['forklift', 'ingest', 'source.csv', '--dest', 'out.parquet', '--input-kind', 'fwf', '--fwf-spec', 'nonexistent.json'])
    with pytest.raises(FileNotFoundError):
        main()

def test_cli_invalid_fwf_spec_json(monkeypatch):
    with tempfile.NamedTemporaryFile('w', delete=False) as tf:
        tf.write('{invalid json}')
        tf.flush()
        monkeypatch.setattr(sys, 'argv', ['forklift', 'ingest', 'source.csv', '--dest', 'out.parquet', '--input-kind', 'fwf', '--fwf-spec', tf.name])
        with pytest.raises(json.JSONDecodeError):
            main()
    os.unlink(tf.name)
