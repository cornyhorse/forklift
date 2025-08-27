import pytest
from forklift.engine.engine import Engine, DuplicateRow

class DummyInput:
    def __init__(self, source, header_override=None, **opts):
        self.rows = opts.get('rows', [])
    def iter_rows(self):
        for row in self.rows:
            yield row

class DummyOutput:
    def __init__(self, dest, schema=None, **opts):
        self.written = []
        self.quarantined = []
    def open(self):
        pass
    def write(self, row):
        self.written.append(row)
    def quarantine(self, rr):
        self.quarantined.append(rr)
    def close(self):
        pass

class DummyPreprocessor:
    def apply(self, row):
        if row.get('fail'): raise ValueError('fail')
        row['processed'] = True
        return row

@pytest.fixture
def engine(monkeypatch):
    monkeypatch.setattr('forklift.engine.engine.get_input_cls', lambda kind: DummyInput)
    monkeypatch.setattr('forklift.engine.engine.get_output_cls', lambda kind: DummyOutput)
    monkeypatch.setattr('forklift.engine.engine.get_preprocessors', lambda pre, schema=None: [DummyPreprocessor()])
    schema = {'required': ['id'], 'x-csv': {'dedupe': {'keys': ['id']}}}
    return Engine('csv', 'parquet', schema=schema)

def test_deduplication(engine):
    rows = [{'id': 1}, {'id': 1}, {'id': 2}]
    engine.input_opts['rows'] = rows
    out = DummyOutput('dest')
    engine.Output = lambda dest, schema=None, **opts: out
    engine.Input = lambda source, header_override=None, **opts: DummyInput(source, rows=rows)
    engine.run('source', 'dest')
    assert any('__forklift_skip__' in r for r in out.written)

def test_error_handling(engine):
    rows = [{'id': None}, {'id': 2, 'fail': True}]
    engine.input_opts['rows'] = rows
    out = DummyOutput('dest')
    engine.Output = lambda dest, schema=None, **opts: out
    engine.Input = lambda source, header_override=None, **opts: DummyInput(source, rows=rows)
    engine.run('source', 'dest')
    assert len(out.quarantined) == 2

def test_run_success(engine):
    rows = [{'id': 1}, {'id': 2}]
    engine.input_opts['rows'] = rows
    out = DummyOutput('dest')
    engine.Output = lambda dest, schema=None, **opts: out
    engine.Input = lambda source, header_override=None, **opts: DummyInput(source, rows=rows)
    engine.run('source', 'dest')
    assert len(out.written) == 2

