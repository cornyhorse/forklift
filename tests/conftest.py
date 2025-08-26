from __future__ import annotations
import json
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "test-files"

@pytest.fixture(scope="session")
def data_dir() -> Path:
    assert DATA_DIR.exists(), f"Missing test data dir: {DATA_DIR}"
    return DATA_DIR

@pytest.fixture
def tmp_out(tmp_path: Path) -> Path:
    d = tmp_path / "out"
    d.mkdir(parents=True, exist_ok=True)
    return d


def load_schema(p: Path) -> dict:
    return json.loads(p.read_text(encoding="utf-8"))


def get_manifest(dest: Path) -> dict:
    m = dest / "_manifest.json"
    return json.loads(m.read_text(encoding="utf-8"))


def get_quarantine_lines(dest: Path) -> list[str]:
    q = dest / "_quarantine" / "rows.jsonl"
    if not q.exists():
        return []
    return q.read_text(encoding="utf-8").splitlines()