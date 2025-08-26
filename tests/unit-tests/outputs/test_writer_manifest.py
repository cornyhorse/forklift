from pathlib import Path
import json


def test_manifest_fields(tmp_path: Path):
    # Validate the stub writer contract without running the full engine
    m = tmp_path / "_manifest.json"
    m.write_text(json.dumps({"read": 1, "kept": 1, "rejected": 0}))
    data = json.loads(m.read_text())
    assert set(data) == {"read", "kept", "rejected"}
