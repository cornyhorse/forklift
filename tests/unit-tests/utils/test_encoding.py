from pathlib import Path
from forklift.utils.encoding import open_text_auto


def test_open_text_auto_fallback(tmp_path: Path):
    p = tmp_path / "f.txt"
    p.write_text("hello", encoding="utf-8")
    # Force first attempt to fail (unknown codec), then rely on final fallback
    fh = open_text_auto(str(p), encodings=["x-bogus-only"])
    try:
        assert fh.read() == "hello"
    finally:
        fh.close()
