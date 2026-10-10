"""detect_encoding returns ``default`` only when no candidate encoding decodes the file."""

from __future__ import annotations

from forklift.utils import detect_encoding as detect_module
from forklift.utils.detect_encoding import detect_encoding


class TestDefaultEncoding:
    def test_default_is_returned_when_every_candidate_fails_verification(
        self, tmp_path, monkeypatch
    ):
        path = tmp_path / "data.txt"
        path.write_bytes(b"plain ascii text")
        tried = []

        def reject(file_path, encoding):
            tried.append(encoding)
            return False

        monkeypatch.setattr(detect_module, "_detect_with_library", lambda sample: None)
        monkeypatch.setattr(detect_module, "verify_encoding", reject)

        assert detect_encoding(path, default="ascii") == "ascii"
        assert tried == ["utf-8", "cp1252", "latin-1"]

    def test_latin1_candidate_means_the_default_is_not_used_for_real_files(self, tmp_path):
        path = tmp_path / "bytes.bin"
        path.write_bytes(bytes(range(256)))
        encoding = detect_encoding(path, default="ascii")
        assert encoding != "ascii"
        path.read_bytes().decode(encoding)  # the chosen encoding decodes every byte
