from __future__ import annotations

import codecs
from pathlib import Path
from typing import List, Optional, TextIO, Union

DEFAULT_ENCODING_PRIORITY = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]

# Fallback candidates when no detector library is installed (plain ``utf-8`` comes first:
# a file with a BOM is recognised separately and reported as ``utf-8-sig``).
_TRIAL_ENCODINGS = ["utf-8", "cp1252", "latin-1"]

_SAMPLE_BYTES = 64 * 1024
_VERIFY_CHUNK_BYTES = 1024 * 1024

_BOMS = [
    (codecs.BOM_UTF8, "utf-8-sig"),
    (codecs.BOM_UTF32_LE, "utf-32"),
    (codecs.BOM_UTF32_BE, "utf-32"),
    (codecs.BOM_UTF16_LE, "utf-16"),
    (codecs.BOM_UTF16_BE, "utf-16"),
]


def verify_encoding(
    path: Union[str, Path], encoding: str, chunk_size: int = _VERIFY_CHUNK_BYTES
) -> bool:
    """Return True if the whole file decodes cleanly with ``encoding``.

    The file is streamed through an incremental decoder, so memory stays bounded and a
    multi-byte sequence split across two chunks is handled correctly.

    Args:
        path: File to check
        encoding: Codec name
        chunk_size: Bytes decoded per step

    Returns:
        False if the codec is unknown or any byte sequence is invalid
    """
    try:
        decoder = codecs.getincrementaldecoder(encoding)(errors="strict")
    except LookupError:
        return False

    try:
        with open(path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                decoder.decode(chunk)
        decoder.decode(b"", final=True)
    except UnicodeDecodeError:
        return False
    return True


def _detect_with_library(sample: bytes) -> Optional[str]:
    """Ask chardet (or charset_normalizer) for a guess; None when unavailable or unsure."""
    try:
        import chardet

        return chardet.detect(sample).get("encoding") or None
    except ImportError:
        pass

    try:
        import charset_normalizer

        best = charset_normalizer.from_bytes(sample).best()
        return best.encoding if best is not None else None
    except ImportError:
        return None


def detect_encoding(
    path: Union[str, Path],
    default: str = "utf-8",
    sample_size: int = _SAMPLE_BYTES,
) -> str:
    """Detect a text file's encoding and verify it against the whole file.

    A byte order mark decides immediately. Otherwise the guess of ``chardet`` (or
    ``charset_normalizer`` when that is what is installed) is tried first, then ``utf-8``,
    ``cp1252`` and ``latin-1``. A candidate is only accepted once the *entire* file decodes
    with it: a detector that sees only the first bytes of a file can be fooled by an ASCII
    head with non-ASCII text further down. With neither library installed the same
    candidates are simply tried in order.

    Args:
        path: File to inspect
        default: Returned only if no candidate decodes the file (not reachable while
            ``latin-1`` is among the candidates)
        sample_size: Number of leading bytes handed to the detector library

    Returns:
        Name of an encoding that decodes the file without errors
    """
    with open(path, "rb") as f:
        sample = f.read(sample_size)

    for bom, name in _BOMS:
        if sample.startswith(bom):
            return name

    candidates: List[str] = []
    guess = _detect_with_library(sample)
    if guess:
        candidates.append(guess)
    candidates.extend(enc for enc in _TRIAL_ENCODINGS if enc not in candidates)

    for candidate in candidates:
        if verify_encoding(path, candidate):
            return candidate
    return default


def open_text_auto(path: str, encodings: List[str] | None = None) -> TextIO:
    """Open a text file trying multiple encodings in order.

    Each candidate is checked by decoding the whole file, so the returned handle can be read
    to the end without a ``UnicodeDecodeError``. On total failure falls back to ``utf-8``
    with ``errors="replace"`` so downstream parsing does not crash.

    :param path: Filesystem path to open.
    :param encodings: Ordered list of candidate encodings. Defaults to
        ``["utf-8-sig", "utf-8", "cp1252", "latin-1"]``.
    :return: Text IO handle opened for reading with universal newline disabled.
    """
    encs = encodings or DEFAULT_ENCODING_PRIORITY
    for enc in encs:
        if verify_encoding(path, enc):
            return open(path, "r", encoding=enc, newline="")
    return open(path, "r", encoding="utf-8", errors="replace", newline="")
