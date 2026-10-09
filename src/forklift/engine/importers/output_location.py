"""Where importers write: local directory or ``s3://`` prefix, with path-safety checks.

Output file names come from the schema file (``outputName``) or from external data (Excel sheet
names), so they are validated before being joined to the output location:

* a name must be a plain file stem (no path separators, no ``:``/control characters, not
  ``.``/``..``), and
* the resolved output file must sit directly inside the resolved output directory (this also
  catches symlinks that point elsewhere).

Either violation raises ``ValueError`` (fail closed). S3 destinations are never wrapped in
``pathlib.Path`` (``Path("s3://b/x")`` silently becomes the local ``s3:/b/x``).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional, Set, Union

from ...io import S3Path, UnifiedIOHandler, is_s3_path

logger = logging.getLogger(__name__)


def validate_output_stem(name: Any) -> str:
    """Return ``name`` if it is a plain file stem, otherwise raise ``ValueError``."""
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Output name must be a non-empty string")
    shown = repr(name if len(name) <= 80 else name[:77] + "...")
    if name in (".", ".."):
        raise ValueError(f"Invalid output name {shown}: not a file name")
    if name != name.strip():
        raise ValueError(f"Invalid output name {shown}: leading/trailing whitespace")
    if any(ch in "/\\:" or ord(ch) < 32 for ch in name):
        raise ValueError(
            f"Invalid output name {shown}: must be a plain file name without path separators"
        )
    return name


def unique_stem(stem: str, used: Set[str]) -> str:
    """Return ``stem`` or ``stem_2``, ``stem_3``... so that it is not in ``used`` (case-folded).

    The chosen name is added to ``used``. Deterministic for a given order of calls.
    """
    candidate, counter = stem, 1
    while candidate.casefold() in used:
        counter += 1
        candidate = f"{stem}_{counter}"
    used.add(candidate.casefold())
    return candidate


class OutputLocation:
    """A local output directory or an S3 prefix that importers write files into."""

    def __init__(self, output_path: Union[str, Path]):
        text = str(output_path)
        if text.startswith("s3:/") and not text.startswith("s3://"):
            raise ValueError(
                "Output path looks like an S3 URI that was collapsed by pathlib; "
                "pass 's3://bucket/prefix' as a str"
            )
        self.is_s3 = is_s3_path(text)
        if self.is_s3:
            S3Path(text)  # validates the bucket
            self.base: Union[Path, str] = text.rstrip("/")
        else:
            self.base = Path(output_path)

    def prepare(self) -> None:
        """Create the local output directory (nothing to do for S3)."""
        if not self.is_s3:
            self.base.mkdir(parents=True, exist_ok=True)

    def target(self, stem: str, suffix: str = ".parquet") -> Union[Path, str]:
        """Path (local) or URI (S3) of ``<stem><suffix>`` inside this location.

        Raises:
            ValueError: If ``stem`` is not a plain file name or the file would not be inside
                the output directory.
        """
        validate_output_stem(stem)
        if self.is_s3:
            return f"{self.base}/{stem}{suffix}"
        candidate = self.base / f"{stem}{suffix}"
        if candidate.resolve().parent != self.base.resolve():
            raise ValueError(f"Output file for {stem!r} would be outside the output directory")
        return candidate

    def write_text(self, stem: str, suffix: str, text: str, s3_client: Any = None) -> str:
        """Write ``text`` to ``<stem><suffix>`` (S3 or local) and return its path/URI."""
        target = self.target(stem, suffix)
        if self.is_s3:
            with UnifiedIOHandler(s3_client).open_for_write(target, encoding="utf-8") as f:
                f.write(text)
        else:
            with open(target, "w", encoding="utf-8") as f:
                f.write(text)
        return str(target)


def discard_partial_output(writer: Optional[Any], target: Union[Path, str]) -> None:
    """Abandon an unfinished parquet writer and remove what it left behind.

    Uses the writer's ``abort()`` when it has one (S3 writers discard the pending upload).
    Otherwise the underlying file writer is closed and the partial file deleted; an S3 writer
    without ``abort()`` is *not* closed through its public ``close()`` because that would
    upload the truncated file.
    """
    if writer is not None:
        abort = getattr(writer, "abort", None)
        try:
            if callable(abort):
                abort()
            else:
                getattr(writer, "_writer", writer).close()
                temp_path = getattr(writer, "_temp_path", None)
                if temp_path is not None:
                    Path(temp_path).unlink(missing_ok=True)
        except Exception as exc:  # best effort: never mask the original failure
            logger.warning(
                "Could not cleanly abort writer for %s (%s)", target, type(exc).__name__
            )
    if not is_s3_path(str(target)):
        Path(target).unlink(missing_ok=True)
