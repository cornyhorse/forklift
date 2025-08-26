from __future__ import annotations
import csv
from typing import Iterable, Dict, Any, List, Optional
from .base import BaseInput
from ..utils.encoding import open_text_auto

_PROLOGUE_PREFIXES = ("#",)
_FOOTER_PREFIXES = ("TOTAL", "SUMMARY")

import re


def _pgsafe(name: str) -> str:
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:63]


def _dedupe(names):
    seen = {}
    out = []
    for n in names:
        base = n
        i = seen.get(base, 0)
        if i == 0:
            out.append(base)
            seen[base] = 1
        else:
            # suffix with _{i}
            new = f"{base}_{i}"
            while new in seen:
                i += 1
                new = f"{base}_{i}"
            out.append(new)
            seen[base] = i + 1
    return out


def _looks_like_header(tokens: list[str]) -> bool:
    return all(not any(ch.isdigit() for ch in t) for t in tokens)


class CSVInput(BaseInput):
    def iter_rows(self) -> Iterable[Dict[str, Any]]:
        delimiter = self.opts.get("delimiter") or ","
        enc_priority: List[str] = self.opts.get("encoding_priority") or ["utf-8-sig", "utf-8", "latin-1"]
        header_override: Optional[List[str]] = self.opts.get("header_override")
        has_header: bool = self.opts.get("has_header", True)

        fh = open_text_auto(self.source, enc_priority)
        try:
            # Skip prologue lines (#, blank)
            while True:
                pos = fh.tell()
                line = fh.readline()
                if not line:
                    return
                s = line.strip()
                if not s or s.startswith(_PROLOGUE_PREFIXES):
                    continue
                fh.seek(pos)
                break

            reader = csv.reader(fh, delimiter=delimiter, skipinitialspace=True)

            raw_header: List[str]
            if has_header:
                try:
                    file_hdr = next(reader)
                except StopIteration:
                    return
                file_hdr = [h.strip() for h in file_hdr]
                raw_header = header_override if header_override else file_hdr
            else:
                # Headerless file: do NOT consume a line. Require header_override.
                if not header_override:
                    raise ValueError("header_override required when has_header=False")
                raw_header = header_override

            # Normalize + dedupe headers to PG-safe names
            norm = [_pgsafe(h) for h in raw_header]
            fieldnames = _dedupe(norm)

            # DictReader reads from the current position (after header line if has_header=True)
            dict_reader = csv.DictReader(
                fh if has_header else fh,  # same handle; position differs
                fieldnames=fieldnames,
                delimiter=delimiter,
                skipinitialspace=True,
            )
            # IMPORTANT: never skip a row here. We already consumed the header when has_header=True,
            # and for has_header=False we intentionally did not consume anything.

            prev_tuple = None
            fns = fieldnames
            first_key = fns[0] if fns else None

            for row in dict_reader:
                # footer
                first = (row.get(first_key) or "").strip() if first_key else ""
                if any(first.startswith(pref) for pref in _FOOTER_PREFIXES):
                    continue
                # empty
                if not any((v or "").strip() for v in row.values()):
                    continue
                # simple consecutive dedupe
                tup = tuple((k, row.get(k, "")) for k in fns)
                if prev_tuple is not None and tup == prev_tuple:
                    continue
                prev_tuple = tup
                yield row
        finally:
            fh.close()
