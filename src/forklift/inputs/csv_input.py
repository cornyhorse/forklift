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


class CSVInput(BaseInput):
    def iter_rows(self) -> Iterable[Dict[str, Any]]:
        delimiter = self.opts.get("delimiter") or ","
        enc_priority: List[str] = self.opts.get("encoding_priority") or ["utf-8-sig", "utf-8", "latin-1"]
        header_override: Optional[List[str]] = self.opts.get("header_override")

        fh = open_text_auto(self.source, enc_priority)
        try:
            if header_override:
                raw = header_override
            else:
                # skip prologue
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
                try:
                    header = next(reader)
                except StopIteration:
                    return
                header = [h.strip() for h in header]
                raw = header

            # normalize + dedupe headers to PG-safe names
            norm = [_pgsafe(h) for h in raw]
            fieldnames = _dedupe(norm)
            dict_reader = csv.DictReader(
                fh,
                fieldnames=fieldnames,
                delimiter=delimiter,
                skipinitialspace=True,
            )

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
