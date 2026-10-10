"""Differences between two schema versions: a line diff of the documents and a summary of the
columns (``properties``) and required columns that were added, removed or changed."""

from __future__ import annotations

import difflib
import json
from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class Line:
    kind: str  # "same", "added", "removed" or "gap" (unchanged lines left out)
    old: Optional[int] = None  # line number in the older document
    new: Optional[int] = None  # line number in the newer document
    text: str = ""

    @property
    def sign(self) -> str:
        return {"added": "+", "removed": "-", "gap": "…"}.get(self.kind, " ")


@dataclass
class Summary:
    added: list = field(default_factory=list)
    removed: list = field(default_factory=list)
    changed: list = field(default_factory=list)
    now_required: list = field(default_factory=list)
    no_longer_required: list = field(default_factory=list)
    other_keys: list = field(default_factory=list)  # top-level keys besides the columns

    @property
    def empty(self) -> bool:
        return not any(vars(self).values())


def document_lines(document) -> list:
    return json.dumps(document, indent=2, ensure_ascii=False).splitlines()


def line_diff(old_document, new_document, *, context: int = 3) -> list:
    old, new = document_lines(old_document), document_lines(new_document)
    matcher = difflib.SequenceMatcher(a=old, b=new, autojunk=False)
    lines: list = []
    for group in matcher.get_grouped_opcodes(context):
        if lines:
            lines.append(Line("gap"))
        for tag, i1, i2, j1, j2 in group:
            if tag == "equal":
                lines += [
                    Line("same", i + 1, j + 1, old[i])
                    for i, j in zip(range(i1, i2), range(j1, j2))
                ]
                continue
            lines += [Line("removed", old=i + 1, text=old[i]) for i in range(i1, i2)]
            lines += [Line("added", new=j + 1, text=new[j]) for j in range(j1, j2)]
    return lines


def _mapping(document, key: str) -> dict:
    value = document.get(key)
    return value if isinstance(value, dict) else {}


def _names(document, key: str) -> set:
    value = document.get(key)
    return {name for name in value if isinstance(name, str)} if isinstance(value, list) else set()


def summary(old_document: dict, new_document: dict) -> Summary:
    old_columns = _mapping(old_document, "properties")
    new_columns = _mapping(new_document, "properties")
    old_required = _names(old_document, "required")
    new_required = _names(new_document, "required")
    keys = (set(old_document) | set(new_document)) - {"properties", "required"}
    return Summary(
        added=sorted(set(new_columns) - set(old_columns)),
        removed=sorted(set(old_columns) - set(new_columns)),
        changed=sorted(
            name
            for name in set(old_columns) & set(new_columns)
            if old_columns[name] != new_columns[name]
        ),
        now_required=sorted(new_required - old_required),
        no_longer_required=sorted(old_required - new_required),
        other_keys=sorted(key for key in keys if old_document.get(key) != new_document.get(key)),
    )
