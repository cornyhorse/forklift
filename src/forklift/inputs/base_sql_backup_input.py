from __future__ import annotations
from typing import Any, Dict, Iterable, List, Tuple
from .base import BaseInput
import os
import re

# Simple single-line INSERT pattern (no multiline support by design)
SINGLE_LINE_INSERT_RE = re.compile(r"^INSERT\s+INTO\s+([a-zA-Z0-9_\"]+)\.([a-zA-Z0-9_\"]+)\s*\(([^)]+)\)\s+VALUES\s*\((.*)\);\s*$", re.IGNORECASE)
CREATE_TABLE_RE = re.compile(r"CREATE\s+TABLE\s+([a-zA-Z0-9_\"]+)\.([a-zA-Z0-9_\"]+)\s*\((.*?)\);", re.IGNORECASE | re.DOTALL)

class BaseSQLBackupInput(BaseInput):
    """Parse SQL backup (pg_dump-like) supporting ONLY single-line INSERT statements.

    Explicit limitations (by design):
    - Multiline INSERT statements (where VALUES list spans lines) are ignored.
    - COPY, multi-row VALUES batches, and other dialect features are unsupported.
    - Each INSERT must appear entirely on one line ending with ");".

    Options:
    - include: list of table patterns (same semantics as BaseSQLInput)
    - multiline (bool): if True, explicitly request multiline INSERT support. Currently *not implemented* and
      will raise NotImplementedError to make the contract explicit.
    """
    def __init__(self, source: str, include: List[str] | None = None, **opts: Any):
        super().__init__(source, **opts)
        self.include = include or ["*.*"]
        if not os.path.isfile(source):
            raise FileNotFoundError(source)
        # Flag retrieval (support both 'multiline' and legacy 'multi_line' just in case)
        self._multiline_requested = bool(opts.get("multiline") or opts.get("multi_line"))
        if self._multiline_requested:
            raise NotImplementedError(
                "Multiline INSERT parsing not implemented. Re-export your dump with single-line INSERT statements or omit multiline=True."
            )
        self._tables: Dict[Tuple[str | None, str], Dict[str, Any]] = {}
        self._parsed = False
        self._skipped: List[dict] = []

    def get_skipped(self) -> List[dict]:
        return list(self._skipped)

    def iter_rows(self) -> Iterable[Dict[str, Any]]:
        for t in self.get_tables():
            for r in t["rows"]:
                yield r

    def get_tables(self) -> List[Dict[str, Any]]:
        if not self._parsed:
            self._parse()
        out: List[Dict[str, Any]] = []
        patterns = self.include or ["*.*"]
        for (schema, name), meta in self._tables.items():
            if self._matches(patterns, schema, name):
                out.append({"schema": schema, "name": name, "rows": meta["rows"]})
        return out

    # ---- parsing helpers ----
    def _matches(self, patterns: List[str], schema: str | None, name: str) -> bool:
        for p in patterns:
            p = p.strip()
            if not p:
                continue
            if p == "*.*":
                return True
            if p.endswith(".*"):
                if (schema or "") == p[:-2]:
                    return True
            if "." in p:
                sch, tbl = p.split(".", 1)
                if sch == (schema or "") and tbl == name:
                    return True
            else:
                if name == p:
                    return True
        return False

    def _ensure_table(self, schema: str | None, name: str, columns: List[str] | None = None):
        key = (schema, name)
        if key not in self._tables:
            self._tables[key] = {"columns": columns[:] if columns else [], "rows": []}
        else:
            if columns and not self._tables[key]["columns"]:
                self._tables[key]["columns"] = columns[:]
        return self._tables[key]

    def _parse(self):
        with open(self.source, "r", encoding="utf-8", errors="ignore") as fh:
            for raw in fh:
                line = raw.rstrip("\n")
                stripped = line.strip()
                if not stripped or stripped.startswith("--"):
                    continue
                # CREATE TABLE (single-line only; multi-line definitions ignored unless complete on one line)
                if stripped.lower().startswith("create table"):
                    self._try_create(stripped)
                    continue
                # Single-line INSERT ONLY
                m = SINGLE_LINE_INSERT_RE.match(stripped)
                if not m:
                    continue  # ignore multiline or unsupported statement
                schema, name, columns_blob, values_blob = m.groups()
                schema = schema.replace('"', '')
                name = name.replace('"', '')
                columns = [c.strip().strip('"') for c in columns_blob.split(',')]
                table_meta = self._ensure_table(schema, name, columns)
                if not table_meta["columns"]:
                    table_meta["columns"] = columns
                values = self._parse_values(values_blob)
                if len(values) != len(columns):
                    self._skipped.append({
                        "schema": schema,
                        "name": name,
                        "reason": "len_mismatch",
                        "expected": len(columns),
                        "got": len(values),
                        "stmt": stripped[:300]
                    })
                    continue
                row = {c: v for c, v in zip(columns, values)}
                # Deduplicate identical rows
                if any(all(r.get(c) == row.get(c) for c in columns) for r in table_meta["rows"]):
                    continue
                table_meta["rows"].append(row)
        self._parsed = True

    def _try_create(self, stmt: str):
        m = CREATE_TABLE_RE.match(stmt)
        if not m:
            return
        schema, name, cols_blob = m.groups()
        schema = schema.replace('"', '')
        name = name.replace('"', '')
        # naive column name extraction: split commas, stop at constraints
        col_names: List[str] = []
        depth = 0
        current: List[str] = []
        for ch in cols_blob:
            if ch == '(':
                depth += 1
            elif ch == ')':
                if depth > 0:
                    depth -= 1
            if ch == ',' and depth == 0:
                seg = ''.join(current).strip()
                if seg and not seg.lower().startswith('constraint'):
                    tok = seg.split()[0].strip('"')
                    if tok.lower() != 'constraint':
                        col_names.append(tok)
                current = []
            else:
                current.append(ch)
        seg = ''.join(current).strip()
        if seg and not seg.lower().startswith('constraint'):
            tok = seg.split()[0].strip('"')
            if tok.lower() != 'constraint':
                col_names.append(tok)
        self._ensure_table(schema, name, col_names)

    def _parse_values(self, blob: str) -> List[Any]:
        out: List[Any] = []
        current: List[str] = []
        in_string = False
        i = 0
        while i < len(blob):
            ch = blob[i]
            if ch == "'":
                if in_string and i + 1 < len(blob) and blob[i + 1] == "'":
                    current.append("'")
                    i += 2
                    continue
                in_string = not in_string
                i += 1
                continue
            if ch == ',' and not in_string:
                out.append(self._coerce(''.join(current).strip()))
                current = []
                i += 1
                continue
            current.append(ch)
            i += 1
        if current:
            trailing = ''.join(current).strip()
            if trailing.endswith(")"):
                trailing = trailing[:-1].rstrip()
            out.append(self._coerce(trailing))
        return out

    def _coerce(self, token: str) -> Any:
        if token.upper() == 'NULL':
            return None
        if token.lower() in ('true', 'false'):
            return token.lower() == 'true'
        if token.startswith("'") and token.endswith("'"):
            return token[1:-1]
        try:
            if '.' in token:
                return float(token)
            return int(token)
        except Exception:
            return token
