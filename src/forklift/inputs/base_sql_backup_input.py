from __future__ import annotations
from typing import Any, Dict, Iterable, List, Tuple
from .base import BaseInput
import os
import re

# Simple single-line INSERT pattern (no multiline support by design)
SINGLE_LINE_INSERT_RE = re.compile(r"^INSERT\s+INTO\s+([a-zA-Z0-9_\"]+)\.([a-zA-Z0-9_\"]+)\s*\(([^)]+)\)\s+VALUES\s*\((.*)\);\s*$", re.IGNORECASE)
CREATE_TABLE_RE = re.compile(r"CREATE\s+TABLE\s+([a-zA-Z0-9_\"]+)\.([a-zA-Z0-9_\"]+)\s*\((.*?)\);", re.IGNORECASE | re.DOTALL)

class BaseSQLBackupInput(BaseInput):
    """Parse a basic SQL dump (pg_dump‑like) with only single-line INSERTs.

    Explicit limitations (intentional):

    * Multiline INSERT statements are ignored.
    * COPY, multi-row VALUES batches, and vendor extensions are unsupported.
    * Each supported INSERT must reside fully on one line ending with ``);``.

    Options (``opts``):

    * ``include`` – list of schema.table patterns (``*.*`` wildcard allowed)
    * ``multiline`` – if True raises :class:`NotImplementedError` (guard rail)
    """
    def __init__(self, source: str, include: List[str] | None = None, **opts: Any):
        super().__init__(source, **opts)
        self.include = include or ["*.*"]
        if not os.path.isfile(source):
            raise FileNotFoundError(source)
        self._multiline_requested = bool(opts.get("multiline") or opts.get("multi_line"))
        if self._multiline_requested:
            raise NotImplementedError(
                "Multiline INSERT parsing not implemented. Re-export your dump with single-line INSERT statements or omit multiline=True."
            )
        self._tables: Dict[Tuple[str | None, str], Dict[str, Any]] = {}
        self._parsed = False
        self._skipped: List[dict] = []

    def get_skipped(self) -> List[dict]:
        """Return metadata for skipped INSERT statements.

        Each dict contains keys like ``schema``, ``name``, ``reason`` and a
        snippet of the offending statement.

        :return: List of skipped statement descriptors.
        """
        return list(self._skipped)

    def iter_rows(self) -> Iterable[Dict[str, Any]]:
        """Iterate over all parsed row dictionaries across included tables.

        :yield: One row dict at a time.
        """
        for t in self.get_tables():
            for r in t["rows"]:
                yield r

    def get_tables(self) -> List[Dict[str, Any]]:
        """Return table descriptors matching include patterns.

        Triggers lazy parsing on first call.

        :return: List of dicts with keys ``schema``, ``name``, ``rows`` (list of row dicts).
        """
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
        """Check if a table name matches any include pattern.

        Supported pattern forms: ``*.*`` (everything), ``schema.*``,
        ``schema.table`` and bare ``table``.

        :param patterns: List of pattern strings.
        :param schema: Schema name (or ``None``).
        :param name: Table name.
        :return: ``True`` if matched, else ``False``.
        """
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
        """Ensure a table entry exists in internal cache.

        :param schema: Schema name or ``None``.
        :param name: Table name.
        :param columns: Optional column list (first declaration wins).
        :return: Table metadata dict with ``columns`` and ``rows`` keys.
        """
        key = (schema, name)
        if key not in self._tables:
            self._tables[key] = {"columns": columns[:] if columns else [], "rows": []}
        else:
            if columns and not self._tables[key]["columns"]:
                self._tables[key]["columns"] = columns[:]
        return self._tables[key]

    def _parse(self):
        """Parse the dump file populating internal table/row structures.

        Skips unsupported statements silently; malformed INSERTs are recorded
        in the ``_skipped`` list.
        """
        with open(self.source, "r", encoding="utf-8", errors="ignore") as fh:
            for raw in fh:
                line = raw.rstrip("\n")
                stripped = line.strip()
                if not stripped or stripped.startswith("--"):
                    continue
                if stripped.lower().startswith("create table"):
                    self._try_create(stripped)
                    continue
                m = SINGLE_LINE_INSERT_RE.match(stripped)
                if not m:
                    continue
                schema, name, columns_blob, values_blob = m.groups()
                schema = schema.replace('"', '')
                name = name.replace('"', '')
                columns = [c.strip().strip('"') for c in columns_blob.split(',')]
                table_meta = self._ensure_table(schema, name, columns)
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
                if any(all(r.get(c) == row.get(c) for c in columns) for r in table_meta["rows"]):
                    continue
                table_meta["rows"].append(row)
        self._parsed = True

    def _try_create(self, stmt: str):
        """Attempt to extract column names from a CREATE TABLE statement.

        Only single-line definitions are supported. Constraint clauses are
        ignored when identifying column tokens.

        :param stmt: Raw CREATE TABLE statement.
        """
        m = CREATE_TABLE_RE.match(stmt)
        if not m:
            return
        schema, name, cols_blob = m.groups()
        schema = schema.replace('"', '')
        name = name.replace('"', '')
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
        """Parse the comma-separated VALUES segment from a single INSERT line.

        Handles simple SQL string escaping via doubled single quotes.

        :param blob: Raw text inside ``VALUES(...)`` excluding leading keyword.
        :return: List of coerced Python values.
        """
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
        """Coerce a raw token string into a primitive Python value.

        Recognizes NULL/boolean/integer/float; returns the original string for
        anything else (including quoted strings once stripped).

        :param token: Raw token text (sans surrounding quotes for strings).
        :return: Coerced Python value or original token.
        """
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
