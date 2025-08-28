from __future__ import annotations
from typing import Any, List, Tuple, Iterable
from .base import BaseInput
from .db.postgres_backup_input import PostgresBackupInput
from .base_sql_backup_input import BaseSQLBackupInput

class SQLBackupInput(BaseInput):
    """Wrapper for SQL backup (.sql dump) inputs.

    Currently only supports Postgres pg_dump style files via PostgresBackupInput.
    Mirrors the delegation pattern used by SQLInput.
    """
    def __init__(self, source: str, include: List[str] | None = None, **opts: Any):
        self._delegate = get_sql_backup_input(source, include, **opts)

    def iter_rows(self) -> Iterable:
        return self._delegate.iter_rows()

    def get_tables(self) -> list:
        return self._delegate.get_tables()

    def _get_all_tables(self) -> List[Tuple[str | None, str]]:
        return [(t["schema"], t["name"]) for t in self.get_tables()]

def get_sql_backup_input(source: str, include: List[str] | None = None, **opts: Any) -> BaseSQLBackupInput:
    # Future: inspect file header to detect dialect/format.
    return PostgresBackupInput(source, include, **opts)

