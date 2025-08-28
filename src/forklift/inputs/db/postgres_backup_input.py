from __future__ import annotations
from typing import Any, List
from forklift.inputs.base_sql_backup_input import BaseSQLBackupInput

class PostgresBackupInput(BaseSQLBackupInput):
    """Postgres-specific SQL backup input.

    Currently relies entirely on BaseSQLBackupInput generic parsing logic for
    pg_dump style single-row INSERT statements and CREATE TABLE definitions.
    """
    def __init__(self, source: str, include: List[str] | None = None, **opts: Any):
        super().__init__(source, include, **opts)

