from __future__ import annotations
from typing import Any, List, Tuple, Iterable
from .base import BaseInput
from .db.postgres_backup_input import PostgresBackupInput
from .base_sql_backup_input import BaseSQLBackupInput

class SQLBackupInput(BaseInput):
    """Wrapper for SQL backup (``.sql`` dump) inputs.

    Currently only Postgres pg_dump style files are supported via
    :class:`PostgresBackupInput`. Mirrors the delegation pattern used by
    :class:`forklift.inputs.sql_input.SQLInput`.

    :param source: Path to SQL dump file.
    :param include: Optional list of include patterns (``schema.table`` / wildcards).
    :param opts: Additional keyword arguments forwarded to concrete input.
    """
    def __init__(self, source: str, include: List[str] | None = None, **opts: Any):
        super().__init__(source, **opts)
        self._delegate = get_sql_backup_input(source, include, **opts)

    def iter_rows(self) -> Iterable:
        """Delegate row iteration to the concrete backup input.

        :return: Iterable yielding row dictionaries.
        """
        return self._delegate.iter_rows()

    def get_tables(self) -> list:
        """Return table descriptors discovered by the delegate.

        :return: List of dicts with ``schema``, ``name`` and ``rows`` keys.
        """
        return self._delegate.get_tables()

    def _get_all_tables(self) -> List[Tuple[str | None, str]]:
        """Return ``(schema, name)`` tuples for all included tables/views.

        :return: List of ``(schema, name)`` pairs.
        """
        return [(t["schema"], t["name"]) for t in self.get_tables()]

def get_sql_backup_input(source: str, include: List[str] | None = None, **opts: Any) -> BaseSQLBackupInput:
    """Factory returning the appropriate SQL backup input implementation.

    Future enhancement may sniff file content to select dialect.

    :param source: Path to dump file.
    :param include: Optional include pattern list.
    :param opts: Extra keyword arguments for implementation.
    :return: Instance of a :class:`BaseSQLBackupInput` subclass.
    """
    return PostgresBackupInput(source, include, **opts)
