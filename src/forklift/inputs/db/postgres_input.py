from typing import List, Tuple, Iterable, Any
from forklift.inputs.base_sql_input import BaseSQLInput

class PostgresInput(BaseSQLInput):
    """
    Postgres-specific SQL input class. Currently inherits all logic from BaseSQLInput,
    but can be extended for Postgres-specific features.
    """
    def __init__(self, source: str, include: List[str] = None, **opts: Any):
        super().__init__(source, include, **opts)

    # You can override _get_all_tables or other methods here if needed for Postgres-specific logic.

