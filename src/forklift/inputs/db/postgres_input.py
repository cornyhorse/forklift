from typing import List, Tuple, Iterable, Any
from forklift.inputs.base_sql_input import BaseSQLInput

class PostgresInput(BaseSQLInput):
    """
    Postgres-specific SQL input class. Currently inherits all logic from BaseSQLInput,
    but can be extended for Postgres-specific features.

    :param source: Database connection string.
    :type source: str
    :param include: List of table/view patterns to include.
    :type include: List[str], optional
    :param opts: Additional options for the input type.
    :type opts: Any
    """
    def __init__(self, source: str, include: List[str] = None, **opts: Any):
        """
        Initialize PostgresInput.

        :param source: Database connection string.
        :type source: str
        :param include: List of table/view patterns to include.
        :type include: List[str], optional
        :param opts: Additional options for the input type.
        :type opts: Any
        """
        super().__init__(source, include, **opts)

    # You can override _get_all_tables or other methods here if needed for Postgres-specific logic.
