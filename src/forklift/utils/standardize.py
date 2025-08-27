import re
from typing import Any

def standardize_postgres_column_name(name: str) -> str:
    """
    Standardize a column name for Postgres compatibility:
    - Lowercase
    - Replace non-alphanumeric characters with underscores
    - Collapse multiple underscores
    - Strip leading/trailing underscores
    - Truncate to 63 characters (Postgres limit)

    :param name: The column name to standardize.
    :returns: Standardized column name string.
    """
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:63]

