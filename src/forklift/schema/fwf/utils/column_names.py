"""Column name processing utilities."""

from __future__ import annotations

from typing import List, Optional

from ....utils.column_name_utilities import dedupe_column_names, standardize_postgres_column_name
from ...naming import camel_case_name, snake_case_name


class ColumnNameProcessor:
    """Handles column name standardization and deduplication."""

    @staticmethod
    def standardize_column_names(
        column_names: List[str],
        standardize_method: Optional[str] = None,
        dedupe_method: Optional[str] = None,
    ) -> List[str]:
        """Apply column name standardization and deduplication if configured.

        Args:
            column_names: List of column names to process
            standardize_method: Standardization method (postgres, snake_case, camelCase)
            dedupe_method: Deduplication method (suffix, prefix, error)

        Returns:
            Processed list of column names
        """
        if not standardize_method and not dedupe_method:
            return column_names

        processed_names = column_names

        if standardize_method:
            if standardize_method == "postgres":
                processed_names = [
                    standardize_postgres_column_name(name) for name in processed_names
                ]
            elif standardize_method == "snake_case":
                processed_names = [snake_case_name(name) for name in processed_names]
            elif standardize_method == "camelCase":
                processed_names = [camel_case_name(name) for name in processed_names]

        if dedupe_method:
            # Postgres identifiers are limited to 63 characters, including dedupe suffixes
            max_length = 63 if standardize_method == "postgres" else None
            processed_names = dedupe_column_names(
                processed_names, dedupe_method, max_length=max_length
            )

        return processed_names
