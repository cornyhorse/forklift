"""Utility functions for CSV schema data extraction and manipulation."""

from typing import Any, Dict, List, Optional

from ...utils.column_name_utilities import standardize_postgres_column_name, dedupe_column_names


class SchemaDataExtractor:
    """Utility class for extracting data from CSV schema configurations."""

    def __init__(self, schema: Dict[str, Any]):
        self.schema = schema
        self.csv_ext = schema.get("x-csv", {})
        self.field_map = schema.get("properties", {})

        # Extract case configuration
        case_cfg = self.csv_ext.get("case", {}) if isinstance(self.csv_ext.get("case", {}), dict) else {}
        self.standardize_names: Optional[str] = case_cfg.get("standardizeNames")
        self.dedupe_names: Optional[str] = case_cfg.get("dedupeNames")

    def get_field_map(self) -> Dict[str, Any]:
        """Get the field mapping from the schema."""
        return self.field_map

    def get_csv_extension(self) -> Dict[str, Any]:
        """Get the CSV-specific extension configuration."""
        return self.csv_ext

    def get_required_fields(self) -> List[str]:
        """Get the list of required fields from the schema."""
        return list(self.schema.get("required", []))

    def get_additional_properties(self) -> bool:
        """Get the additionalProperties flag from the schema."""
        return bool(self.schema.get("additionalProperties", True))

    def get_parquet_type_mapping(self) -> Dict[str, str]:
        """Get the Parquet type mapping for all fields."""
        return self.csv_ext.get("parquetTypeMapping", {})

    def get_encoding_priority(self) -> List[str]:
        """Get the encoding detection priority list."""
        return self.csv_ext.get("encodingPriority", ["utf-8"])

    def get_delimiter(self) -> str:
        """Get the delimiter configuration."""
        return self.csv_ext.get("delimiter", ",")

    def get_null_values(self, column_name: Optional[str] = None) -> List[str]:
        """Get null values for a specific column or global defaults."""
        nulls = self.csv_ext.get("nulls", {})
        global_nulls = nulls.get("global", [""])

        if column_name:
            per_column = nulls.get("perColumn", {})
            return per_column.get(column_name, global_nulls)

        return global_nulls

    def standardize_column_names(self, column_names: List[str]) -> List[str]:
        """Apply column name standardization if configured."""
        if not self.standardize_names:
            return column_names

        if self.standardize_names == "postgres":
            standardized = [standardize_postgres_column_name(name) for name in column_names]
        else:
            # Add other standardization methods as needed
            standardized = column_names

        if self.dedupe_names:
            return dedupe_column_names(standardized, self.dedupe_names)

        return standardized

    def get_calculated_columns_config(self) -> Optional[Dict[str, Any]]:
        """Extract calculated columns configuration from schema.

        Returns:
            Dictionary containing calculated columns configuration or None if not present
        """
        return self.schema.get("x-calculatedColumns")

    def get_row_hash_config(self) -> Optional[Dict[str, Any]]:
        """Extract row hash configuration from schema.

        Returns:
            Dictionary containing row hash configuration or None if not present
        """
        return self.schema.get("x-rowHash")

    def has_calculated_columns(self) -> bool:
        """Check if schema defines calculated columns.

        Returns:
            True if calculated columns are defined, False otherwise
        """
        calc_cols = self.get_calculated_columns_config()
        if not calc_cols:
            return False

        return (
            bool(calc_cols.get("constants")) or
            bool(calc_cols.get("expressions")) or
            bool(calc_cols.get("calculated"))
        )

    def get_partition_columns(self) -> List[str]:
        """Get partition columns from calculated columns configuration.

        Returns:
            List of column names to be used for partitioning
        """
        calc_cols = self.get_calculated_columns_config()
        if calc_cols:
            return calc_cols.get("partitionColumns", [])
        return []

    def get_index_columns(self) -> List[str]:
        """Get index columns from calculated columns configuration.

        Returns:
            List of column names to be used for indexing
        """
        calc_cols = self.get_calculated_columns_config()
        if calc_cols:
            return calc_cols.get("indexColumns", [])
        return []
