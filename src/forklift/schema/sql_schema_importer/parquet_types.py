"""Parquet type handling and validation for SQL Schema Importer."""

from typing import Dict, List


class ParquetTypeHandler:
    """Handles Parquet type mapping and validation."""

    # Define supported Parquet data types
    SUPPORTED_PARQUET_TYPES = {
        "int8", "int16", "int32", "int64",
        "uint8", "uint16", "uint32", "uint64",
        "float32", "double", "bool", "string", "binary",
        "date32", "date64", "timestamp[s]", "timestamp[ms]",
        "timestamp[us]", "timestamp[ns]", "duration[s]", "duration[ms]",
        "duration[us]", "duration[ns]", "decimal128(10,2)",
        "list<string>", "struct", "dictionary<values=string, indices=int32>"
    }

    def __init__(self, parquet_type_mapping: Dict[str, any]):
        """Initialize with parquet type mapping configuration."""
        self.parquet_type_mapping = parquet_type_mapping

    def is_valid_parquet_type(self, parquet_type: str) -> bool:
        """Check if a Parquet type is valid."""
        if parquet_type in self.SUPPORTED_PARQUET_TYPES:
            return True

        # Check for parameterized types like decimal128(precision,scale)
        if parquet_type.startswith("decimal128(") and parquet_type.endswith(")"):
            return True

        # Check for timestamp with timezone
        if parquet_type.startswith("timestamp[") and parquet_type.endswith("]"):
            return True

        # Check for duration types
        if parquet_type.startswith("duration[") and parquet_type.endswith("]"):
            return True

        # Check for list types
        if parquet_type.startswith("list<") and parquet_type.endswith(">"):
            return True

        # Check for dictionary types
        if parquet_type.startswith("dictionary<") and parquet_type.endswith(">"):
            return True

        return False

    def get_sql_to_parquet_mapping(self) -> Dict[str, str]:
        """Get the SQL to Parquet type mapping."""
        mapping = self.parquet_type_mapping.get("sqlToParquet", {})

        # Provide default mappings if not specified
        if not mapping:
            mapping = {
                "INTEGER": "int64",
                "BIGINT": "int64",
                "SMALLINT": "int32",
                "TINYINT": "int8",
                "DECIMAL": "decimal128(10,2)",
                "NUMERIC": "decimal128(10,2)",
                "FLOAT": "float32",
                "DOUBLE": "double",
                "REAL": "float32",
                "BOOLEAN": "bool",
                "VARCHAR": "string",
                "TEXT": "string",
                "CHAR": "string",
                "DATE": "date32",
                "TIMESTAMP": "timestamp[us]",
                "DATETIME": "timestamp[us]",
                "TIME": "duration[us]",
                "INTERVAL": "duration[us]",
                "BINARY": "binary",
                "VARBINARY": "binary",
                "BLOB": "binary",
                "ARRAY": "list<string>",
                "JSON": "struct",
                "JSONB": "struct",
                "UUID": "string"
            }

        return mapping

    def validate_parquet_types(self, tables: List[Dict[str, any]]) -> List[str]:
        """Validate Parquet type mappings in table columns."""
        errors = []

        for i, table in enumerate(tables):
            # Skip invalid table entries (they'll be caught by table validation)
            if not isinstance(table, dict):
                continue

            columns = table.get("columns", {})
            for col_name, col_def in columns.items():
                if isinstance(col_def, dict):
                    parquet_type = col_def.get("parquetType")
                    if parquet_type and not self.is_valid_parquet_type(parquet_type):
                        errors.append(f"Table {i} column '{col_name}' invalid Parquet type '{parquet_type}'")

        return errors
