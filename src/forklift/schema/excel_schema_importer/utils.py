"""Utility functions for Excel schema data extraction and manipulation."""

from typing import Any, Dict, List, Optional


class SchemaDataExtractor:
    """Utility class for extracting data from Excel schema configurations."""

    def __init__(self, schema: Dict[str, Any]):
        self.schema = schema
        self.excel_ext = schema.get("x-excel", {})
        self.field_map = schema.get("properties", {})
        self.sheets = self.excel_ext.get("sheets", [])
        self.nulls = self.excel_ext.get("nulls", {})

    def get_field_map(self) -> Dict[str, Any]:
        """Get the field mapping from the schema."""
        return self.field_map

    def get_excel_extension(self) -> Dict[str, Any]:
        """Get the Excel-specific extension configuration."""
        return self.excel_ext

    def get_sheets(self) -> List[Dict[str, Any]]:
        """Get the sheet configurations."""
        return self.sheets

    def get_null_values(self, column_name: Optional[str] = None) -> List[str]:
        """Get null values for a specific column or global defaults."""
        global_nulls = self.nulls.get("global", [""])

        if column_name:
            per_column = self.nulls.get("perColumn", {})
            return per_column.get(column_name, global_nulls)

        return global_nulls

    def get_date_system(self) -> str:
        """Get the Excel date system (1900 or 1904)."""
        return self.excel_ext.get("dateSystem", "1900")

    def get_values_only(self) -> bool:
        """Get the values-only flag for Excel reading."""
        return self.excel_ext.get("valuesOnly", True)

    def get_column_mapping(self, sheet_name: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        """Get column mapping for a specific sheet or the first sheet."""
        target_sheet = None

        if sheet_name:
            for sheet in self.sheets:
                select = sheet.get("select", {})
                if select.get("name") == sheet_name:
                    target_sheet = sheet
                    break
        else:
            target_sheet = self.sheets[0] if self.sheets else None

        if not target_sheet:
            return {}

        columns = target_sheet.get("columns", [])
        mapping = {}

        for column in columns:
            if isinstance(column, dict) and column.get("name"):
                mapping[column["name"]] = column

        return mapping

    def get_required_fields(self) -> List[str]:
        """Get the list of required fields from the schema."""
        return list(self.schema.get("required", []))

    def get_additional_properties(self) -> bool:
        """Get the additionalProperties flag from the schema."""
        return bool(self.schema.get("additionalProperties", True))
