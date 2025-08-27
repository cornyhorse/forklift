from typing import Any, Dict, Optional, List
import json
import jsonschema
import re
from ..utils.dedupe import dedupe_column_names

class ExcelSchemaImporter:
    def __init__(self, schema_path: str):
        """
        Initialize the ExcelSchemaImporter with a given schema file path.

        :param schema_path: Path to the JSON schema file.
        :returns: None
        """
        with open(schema_path, 'r', encoding='utf-8') as f:
            self.schema = json.load(f)
        self.validator = jsonschema.Draft202012Validator(self.schema)
        self.excel_options = self.schema.get('x-excel', {})
        self.field_map = self.schema.get('properties', {})
        self.required = self.schema.get('required', [])
        self.additional_properties = self.schema.get('additionalProperties', True)
        self.sheets = self.excel_options.get('sheets', [])
        self.nulls = self.excel_options.get('nulls', {}).get('global', [])
        self.standardize_names = self.excel_options.get('standardizeNames', None)
        self.dedupe_names = self.excel_options.get('dedupeNames', None)

    def get_excel_options(self) -> Dict[str, Any]:
        """
        Get Excel-specific options from the schema.

        :returns: Dictionary of Excel options.
        """
        return self.excel_options

    def get_sheet_options(self) -> List[Dict[str, Any]]:
        """
        Get per-sheet options from the schema.

        :returns: List of sheet option dictionaries.
        """
        return self.sheets

    def get_field_map(self) -> Dict[str, Any]:
        """
        Get mapping of schema fields to expected columns.

        :returns: Dictionary mapping field names to column definitions.
        """
        return self.field_map

    def get_nulls(self) -> List[str]:
        """
        Get list of global null indicators from the schema.

        :returns: List of null indicator strings.
        """
        return self.nulls

    def standardize_column_name(self, name: str) -> str:
        """
        Standardize column name according to schema options.

        :param name: The column name to standardize.
        :returns: Standardized column name string.
        """
        if self.standardize_names == "postgres":
            s = name.strip().lower()
            s = re.sub(r"[^a-z0-9]+", "_", s)
            s = re.sub(r"_+", "_", s).strip("_")
            return s[:63]
        return name

    def build_column_field_mapping(self, columns: List[str]) -> Dict[str, str]:
        """
        Build mapping of columns to schema fields, applying standardization and deduplication.

        :param columns: List of column names to map.
        :returns: Dictionary mapping column names to schema field names.
        """
        std_names = [self.standardize_column_name(c) for c in columns]
        # Use shared dedupe_column_names utility
        deduped_names = dedupe_column_names(std_names) if self.dedupe_names == "suffix" else std_names
        mapping = {}
        for col in deduped_names:
            if col in self.field_map:
                mapping[col] = col
            else:
                # Try to match by lowercased, stripped name
                for field in self.field_map:
                    if col.lower() == field.lower():
                        mapping[col] = field
                        break
        return mapping

    def coerce_types(self, row: Dict[str, Any], sheet_opts: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Coerce types of row values according to schema type coercion rules.

        :param row: The row data to coerce.
        :param sheet_opts: Optional sheet-specific options for coercion.
        :returns: Row data with coerced types.
        """
        if not sheet_opts or 'coerce' not in sheet_opts:
            return row
        coerce = sheet_opts['coerce']
        coerced_row = row.copy()
        # Booleans
        if 'booleans' in coerce:
            for field, value in row.items():
                if field in self.field_map and self.field_map[field].get('type') == 'boolean':
                    true_vals = set(coerce['booleans'].get('true', []))
                    false_vals = set(coerce['booleans'].get('false', []))
                    if isinstance(value, str):
                        if value in true_vals:
                            coerced_row[field] = True
                        elif value in false_vals:
                            coerced_row[field] = False
        # Dates
        # (Date coercion can be implemented here as needed)
        return coerced_row

    def validate_row(self, row: Dict[str, Any]) -> Optional[List[str]]:
        """
        Validate a row against the schema. Returns a list of errors, or None if valid.

        :param row: The row data to validate.
        :returns: List of error messages, or None if the row is valid.
        """
        errors = list(self.validator.iter_errors(row))
        if errors:
            return [e.message for e in errors]
        return None
