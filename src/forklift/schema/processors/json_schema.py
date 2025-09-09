"""JSON Schema processing utilities."""

from typing import Dict, Any
import pyarrow as pa
from ..types.data_types import DataTypeConverter
from ..utils.helpers import get_parquet_type_string


class JSONSchemaProcessor:
    """Handles JSON Schema generation and processing."""

    def __init__(self):
        self.converter = DataTypeConverter()

    def generate_properties_from_table(self, table: pa.Table) -> Dict[str, Any]:
        """Generate JSON Schema properties from PyArrow table.

        Args:
            table: PyArrow table to analyze

        Returns:
            Dict: JSON Schema properties
        """
        properties = {}

        for field in table.schema:
            column_name = field.name
            arrow_type = field.type

            # Convert Arrow type to JSON Schema type
            json_type = self.converter.arrow_to_json_schema_type(arrow_type)
            properties[column_name] = json_type

        return properties

    def determine_required_fields(self, table: pa.Table) -> list:
        """Determine which fields should be required based on data analysis.

        Args:
            table: PyArrow table to analyze

        Returns:
            list: List of required field names
        """
        required_fields = []

        for i, field in enumerate(table.schema):
            column_name = field.name
            column_data = table.column(i)

            # Check for required fields (non-nullable and has data)
            if not field.nullable and column_data.null_count == 0:
                required_fields.append(column_name)

        return required_fields

    def generate_csv_extension(self, table: pa.Table, config) -> Dict[str, Any]:
        """Generate CSV-specific extension configuration.

        Args:
            table: PyArrow table
            config: Schema generation configuration

        Returns:
            Dict: CSV extension configuration
        """
        return {
            "encodingPriority": [config.encoding, "utf-8-sig", "utf-8", "latin-1"],
            "delimiter": config.delimiter,
            "quotechar": "\"",
            "escapechar": "\\",
            "multiline": True,
            "header": {
                "mode": "present",
                "keywords": list(table.schema.names)[:4]
            },
            "footer": {
                "mode": "regex",
                "pattern": "^(total|summary|count)\\b"
            },
            "nulls": {
                "global": ["", "NA", "N/A", "-", "NULL", "null"],
                "perColumn": {}
            },
            "dataTypes": {
                col_name: get_parquet_type_string(table.schema.field(col_name).type)
                for col_name in table.schema.names
            },
            "validation": {
                "enabled": True,
                "onError": "log",
                "maxErrors": 1000
            }
        }

    def generate_excel_extension(self, config) -> Dict[str, Any]:
        """Generate Excel-specific extension configuration.

        Args:
            config: Schema generation configuration

        Returns:
            Dict: Excel extension configuration
        """
        return {
            "sheet": config.sheet_name or 0,
            "header": {"mode": "present"},
            "skipRows": 0,
            "skipFooter": 0,
            "nulls": {
                "global": ["", "NA", "N/A", "-", "NULL"]
            },
            "validation": {
                "enabled": True,
                "onError": "log",
                "maxErrors": 1000
            }
        }

    def generate_sample_data(self, table: pa.Table) -> Dict[str, Any]:
        """Generate sample data from the table.

        Args:
            table: PyArrow table

        Returns:
            Dict: Sample data configuration
        """
        # Take first 3 rows as sample
        sample_size = min(3, table.num_rows)
        sample_table = table.slice(0, sample_size)

        # Convert to pandas for easier JSON serialization
        df = sample_table.to_pandas()

        # Convert to records format
        records = df.to_dict('records')

        return {
            "description": f"Sample data from first {sample_size} rows",
            "rows": records
        }
