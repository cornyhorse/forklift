import json
import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch

from forklift.schema.csv_schema_importer import CsvSchemaImporter


class TestCsvSchemaImporter:
    """Unit tests for CsvSchemaImporter class."""

    def test_init_with_dict(self):
        """Test initialization with a dictionary schema."""
        schema = {
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "required": ["id"],
            "additionalProperties": True,
            "x-csv": {
                "delimiter": ",",
                "case": {"standardizeNames": "postgres", "dedupeNames": "suffix"}
            }
        }
        importer = CsvSchemaImporter(schema)

        assert importer.schema == schema
        assert importer.csv_ext == schema["x-csv"]
        assert importer.field_map == {"id": {"type": "integer"}}
        assert importer.required == ["id"]
        assert importer.additional_properties is True
        assert importer.standardize_names == "postgres"
        assert importer.dedupe_names == "suffix"

    def test_init_with_file_path(self):
        """Test initialization with a file path."""
        schema = {
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "x-csv": {"delimiter": "|"}
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(schema, f)
            temp_path = f.name

        try:
            importer = CsvSchemaImporter(temp_path)
            assert importer.schema == schema
            assert importer.csv_ext == {"delimiter": "|"}
        finally:
            Path(temp_path).unlink()

    def test_init_with_pathlib_path(self):
        """Test initialization with a pathlib.Path object."""
        schema = {"type": "object", "x-csv": {"delimiter": "\\t"}}

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(schema, f)
            temp_path = Path(f.name)

        try:
            importer = CsvSchemaImporter(temp_path)
            assert importer.schema == schema
        finally:
            temp_path.unlink()

    def test_init_with_invalid_type(self):
        """Test initialization with invalid input type."""
        with pytest.raises(TypeError, match="schema must be path-like or dict"):
            CsvSchemaImporter(123)

    def test_init_with_missing_extensions(self):
        """Test initialization with schema missing optional extensions."""
        schema = {"type": "object"}
        importer = CsvSchemaImporter(schema)

        assert importer.csv_ext == {}
        assert importer.field_map == {}
        assert importer.required == []
        assert importer.additional_properties is True
        assert importer.standardize_names is None
        assert importer.dedupe_names is None

    def test_init_with_invalid_case_config(self):
        """Test initialization with invalid case configuration."""
        schema = {
            "type": "object",
            "x-csv": {"case": "invalid"}  # Not a dict
        }
        importer = CsvSchemaImporter(schema)

        assert importer.standardize_names is None
        assert importer.dedupe_names is None

    def test_as_dict(self):
        """Test as_dict method returns the original schema."""
        schema = {"type": "object", "properties": {"id": {"type": "integer"}}}
        importer = CsvSchemaImporter(schema)
        assert importer.as_dict() == schema

    def test_get_field_map(self):
        """Test get_field_map method."""
        schema = {
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"}
            }
        }
        importer = CsvSchemaImporter(schema)
        assert importer.get_field_map() == schema["properties"]

    def test_standardize_column_name_postgres(self):
        """Test column name standardization with postgres mode."""
        schema = {
            "x-csv": {"case": {"standardizeNames": "postgres"}}
        }
        importer = CsvSchemaImporter(schema)

        assert importer._standardize_column_name("User ID") == "user_id"
        assert importer._standardize_column_name("FIRST_NAME") == "first_name"
        assert importer._standardize_column_name("email@domain") == "email_domain"

    def test_standardize_column_name_no_mode(self):
        """Test column name standardization with no standardization mode."""
        schema = {"x-csv": {}}
        importer = CsvSchemaImporter(schema)

        original_name = "User ID"
        assert importer._standardize_column_name(original_name) == original_name

    def test_standardize_and_dedupe_with_suffix(self):
        """Test standardize_and_dedupe with suffix deduplication."""
        schema = {
            "x-csv": {
                "case": {
                    "standardizeNames": "postgres",
                    "dedupeNames": "suffix"
                }
            }
        }
        importer = CsvSchemaImporter(schema)

        columns = ["User ID", "USER_ID", "user-id", "Name"]
        result = importer.standardize_and_dedupe(columns)

        # All three variations of "user_id" should be deduplicated
        assert result == ["user_id", "user_id_1", "user_id_2", "name"]

    def test_standardize_and_dedupe_no_dedupe(self):
        """Test standardize_and_dedupe without deduplication."""
        schema = {
            "x-csv": {
                "case": {"standardizeNames": "postgres"}
            }
        }
        importer = CsvSchemaImporter(schema)

        columns = ["User ID", "USER_ID", "Name"]
        result = importer.standardize_and_dedupe(columns)

        # Should standardize but not deduplicate
        assert result == ["user_id", "user_id", "name"]

    def test_derive_reader_options_empty_extension(self):
        """Test derive_reader_options with empty CSV extension."""
        schema = {"x-csv": {}}
        importer = CsvSchemaImporter(schema)

        options = importer.derive_reader_options()
        assert options == {}

    def test_derive_reader_options_encoding_priority(self):
        """Test derive_reader_options with encoding priority."""
        schema = {
            "x-csv": {
                "encodingPriority": ["utf-8-sig", "utf-8", "latin-1"]
            }
        }
        importer = CsvSchemaImporter(schema)

        options = importer.derive_reader_options()
        assert options["encoding"] == "utf-8-sig"

    def test_derive_reader_options_encoding_priority_empty(self):
        """Test derive_reader_options with empty encoding priority."""
        schema = {
            "x-csv": {
                "encodingPriority": []
            }
        }
        importer = CsvSchemaImporter(schema)

        options = importer.derive_reader_options()
        assert "encoding" not in options

    def test_derive_reader_options_delimiter_simple(self):
        """Test derive_reader_options with simple delimiter."""
        schema = {
            "x-csv": {
                "delimiter": ","
            }
        }
        importer = CsvSchemaImporter(schema)

        options = importer.derive_reader_options()
        assert options["delimiter"] == ","

    def test_derive_reader_options_delimiter_auto(self):
        """Test derive_reader_options with auto delimiter."""
        schema = {
            "x-csv": {
                "delimiter": "auto"
            }
        }
        importer = CsvSchemaImporter(schema)

        options = importer.derive_reader_options()
        assert "delimiter" not in options

    def test_derive_reader_options_delimiter_escape_tab(self):
        """Test derive_reader_options with escaped tab delimiter."""
        schema = {
            "x-csv": {
                "delimiter": "\\t"
            }
        }
        importer = CsvSchemaImporter(schema)

        options = importer.derive_reader_options()
        assert options["delimiter"] == "\t"

    def test_derive_reader_options_delimiter_escape_newline(self):
        """Test derive_reader_options with escaped newline delimiter."""
        schema = {
            "x-csv": {
                "delimiter": "\\n"
            }
        }
        importer = CsvSchemaImporter(schema)

        options = importer.derive_reader_options()
        assert options["delimiter"] == "\n"

    def test_derive_reader_options_delimiter_unicode_escape(self):
        """Test derive_reader_options with unicode escape delimiter."""
        schema = {
            "x-csv": {
                "delimiter": "\\u001f"  # Unit separator
            }
        }
        importer = CsvSchemaImporter(schema)

        options = importer.derive_reader_options()
        assert options["delimiter"] == "\u001f"

    def test_derive_reader_options_delimiter_invalid_unicode(self):
        """Test derive_reader_options with invalid unicode escape."""
        schema = {
            "x-csv": {
                "delimiter": "\\uZZZZ"  # Invalid hex
            }
        }
        importer = CsvSchemaImporter(schema)

        options = importer.derive_reader_options()
        # Should fallback to original string
        assert options["delimiter"] == "\\uZZZZ"

    def test_derive_reader_options_delimiter_escape_exception(self):
        """Test derive_reader_options with delimiter that causes decode exception."""
        schema = {
            "x-csv": {
                "delimiter": "\\x"  # Incomplete escape
            }
        }
        importer = CsvSchemaImporter(schema)

        options = importer.derive_reader_options()
        # Should fallback to original string
        assert options["delimiter"] == "\\x"

    def test_derive_reader_options_quote_char(self):
        """Test derive_reader_options with quote character."""
        schema = {
            "x-csv": {
                "quotechar": "'"
            }
        }
        importer = CsvSchemaImporter(schema)

        options = importer.derive_reader_options()
        assert options["quote_char"] == "'"

    def test_derive_reader_options_null_values_global(self):
        """Test derive_reader_options with global null values."""
        schema = {
            "x-csv": {
                "nulls": {
                    "global": ["", "NA", "N/A", "-"]
                }
            }
        }
        importer = CsvSchemaImporter(schema)

        options = importer.derive_reader_options()
        assert options["null_values"] == ["", "NA", "N/A", "-"]

    def test_derive_reader_options_null_values_empty_list(self):
        """Test derive_reader_options with empty global null values."""
        schema = {
            "x-csv": {
                "nulls": {
                    "global": []
                }
            }
        }
        importer = CsvSchemaImporter(schema)

        options = importer.derive_reader_options()
        assert "null_values" not in options

    def test_derive_reader_options_null_values_invalid(self):
        """Test derive_reader_options with invalid null values."""
        schema = {
            "x-csv": {
                "nulls": {
                    "global": "invalid"  # Not a list
                }
            }
        }
        importer = CsvSchemaImporter(schema)

        options = importer.derive_reader_options()
        assert "null_values" not in options

    def test_derive_reader_options_nulls_not_dict(self):
        """Test derive_reader_options with nulls not being a dict."""
        schema = {
            "x-csv": {
                "nulls": "invalid"
            }
        }
        importer = CsvSchemaImporter(schema)

        options = importer.derive_reader_options()
        assert "null_values" not in options

    def test_derive_reader_options_header_provided_mode(self):
        """Test derive_reader_options with provided header mode."""
        schema = {
            "x-csv": {
                "header": {
                    "mode": "provided",
                    "columns": ["id", "name", "email"]
                }
            }
        }
        importer = CsvSchemaImporter(schema)

        options = importer.derive_reader_options()
        assert options["has_header"] is False
        assert options["_provided_header_columns"] == ["id", "name", "email"]

    def test_derive_reader_options_header_provided_mode_cols_key(self):
        """Test derive_reader_options with provided header mode using 'cols' key."""
        schema = {
            "x-csv": {
                "header": {
                    "mode": "provided",
                    "cols": ["id", "name", "email"]
                }
            }
        }
        importer = CsvSchemaImporter(schema)

        options = importer.derive_reader_options()
        assert options["has_header"] is False
        assert options["_provided_header_columns"] == ["id", "name", "email"]

    def test_derive_reader_options_header_provided_mode_no_columns(self):
        """Test derive_reader_options with provided header mode but no columns."""
        schema = {
            "x-csv": {
                "header": {
                    "mode": "provided"
                }
            }
        }
        importer = CsvSchemaImporter(schema)

        options = importer.derive_reader_options()
        assert "has_header" not in options
        assert "_provided_header_columns" not in options

    def test_derive_reader_options_header_provided_mode_empty_columns(self):
        """Test derive_reader_options with provided header mode and empty columns."""
        schema = {
            "x-csv": {
                "header": {
                    "mode": "provided",
                    "columns": []
                }
            }
        }
        importer = CsvSchemaImporter(schema)

        options = importer.derive_reader_options()
        assert "has_header" not in options
        assert "_provided_header_columns" not in options

    def test_derive_reader_options_header_other_mode(self):
        """Test derive_reader_options with non-provided header mode."""
        schema = {
            "x-csv": {
                "header": {
                    "mode": "stability_scan",
                    "keywords": ["id", "name"]
                }
            }
        }
        importer = CsvSchemaImporter(schema)

        options = importer.derive_reader_options()
        assert "has_header" not in options
        assert "_provided_header_columns" not in options

    def test_derive_reader_options_header_not_dict(self):
        """Test derive_reader_options with header not being a dict."""
        schema = {
            "x-csv": {
                "header": "invalid"
            }
        }
        importer = CsvSchemaImporter(schema)

        options = importer.derive_reader_options()
        assert "has_header" not in options
        assert "_provided_header_columns" not in options

    def test_derive_reader_options_extra_columns_drop(self):
        """Test derive_reader_options with extraColumns drop policy."""
        schema = {
            "x-csv": {
                "extraColumns": "drop"
            }
        }
        importer = CsvSchemaImporter(schema)

        options = importer.derive_reader_options()
        assert options["truncate_ragged_lines"] is True

    def test_derive_reader_options_extra_columns_other(self):
        """Test derive_reader_options with non-drop extraColumns policy."""
        schema = {
            "x-csv": {
                "extraColumns": "keep"
            }
        }
        importer = CsvSchemaImporter(schema)

        options = importer.derive_reader_options()
        assert "truncate_ragged_lines" not in options

    def test_derive_reader_options_comprehensive(self):
        """Test derive_reader_options with all options configured."""
        schema = {
            "x-csv": {
                "encodingPriority": ["utf-8", "latin-1"],
                "delimiter": "\\t",
                "quotechar": "'",
                "nulls": {
                    "global": ["", "NULL"]
                },
                "header": {
                    "mode": "provided",
                    "columns": ["col1", "col2"]
                },
                "extraColumns": "drop"
            }
        }
        importer = CsvSchemaImporter(schema)

        options = importer.derive_reader_options()

        expected = {
            "encoding": "utf-8",
            "delimiter": "\t",
            "quote_char": "'",
            "null_values": ["", "NULL"],
            "has_header": False,
            "_provided_header_columns": ["col1", "col2"],
            "truncate_ragged_lines": True
        }

        assert options == expected

    def test_derive_reader_options_real_world_schema(self):
        """Test derive_reader_options with a real-world schema example."""
        # Using the badcsv1.json schema structure
        schema = {
            "x-csv": {
                "encodingPriority": ["utf-8-sig", "utf-8", "latin-1"],
                "delimiter": ",",
                "quotechar": "\"",
                "escapechar": "\\\\",
                "multiline": True,
                "header": {"mode": "stability_scan", "keywords": ["id","email","amount"]},
                "footer": {"mode": "regex", "pattern": "^(TOTAL|SUMMARY)\\b"},
                "nulls": {"global": ["", "NA", "N/A", "-"]},
                "case": {"standardizeNames": "postgres", "dedupeNames": "suffix"}
            }
        }

        importer = CsvSchemaImporter(schema)
        options = importer.derive_reader_options()

        expected = {
            "encoding": "utf-8-sig",
            "delimiter": ",",
            "quote_char": "\"",
            "null_values": ["", "NA", "N/A", "-"]
        }

        assert options == expected


if __name__ == "__main__":
    pytest.main([__file__])
