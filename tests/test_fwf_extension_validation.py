"""Tests for FWF extension validation functionality."""

import pytest
from forklift.schema.fwf.validation.fwf_extension import FwfExtensionValidator


class TestFwfExtensionValidator:
    """Test cases for FwfExtensionValidator class."""

    def test_validate_empty_extension(self):
        """Test validation with empty/None extension."""
        errors = FwfExtensionValidator.validate(None)
        assert len(errors) == 1
        assert "Missing required 'x-fwf' extension" in errors[0]

        errors = FwfExtensionValidator.validate({})
        assert len(errors) == 1
        assert "Missing required 'x-fwf' extension" in errors[0]

    def test_validate_valid_minimal_extension(self):
        """Test validation with minimal valid extension."""
        fwf_ext = {"encoding": "utf-8"}
        errors = FwfExtensionValidator.validate(fwf_ext)
        assert errors == []

    def test_validate_encoding_valid(self):
        """Test validation with valid encoding values."""
        valid_encodings = ["utf-8", "utf-8-sig", "latin-1", "cp1252", "ascii"]

        for encoding in valid_encodings:
            fwf_ext = {"encoding": encoding}
            errors = FwfExtensionValidator.validate(fwf_ext)
            assert errors == [], f"Valid encoding {encoding} should not produce errors"

    def test_validate_encoding_invalid(self):
        """Test validation with invalid encoding values."""
        invalid_encodings = ["invalid-encoding", "utf-16", "iso-8859-1", "windows-1252"]

        for encoding in invalid_encodings:
            fwf_ext = {"encoding": encoding}
            errors = FwfExtensionValidator.validate(fwf_ext)
            assert len(errors) == 1
            assert f"Invalid encoding '{encoding}'" in errors[0]
            assert "must be one of" in errors[0]

    def test_validate_header_rows_valid(self):
        """Test validation with valid headerRows values."""
        valid_values = [0, 1, 5, 100]

        for value in valid_values:
            fwf_ext = {"headerRows": value}
            errors = FwfExtensionValidator.validate(fwf_ext)
            assert errors == [], f"Valid headerRows {value} should not produce errors"

    def test_validate_header_rows_invalid(self):
        """Test validation with invalid headerRows values."""
        invalid_values = [-1, -5, "5", 3.14, None, [], {}]

        for value in invalid_values:
            fwf_ext = {"headerRows": value}
            errors = FwfExtensionValidator.validate(fwf_ext)
            assert len(errors) == 1
            assert "headerRows must be a non-negative integer" in errors[0]

    def test_validate_footer_rows_valid(self):
        """Test validation with valid footerRows values."""
        valid_values = [0, 1, 5, 100]

        for value in valid_values:
            fwf_ext = {"footerRows": value}
            errors = FwfExtensionValidator.validate(fwf_ext)
            assert errors == [], f"Valid footerRows {value} should not produce errors"

    def test_validate_footer_rows_invalid(self):
        """Test validation with invalid footerRows values."""
        invalid_values = [-1, -5, "5", 3.14, None, [], {}]

        for value in invalid_values:
            fwf_ext = {"footerRows": value}
            errors = FwfExtensionValidator.validate(fwf_ext)
            assert len(errors) == 1
            assert "footerRows must be a non-negative integer" in errors[0]

    def test_validate_trim_config_valid(self):
        """Test validation with valid trim configuration."""
        valid_configs = [
            {},
            {"field1": True},
            {"field1": False, "field2": True},
            {"name": True, "id": False, "description": True}
        ]

        for trim_config in valid_configs:
            fwf_ext = {"trim": trim_config}
            errors = FwfExtensionValidator.validate(fwf_ext)
            assert errors == [], f"Valid trim config {trim_config} should not produce errors"

    def test_validate_trim_config_invalid_type(self):
        """Test validation with invalid trim configuration type."""
        # Only truthy non-dict values should trigger validation errors
        invalid_configs = ["string", 123, [1, 2], True]

        for trim_config in invalid_configs:
            fwf_ext = {"trim": trim_config}
            errors = FwfExtensionValidator.validate(fwf_ext)
            assert len(errors) == 1
            assert "trim configuration must be a dictionary" in errors[0]

        # Falsy values should not trigger validation errors
        falsy_configs = [None, [], {}, False, ""]
        for trim_config in falsy_configs:
            fwf_ext = {"trim": trim_config}
            errors = FwfExtensionValidator.validate(fwf_ext)
            # These should not produce trim-related errors
            trim_errors = [e for e in errors if "trim" in e]
            assert len(trim_errors) == 0

    def test_validate_trim_config_invalid_values(self):
        """Test validation with invalid trim configuration values."""
        invalid_configs = [
            {"field1": "true"},
            {"field1": 1},
            {"field1": None},
            {"field1": []},
            {"field1": True, "field2": "false"}
        ]

        for trim_config in invalid_configs:
            fwf_ext = {"trim": trim_config}
            errors = FwfExtensionValidator.validate(fwf_ext)
            assert len(errors) >= 1
            assert any("must be a boolean" in error for error in errors)

    def test_validate_nulls_config_valid(self):
        """Test validation with valid nulls configuration."""
        valid_configs = [
            {},
            {"global": []},
            {"global": ["", "NULL", "N/A"]},
            {"perColumn": {}},
            {"perColumn": {"field1": ["", "NULL"]}},
            {"global": [""], "perColumn": {"field1": ["NULL"]}}
        ]

        for nulls_config in valid_configs:
            fwf_ext = {"nulls": nulls_config}
            errors = FwfExtensionValidator.validate(fwf_ext)
            assert errors == [], f"Valid nulls config {nulls_config} should not produce errors"

    def test_validate_nulls_config_invalid_global(self):
        """Test validation with invalid nulls.global configuration."""
        invalid_configs = [
            {"global": "string"},
            {"global": 123},
            {"global": {}},
            {"global": None},
            {"global": True}
        ]

        for nulls_config in invalid_configs:
            fwf_ext = {"nulls": nulls_config}
            errors = FwfExtensionValidator.validate(fwf_ext)
            assert len(errors) == 1
            assert "x-fwf.nulls.global must be a list" in errors[0]

    def test_validate_nulls_config_invalid_per_column(self):
        """Test validation with invalid nulls.perColumn configuration."""
        invalid_configs = [
            {"perColumn": "string"},
            {"perColumn": 123},
            {"perColumn": []},
            {"perColumn": None},
            {"perColumn": True}
        ]

        for nulls_config in invalid_configs:
            fwf_ext = {"nulls": nulls_config}
            errors = FwfExtensionValidator.validate(fwf_ext)
            assert len(errors) == 1
            assert "x-fwf.nulls.perColumn must be a dictionary" in errors[0]

    def test_validate_case_config_valid(self):
        """Test validation with valid case configuration."""
        valid_configs = [
            {},
            {"standardizeNames": "postgres"},
            {"standardizeNames": "snake_case"},
            {"standardizeNames": "camelCase"},
            {"dedupeNames": "suffix"},
            {"dedupeNames": "prefix"},
            {"dedupeNames": "error"},
            {"standardizeNames": "postgres", "dedupeNames": "suffix"}
        ]

        for case_config in valid_configs:
            fwf_ext = {"case": case_config}
            errors = FwfExtensionValidator.validate(fwf_ext)
            assert errors == [], f"Valid case config {case_config} should not produce errors"

    def test_validate_case_config_invalid_standardize_names(self):
        """Test validation with invalid standardizeNames values."""
        # Only truthy invalid values should trigger validation errors
        invalid_values = ["UPPERCASE", "lowercase", "PascalCase", "kebab-case"]

        for value in invalid_values:
            fwf_ext = {"case": {"standardizeNames": value}}
            errors = FwfExtensionValidator.validate(fwf_ext)
            assert len(errors) == 1
            assert f"Invalid standardizeNames value '{value}'" in errors[0]

        # Falsy values should not trigger validation errors
        falsy_values = ["", None, 0, False]
        for value in falsy_values:
            fwf_ext = {"case": {"standardizeNames": value}}
            errors = FwfExtensionValidator.validate(fwf_ext)
            # These should not produce standardizeNames-related errors
            standardize_errors = [e for e in errors if "standardizeNames" in e]
            assert len(standardize_errors) == 0

    def test_validate_case_config_invalid_dedupe_names(self):
        """Test validation with invalid dedupeNames values."""
        # Only truthy invalid values should trigger validation errors
        invalid_values = ["append", "prepend", "fail", "ignore"]

        for value in invalid_values:
            fwf_ext = {"case": {"dedupeNames": value}}
            errors = FwfExtensionValidator.validate(fwf_ext)
            assert len(errors) == 1
            assert f"Invalid dedupeNames value '{value}'" in errors[0]

        # Falsy values should not trigger validation errors
        falsy_values = ["", None, 0, False]
        for value in falsy_values:
            fwf_ext = {"case": {"dedupeNames": value}}
            errors = FwfExtensionValidator.validate(fwf_ext)
            # These should not produce dedupeNames-related errors
            dedupe_errors = [e for e in errors if "dedupeNames" in e]
            assert len(dedupe_errors) == 0

    def test_validate_case_config_non_dict(self):
        """Test validation with non-dictionary case configuration."""
        non_dict_configs = ["string", 123, [], None, True]

        for case_config in non_dict_configs:
            fwf_ext = {"case": case_config}
            errors = FwfExtensionValidator.validate(fwf_ext)
            # Non-dict case configs are ignored (no errors generated)
            assert errors == []

    def test_validate_multiple_errors(self):
        """Test validation with multiple invalid configurations."""
        fwf_ext = {
            "encoding": "invalid-encoding",
            "headerRows": -1,
            "footerRows": "invalid",
            "trim": "not-a-dict",
            "nulls": {"global": "not-a-list", "perColumn": "not-a-dict"},
            "case": {"standardizeNames": "invalid", "dedupeNames": "invalid"}
        }

        errors = FwfExtensionValidator.validate(fwf_ext)

        # Should have multiple errors
        assert len(errors) >= 6

        # Check specific error types are present
        error_text = " ".join(errors)
        assert "Invalid encoding" in error_text
        assert "headerRows must be a non-negative integer" in error_text
        assert "footerRows must be a non-negative integer" in error_text
        assert "trim configuration must be a dictionary" in error_text
        assert "x-fwf.nulls.global must be a list" in error_text
        assert "x-fwf.nulls.perColumn must be a dictionary" in error_text
        assert "Invalid standardizeNames value" in error_text
        assert "Invalid dedupeNames value" in error_text

    def test_validate_comprehensive_valid_config(self):
        """Test validation with comprehensive valid configuration."""
        fwf_ext = {
            "encoding": "utf-8",
            "headerRows": 2,
            "footerRows": 1,
            "trim": {
                "name": True,
                "id": False,
                "description": True
            },
            "nulls": {
                "global": ["", "NULL", "N/A"],
                "perColumn": {
                    "optional_field": ["", "NULL", "MISSING"]
                }
            },
            "case": {
                "standardizeNames": "snake_case",
                "dedupeNames": "suffix"
            }
        }

        errors = FwfExtensionValidator.validate(fwf_ext)
        assert errors == [], "Comprehensive valid config should not produce any errors"

    def test_validate_empty_nested_configs(self):
        """Test validation with empty nested configurations."""
        fwf_ext = {
            "trim": {},
            "nulls": {},
            "case": {}
        }

        errors = FwfExtensionValidator.validate(fwf_ext)
        assert errors == [], "Empty nested configs should be valid"

    def test_validate_partial_nulls_config(self):
        """Test validation with partial nulls configurations."""
        # Test with only global
        fwf_ext = {"nulls": {"global": ["", "NULL"]}}
        errors = FwfExtensionValidator.validate(fwf_ext)
        assert errors == []

        # Test with only perColumn
        fwf_ext = {"nulls": {"perColumn": {"field1": ["NULL"]}}}
        errors = FwfExtensionValidator.validate(fwf_ext)
        assert errors == []

    def test_validate_partial_case_config(self):
        """Test validation with partial case configurations."""
        # Test with only standardizeNames
        fwf_ext = {"case": {"standardizeNames": "postgres"}}
        errors = FwfExtensionValidator.validate(fwf_ext)
        assert errors == []

        # Test with only dedupeNames
        fwf_ext = {"case": {"dedupeNames": "prefix"}}
        errors = FwfExtensionValidator.validate(fwf_ext)
        assert errors == []
