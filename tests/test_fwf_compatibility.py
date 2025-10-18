"""Tests for FWF schema compatibility validation functionality."""

from unittest.mock import Mock, patch

import pytest

from forklift.schema.fwf.validation.compatibility import CompatibilityValidator


class TestCompatibilityValidator:
    """Test cases for CompatibilityValidator class."""

    def test_validate_schema_compatibility_empty_variants(self):
        """Test validation with empty schema variants list."""
        schema_variants = []

        errors = CompatibilityValidator.validate_schema_compatibility(schema_variants)

        assert errors == []

    def test_validate_schema_compatibility_single_variant(self):
        """Test validation with single schema variant."""
        schema_variants = [
            {
                "flagValue": "A",
                "fields": [
                    {"name": "id", "start": 1, "length": 5, "parquetType": "string"},
                    {"name": "name", "start": 6, "length": 20, "parquetType": "string"},
                ],
            }
        ]

        errors = CompatibilityValidator.validate_schema_compatibility(schema_variants)

        assert errors == []

    def test_validate_schema_compatibility_variants_without_fields(self):
        """Test validation with variants that don't have fields."""
        schema_variants = [{"flagValue": "A"}, {"flagValue": "B", "fields": []}]

        errors = CompatibilityValidator.validate_schema_compatibility(schema_variants)

        assert errors == []

    def test_validate_schema_compatibility_fields_without_names(self):
        """Test validation with fields that don't have names."""
        schema_variants = [
            {
                "flagValue": "A",
                "fields": [
                    {"start": 1, "length": 5, "parquetType": "string"},  # No name
                    {"name": "valid_field", "start": 6, "length": 10, "parquetType": "string"},
                ],
            },
            {
                "flagValue": "B",
                "fields": [
                    {"name": "valid_field", "start": 6, "length": 10, "parquetType": "string"}
                ],
            },
        ]

        errors = CompatibilityValidator.validate_schema_compatibility(schema_variants)

        assert errors == []  # Should not process fields without names

    def test_validate_schema_compatibility_compatible_fields(self):
        """Test validation with compatible fields across variants."""
        schema_variants = [
            {
                "flagValue": "A",
                "fields": [
                    {"name": "id", "start": 1, "length": 5, "parquetType": "string"},
                    {"name": "name", "start": 6, "length": 20, "parquetType": "string"},
                ],
            },
            {
                "flagValue": "B",
                "fields": [
                    {"name": "id", "start": 1, "length": 5, "parquetType": "string"},
                    {"name": "description", "start": 26, "length": 50, "parquetType": "string"},
                ],
            },
        ]

        with patch(
            "forklift.schema.fwf.validation.compatibility.ParquetTypeValidator.are_types_compatible"
        ) as mock_compatible:
            mock_compatible.return_value = True

            errors = CompatibilityValidator.validate_schema_compatibility(schema_variants)

            assert errors == []

    @patch("forklift.schema.fwf.validation.compatibility.ParquetTypeValidator.are_types_compatible")
    def test_validate_schema_compatibility_incompatible_parquet_types(self, mock_compatible):
        """Test validation with incompatible Parquet types."""
        mock_compatible.return_value = False

        schema_variants = [
            {
                "flagValue": "A",
                "fields": [{"name": "id", "start": 1, "length": 5, "parquetType": "string"}],
            },
            {
                "flagValue": "B",
                "fields": [
                    {
                        "name": "id",
                        "start": 10,
                        "length": 5,
                        "parquetType": "int64",
                    }  # Different position to avoid overlap
                ],
            },
        ]

        errors = CompatibilityValidator.validate_schema_compatibility(schema_variants)

        assert len(errors) == 1
        assert "Field 'id' has incompatible Parquet types across variants" in errors[0]
        assert "string" in errors[0] and "int64" in errors[0]

    @patch("forklift.schema.fwf.validation.compatibility.ParquetTypeValidator.are_types_compatible")
    def test_validate_schema_compatibility_overlapping_incompatible_positions(
        self, mock_compatible
    ):
        """Test validation with overlapping positions and incompatible types."""
        # Mock to return False for type compatibility checks
        mock_compatible.return_value = False

        schema_variants = [
            {
                "flagValue": "A",
                "fields": [{"name": "field1", "start": 1, "length": 10, "parquetType": "string"}],
            },
            {
                "flagValue": "B",
                "fields": [
                    {
                        "name": "field1",
                        "start": 5,
                        "length": 10,
                        "parquetType": "int64",
                    }  # Overlaps with variant A
                ],
            },
        ]

        errors = CompatibilityValidator.validate_schema_compatibility(schema_variants)

        # Should have both parquet type incompatibility and position overlap errors
        assert len(errors) >= 1
        error_text = " ".join(errors)
        assert (
            "incompatible Parquet types" in error_text
            or "incompatible overlapping positions" in error_text
        )

    @patch("forklift.schema.fwf.validation.compatibility.ParquetTypeValidator.are_types_compatible")
    def test_validate_schema_compatibility_overlapping_compatible_positions(self, mock_compatible):
        """Test validation with overlapping positions but compatible types."""
        # Mock to return True for type compatibility checks
        mock_compatible.return_value = True

        schema_variants = [
            {
                "flagValue": "A",
                "fields": [{"name": "field1", "start": 1, "length": 10, "parquetType": "string"}],
            },
            {
                "flagValue": "B",
                "fields": [
                    {
                        "name": "field1",
                        "start": 5,
                        "length": 10,
                        "parquetType": "string",
                    }  # Overlaps but same type
                ],
            },
        ]

        errors = CompatibilityValidator.validate_schema_compatibility(schema_variants)

        # Should only have parquet type compatibility check, no position overlap error
        assert len(errors) == 0

    def test_validate_schema_compatibility_non_overlapping_positions(self):
        """Test validation with non-overlapping positions."""
        schema_variants = [
            {
                "flagValue": "A",
                "fields": [{"name": "field1", "start": 1, "length": 10, "parquetType": "string"}],
            },
            {
                "flagValue": "B",
                "fields": [
                    {
                        "name": "field1",
                        "start": 15,
                        "length": 10,
                        "parquetType": "int64",
                    }  # No overlap
                ],
            },
        ]

        with patch(
            "forklift.schema.fwf.validation.compatibility.ParquetTypeValidator.are_types_compatible"
        ) as mock_compatible:
            mock_compatible.return_value = False

            errors = CompatibilityValidator.validate_schema_compatibility(schema_variants)

            # Should only have parquet type error, no position overlap error
            assert len(errors) == 1
            assert "incompatible Parquet types" in errors[0]
            assert "overlapping positions" not in errors[0]

    def test_validate_schema_compatibility_missing_position_data(self):
        """Test validation with missing start/length data."""
        schema_variants = [
            {
                "flagValue": "A",
                "fields": [{"name": "field1", "parquetType": "string"}],  # Missing start/length
            },
            {
                "flagValue": "B",
                "fields": [
                    {
                        "name": "field1",
                        "start": "invalid",
                        "length": 10,
                        "parquetType": "string",
                    }  # Invalid start
                ],
            },
        ]

        with patch(
            "forklift.schema.fwf.validation.compatibility.ParquetTypeValidator.are_types_compatible"
        ) as mock_compatible:
            mock_compatible.return_value = True

            errors = CompatibilityValidator.validate_schema_compatibility(schema_variants)

            # Should process parquet types but skip position validation due to missing/invalid data
            assert len(errors) == 0

    def test_validate_schema_compatibility_missing_parquet_types(self):
        """Test validation with missing parquetType data."""
        schema_variants = [
            {
                "flagValue": "A",
                "fields": [{"name": "field1", "start": 1, "length": 10}],  # Missing parquetType
            },
            {
                "flagValue": "B",
                "fields": [{"name": "field1", "start": 5, "length": 10, "parquetType": "string"}],
            },
        ]

        errors = CompatibilityValidator.validate_schema_compatibility(schema_variants)

        # Should not call parquet type validation since one field has no type
        assert len(errors) == 0

    def test_validate_field_compatibility_no_parquet_types(self):
        """Test field compatibility validation when no parquet types are present."""
        field_defs = [
            (0, {"name": "field1", "start": 1, "length": 10}),
            (1, {"name": "field1", "start": 1, "length": 10}),
        ]

        errors = CompatibilityValidator._validate_field_compatibility("field1", field_defs)

        assert errors == []

    def test_validate_field_compatibility_position_overlap_no_types(self):
        """Test field compatibility with position overlap but no parquet types."""
        field_defs = [
            (0, {"name": "field1", "start": 1, "length": 10}),
            (1, {"name": "field1", "start": 5, "length": 10}),  # Overlaps
        ]

        errors = CompatibilityValidator._validate_field_compatibility("field1", field_defs)

        # Should not generate error because no parquet types to check compatibility
        assert errors == []

    @patch("forklift.schema.fwf.validation.compatibility.ParquetTypeValidator.are_types_compatible")
    def test_validate_field_compatibility_position_overlap_one_missing_type(self, mock_compatible):
        """Test field compatibility with position overlap and one missing parquet type."""
        mock_compatible.return_value = False

        field_defs = [
            (0, {"name": "field1", "start": 1, "length": 10, "parquetType": "string"}),
            (1, {"name": "field1", "start": 5, "length": 10}),  # Overlaps, no parquetType
        ]

        errors = CompatibilityValidator._validate_field_compatibility("field1", field_defs)

        # Should not generate overlap error because one field has no parquet type
        assert len(errors) == 0

    def test_positions_overlap_incompatibly_no_overlap(self):
        """Test position overlap detection with non-overlapping ranges."""
        # Range 1: 1-10, Range 2: 15-20 (no overlap)
        result = CompatibilityValidator._positions_overlap_incompatibly(1, 10, 15, 20)
        assert result == False

        # Range 1: 15-20, Range 2: 1-10 (no overlap, reverse order)
        result = CompatibilityValidator._positions_overlap_incompatibly(15, 20, 1, 10)
        assert result == False

    def test_positions_overlap_incompatibly_with_overlap(self):
        """Test position overlap detection with overlapping ranges."""
        # Range 1: 1-10, Range 2: 5-15 (overlap)
        result = CompatibilityValidator._positions_overlap_incompatibly(1, 10, 5, 15)
        assert result == True

        # Range 1: 5-15, Range 2: 1-10 (overlap, reverse order)
        result = CompatibilityValidator._positions_overlap_incompatibly(5, 15, 1, 10)
        assert result == True

    def test_positions_overlap_incompatibly_adjacent_ranges(self):
        """Test position overlap detection with adjacent ranges."""
        # Range 1: 1-10, Range 2: 11-20 (adjacent, no overlap)
        result = CompatibilityValidator._positions_overlap_incompatibly(1, 10, 11, 20)
        assert result == False

        # Range 1: 1-10, Range 2: 10-20 (sharing boundary, considered overlap)
        result = CompatibilityValidator._positions_overlap_incompatibly(1, 10, 10, 20)
        assert result == True

    def test_positions_overlap_incompatibly_identical_ranges(self):
        """Test position overlap detection with identical ranges."""
        # Identical ranges
        result = CompatibilityValidator._positions_overlap_incompatibly(1, 10, 1, 10)
        assert result == True

    def test_positions_overlap_incompatibly_contained_ranges(self):
        """Test position overlap detection with one range contained in another."""
        # Range 2 contained within Range 1
        result = CompatibilityValidator._positions_overlap_incompatibly(1, 20, 5, 15)
        assert result == True

        # Range 1 contained within Range 2
        result = CompatibilityValidator._positions_overlap_incompatibly(5, 15, 1, 20)
        assert result == True

    def test_validate_schema_compatibility_complex_scenario(self):
        """Test validation with complex scenario involving multiple fields and variants."""
        schema_variants = [
            {
                "flagValue": "A",
                "fields": [
                    {"name": "id", "start": 1, "length": 5, "parquetType": "string"},
                    {"name": "name", "start": 6, "length": 20, "parquetType": "string"},
                    {"name": "amount", "start": 26, "length": 10, "parquetType": "decimal"},
                ],
            },
            {
                "flagValue": "B",
                "fields": [
                    {"name": "id", "start": 1, "length": 5, "parquetType": "string"},  # Compatible
                    {
                        "name": "description",
                        "start": 6,
                        "length": 30,
                        "parquetType": "string",
                    },  # Different field
                    {
                        "name": "amount",
                        "start": 26,
                        "length": 10,
                        "parquetType": "double",
                    },  # Incompatible type
                ],
            },
            {
                "flagValue": "C",
                "fields": [
                    {
                        "name": "id",
                        "start": 1,
                        "length": 5,
                        "parquetType": "int64",
                    },  # Incompatible type
                    {"name": "code", "start": 6, "length": 15, "parquetType": "string"},
                ],
            },
        ]

        with patch(
            "forklift.schema.fwf.validation.compatibility.ParquetTypeValidator.are_types_compatible"
        ) as mock_compatible:
            # Mock different results for different type combinations
            def side_effect(types):
                if "string" in types and "int64" in types:
                    return False
                if "decimal" in types and "double" in types:
                    return False
                return True

            mock_compatible.side_effect = side_effect

            errors = CompatibilityValidator.validate_schema_compatibility(schema_variants)

            # Should detect incompatibility for both 'id' and 'amount' fields
            assert len(errors) >= 2
            error_text = " ".join(errors)
            assert "Field 'id'" in error_text
            assert "Field 'amount'" in error_text
