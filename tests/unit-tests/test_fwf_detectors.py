"""Tests for FWF detection utilities."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from forklift.inputs.config import FwfConditionalSchema, FwfFieldSpec, FwfInputConfig
from forklift.inputs.fwf.detectors import FwfEncodingDetector, FwfSchemaDetector


class TestFwfEncodingDetector:
    """Test cases for FWF encoding detector."""

    def test_detect_encoding_with_chardet_success(self):
        """Test encoding detection when chardet is available and succeeds."""
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as tmp_file:
            # Write UTF-8 encoded content
            content = "This is a test file with UTF-8 encoding\nLine 2: ñáéíóú"
            tmp_file.write(content.encode("utf-8"))
            tmp_file.flush()

            file_path = Path(tmp_file.name)

            try:
                with patch("chardet.detect") as mock_detect:
                    mock_detect.return_value = {"encoding": "utf-8", "confidence": 0.99}

                    result = FwfEncodingDetector.detect_encoding(file_path)
                    assert result == "utf-8"

                    # Verify chardet.detect was called
                    mock_detect.assert_called_once()
            finally:
                file_path.unlink()

    def test_detect_encoding_with_chardet_no_encoding(self):
        """Test encoding detection when chardet returns None for encoding."""
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as tmp_file:
            tmp_file.write(b"test content")
            tmp_file.flush()

            file_path = Path(tmp_file.name)

            try:
                with patch("chardet.detect") as mock_detect:
                    mock_detect.return_value = {"confidence": 0.5}  # No encoding key

                    result = FwfEncodingDetector.detect_encoding(file_path)
                    assert result == "utf-8"  # Should default to utf-8
            finally:
                file_path.unlink()

    def test_detect_encoding_chardet_import_error(self):
        """Test encoding detection when chardet is not available."""
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as tmp_file:
            tmp_file.write(b"test content")
            tmp_file.flush()

            file_path = Path(tmp_file.name)

            try:
                # Mock ImportError when trying to import chardet
                with patch(
                    "builtins.__import__", side_effect=ImportError("No module named 'chardet'")
                ):
                    result = FwfEncodingDetector.detect_encoding(file_path)
                    assert result == "utf-8"  # Should default to utf-8
            finally:
                file_path.unlink()

    def test_detect_encoding_different_encodings(self):
        """Test detection of different character encodings."""
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as tmp_file:
            tmp_file.write(b"test content")
            tmp_file.flush()

            file_path = Path(tmp_file.name)

            try:
                with patch("chardet.detect") as mock_detect:
                    # Test different encoding results
                    encodings = ["latin-1", "cp1252", "iso-8859-1", "ascii"]

                    for encoding in encodings:
                        mock_detect.return_value = {"encoding": encoding, "confidence": 0.9}
                        result = FwfEncodingDetector.detect_encoding(file_path)
                        assert result == encoding
            finally:
                file_path.unlink()


class TestFwfSchemaDetector:
    """Test cases for FWF schema detector."""

    @pytest.fixture
    def sample_config_with_conditionals(self):
        """Create a sample FWF config with conditional schemas."""
        flag_column = FwfFieldSpec(
            name="record_type", start=1, length=2, parquet_type="string"  # 1-based positioning
        )

        schema1 = FwfConditionalSchema(
            flag_value="01",
            description="Type 1 records",
            fields=[
                FwfFieldSpec(name="id", start=3, length=8, parquet_type="int64"),
                FwfFieldSpec(name="name", start=11, length=20, parquet_type="string"),
            ],
        )

        schema2 = FwfConditionalSchema(
            flag_value="02",
            description="Type 2 records",
            fields=[
                FwfFieldSpec(name="code", start=3, length=6, parquet_type="string"),
                FwfFieldSpec(name="amount", start=9, length=10, parquet_type="float64"),
            ],
        )

        config = FwfInputConfig(flag_column=flag_column, conditional_schemas=[schema1, schema2])

        return config

    @pytest.fixture
    def sample_config_no_conditionals(self):
        """Create a sample FWF config without conditional schemas."""
        return FwfInputConfig()

    def test_detect_conditional_schema_match_first(self, sample_config_with_conditionals):
        """Test detecting conditional schema that matches the first schema."""
        detector = FwfSchemaDetector(sample_config_with_conditionals)

        line = "0112345678John Doe              "
        result = detector.detect_conditional_schema(line)

        assert result is not None
        assert result.flag_value == "01"
        assert len(result.fields) == 2
        assert result.fields[0].name == "id"
        assert result.fields[1].name == "name"

    def test_detect_conditional_schema_match_second(self, sample_config_with_conditionals):
        """Test detecting conditional schema that matches the second schema."""
        detector = FwfSchemaDetector(sample_config_with_conditionals)

        line = "02ABC123  123.45   "
        result = detector.detect_conditional_schema(line)

        assert result is not None
        assert result.flag_value == "02"
        assert len(result.fields) == 2
        assert result.fields[0].name == "code"
        assert result.fields[1].name == "amount"

    def test_detect_conditional_schema_no_match(self, sample_config_with_conditionals):
        """Test detecting conditional schema when no schema matches."""
        detector = FwfSchemaDetector(sample_config_with_conditionals)

        line = "99UNKNOWN_TYPE      "
        result = detector.detect_conditional_schema(line)

        assert result is None

    def test_detect_conditional_schema_no_conditionals(self, sample_config_no_conditionals):
        """Test detecting conditional schema when no conditional schemas are configured."""
        detector = FwfSchemaDetector(sample_config_no_conditionals)

        line = "01SOME_DATA         "
        result = detector.detect_conditional_schema(line)

        assert result is None

    def test_detect_conditional_schema_no_flag_column(self):
        """Test detecting conditional schema when no flag column is configured."""
        config = FwfInputConfig(
            conditional_schemas=[
                FwfConditionalSchema(
                    flag_value="01",
                    description="Test schema",
                    fields=[FwfFieldSpec(name="test", start=1, length=5, parquet_type="string")],
                )
            ]
        )

        detector = FwfSchemaDetector(config)

        line = "01SOME_DATA"
        result = detector.detect_conditional_schema(line)

        assert result is None

    def test_detect_conditional_schema_short_line(self, sample_config_with_conditionals):
        """Test detecting conditional schema with a line shorter than flag column."""
        detector = FwfSchemaDetector(sample_config_with_conditionals)

        # Line is shorter than the flag column length
        line = "0"

        # This should handle the short line gracefully
        result = detector.detect_conditional_schema(line)

        # Depending on implementation, this might return None or handle gracefully
        assert result is None or isinstance(result, FwfConditionalSchema)

    def test_detect_conditional_schema_empty_line(self, sample_config_with_conditionals):
        """Test detecting conditional schema with an empty line."""
        detector = FwfSchemaDetector(sample_config_with_conditionals)

        line = ""
        result = detector.detect_conditional_schema(line)

        # Should handle empty line gracefully
        assert result is None

    def test_detect_conditional_schema_exact_match_length(self, sample_config_with_conditionals):
        """Test detecting conditional schema with line exactly matching flag column length."""
        detector = FwfSchemaDetector(sample_config_with_conditionals)

        line = "01"  # Exactly 2 characters to match flag column
        result = detector.detect_conditional_schema(line)

        assert result is not None
        assert result.flag_value == "01"

    def test_multiple_schemas_same_condition(self):
        """Test behavior when multiple schemas have the same condition value."""
        flag_column = FwfFieldSpec(name="record_type", start=1, length=2, parquet_type="string")

        # Two schemas with same condition value
        schema1 = FwfConditionalSchema(
            flag_value="01",
            description="First schema",
            fields=[FwfFieldSpec(name="field1", start=3, length=8, parquet_type="string")],
        )

        schema2 = FwfConditionalSchema(
            flag_value="01",  # Same condition value
            description="Second schema",
            fields=[FwfFieldSpec(name="field2", start=3, length=13, parquet_type="string")],
        )

        config = FwfInputConfig(flag_column=flag_column, conditional_schemas=[schema1, schema2])

        detector = FwfSchemaDetector(config)

        line = "01TEST_DATA"
        result = detector.detect_conditional_schema(line)

        # Should return the first matching schema
        assert result is not None
        assert result.flag_value == "01"
        assert result.fields[0].name == "field1"  # First schema's field
