"""Tests for configuration parser functionality."""

from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from forklift.schema.processors.config_parser import ConfigurationParser


class TestConfigurationParser:
    """Test cases for ConfigurationParser class."""

    @pytest.fixture
    def parser(self):
        """Create a ConfigurationParser instance for testing."""
        return ConfigurationParser()

    @pytest.fixture
    def sample_table(self):
        """Create a sample PyArrow table for testing."""
        return pa.table(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
                "email": [
                    "alice@test.com",
                    "bob@test.com",
                    "charlie@test.com",
                    "david@test.com",
                    "eve@test.com",
                ],
                "age": [25, 30, 35, 40, 45],
            }
        )

    @pytest.fixture
    def table_with_primary_key_candidates(self):
        """Create a table with various primary key candidates."""
        return pa.table(
            {
                "user_id": [1, 2, 3, 4, 5],  # Good PK candidate
                "uuid_field": [
                    "a1b2c3",
                    "b2c3d4",
                    "c3d4e5",
                    "d4e5f6",
                    "e5f6g7",
                ],  # Good PK candidate
                "name": ["Alice", "Bob", "Charlie", "David", "Eve"],  # Poor PK candidate
                "email": [
                    "alice@test.com",
                    "bob@test.com",
                    "charlie@test.com",
                    "david@test.com",
                    "eve@test.com",
                ],  # OK PK candidate
                "status": [
                    "active",
                    "active",
                    "inactive",
                    "active",
                    "inactive",
                ],  # Poor PK candidate
            }
        )

    def test_init(self, parser):
        """Test ConfigurationParser initialization."""
        assert parser is not None
        assert hasattr(parser, "analyzer")

    @patch("forklift.schema.processors.config_parser.TransformationAnalyzer")
    def test_generate_transformation_extension_with_suggestions(
        self, mock_analyzer_class, parser, sample_table
    ):
        """Test generating transformation extension with column suggestions."""
        # Mock the analyzer
        mock_analyzer = Mock()
        mock_analyzer.analyze_column_for_transformations.side_effect = [
            {"transform": "numeric"},
            {"transform": "text_clean"},
            {"transform": "email_validate"},
            {"transform": "numeric"},
        ]
        mock_analyzer.get_transformation_types_config.return_value = {"types": "config"}
        parser.analyzer = mock_analyzer

        result = parser.generate_transformation_extension(sample_table)

        assert "description" in result
        assert "version" in result
        assert "global_settings" in result
        assert "column_transformations" in result
        assert "transformation_types" in result

        # Check global settings structure
        assert "nan_handling" in result["global_settings"]
        assert "error_handling" in result["global_settings"]

        # Check column transformations were generated
        assert len(result["column_transformations"]) == 4
        assert "id" in result["column_transformations"]
        assert "name" in result["column_transformations"]

    @patch("forklift.schema.processors.config_parser.TransformationAnalyzer")
    def test_generate_transformation_extension_no_suggestions(
        self, mock_analyzer_class, parser, sample_table
    ):
        """Test generating transformation extension when no suggestions are found."""
        # Mock the analyzer to return no suggestions
        mock_analyzer = Mock()
        mock_analyzer.analyze_column_for_transformations.return_value = None
        mock_analyzer.get_transformation_types_config.return_value = {"types": "config"}
        parser.analyzer = mock_analyzer

        result = parser.generate_transformation_extension(sample_table)

        assert "column_transformations" in result
        assert len(result["column_transformations"]) == 0

    def test_generate_primary_key_config_user_specified_single(self, parser, sample_table):
        """Test generating primary key config with user-specified single column."""
        config = Mock()
        config.user_specified_primary_key = ["id"]
        config.infer_primary_key_from_metadata = False

        result = parser.generate_primary_key_config(sample_table, config)

        assert result is not None
        assert result["columns"] == ["id"]
        assert result["type"] == "single"
        assert result["enforceUniqueness"] == True
        assert result["allowNulls"] == False
        assert "User-specified primary key" in result["description"]

    def test_generate_primary_key_config_user_specified_composite(self, parser, sample_table):
        """Test generating primary key config with user-specified composite key."""
        config = Mock()
        config.user_specified_primary_key = ["id", "name"]
        config.infer_primary_key_from_metadata = False

        result = parser.generate_primary_key_config(sample_table, config)

        assert result is not None
        assert result["columns"] == ["id", "name"]
        assert result["type"] == "composite"
        assert result["enforceUniqueness"] == True
        assert result["allowNulls"] == False

    def test_generate_primary_key_config_infer_from_metadata(self, parser, sample_table):
        """Test generating primary key config by inferring from metadata."""
        config = Mock()
        config.user_specified_primary_key = None
        config.infer_primary_key_from_metadata = True

        with patch.object(parser, "_infer_primary_key_from_metadata") as mock_infer:
            mock_infer.return_value = {
                "description": "Inferred primary key",
                "columns": ["id"],
                "type": "single",
            }

            result = parser.generate_primary_key_config(sample_table, config)

            mock_infer.assert_called_once_with(sample_table)
            assert result is not None
            assert result["columns"] == ["id"]

    def test_generate_primary_key_config_no_config(self, parser, sample_table):
        """Test generating primary key config when no configuration is provided."""
        config = Mock()
        config.user_specified_primary_key = None
        config.infer_primary_key_from_metadata = False

        result = parser.generate_primary_key_config(sample_table, config)

        assert result is None

    def test_infer_primary_key_from_metadata_no_metadata(self, parser, sample_table):
        """Test inferring primary key when no metadata is available."""
        with patch(
            "forklift.schema.processors.metadata.MetadataGenerator"
        ) as mock_metadata_gen_class:
            # Mock MetadataGenerator to return None
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = None
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is None

    def test_infer_primary_key_from_metadata_no_column_metadata(self, parser, sample_table):
        """Test inferring primary key when metadata lacks column_metadata."""
        with patch(
            "forklift.schema.processors.metadata.MetadataGenerator"
        ) as mock_metadata_gen_class:
            # Mock MetadataGenerator to return metadata without column_metadata
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {"some_key": "some_value"}
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is None

    def test_infer_primary_key_from_metadata_no_candidates(self, parser, sample_table):
        """Test inferring primary key when no good candidates are found."""
        with patch(
            "forklift.schema.processors.metadata.MetadataGenerator"
        ) as mock_metadata_gen_class:
            # Mock MetadataGenerator to return metadata with poor candidates
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                "column_metadata": {
                    "name": {
                        "null_percentage": 0.0,
                        "uniqueness_ratio": 0.8,  # Too low
                        "distinct_count": 100,
                    },
                    "status": {
                        "null_percentage": 10.0,  # Has nulls
                        "uniqueness_ratio": 0.3,
                        "distinct_count": 5,
                    },
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is None

    def test_infer_primary_key_from_metadata_good_candidate(
        self, parser, table_with_primary_key_candidates
    ):
        """Test inferring primary key when a good candidate is found."""
        with patch(
            "forklift.schema.processors.metadata.MetadataGenerator"
        ) as mock_metadata_gen_class:
            # Mock MetadataGenerator to return metadata with good candidates
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                "column_metadata": {
                    "user_id": {
                        "null_percentage": 0.0,
                        "uniqueness_ratio": 1.0,
                        "distinct_count": 5,
                    },
                    "uuid_field": {
                        "null_percentage": 0.0,
                        "uniqueness_ratio": 1.0,
                        "distinct_count": 5,
                    },
                    "name": {"null_percentage": 0.0, "uniqueness_ratio": 1.0, "distinct_count": 5},
                    "email": {
                        "null_percentage": 0.0,
                        "uniqueness_ratio": 1.0,
                        "distinct_count": 5,
                    },
                    "status": {
                        "null_percentage": 0.0,
                        "uniqueness_ratio": 0.4,
                        "distinct_count": 2,
                    },
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(table_with_primary_key_candidates)

            assert result is not None
            assert result["columns"] == ["user_id"]  # Should pick user_id due to 'id' pattern
            assert result["type"] == "single"
            assert result["enforceUniqueness"] == True
            assert result["allowNulls"] == False
            assert "inference_metadata" in result

    def test_infer_primary_key_uniqueness_ratio_099_branch(self, parser, sample_table):
        """Test inferring primary key with exactly 0.99 uniqueness to cover line 126."""
        with patch(
            "forklift.schema.processors.metadata.MetadataGenerator"
        ) as mock_metadata_gen_class:
            # Mock MetadataGenerator with exactly 0.99 uniqueness ratio
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                "column_metadata": {
                    "test_id": {
                        "null_percentage": 0.0,
                        "uniqueness_ratio": 0.99,  # Exactly 0.99 to trigger line 126
                        "distinct_count": 1000,  # No penalty
                        # Gets 8 points for 0.99 uniqueness + 5 for 'id' pattern = 13 total
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is not None
            assert result["columns"] == ["test_id"]
            assert result["inference_metadata"]["score"] == 13

    def test_infer_primary_key_uniqueness_ratio_095_branch(self, parser, sample_table):
        """Test inferring primary key with exactly 0.95 uniqueness to cover line 128."""
        with patch(
            "forklift.schema.processors.metadata.MetadataGenerator"
        ) as mock_metadata_gen_class:
            # Mock MetadataGenerator with exactly 0.95 uniqueness ratio
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                "column_metadata": {
                    "test_id": {
                        "null_percentage": 0.0,
                        "uniqueness_ratio": 0.95,  # Exactly 0.95 to trigger line 128
                        "distinct_count": 1000,  # No penalty
                        # Gets 5 points for 0.95 uniqueness + 5 for 'id' pattern = 10 total
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is not None
            assert result["columns"] == ["test_id"]
            assert result["inference_metadata"]["score"] == 10

    def test_infer_primary_key_key_pk_naming_patterns(self, parser, sample_table):
        """Test inferring primary key with 'key' and 'pk' naming patterns to cover lines 132-133."""
        with patch(
            "forklift.schema.processors.metadata.MetadataGenerator"
        ) as mock_metadata_gen_class:
            # Mock MetadataGenerator with key/pk patterns that don't contain 'id'
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                "column_metadata": {
                    "primary_key": {  # Contains 'key' but not 'id'
                        "null_percentage": 0.0,
                        "uniqueness_ratio": 1.0,  # 10 points
                        "distinct_count": 1000,  # 0 penalty
                        # Gets +3 for 'key' pattern = 13 total
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is not None
            assert result["columns"] == ["primary_key"]
            assert result["inference_metadata"]["score"] == 13

    def test_infer_primary_key_uuid_guid_naming_patterns(self, parser, sample_table):
        """Test inferring primary key with 'uuid'/'guid' patterns to cover lines 130-131."""
        with patch(
            "forklift.schema.processors.metadata.MetadataGenerator"
        ) as mock_metadata_gen_class:
            # Mock MetadataGenerator with uuid/guid patterns that should get the 4-point bonus
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                "column_metadata": {
                    "session_uuid": {  # Contains 'uuid' - should get uuid bonus of +4
                        "null_percentage": 0.0,
                        "uniqueness_ratio": 1.0,  # 10 points
                        "distinct_count": 1000,  # 0 penalty
                        # Gets +4 for 'uuid' pattern = 14 total
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is not None
            assert result["columns"] == ["session_uuid"]
            assert result["inference_metadata"]["score"] == 14

    def test_infer_primary_key_medium_distinct_count_penalty(self, parser, sample_table):
        """Test inferring primary key with medium distinct count penalty to cover line 137."""
        with patch(
            "forklift.schema.processors.metadata.MetadataGenerator"
        ) as mock_metadata_gen_class:
            # Mock MetadataGenerator with medium distinct count (10001-100000 range)
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                "column_metadata": {
                    "test_id": {
                        "null_percentage": 0.0,
                        "uniqueness_ratio": 1.0,  # 10 points
                        "distinct_count": 50000,  # -1 penalty (10000 < count <= 100000)
                        # Gets 10 + 5 for 'id' pattern - 1 for medium count = 14 total
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is not None
            assert result["columns"] == ["test_id"]
            assert result["inference_metadata"]["score"] == 14

    def test_infer_primary_key_large_distinct_count_penalty(self, parser, sample_table):
        """Test inferring primary key with large distinct count penalty to cover line 135."""
        with patch(
            "forklift.schema.processors.metadata.MetadataGenerator"
        ) as mock_metadata_gen_class:
            # Mock MetadataGenerator with large distinct count (>100000)
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                "column_metadata": {
                    "test_id": {
                        "null_percentage": 0.0,
                        "uniqueness_ratio": 1.0,  # 10 points
                        "distinct_count": 150000,  # -2 penalty (count > 100000)
                        # Gets 10 + 5 for 'id' pattern - 2 for large count = 13 total
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is not None
            assert result["columns"] == ["test_id"]
            assert result["inference_metadata"]["score"] == 13

    def test_infer_primary_key_score_below_threshold_returns_none(self, parser, sample_table):
        """Test inferring primary key when score is below 8 threshold to cover line 178."""
        with patch(
            "forklift.schema.processors.metadata.MetadataGenerator"
        ) as mock_metadata_gen_class:
            # Mock MetadataGenerator with candidate that scores below 8 threshold
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                "column_metadata": {
                    "some_key": {  # Has 'key' pattern but will score below 8
                        "null_percentage": 0.0,
                        "uniqueness_ratio": 0.95,  # 5 points (>= 0.95)
                        "distinct_count": 200000,  # -2 penalty (count > 100000)
                        # Gets 5 + 3 for 'key' - 2 for large count = 6 total (below 8 threshold)
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            # Should return None because score is below 8 threshold (line 178)
            assert result is None

    def test_infer_primary_key_specific_099_uniqueness_branch(self, parser, sample_table):
        """Test to specifically hit line 126 (elif uniqueness_ratio >= 0.99)."""
        with patch(
            "forklift.schema.processors.metadata.MetadataGenerator"
        ) as mock_metadata_gen_class:
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                "column_metadata": {
                    "some_key": {  # Has 'key' pattern to enter scoring logic
                        "null_percentage": 0.0,
                        "uniqueness_ratio": 0.99,  # Exactly 0.99 to hit line 126
                        "distinct_count": 1000,  # No penalty
                        # Gets 8 points for >= 0.99 + 3 for 'key' = 11 total
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is not None
            assert result["inference_metadata"]["score"] == 11

    def test_infer_primary_key_specific_095_uniqueness_branch(self, parser, sample_table):
        """Test to specifically hit line 128 (elif uniqueness_ratio >= 0.95)."""
        with patch(
            "forklift.schema.processors.metadata.MetadataGenerator"
        ) as mock_metadata_gen_class:
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                "column_metadata": {
                    "user_id": {  # Has 'id' pattern to get above threshold
                        "null_percentage": 0.0,
                        "uniqueness_ratio": 0.96,  # Between 0.95 and 0.99 to hit line 128
                        "distinct_count": 1000,  # No penalty
                        # Gets 5 points for >= 0.95 + 5 for 'id' = 10 total
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is not None
            assert result["inference_metadata"]["score"] == 10

    def test_infer_primary_key_medium_count_penalty_branch(self, parser, sample_table):
        """Test to specifically hit line 137 (elif distinct_count > 10000)."""
        with patch(
            "forklift.schema.processors.metadata.MetadataGenerator"
        ) as mock_metadata_gen_class:
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                "column_metadata": {
                    "user_id": {
                        "null_percentage": 0.0,
                        "uniqueness_ratio": 1.0,  # 10 points
                        "distinct_count": 50000,  # Between 10000 and 100000 to hit line 137
                        # Gets 10 + 5 for 'id' - 1 for medium count = 14 total
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is not None
            assert result["inference_metadata"]["score"] == 14

    def test_infer_primary_key_score_below_8_returns_none(self, parser, sample_table):
        """Test to specifically hit line 178 (return None when score < 8)."""
        with patch(
            "forklift.schema.processors.metadata.MetadataGenerator"
        ) as mock_metadata_gen_class:
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                "column_metadata": {
                    "some_key": {  # Has 'key' pattern but low score
                        "null_percentage": 0.0,
                        "uniqueness_ratio": 0.95,  # 5 points for >= 0.95
                        "distinct_count": 200000,  # -2 penalty for > 100000
                        # Gets 5 + 3 for 'key' - 2 for large count = 6 total (below 8 threshold)
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            # Should hit line 178 and return None
            assert result is None

    def test_infer_primary_key_uuid_guid_patterns_specific(self, parser, sample_table):
        """Test to specifically hit lines 130-131 (uuid/guid patterns)."""
        with patch(
            "forklift.schema.processors.metadata.MetadataGenerator"
        ) as mock_metadata_gen_class:
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                "column_metadata": {
                    "session_uuid": {  # Has 'uuid' but not 'id'
                        "null_percentage": 0.0,
                        "uniqueness_ratio": 1.0,  # 10 points
                        "distinct_count": 1000,  # No penalty
                        # Gets 10 + 4 for 'uuid' = 14 total
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is not None
            assert result["inference_metadata"]["score"] == 14

    def test_infer_primary_key_key_pk_patterns_specific(self, parser, sample_table):
        """Test to specifically hit lines 132-133 (key/pk patterns)."""
        with patch(
            "forklift.schema.processors.metadata.MetadataGenerator"
        ) as mock_metadata_gen_class:
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                "column_metadata": {
                    "primary_key": {  # Has 'key' but not 'uuid', 'guid', or 'id'
                        "null_percentage": 0.0,
                        "uniqueness_ratio": 1.0,  # 10 points
                        "distinct_count": 1000,  # No penalty
                        # Gets 10 + 3 for 'key' = 13 total
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is not None
            assert result["inference_metadata"]["score"] == 13

    def test_hit_elif_099_uniqueness_branch_specifically(self, parser, sample_table):
        """Test to hit the elif uniqueness_ratio >= 0.99 branch when != 1.0."""
        with patch(
            "forklift.schema.processors.metadata.MetadataGenerator"
        ) as mock_metadata_gen_class:
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                "column_metadata": {
                    "some_key": {  # Has 'key' pattern but not 'id', 'uuid', or 'guid'
                        "null_percentage": 0.0,
                        "uniqueness_ratio": 0.995,  # Between 0.99 and 1.0 to hit elif
                        "distinct_count": 5000,  # No penalty (< 10000)
                        # Gets 8 points for >= 0.99 + 3 for 'key' = 11 total
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is not None
            assert result["inference_metadata"]["score"] == 11

    def test_hit_elif_10000_distinct_count_branch_specifically(self, parser, sample_table):
        """Test to hit the elif distinct_count > 10000 branch when <= 100000."""
        with patch(
            "forklift.schema.processors.metadata.MetadataGenerator"
        ) as mock_metadata_gen_class:
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                "column_metadata": {
                    "user_id": {
                        "null_percentage": 0.0,
                        "uniqueness_ratio": 1.0,  # 10 points
                        "distinct_count": 25000,  # Between 10000 and 100000 to hit elif
                        # Gets 10 + 5 for 'id' - 1 for medium count = 14 total
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is not None
            assert result["inference_metadata"]["score"] == 14

    def test_hit_exact_elif_099_branch_when_not_10(self, parser, sample_table):
        """Test to hit the exact elif uniqueness_ratio >= 0.99 branch when ratio is not 1.0."""
        with patch(
            "forklift.schema.processors.metadata.MetadataGenerator"
        ) as mock_metadata_gen_class:
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                "column_metadata": {
                    "some_pk": {  # Has 'pk' pattern (not uuid/guid/key/id to avoid other branches)
                        "null_percentage": 0.0,
                        "uniqueness_ratio": 0.992,  # > 0.99 but < 1.0 to hit elif
                        "distinct_count": 8000,  # < 10000 to avoid penalty branches
                        # Gets 8 points for >= 0.99 + 3 for 'pk' = 11 total
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is not None
            assert result["inference_metadata"]["score"] == 11

    def test_hit_exact_elif_10000_branch_when_not_100000(self, parser, sample_table):
        """Test to hit the exact elif distinct_count > 10000 branch when count is not > 100000."""
        with patch(
            "forklift.schema.processors.metadata.MetadataGenerator"
        ) as mock_metadata_gen_class:
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                "column_metadata": {
                    "some_pk": {  # Has 'pk' pattern
                        "null_percentage": 0.0,
                        "uniqueness_ratio": 1.0,  # 10 points
                        "distinct_count": 75000,  # > 10000 but <= 100000 to hit elif
                        # Gets 10 + 3 for 'pk' - 1 for medium count = 12 total
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is not None
            assert result["inference_metadata"]["score"] == 12

    def test_final_elif_099_branch_coverage(self, parser, sample_table):
        """Final test to hit the exact elif uniqueness_ratio >= 0.99 branch."""
        with patch(
            "forklift.schema.processors.metadata.MetadataGenerator"
        ) as mock_metadata_gen_class:
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                "column_metadata": {
                    "data_pk": {  # Has 'pk' pattern to ensure we enter scoring logic
                        "null_percentage": 0.0,
                        "uniqueness_ratio": 0.991,  # Specifically between 0.99 and 1.0
                        "distinct_count": 9999,  # Just under 10000 to avoid penalty branches
                        # Gets 8 points for >= 0.99 + 3 for 'pk' = 11 total
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is not None
            assert result["inference_metadata"]["score"] == 11

    def test_final_elif_10000_branch_coverage(self, parser, sample_table):
        """Final test to hit the exact elif distinct_count > 10000 branch."""
        with patch(
            "forklift.schema.processors.metadata.MetadataGenerator"
        ) as mock_metadata_gen_class:
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                "column_metadata": {
                    "data_pk": {  # Has 'pk' pattern
                        "null_percentage": 0.0,
                        "uniqueness_ratio": 1.0,  # 10 points
                        "distinct_count": 99999,  # Specifically between 10000 and 100000
                        # Gets 10 + 3 for 'pk' - 1 for medium count = 12 total
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is not None
            assert result["inference_metadata"]["score"] == 12
