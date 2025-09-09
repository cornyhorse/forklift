"""Tests for configuration parser functionality."""

import pytest
import pyarrow as pa
from unittest.mock import Mock, patch, MagicMock

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
        return pa.table({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'email': ['alice@test.com', 'bob@test.com', 'charlie@test.com', 'david@test.com', 'eve@test.com'],
            'age': [25, 30, 35, 40, 45]
        })

    @pytest.fixture
    def table_with_primary_key_candidates(self):
        """Create a table with various primary key candidates."""
        return pa.table({
            'user_id': [1, 2, 3, 4, 5],  # Good PK candidate
            'uuid_field': ['a1b2c3', 'b2c3d4', 'c3d4e5', 'd4e5f6', 'e5f6g7'],  # Good PK candidate
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],  # Poor PK candidate
            'email': ['alice@test.com', 'bob@test.com', 'charlie@test.com', 'david@test.com', 'eve@test.com'],  # OK PK candidate
            'status': ['active', 'active', 'inactive', 'active', 'inactive']  # Poor PK candidate
        })

    def test_init(self, parser):
        """Test ConfigurationParser initialization."""
        assert parser is not None
        assert hasattr(parser, 'analyzer')

    @patch('forklift.schema.processors.config_parser.TransformationAnalyzer')
    def test_generate_transformation_extension_with_suggestions(self, mock_analyzer_class, parser, sample_table):
        """Test generating transformation extension with column suggestions."""
        # Mock the analyzer
        mock_analyzer = Mock()
        mock_analyzer.analyze_column_for_transformations.side_effect = [
            {'transform': 'numeric'},
            {'transform': 'text_clean'},
            {'transform': 'email_validate'},
            {'transform': 'numeric'}
        ]
        mock_analyzer.get_transformation_types_config.return_value = {'types': 'config'}
        parser.analyzer = mock_analyzer

        result = parser.generate_transformation_extension(sample_table)

        assert 'description' in result
        assert 'version' in result
        assert 'global_settings' in result
        assert 'column_transformations' in result
        assert 'transformation_types' in result

        # Check global settings structure
        assert 'nan_handling' in result['global_settings']
        assert 'error_handling' in result['global_settings']

        # Check column transformations were generated
        assert len(result['column_transformations']) == 4
        assert 'id' in result['column_transformations']
        assert 'name' in result['column_transformations']

    @patch('forklift.schema.processors.config_parser.TransformationAnalyzer')
    def test_generate_transformation_extension_no_suggestions(self, mock_analyzer_class, parser, sample_table):
        """Test generating transformation extension when no suggestions are found."""
        # Mock the analyzer to return no suggestions
        mock_analyzer = Mock()
        mock_analyzer.analyze_column_for_transformations.return_value = None
        mock_analyzer.get_transformation_types_config.return_value = {'types': 'config'}
        parser.analyzer = mock_analyzer

        result = parser.generate_transformation_extension(sample_table)

        assert 'column_transformations' in result
        assert len(result['column_transformations']) == 0

    def test_generate_primary_key_config_user_specified_single(self, parser, sample_table):
        """Test generating primary key config with user-specified single column."""
        config = Mock()
        config.user_specified_primary_key = ['id']
        config.infer_primary_key_from_metadata = False

        result = parser.generate_primary_key_config(sample_table, config)

        assert result is not None
        assert result['columns'] == ['id']
        assert result['type'] == 'single'
        assert result['enforceUniqueness'] == True
        assert result['allowNulls'] == False
        assert 'User-specified primary key' in result['description']

    def test_generate_primary_key_config_user_specified_composite(self, parser, sample_table):
        """Test generating primary key config with user-specified composite key."""
        config = Mock()
        config.user_specified_primary_key = ['id', 'name']
        config.infer_primary_key_from_metadata = False

        result = parser.generate_primary_key_config(sample_table, config)

        assert result is not None
        assert result['columns'] == ['id', 'name']
        assert result['type'] == 'composite'
        assert result['enforceUniqueness'] == True
        assert result['allowNulls'] == False

    def test_generate_primary_key_config_infer_from_metadata(self, parser, sample_table):
        """Test generating primary key config by inferring from metadata."""
        config = Mock()
        config.user_specified_primary_key = None
        config.infer_primary_key_from_metadata = True

        with patch.object(parser, '_infer_primary_key_from_metadata') as mock_infer:
            mock_infer.return_value = {
                'description': 'Inferred primary key',
                'columns': ['id'],
                'type': 'single'
            }

            result = parser.generate_primary_key_config(sample_table, config)

            mock_infer.assert_called_once_with(sample_table)
            assert result is not None
            assert result['columns'] == ['id']

    def test_generate_primary_key_config_no_config(self, parser, sample_table):
        """Test generating primary key config when no configuration is provided."""
        config = Mock()
        config.user_specified_primary_key = None
        config.infer_primary_key_from_metadata = False

        result = parser.generate_primary_key_config(sample_table, config)

        assert result is None

    def test_infer_primary_key_from_metadata_no_metadata(self, parser, sample_table):
        """Test inferring primary key when no metadata is available."""
        with patch('forklift.schema.processors.metadata.MetadataGenerator') as mock_metadata_gen_class:
            # Mock MetadataGenerator to return None
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = None
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is None

    def test_infer_primary_key_from_metadata_no_column_metadata(self, parser, sample_table):
        """Test inferring primary key when metadata lacks column_metadata."""
        with patch('forklift.schema.processors.metadata.MetadataGenerator') as mock_metadata_gen_class:
            # Mock MetadataGenerator to return metadata without column_metadata
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {'some_key': 'some_value'}
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is None

    def test_infer_primary_key_from_metadata_no_candidates(self, parser, sample_table):
        """Test inferring primary key when no good candidates are found."""
        with patch('forklift.schema.processors.metadata.MetadataGenerator') as mock_metadata_gen_class:
            # Mock MetadataGenerator to return metadata with poor candidates
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                'column_metadata': {
                    'name': {
                        'null_percentage': 0.0,
                        'uniqueness_ratio': 0.8,  # Too low
                        'distinct_count': 100
                    },
                    'status': {
                        'null_percentage': 10.0,  # Has nulls
                        'uniqueness_ratio': 0.3,
                        'distinct_count': 5
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is None

    def test_infer_primary_key_from_metadata_good_candidate(self, parser, table_with_primary_key_candidates):
        """Test inferring primary key when a good candidate is found."""
        with patch('forklift.schema.processors.metadata.MetadataGenerator') as mock_metadata_gen_class:
            # Mock MetadataGenerator to return metadata with good candidates
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                'column_metadata': {
                    'user_id': {
                        'null_percentage': 0.0,
                        'uniqueness_ratio': 1.0,
                        'distinct_count': 5
                    },
                    'uuid_field': {
                        'null_percentage': 0.0,
                        'uniqueness_ratio': 1.0,
                        'distinct_count': 5
                    },
                    'name': {
                        'null_percentage': 0.0,
                        'uniqueness_ratio': 1.0,
                        'distinct_count': 5
                    },
                    'email': {
                        'null_percentage': 0.0,
                        'uniqueness_ratio': 1.0,
                        'distinct_count': 5
                    },
                    'status': {
                        'null_percentage': 0.0,
                        'uniqueness_ratio': 0.4,
                        'distinct_count': 2
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(table_with_primary_key_candidates)

            assert result is not None
            assert result['columns'] == ['user_id']  # Should pick user_id due to 'id' pattern
            assert result['type'] == 'single'
            assert result['enforceUniqueness'] == True
            assert result['allowNulls'] == False
            assert 'inference_metadata' in result

    def test_infer_primary_key_from_metadata_low_score_candidate(self, parser, sample_table):
        """Test inferring primary key when candidate score is too low."""
        with patch('forklift.schema.processors.metadata.MetadataGenerator') as mock_metadata_gen_class:
            # Mock MetadataGenerator to return metadata with low-scoring candidates
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                'column_metadata': {
                    'some_field': {  # No 'id' pattern, so lower base score
                        'null_percentage': 0.0,
                        'uniqueness_ratio': 0.95,  # Gets 5 points
                        'distinct_count': 150000  # Gets -2 penalty, total score = 3
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is None

    def test_infer_primary_key_different_naming_patterns(self, parser, sample_table):
        """Test inferring primary key with different naming patterns for scoring."""
        with patch('forklift.schema.processors.metadata.MetadataGenerator') as mock_metadata_gen_class:
            # Mock MetadataGenerator to return metadata with different naming patterns
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                'column_metadata': {
                    'primary_key': {
                        'null_percentage': 0.0,
                        'uniqueness_ratio': 1.0,
                        'distinct_count': 100
                    },
                    'uuid_column': {
                        'null_percentage': 0.0,
                        'uniqueness_ratio': 1.0,
                        'distinct_count': 100
                    },
                    'guid_field': {
                        'null_percentage': 0.0,
                        'uniqueness_ratio': 1.0,
                        'distinct_count': 100
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is not None
            # Should pick one with the highest score based on naming patterns
            assert len(result['columns']) == 1
            assert result['inference_metadata']['score'] >= 8

    def test_infer_primary_key_very_large_distinct_count(self, parser, sample_table):
        """Test inferring primary key with very large distinct count (penalty applied)."""
        with patch('forklift.schema.processors.metadata.MetadataGenerator') as mock_metadata_gen_class:
            # Mock MetadataGenerator to return metadata with very large distinct count
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                'column_metadata': {
                    'record_id': {
                        'null_percentage': 0.0,
                        'uniqueness_ratio': 1.0,
                        'distinct_count': 200000  # Large count, penalty applied but still under limit
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            # Should still return a result but with penalty applied to score
            assert result is not None  # Should still pass with penalty
            assert result['columns'] == ['record_id']

    def test_infer_primary_key_exceeds_distinct_count_limit(self, parser, sample_table):
        """Test inferring primary key when distinct count exceeds limit."""
        with patch('forklift.schema.processors.metadata.MetadataGenerator') as mock_metadata_gen_class:
            # Mock MetadataGenerator to return metadata with count exceeding limit
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                'column_metadata': {
                    'huge_id': {
                        'null_percentage': 0.0,
                        'uniqueness_ratio': 1.0,
                        'distinct_count': 2000000  # Exceeds 1M limit
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            # Should not consider this candidate due to distinct count limit
            assert result is None

    def test_infer_primary_key_multiple_candidates_ranking(self, parser, sample_table):
        """Test inferring primary key with multiple candidates to test ranking."""
        with patch('forklift.schema.processors.metadata.MetadataGenerator') as mock_metadata_gen_class:
            # Mock MetadataGenerator to return metadata with multiple candidates
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                'column_metadata': {
                    'record_id': {  # Should score highest (id pattern + perfect uniqueness)
                        'null_percentage': 0.0,
                        'uniqueness_ratio': 1.0,
                        'distinct_count': 1000
                    },
                    'primary_key': {  # Should score lower (key pattern)
                        'null_percentage': 0.0,
                        'uniqueness_ratio': 1.0,
                        'distinct_count': 1000
                    },
                    'unique_code': {  # Should score lowest (no good pattern)
                        'null_percentage': 0.0,
                        'uniqueness_ratio': 1.0,
                        'distinct_count': 1000
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is not None
            assert result['columns'] == ['record_id']  # Should pick the highest scoring candidate
            assert 'alternative_candidates' in result['inference_metadata']
            assert len(result['inference_metadata']['alternative_candidates']) <= 2

    def test_infer_primary_key_borderline_uniqueness(self, parser, sample_table):
        """Test inferring primary key with borderline uniqueness ratio."""
        with patch('forklift.schema.processors.metadata.MetadataGenerator') as mock_metadata_gen_class:
            # Mock MetadataGenerator to return metadata with borderline uniqueness
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                'column_metadata': {
                    'borderline_id': {
                        'null_percentage': 0.0,
                        'uniqueness_ratio': 0.99,  # Borderline uniqueness
                        'distinct_count': 1000
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is not None
            assert result['columns'] == ['borderline_id']
            assert result['inference_metadata']['score'] >= 8

    def test_infer_primary_key_medium_distinct_count_penalty(self, parser, sample_table):
        """Test inferring primary key with medium distinct count that gets penalty."""
        with patch('forklift.schema.processors.metadata.MetadataGenerator') as mock_metadata_gen_class:
            # Mock MetadataGenerator with medium distinct count
            mock_metadata_gen = Mock()
            mock_metadata_gen.generate_metadata.return_value = {
                'column_metadata': {
                    'medium_id': {
                        'null_percentage': 0.0,
                        'uniqueness_ratio': 1.0,
                        'distinct_count': 50000  # Medium count, gets -1 penalty
                    }
                }
            }
            mock_metadata_gen_class.return_value = mock_metadata_gen

            result = parser._infer_primary_key_from_metadata(sample_table)

            assert result is not None
            assert result['columns'] == ['medium_id']
            # Score should be reduced by 1 due to medium distinct count
            assert result['inference_metadata']['score'] >= 8

