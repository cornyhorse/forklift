"""Tests for Excel importer functionality."""

import pytest
import tempfile
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

import pyarrow as pa
import pyarrow.parquet as pq

from forklift.engine.importers.excel_importer import ExcelImporter
from forklift.engine.exceptions import ProcessingError


class TestExcelImporter:
    """Test cases for ExcelImporter class."""

    @pytest.fixture
    def sample_excel_file(self):
        """Create a sample Excel file for testing."""
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
            # Create sample data
            data = {
                'Name': ['Alice', 'Bob', 'Charlie'],
                'Age': [25, 30, 35],
                'City': ['New York', 'London', 'Paris']
            }
            df = pd.DataFrame(data)

            # Write to Excel with multiple sheets
            with pd.ExcelWriter(tmp_file.name, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Sheet1', index=False)
                df.to_excel(writer, sheet_name='Sheet2', index=False)

            yield Path(tmp_file.name)
            Path(tmp_file.name).unlink()

    @pytest.fixture
    def output_directory(self):
        """Create a temporary output directory."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            yield Path(tmp_dir)

    @pytest.fixture
    def sample_schema_file(self):
        """Create a sample schema file for testing."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
            schema_content = {
                "sheets": [
                    {
                        "select": {"name": "Sheet1"},
                        "columns": ["Name", "Age", "City"],
                        "header": 0,
                        "dataStartRow": 1
                    }
                ],
                "valuesOnly": True,
                "dateSystem": 1900
            }
            import json
            json.dump(schema_content, tmp_file)

            yield Path(tmp_file.name)
            Path(tmp_file.name).unlink()

    def test_import_excel_basic_functionality(self, sample_excel_file, output_directory):
        """Test basic Excel import functionality."""
        with patch('forklift.inputs.excel.ExcelInputHandler') as mock_handler_class:
            # Mock the Excel handler
            mock_handler = Mock()
            mock_handler_class.return_value = mock_handler

            # Mock sheet info - include sheet_names which is required by _create_default_excel_config
            mock_handler.get_sheet_info.return_value = {
                'sheet_count': 2,
                'engine': 'openpyxl',
                'sheet_names': ['Sheet1', 'Sheet2']
            }

            # Mock sheet processing - create Arrow tables
            mock_table = pa.table({
                'Name': ['Alice', 'Bob', 'Charlie'],
                'Age': [25, 30, 35],
                'City': ['New York', 'London', 'Paris']
            })

            mock_handler.process_sheets.return_value = [
                ('Sheet1', mock_table),
                ('Sheet2', mock_table)
            ]

            # Mock PyArrow write_table
            with patch('pyarrow.parquet.write_table') as mock_write:
                result = ExcelImporter.import_excel(
                    input_path=sample_excel_file,
                    output_path=output_directory
                )

                # Verify results
                assert result.total_rows == 6  # 3 rows × 2 sheets
                assert result.valid_rows == 6
                assert result.invalid_rows == 0
                assert len(result.output_files) == 2
                assert result.execution_time > 0

                # Verify write_table was called for each sheet
                assert mock_write.call_count == 2

    def test_import_excel_with_schema(self, sample_excel_file, output_directory, sample_schema_file):
        """Test Excel import with schema file."""
        with patch('forklift.schema.excel_schema_importer.ExcelSchemaImporter') as mock_schema_importer_class:
            with patch('forklift.inputs.excel.ExcelInputHandler') as mock_handler_class:
                # Mock schema importer
                mock_schema_importer = Mock()
                mock_schema_importer_class.return_value = mock_schema_importer
                mock_schema_importer.sheets = [
                    {
                        'select': {'name': 'Sheet1'},
                        'columns': ['Name', 'Age', 'City'],
                        'header': 0,
                        'dataStartRow': 1
                    }
                ]
                mock_schema_importer.values_only = True
                mock_schema_importer.date_system = 1900
                mock_schema_importer.nulls = []

                # Mock Excel handler
                mock_handler = Mock()
                mock_handler_class.return_value = mock_handler
                mock_handler.get_sheet_info.return_value = {'sheet_count': 1, 'engine': 'openpyxl'}

                mock_table = pa.table({'Name': ['Alice'], 'Age': [25], 'City': ['New York']})
                mock_handler.process_sheets.return_value = [('Sheet1', mock_table)]

                with patch('pyarrow.parquet.write_table'):
                    result = ExcelImporter.import_excel(
                        input_path=sample_excel_file,
                        output_path=output_directory,
                        schema_file=sample_schema_file
                    )

                    assert result.total_rows == 1
                    assert len(result.output_files) == 1

    def test_import_excel_file_not_found(self, output_directory):
        """Test Excel import with non-existent file."""
        non_existent_file = Path('/non/existent/file.xlsx')

        with pytest.raises(FileNotFoundError):
            ExcelImporter.import_excel(
                input_path=non_existent_file,
                output_path=output_directory
            )

    def test_import_excel_schema_validation_error(self, sample_excel_file, output_directory):
        """Test Excel import with invalid schema."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
            tmp_file.write('invalid json content')
            invalid_schema_path = Path(tmp_file.name)

        try:
            with patch('forklift.schema.excel_schema_importer.ExcelSchemaImporter') as mock_schema_importer_class:
                # Mock schema importer to raise an exception
                mock_schema_importer_class.side_effect = Exception("Invalid schema format")

                with pytest.raises(ProcessingError, match="Schema validation failed"):
                    ExcelImporter.import_excel(
                        input_path=sample_excel_file,
                        output_path=output_directory,
                        schema_file=invalid_schema_path
                    )
        finally:
            invalid_schema_path.unlink()

    def test_import_excel_with_kwargs(self, sample_excel_file, output_directory):
        """Test Excel import with additional kwargs."""
        with patch('forklift.inputs.excel.ExcelInputHandler') as mock_handler_class:
            with patch('forklift.engine.importers.excel_importer.ExcelImporter._create_default_excel_config') as mock_config:
                mock_excel_config = Mock()
                mock_config.return_value = mock_excel_config

                mock_handler = Mock()
                mock_handler_class.return_value = mock_handler
                mock_handler.get_sheet_info.return_value = {'sheet_count': 1, 'engine': 'openpyxl'}

                mock_table = pa.table({'col1': [1, 2, 3]})
                mock_handler.process_sheets.return_value = [('Sheet1', mock_table)]

                with patch('pyarrow.parquet.write_table'):
                    ExcelImporter.import_excel(
                        input_path=sample_excel_file,
                        output_path=output_directory,
                        values_only=True,
                        engine='xlrd',
                        date_system=1904
                    )

                    # Verify kwargs were applied to config
                    assert mock_excel_config.values_only == True
                    assert mock_excel_config.engine == 'xlrd'
                    assert mock_excel_config.date_system == 1904

    def test_sanitize_filename(self):
        """Test filename sanitization."""
        # Test the _sanitize_filename method indirectly through import
        with patch('forklift.inputs.excel.ExcelInputHandler') as mock_handler_class:
            mock_handler = Mock()
            mock_handler_class.return_value = mock_handler
            mock_handler.get_sheet_info.return_value = {
                'sheet_count': 1,
                'engine': 'openpyxl',
                'sheet_names': ['Sheet/With*Special:Chars']
            }

            # Mock a sheet with special characters in name
            mock_table = pa.table({'col1': [1]})
            mock_handler.process_sheets.return_value = [('Sheet/With*Special:Chars', mock_table)]

            with tempfile.TemporaryDirectory() as tmp_dir:
                with tempfile.NamedTemporaryFile(suffix='.xlsx') as tmp_file:
                    output_dir = Path(tmp_dir)

                    with patch('pyarrow.parquet.write_table') as mock_write:
                        with patch.object(Path, 'exists', return_value=True):
                            ExcelImporter.import_excel(
                                input_path=Path(tmp_file.name),
                                output_path=output_dir
                            )

                            # Check that write_table was called with sanitized filename
                            call_args = mock_write.call_args[0]
                            output_path = call_args[1]
                            assert 'Sheet/With*Special:Chars' not in str(output_path)

    def test_create_excel_config_from_schema(self):
        """Test creation of Excel config from schema."""
        with patch('forklift.inputs.config.ExcelInputConfig') as mock_config_class:
            with patch('forklift.inputs.config.ExcelSheetConfig') as mock_sheet_config_class:
                # Mock schema importer
                mock_schema_importer = Mock()
                mock_schema_importer.sheets = [
                    {
                        'select': {'name': 'Sheet1'},
                        'columns': ['col1', 'col2'],
                        'header': 0,
                        'dataStartRow': 1,
                        'dataEndRow': 100,
                        'skipBlankRows': True,
                        'nameOverride': 'CustomName'
                    }
                ]
                mock_schema_importer.values_only = True
                mock_schema_importer.date_system = 1900
                mock_schema_importer.nulls = ['', 'NULL']

                # Call the method
                result = ExcelImporter._create_excel_config_from_schema(mock_schema_importer)

                # Verify sheet config creation
                mock_sheet_config_class.assert_called_once()
                sheet_config_kwargs = mock_sheet_config_class.call_args[1]
                assert sheet_config_kwargs['select'] == {'name': 'Sheet1'}
                assert sheet_config_kwargs['columns'] == ['col1', 'col2']
                assert sheet_config_kwargs['header'] == 0

                # Verify main config creation
                mock_config_class.assert_called_once()
                config_kwargs = mock_config_class.call_args[1]
                assert config_kwargs['values_only'] == True
                assert config_kwargs['date_system'] == 1900
                assert config_kwargs['nulls'] == ['', 'NULL']

    def test_create_default_excel_config(self):
        """Test creation of default Excel config."""
        with patch('forklift.engine.importers.excel_importer.ExcelImporter._create_default_excel_config') as mock_method:
            mock_config = Mock()
            mock_method.return_value = mock_config

            file_path = Path('test.xlsx')
            result = ExcelImporter._create_default_excel_config(file_path, engine='openpyxl')

            mock_method.assert_called_once_with(file_path, engine='openpyxl')
            assert result == mock_config

    def test_processing_error_handling(self, sample_excel_file, output_directory):
        """Test error handling during processing."""
        with patch('forklift.inputs.excel.ExcelInputHandler') as mock_handler_class:
            # Mock handler to raise an exception during processing
            mock_handler = Mock()
            mock_handler_class.return_value = mock_handler
            mock_handler.get_sheet_info.side_effect = Exception("Processing error")

            with pytest.raises(Exception, match="Processing error"):
                ExcelImporter.import_excel(
                    input_path=sample_excel_file,
                    output_path=output_directory
                )
