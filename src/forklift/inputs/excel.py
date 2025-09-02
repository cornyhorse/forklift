"""Excel input handler for reading and preprocessing Excel files."""

from __future__ import annotations
import re
from pathlib import Path
from typing import Dict, List, Optional, Union, Any, Iterator, Tuple
import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import ExcelInputConfig, ExcelSheetConfig

logger = logging.getLogger(__name__)


class ExcelInputHandler:
    """Handles Excel file input with multi-sheet processing.

    This class provides functionality for reading Excel files (.xlsx and .xls)
    with support for multiple sheets, custom column mappings, header detection,
    and data range specification. Uses an approach of opening the file once
    and streaming sheets from the already opened file for efficiency.

    Args:
        config: ExcelInputConfig instance with processing configuration

    Attributes:
        config: The configuration object for this input handler
        _workbook: Cached workbook object for efficient sheet access
        _engine: Excel engine being used (openpyxl or xlrd)
    """

    def __init__(self, config: ExcelInputConfig):
        """Initialize the Excel input handler.

        Args:
            config: Configuration object containing Excel processing parameters
        """
        self.config = config
        self._workbook = None
        self._engine = None

    def detect_engine(self, file_path: Path) -> str:
        """Detect appropriate Excel engine based on file extension.

        Args:
            file_path: Path to the Excel file

        Returns:
            Engine name ('openpyxl' for .xlsx, 'xlrd' for .xls)

        Raises:
            ValueError: If file extension is not supported
        """
        if self.config.engine:
            return self.config.engine

        suffix = file_path.suffix.lower()
        if suffix == '.xlsx':
            return 'openpyxl'
        elif suffix == '.xls':
            return 'xlrd'
        else:
            raise ValueError(f"Unsupported Excel file extension: {suffix}")

    def open_workbook(self, file_path: Path) -> None:
        """Open Excel workbook and cache it for efficient sheet access.

        Args:
            file_path: Path to the Excel file to open

        Raises:
            ImportError: If required Excel engine is not installed
            FileNotFoundError: If the Excel file doesn't exist
        """
        self._engine = self.detect_engine(file_path)

        try:
            if self._engine == 'openpyxl':
                import openpyxl
                self._workbook = openpyxl.load_workbook(
                    file_path,
                    data_only=self.config.values_only
                )
            elif self._engine == 'xlrd':
                import xlrd
                self._workbook = xlrd.open_workbook(str(file_path))
            else:
                raise ValueError(f"Unsupported engine: {self._engine}")

        except ImportError as e:
            raise ImportError(
                f"Required library for {self._engine} engine not found. "
                f"Install with: pip install {self._engine}"
            ) from e

    def close_workbook(self) -> None:
        """Close the cached workbook and clean up resources."""
        if self._workbook is not None:
            if hasattr(self._workbook, 'close'):
                self._workbook.close()
            self._workbook = None
            self._engine = None

    def get_sheet_names(self) -> List[str]:
        """Get list of all sheet names in the workbook.

        Returns:
            List of sheet names

        Raises:
            RuntimeError: If workbook is not opened
        """
        if self._workbook is None:
            raise RuntimeError("Workbook not opened. Call open_workbook() first.")

        if self._engine == 'openpyxl':
            return self._workbook.sheetnames
        elif self._engine == 'xlrd':
            return self._workbook.sheet_names()
        else:
            raise ValueError(f"Unsupported engine: {self._engine}")

    def select_sheets(self, sheet_configs: List[ExcelSheetConfig]) -> List[Tuple[str, ExcelSheetConfig]]:
        """Select sheets based on configuration criteria.

        Args:
            sheet_configs: List of sheet selection configurations

        Returns:
            List of tuples containing (sheet_name, config) for selected sheets

        Raises:
            ValueError: If sheet selection criteria don't match any sheets
        """
        available_sheets = self.get_sheet_names()
        selected_sheets = []

        for config in sheet_configs:
            select_criteria = config.select
            matched_sheets = []

            if 'name' in select_criteria:
                # Exact name match
                name = select_criteria['name']
                if name in available_sheets:
                    matched_sheets.append(name)

            elif 'index' in select_criteria:
                # Index-based selection (0-based)
                index = select_criteria['index']
                if 0 <= index < len(available_sheets):
                    matched_sheets.append(available_sheets[index])

            elif 'regex' in select_criteria:
                # Regex pattern matching
                pattern = re.compile(select_criteria['regex'])
                matched_sheets = [name for name in available_sheets if pattern.search(name)]

            if not matched_sheets:
                logger.warning(f"No sheets matched selection criteria: {select_criteria}")
                continue

            for sheet_name in matched_sheets:
                selected_sheets.append((sheet_name, config))

        if not selected_sheets:
            raise ValueError("No sheets selected based on configuration criteria")

        return selected_sheets

    def read_sheet_data(self, sheet_name: str, sheet_config: ExcelSheetConfig) -> pd.DataFrame:
        """Read data from a specific sheet using pandas.

        Args:
            sheet_name: Name of the sheet to read
            sheet_config: Configuration for this sheet

        Returns:
            DataFrame containing the sheet data

        Raises:
            RuntimeError: If workbook is not opened
        """
        if self._workbook is None:
            raise RuntimeError("Workbook not opened. Call open_workbook() first.")

        # Prepare pandas read_excel parameters
        read_params = {
            'sheet_name': sheet_name,
            'engine': self._engine,
            'na_values': self.config.na_values or [],
            'keep_default_na': self.config.keep_default_na,
        }

        # Handle header configuration
        header_config = sheet_config.header or {}
        header_mode = header_config.get('mode', 'present')

        if header_mode == 'present':
            header_row = header_config.get('row', 0)  # 0-based for pandas
            read_params['header'] = header_row
        elif header_mode == 'absent':
            read_params['header'] = None
        # For 'auto' mode, let pandas auto-detect

        # Handle data range
        if sheet_config.data_start_row is not None:
            # Convert 1-based to 0-based for pandas
            skiprows = sheet_config.data_start_row - 1
            if header_mode == 'present':
                # Account for header row
                header_row = header_config.get('row', 0)
                if skiprows <= header_row:
                    read_params['skiprows'] = list(range(skiprows)) + list(range(header_row + 1, skiprows))
                else:
                    read_params['skiprows'] = skiprows
            else:
                read_params['skiprows'] = skiprows

        if sheet_config.data_end_row is not None:
            # Calculate nrows based on end row
            start_row = sheet_config.data_start_row or 1
            read_params['nrows'] = sheet_config.data_end_row - start_row + 1

        # Read the sheet data
        # We'll use the file path since we need pandas to handle the reading
        # The workbook is kept open for metadata access
        df = pd.read_excel(self._workbook, **read_params)

        # Handle column mapping if specified
        if sheet_config.columns:
            df = self._apply_column_mapping(df, sheet_config.columns)

        # Skip blank rows if configured
        if sheet_config.skip_blank_rows:
            df = df.dropna(how='all')

        return df

    def _apply_column_mapping(self, df: pd.DataFrame, column_mappings: List[Dict[str, Any]]) -> pd.DataFrame:
        """Apply column mappings to the DataFrame.

        Args:
            df: Input DataFrame
            column_mappings: List of column mapping configurations

        Returns:
            DataFrame with applied column mappings
        """
        # Create mapping dictionaries
        rename_map = {}
        select_columns = []
        type_conversions = {}

        for mapping in column_mappings:
            source_col = mapping.get('source')
            target_col = mapping.get('name')
            data_type = mapping.get('type')

            if source_col and target_col:
                # Handle different source column specifications
                if isinstance(source_col, str):
                    # Column name
                    if source_col in df.columns:
                        rename_map[source_col] = target_col
                        select_columns.append(target_col)
                elif isinstance(source_col, int):
                    # Column index (0-based)
                    if 0 <= source_col < len(df.columns):
                        original_name = df.columns[source_col]
                        rename_map[original_name] = target_col
                        select_columns.append(target_col)
                elif isinstance(source_col, dict) and 'position' in source_col:
                    # Excel position like "A", "B", "AA"
                    col_index = self._excel_col_to_index(source_col['position'])
                    if 0 <= col_index < len(df.columns):
                        original_name = df.columns[col_index]
                        rename_map[original_name] = target_col
                        select_columns.append(target_col)

                # Store type conversion if specified
                if data_type and target_col:
                    type_conversions[target_col] = data_type

        # Apply renaming
        if rename_map:
            df = df.rename(columns=rename_map)

        # Select only mapped columns if mappings were provided
        if select_columns:
            # Ensure we only select columns that actually exist
            existing_columns = [col for col in select_columns if col in df.columns]
            df = df[existing_columns]

        # Apply type conversions
        for col, target_type in type_conversions.items():
            if col in df.columns:
                df[col] = self._convert_column_type(df[col], target_type)

        return df

    def _excel_col_to_index(self, col_str: str) -> int:
        """Convert Excel column string (A, B, AA, etc.) to 0-based index.

        Args:
            col_str: Excel column string

        Returns:
            0-based column index
        """
        result = 0
        for char in col_str.upper():
            result = result * 26 + (ord(char) - ord('A') + 1)
        return result - 1

    def _convert_column_type(self, series: pd.Series, target_type: str) -> pd.Series:
        """Convert pandas Series to target data type.

        Args:
            series: Input pandas Series
            target_type: Target Parquet data type

        Returns:
            Converted pandas Series
        """
        # Handle common Parquet type conversions
        type_map = {
            'string': 'str',
            'int32': 'int32',
            'int64': 'int64',
            'float32': 'float32',
            'double': 'float64',
            'bool': 'bool',
            'date32': 'datetime64[ns]',
            'date64': 'datetime64[ns]',
        }

        pandas_type = type_map.get(target_type, target_type)

        try:
            if pandas_type == 'str':
                return series.astype('string')
            elif pandas_type.startswith('datetime'):
                return pd.to_datetime(series, errors='coerce')
            else:
                return series.astype(pandas_type)
        except Exception as e:
            logger.warning(f"Failed to convert column to {target_type}: {e}")
            return series

    def process_sheets(self, file_path: Path) -> Iterator[Tuple[str, pa.Table]]:
        """Process all configured sheets and yield Arrow tables.

        Args:
            file_path: Path to the Excel file

        Yields:
            Tuple of (sheet_name, arrow_table) for each processed sheet

        Raises:
            Various exceptions related to file access or data processing
        """
        try:
            # Open the workbook once
            self.open_workbook(file_path)

            # Select sheets based on configuration
            if not self.config.sheets:
                raise ValueError("No sheet configurations provided")

            selected_sheets = self.select_sheets(self.config.sheets)

            # Process each selected sheet
            for sheet_name, sheet_config in selected_sheets:
                logger.info(f"Processing sheet: {sheet_name}")

                # Read sheet data
                df = self.read_sheet_data(sheet_name, sheet_config)

                # Convert to Arrow table
                table = pa.Table.from_pandas(df)

                # Use override name if provided, otherwise use sheet name
                output_name = sheet_config.name_override or sheet_name

                yield output_name, table

        finally:
            # Always close the workbook
            self.close_workbook()

    def get_sheet_info(self, file_path: Path) -> Dict[str, Any]:
        """Get information about all sheets in the Excel file.

        Args:
            file_path: Path to the Excel file

        Returns:
            Dictionary containing sheet information

        Raises:
            Various exceptions related to file access
        """
        try:
            self.open_workbook(file_path)
            sheet_names = self.get_sheet_names()

            info = {
                'engine': self._engine,
                'sheet_count': len(sheet_names),
                'sheet_names': sheet_names,
                'file_size': file_path.stat().st_size if file_path.exists() else 0
            }

            return info

        finally:
            self.close_workbook()
