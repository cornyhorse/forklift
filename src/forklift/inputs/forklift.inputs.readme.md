# Forklift Inputs Package

The `forklift.inputs` package provides comprehensive data input capabilities for the Forklift ETL framework. This package serves as the data ingestion layer, supporting multiple file formats and data sources with standardized interfaces, type conversion, and schema validation.

## Package Overview

Forklift Inputs is designed as a modular, extensible system that handles the complexity of reading data from various sources while providing a consistent interface for downstream processing. The package supports:

- **Multiple Data Sources**: CSV files, Excel spreadsheets, Fixed-Width Files (FWF), and SQL databases
- **Schema-Aware Processing**: Automatic type detection and conversion with PyArrow integration
- **Configuration-Driven**: Declarative configuration for all input sources
- **Streaming Support**: Memory-efficient processing of large datasets
- **Error Handling**: Validation at construction time; problems in the data are recorded (FWF) or raised with row/column positions, never with cell values

The handlers in this package are the building blocks. `forklift.import_csv()` uses its own
engine (`forklift.engine`) rather than `CsvInputHandler`; `import_excel()` uses `ExcelInputHandler`
and `import_sql()` uses `SqlInputHandler`. `FwfInputHandler` is not wired into the engine yet
(`import_fwf()` raises `NotImplementedError`) but can be used directly.

## Architecture

The package follows a layered architecture with clear separation of concerns:

```
┌─────────────────────────────────────────┐
│             Forklift ETL Framework      │
├─────────────────────────────────────────┤
│           forklift.inputs Package       │
├─────────────────────────────────────────┤
│  Handler Layer (Orchestration)          │
│  ├── CsvInputHandler                   │
│  ├── ExcelInputHandler                 │
│  ├── FwfInputHandler                   │
│  └── SqlInputHandler                   │
├─────────────────────────────────────────┤
│  Specialized Processing Layers          │
│  ├── FWF Package (6 modules)          │
│  └── SQL Package (6 modules)          │
├─────────────────────────────────────────┤
│  Configuration Layer                    │
│  └── config.py (All config classes)   │
├─────────────────────────────────────────┤
│  Utility Layer                         │
│  └── fwf_utils.py (Schema helpers)    │
└─────────────────────────────────────────┘
```

## Core Modules

### `__init__.py` - Package Interface
**Purpose**: Provides the public API for the forklift.inputs package

**Exports**:
- All input handler classes (`CsvInputHandler`, `ExcelInputHandler`, `FwfInputHandler`, `SqlInputHandler`)
- All configuration classes (`CsvInputConfig`, `ExcelInputConfig`, `FwfInputConfig`, etc.)
- Specialized configuration classes (`FwfFieldSpec`, `FwfConditionalSchema`, `ExcelSheetConfig`)

**Role in Framework**: Serves as the single import point for all input functionality, ensuring clean API boundaries.

### `config.py` - Configuration Management
**Purpose**: Centralized configuration classes for all input types using dataclasses for type safety

**Configuration Classes**:
- **`CsvInputConfig`**: CSV processing parameters (delimiters, encoding, header detection)
- **`ExcelInputConfig`**: Excel file processing (sheet selection, date systems, engines)
- **`FwfInputConfig`**: Fixed-width file configuration (field specs, conditional schemas)
- **`SqlInputConfig`**: Database connection and query configuration (`read_only` defaults to `True`; the connection string and connection parameters are left out of `repr()`)
- **Helper Classes**: `FwfFieldSpec`, `FwfConditionalSchema`, `ExcelSheetConfig`

**Key Features**:
- Type-safe configuration using dataclasses
- Sensible defaults for all parameters
- Support for complex configurations (conditional schemas, multi-sheet processing)
- Comprehensive null value and encoding handling

**Role in Framework**: Provides declarative configuration that drives all input processing, enabling consistent behavior across different data sources.

### `csv.py` - CSV Input Handler
**Purpose**: Handles CSV file reading with header detection, encoding detection, and preprocessing

**Key Classes**:
- **`CsvInputHandler`**: Main orchestrator for CSV processing

**Core Functionality**:
- **Encoding Detection**: Statistical detection with `chardet` (or `charset-normalizer`), confirmed by decoding the whole file; without either library utf-8, cp1252 and latin-1 are tried
- **Header Discovery**: Header row detection with configurable search depth (`header_mode` `present`, `absent` or `auto`)
- **Comment Handling**: Regex-based comment row filtering (invalid patterns raise `ValueError` when the config is created)
- **PyArrow Integration**: Direct PyArrow CSV streaming reader creation
- **Preprocessing**: Blank line handling

**Methods**:
- `detect_encoding()`: Analyzes file encoding using statistical detection
- `find_header_row()`: Locates header row while respecting comment patterns; `absent` returns `(-1, [])`, a UTF-8 byte order mark is ignored, and `ValueError` is raised if no header row is found within `header_search_rows`
- `create_arrow_reader()`: Creates configured PyArrow streaming reader
- `_is_comment_row()`: Filters comment rows based on regex patterns

**Role in Framework**: Provides robust CSV ingestion with automatic format detection and preprocessing, handling the complexities of real-world CSV files.

### `excel.py` - Excel Input Handler
**Purpose**: Handles Excel file reading with multi-sheet support and flexible sheet selection

**Key Classes**:
- **`ExcelInputHandler`**: Main orchestrator for Excel processing

**Core Functionality**:
- **Streaming readers**: `.xlsx` is read with openpyxl in read-only mode, legacy `.xls` with xlrd (chosen by extension unless `ExcelInputConfig.engine` is set); no DataFrame library is involved and sheets are turned into PyArrow tables column by column
- **Resource limits**: `.xlsx` archives are inspected before they are opened (`max_uncompressed_bytes`, `max_compression_ratio`; the check trusts the sizes declared in the archive) and sheets are read with `max_rows` / `max_cells` caps; exceeding a limit raises `ValueError`. Sparse sheets are never expanded to their bounding box
- **Sheet Selection**: By name, 0-based index or regex; a name, index or regex that matches nothing raises `ValueError`
- **Data Range Control**: `header.row` is a **0-based** offset (`0` = sheet row 1); `data_start_row` / `data_end_row` are **1-based** sheet rows, inclusive, and data never starts on or before the header row. `header.mode` is `present` (default), `absent` (columns `col_1`..`col_N`, or `header.override`) or `auto` (the row is a header only if it consists of unique, non-numeric labels)
- **Null handling**: `keep_default_na=True` (default) means only empty and whitespace-only cells are null; `NA`, `N/A` etc. stay text unless listed in `na_values` or `nulls` (global or per column). Formula cells without a cached value are null
- **Types**: Cells keep their Excel type (int, float, bool, date, datetime, time); a column that mixes types becomes text, and `parquetType` entries in the column mappings cast explicitly

**Methods**:
- `detect_engine()`: Selects appropriate Excel engine based on file extension
- `open_workbook()`/`close_workbook()`: Workbook lifecycle management (the handler is also a context manager)
- `get_sheet_info(file_path)`: `{"engine", "sheet_count", "sheet_names"}` without reading any sheet data
- `process_sheets(file_path)`: Generator of `(sheet_name, pyarrow.Table)` for every configured sheet (all sheets when `config.sheets` is unset)
- `get_sheet_names()`: Extracts available sheet names
- `select_sheets()`: Matches sheets against configuration criteria
- `read_sheet_data()`: Reads configured data ranges from sheets

**Role in Framework**: Enables processing of complex Excel files with multiple sheets and varied layouts, supporting enterprise data extraction scenarios.

### `fwf.py` - Fixed-Width File Public Interface
**Purpose**: Provides the public API for Fixed-Width File processing by delegating to the modular FWF package

**Functionality**:
- Imports all classes from the `fwf/` package structure
- Provides clean public interface for FWF processing
- Enables organized separation of concerns with specialized modules

**Role in Framework**: Serves as the primary interface for FWF processing while leveraging the modular architecture for implementation.

### `fwf_utils.py` - FWF Configuration Utilities
**Purpose**: Provides helper functions for creating FWF configurations from various sources

**Key Functions**:
- **`create_fwf_config_from_schema()`**: Creates FwfInputConfig from JSON schema files
- **`create_simple_fwf_config()`**: Creates basic FWF configurations programmatically

**Features**:
- **Schema Standard Support**: Reads x-fwf configurations from schema files
- **Conditional Schema Handling**: Supports complex conditional FWF processing
- **Programmatic Configuration**: Enables dynamic configuration creation
- **Validation**: Built-in configuration validation and error handling

**Role in Framework**: Bridges between declarative schema definitions and runtime configuration, enabling schema-driven FWF processing.

### `sql.py` - SQL Database Public Interface
**Purpose**: Provides the public API for SQL database processing by delegating to the modular SQL package

**Functionality**:
- Imports all classes from the `sql/` package structure
- Provides clean public interface for SQL processing components
- Maintains consistent logging across the framework

**Role in Framework**: Serves as the primary interface for SQL processing while leveraging the modular architecture for implementation.

## Specialized Packages

### FWF Package (`fwf/`)
The Fixed-Width File package is organized into six specialized modules:

**`handlers.py`** - **FwfInputHandler**
- Main orchestrator coordinating all FWF processing
- File reading, PyArrow table creation, schema generation
- Methods delegating to specialized components for separation of concerns
- Problems are recorded instead of silently dropped: `handler.errors` lists the (line number, field) pairs whose value could not be converted to the declared type (the field becomes null), and `handler.rejected_lines` lists lines that were dropped because no conditional schema matches their flag (`no_matching_schema`) or a required field is blank (`required_missing`). Entries contain line numbers and field names only, never the data
- Type conversion is strict: an integer field that is not an integer (or is out of range for its width) and a boolean other than `true/false/1/0/yes/no/y/n/t/f` are invalid values, not silent zeros or `False`; a UTF-8 byte order mark on the first line is ignored; the flag column of a conditional schema is populated for every record, even if the matched variant does not list it among its own fields
- Specs are validated when the handler is created (`ValueError`)

**`parsers.py`** - **FwfLineParser, FwfFieldExtractor**
- Core parsing logic for individual lines and fields
- Conditional schema detection and field extraction
- Line filtering (comments, blanks, footers)

**`converters.py`** - **FwfTypeConverter, FwfValueProcessor**
- Type conversion from Parquet types to PyArrow types
- Value processing including null value handling
- Support for complex types (decimals, timestamps, lists)

**`detectors.py`** - **FwfEncodingDetector, FwfSchemaDetector**
- Automatic encoding detection using chardet
- Conditional schema detection and pattern matching
- Comment and footer detection using regex patterns

**`validators.py`** - **FwfFieldValidator, FwfSchemaValidator, FwfConfigValidator**
- Comprehensive validation of field specifications
- Schema validation including overlap detection
- Configuration validation ensuring component consistency

**`__init__.py`** - Package interface exposing all FWF components

### SQL Package (`sql/`)
The SQL database package is organized into six specialized modules:

**`handler.py`** - **SqlInputHandler**
- Main orchestrator for all SQL operations
- Connection management, schema discovery, batch processing
- Context manager support for resource cleanup

**`connection.py`** - **SqlConnectionManager**
- ODBC connection management with proper error handling (read-only by default)
- Connection and query timeout configuration
- Connection state validation and lifecycle management

**`schema.py`** - **SqlSchemaManager**
- Database schema discovery and PyArrow schema generation
- Table and view enumeration across database schemas
- Table specification parsing; names are validated against the catalog and identifiers are always quoted

**`reader.py`** - **SqlDataReader**
- Streaming data reads with configurable batch processing
- PyArrow RecordBatch generation for memory efficiency
- SQL query construction with proper identifier handling

**`types.py`** - **SqlTypeConverter**
- Comprehensive SQL to PyArrow type mapping
- ODBC type constant conversion and fallback handling
- Support for all major database types (numeric, date/time, binary, text)

**`__init__.py`** - Package interface exposing all components

## Integration with Forklift Framework

### Data Flow
1. **Configuration**: Declarative configuration defines processing parameters
2. **Input Processing**: Handler classes coordinate data ingestion from various sources
3. **Type Conversion**: Automatic conversion to PyArrow types for standardized processing
4. **Schema Generation**: Dynamic schema creation based on data source metadata
5. **Streaming Output**: Memory-efficient PyArrow tables/batches for downstream processing

### PyArrow Integration
All input handlers provide seamless PyArrow integration:
- **Standardized Types**: Consistent type system across all data sources
- **Streaming Support**: Memory-efficient processing of large datasets
- **Schema Preservation**: Metadata and type information maintained throughout pipeline
- **Performance**: Native PyArrow operations for optimal processing speed

### Error Handling Strategy
- **Configuration Validation**: Comprehensive validation at initialization
- **Runtime Recovery**: Graceful handling of parsing and conversion errors
- **Resource Management**: Proper cleanup using context managers
- **Informative Errors**: Detailed error messages for debugging and monitoring

### Extensibility
The modular architecture supports easy extension:
- **New Data Sources**: Add new handler classes following existing patterns
- **Custom Processing**: Override specific components while reusing infrastructure
- **Format Variations**: Extend existing handlers for specialized formats
- **Type System**: Add new type converters for custom data types

## Usage Patterns

### Basic Usage
```python
from forklift.inputs import CsvInputHandler, CsvInputConfig

# Configure and process CSV
config = CsvInputConfig(delimiter=';', encoding='utf-8')
handler = CsvInputHandler(config)
# Process using PyArrow streaming reader
```

### Advanced Configuration
```python
from forklift.inputs import FwfInputHandler
from forklift.inputs.fwf_utils import create_fwf_config_from_schema

# Schema-driven FWF processing
config = create_fwf_config_from_schema(schema_path)
handler = FwfInputHandler(config)
table = handler.create_arrow_table(file_path)
```

### Multi-Source Processing
```python
from forklift.inputs import SqlInputHandler, ExcelInputHandler

# Database processing with context management
with SqlInputHandler(sql_config) as sql_handler:
    for batch in sql_handler.read_table_data("schema", "table"):
        # Process PyArrow RecordBatch
        pass

# Excel multi-sheet processing
excel_handler = ExcelInputHandler(excel_config)
print(excel_handler.get_sheet_info(file_path)["sheet_names"])
for sheet_name, table in excel_handler.process_sheets(file_path):
    print(sheet_name, table.num_rows)  # table is a pyarrow.Table
```

## Performance Characteristics

- **Memory Efficiency**: Streaming processing with configurable batch sizes
- **Scalability**: CSV, FWF line parsing and SQL are streamed; Excel sheets are read into memory one sheet at a time (bounded by `max_rows` / `max_cells`)
- **Resource Management**: Context managers close workbooks and database connections (pyodbc pooling is switched off)
- **Error Recovery**: FWF records unconvertible values and rejected lines; Excel and SQL raise on problems they cannot represent

The `forklift.inputs` package provides a robust foundation for data ingestion in the Forklift ETL framework, handling the complexity of various data sources while maintaining performance and reliability.
