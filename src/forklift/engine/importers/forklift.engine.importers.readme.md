# Forklift Engine Importers

The `forklift.engine.importers` package provides format-specific importers for converting various data sources into Parquet format. This package currently supports Excel files and SQL databases through dedicated importer classes.

## Overview

The importers package contains two main components:

- **ExcelImporter**: Handles Excel file (.xlsx, .xls) import operations with multi-sheet support
- **SqlImporter**: Handles SQL database import operations with ODBC connectivity

Both importers output data in Apache Parquet format for efficient storage and processing.

## ExcelImporter

The `ExcelImporter` class provides functionality to import Excel files with support for multiple sheets, custom schemas, and various Excel-specific configurations.

### Key Features

- **Multi-sheet processing**: Import all sheets or specific sheets from Excel workbooks
- **Schema validation**: Optional schema-based configuration for precise data extraction
- **Engine flexibility**: Support for different Excel engines (openpyxl, xlrd, etc.)
- **Automatic sanitization**: Safe filename generation for output files
- **Flexible sheet selection**: Select sheets by name or index

### Usage

#### Basic Import (All Sheets)

```python
from forklift.engine.importers import ExcelImporter

results = ExcelImporter.import_excel(
    input_path="data/workbook.xlsx",
    output_path="output/",
    values_only=True
)
```

#### Import with Schema

```python
results = ExcelImporter.import_excel(
    input_path="data/workbook.xlsx",
    output_path="output/",
    schema_file="config/excel_schema.json",
    engine="openpyxl"
)
```

#### Import Specific Sheet

```python
results = ExcelImporter.import_excel(
    input_path="data/workbook.xlsx",
    output_path="output/",
    sheet="Sales Data"  # By name
)

# Or by index
results = ExcelImporter.import_excel(
    input_path="data/workbook.xlsx",
    output_path="output/",
    sheet=0  # First sheet
)
```

### Parameters

- **input_path** (Union[str, Path]): Path to the Excel file to import
- **output_path** (Union[str, Path]): Directory where Parquet files will be saved
- **schema_file** (Union[str, Path], optional): Path to Excel schema configuration file
- **values_only** (bool, optional): Whether to read only cell values (default: True)
- **engine** (str, optional): Excel engine to use (openpyxl, xlrd, etc.)
- **date_system** (str, optional): Excel date system ("1900" or "1904")
- **sheet** (Union[str, int], optional): Specific sheet to process (name or index)

### Output

Each sheet is saved as a separate Parquet file with the naming pattern: `{workbook_name}_{sheet_name}.parquet`

The method returns a `ProcessingResults` object containing:
- Total rows processed
- Processing execution time
- List of output files created
- Any errors encountered

## SqlImporter

The `SqlImporter` class provides functionality to import data from SQL databases using ODBC connectivity, with support for multiple tables and batch processing.

### Key Features

- **ODBC connectivity**: Connect to various SQL databases (SQL Server, PostgreSQL, MySQL, etc.)
- **Schema-driven processing**: Required schema file specifies which tables to import
- **Batch processing**: Configurable batch sizes for memory-efficient processing
- **Streaming support**: Enable streaming for large datasets
- **Connection management**: Automatic connection handling and cleanup
- **Flexible naming**: Custom output names for tables

### Usage

#### Basic SQL Import

```python
from forklift.engine.importers import SqlImporter

results = SqlImporter.import_sql(
    connection_string="DRIVER={SQL Server};SERVER=localhost;DATABASE=mydb;UID=user;PWD=pass",
    output_path="output/",
    schema_file="config/sql_schema.json"
)
```

#### Import with Custom Configuration

```python
results = SqlImporter.import_sql(
    connection_string="postgresql://user:pass@localhost:5432/mydb",
    output_path="output/",
    schema_file="config/sql_schema.json",
    batch_size=50000,
    query_timeout=600,
    enable_streaming=True,
    use_quoted_identifiers=True
)
```

### Parameters

- **connection_string** (str): ODBC connection string for the database
- **output_path** (Union[str, Path]): Directory where Parquet files will be saved
- **schema_file** (Union[str, Path]): **Required** - Path to SQL schema configuration file
- **batch_size** (int, optional): Number of rows per batch (default: 10000)
- **query_timeout** (int, optional): Query timeout in seconds (default: 300)
- **connection_timeout** (int, optional): Connection timeout in seconds (default: 30)
- **use_quoted_identifiers** (bool, optional): Whether to use quoted SQL identifiers
- **schema_name** (str, optional): Default schema name for tables
- **enable_streaming** (bool, optional): Enable streaming mode (default: True)
- **null_values** (list, optional): Custom null value representations

### Output

Each table is saved as a separate Parquet file. The naming convention depends on the schema configuration:
- If `output_name` is specified: `{output_name}.parquet`
- If schema name exists: `{schema_name}_{table_name}.parquet`
- Default: `{table_name}.parquet`

Additionally, a `metadata.json` file is created containing:
- Processing summary (tables processed, row counts, execution time)
- Input configuration details
- List of output files

## Error Handling

Both importers implement comprehensive error handling:

- **File validation**: Check for file existence and accessibility
- **Schema validation**: Validate schema files before processing
- **Connection errors**: Handle database connectivity issues
- **Processing errors**: Capture and report data processing failures
- **Resource cleanup**: Ensure proper cleanup of connections and file handles

## ProcessingResults

Both importers return a `ProcessingResults` object with the following attributes:

```python
class ProcessingResults:
    total_rows: int          # Total number of rows processed
    valid_rows: int          # Number of successfully processed rows
    invalid_rows: int        # Number of rows that failed processing
    execution_time: float    # Processing time in seconds
    output_files: List[str]  # List of generated output file paths
    errors: List[str]        # List of error messages encountered
```

## Dependencies

The importers package relies on several core forklift modules:

- `forklift.inputs.excel`: Excel input handling and configuration
- `forklift.inputs.sql`: SQL input handling and configuration  
- `forklift.schema.excel_schema_importer`: Excel schema validation
- `forklift.schema.sql_schema_importer`: SQL schema validation
- `forklift.io`: Parquet writer functionality
- `forklift.engine.config`: Processing configuration and results

## Best Practices

### Excel Import

1. **Use schema files** for complex Excel structures with specific column mappings
2. **Specify sheet selection** when you only need specific sheets to improve performance
3. **Set appropriate engines** based on your Excel file format and requirements
4. **Enable values_only** for better performance when formulas aren't needed

### SQL Import

1. **Always provide a schema file** - it's required and ensures explicit table selection
2. **Tune batch sizes** based on available memory and network conditions
3. **Use connection pooling** for multiple import operations
4. **Enable streaming** for large datasets to manage memory usage
5. **Set appropriate timeouts** based on query complexity and network conditions

## Example Schema Files

### Excel Schema Example

```json
{
  "sheets": [
    {
      "select": {"name": "Sales Data"},
      "columns": ["Date", "Product", "Amount"],
      "header": 0,
      "dataStartRow": 1,
      "skipBlankRows": true,
      "nameOverride": "sales"
    }
  ],
  "valuesOnly": true,
  "dateSystem": "1900"
}
```

### SQL Schema Example

```json
{
  "tables": [
    {
      "schema": "dbo",
      "name": "customers",
      "outputName": "customer_data"
    },
    {
      "schema": "dbo", 
      "name": "orders"
    }
  ]
}
```
