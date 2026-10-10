# Forklift SQL Input Package

The `forklift.inputs.sql` package provides comprehensive SQL database connectivity and data reading capabilities for the Forklift ETL framework. It supports various database engines (SQLite, PostgreSQL, MySQL, Oracle, SQL Server, etc.) through ODBC drivers and PyArrow for efficient data processing.

## Architecture Overview

The package is organized into modular components that follow separation of concerns:

- **`handler.py`** - Main orchestrator that coordinates all SQL operations
- **`connection.py`** - Database connection management with ODBC
- **`schema.py`** - Schema discovery and PyArrow schema generation
- **`reader.py`** - Data reading and batch processing
- **`types.py`** - SQL to PyArrow type conversion utilities
- **`__init__.py`** - Package exports and public API

## Core Components

### SqlInputHandler (handler.py)

The main entry point that orchestrates all SQL input operations. It provides a high-level interface for reading data from SQL databases with streaming support.

**Key Features:**
- Database connection management
- Schema discovery and validation
- Batch data reading with configurable batch sizes
- PyArrow integration for efficient data processing
- Support for multiple database engines via ODBC
- Context manager support for automatic resource cleanup

**Usage Example:**
```python
from forklift.inputs.config import SqlInputConfig
from forklift.inputs.sql import SqlInputHandler

config = SqlInputConfig(
    connection_string="Driver={SQLite3};Database=example.db;",
    batch_size=1000
)

with SqlInputHandler(config) as handler:
    tables = handler.get_table_list()
    schema = handler.get_table_schema("main", "users")
    for batch in handler.read_table_data("main", "users"):
        # Process PyArrow RecordBatch
        print(f"Processed {len(batch)} rows")
```

### SqlConnectionManager (connection.py)

Manages database connections using pyodbc with proper error handling and timeout configuration.

**Key Features:**
- ODBC connection establishment with custom parameters
- Connection timeout and query timeout configuration
- Connection state management and validation
- Context manager support for automatic cleanup
- Comprehensive error handling for connection failures

**Configuration Options:**
- Connection string with driver specifications
- Additional connection parameters (appended as `key=value`; values containing `;`, `=`, `{` or `}` are brace-escaped, and a parameter name that could inject another attribute is rejected with `ValueError`)
- Connection timeout settings
- Query timeout configuration
- `read_only` (default `True`): the database session itself is made read-only, because ODBC drivers commonly ignore pyodbc's `readonly` flag (psqlODBC and MariaDB Connector/ODBC both do). PostgreSQL gets `SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY`, MySQL and MariaDB `SET SESSION TRANSACTION READ ONLY`, SQLite `PRAGMA query_only = ON`; if that statement fails, `connect()` raises `ConnectionError` (pass `read_only=False` to connect without it). For other databases forklift can only ask the driver, and logs a warning: connect as a login that may only `SELECT`.
- `query_timeout`: applied as `statement_timeout` (PostgreSQL), `max_execution_time` (MySQL) or `max_statement_time` (MariaDB); elsewhere through pyodbc's `Connection.timeout`, with a warning when the driver does not support it

### SqlSchemaManager (schema.py)

Handles database schema discovery and PyArrow schema generation.

**Key Features:**
- Automatic table and view discovery across schemas
- Column metadata extraction (types, sizes, nullability)
- PyArrow schema generation with proper type mapping
- Table specification parsing (schema.table format)
- Catalog validation: every requested schema/table name is resolved against the database catalog (an exact match first, then a unique case-insensitive match) before it is used; an unknown table or a name that exists in several schemas raises `ValueError`. An explicit schema never falls back to a table of another schema
- Database identifier quoting: identifiers are **always** quoted (using the quote character the driver reports, `"` if unknown) with embedded quote characters doubled

**Supported Operations:**
- List all available tables and views
- Parse table specifications (e.g., "schema.table" or "table")
- Generate PyArrow schemas from database metadata
- Validate table existence and accessibility

### SqlDataReader (reader.py)

Handles reading data from SQL databases and converting to PyArrow format with batch processing.

**Key Features:**
- Streaming data reads with configurable batch sizes
- Automatic PyArrow RecordBatch generation
- Memory-efficient processing of large datasets
- Query construction from catalog-verified, always-quoted identifiers (`SELECT * FROM <quoted schema>.<quoted table>`)
- Row-to-column transposition for PyArrow compatibility

**Performance Options:**
- Configurable fetch size for ODBC cursor
- Batch size control for memory management
- Efficient data type conversion

### SqlTypeConverter (types.py)

Handles conversion between SQL data types and PyArrow data types with comprehensive type mapping.

**Supported Type Mappings:**
- **Integer Types**: INT/INTEGER → int32, BIGINT → int64, SMALLINT → int16, TINYINT → int16 (SQL Server's TINYINT is unsigned 0-255 and would overflow int8; MySQL's is signed; int16 holds both)
- **Floating Point**: REAL → float32, FLOAT/DOUBLE → float64 (FLOAT is a double precision type on SQL Server, SQLite and Oracle, so it is not narrowed to float32)
- **Decimal**: DECIMAL/NUMERIC → decimal128 with the column's precision and scale; a column that does not fit decimal128 (more than 38 digits) is read as string rather than failing the table; without precision information the column is float64
- **Money**: MONEY → decimal128(19,4), SMALLMONEY → decimal128(10,4)
- **Boolean**: BOOLEAN/BOOL/BIT → bool
- **Date/Time**: DATE → date32, TIME → time64[us], TIMESTAMP/DATETIME/DATETIME2/SMALLDATETIME → timestamp[us], TIMESTAMPTZ → timestamp[us, UTC]
- **Binary**: BINARY/VARBINARY/BLOB/BYTEA → binary
- **Text**: VARCHAR/CHAR/TEXT → string (default for unknown types, and UNIQUEIDENTIFIER)

A trailing `IDENTITY` in the reported type name (SQL Server reports `int identity`) is ignored.

**Features:**
- ODBC type constant conversion
- Custom null value handling
- String fallback for a text column whose values are not str; any other value that cannot be converted to the declared type raises `ValueError` (the message never contains the value)
- `SqlSchemaImporter` can override the mapping through `x-sql.parquetTypeMapping.sqlToParquet`; the overrides are merged over the defaults (see the schema package)

## Configuration

The package uses `SqlInputConfig` for configuration management:

```python
config = SqlInputConfig(
    connection_string="Driver={PostgreSQL};Server=localhost;Database=mydb;",
    connection_params={"Trusted_Connection": "yes"},
    connection_timeout=30,
    query_timeout=300,
    batch_size=1000,
    fetch_size=10000,
    null_values=["NULL", ""],
    read_only=True,
)
```

`repr(config)` leaves out `connection_string` and `connection_params`, because they usually hold
credentials. The importers (`import_sql`) additionally redact the connection string in the
`metadata.json` they write and in logged errors (`Pwd=***`).

### Configuration Parameters

- **`connection_string`**: ODBC connection string with driver and database details
- **`connection_params`**: Additional connection parameters as key-value pairs  
- **`connection_timeout`**: Timeout for establishing connections (seconds)
- **`query_timeout`**: Timeout for query execution (seconds)
- **`batch_size`**: Number of rows per PyArrow RecordBatch
- **`fetch_size`**: ODBC cursor fetch size for performance tuning
- **`null_values`**: List of string values to treat as NULL
- **`use_quoted_identifiers`**: Accepted for backward compatibility only. Identifiers are always quoted, because they are interpolated into SQL text
- **`read_only`**: Make the session read-only (default: `True`; enforced by the database on PostgreSQL, MySQL, MariaDB and SQLite, see above)
- **`schema_name`**, **`enable_streaming`**, **`date_formats`**, **`timestamp_formats`**: accepted by `SqlInputConfig` but not used by the reader at present (rows are always fetched in `batch_size` chunks and no date/time text formats are parsed)

## Database Support

The package supports any database with an ODBC driver:

### Tested Databases
- **SQLite** - File-based database for development and testing
- **PostgreSQL** - Open-source relational database
- **MySQL/MariaDB** - Popular open-source databases
- **Microsoft SQL Server** - Enterprise database system
- **Oracle Database** - Enterprise database system

### Driver Requirements
Each database requires appropriate ODBC drivers:
- SQLite: SQLite ODBC Driver
- PostgreSQL: psqlODBC
- MySQL: MySQL ODBC Connector
- SQL Server: ODBC Driver for SQL Server
- Oracle: Oracle ODBC Driver

## Error Handling

The package provides comprehensive error handling:

- **Connection Errors**: Detailed error messages for connection failures
- **Schema Errors**: A missing or ambiguous table raises `ValueError`; a table without any column in the catalog raises `ValueError`
- **Type Conversion Errors**: Text columns fall back to strings; other values that do not fit the declared type raise `ValueError`
- **Query Errors**: Proper cleanup and error propagation
- **Timeout Errors**: Configurable timeouts with appropriate error messages

## Integration Points

### Schema Validation
The package integrates with Forklift's schema validation system:
- Optional `SqlSchemaImporter` integration for custom type mappings
- Validation of table existence and accessibility
- Schema-driven table discovery and processing

### PyArrow Integration
Seamless integration with PyArrow for efficient data processing:
- Direct conversion to PyArrow RecordBatch format
- Proper type mapping from SQL to PyArrow types
- Memory-efficient streaming with configurable batch sizes
- Schema preservation with nullable field support

## Best Practices

1. **Use Context Managers**: Always use `with` statements for automatic resource cleanup
2. **Configure Batch Sizes**: Tune batch_size and fetch_size based on available memory
3. **Handle Timeouts**: Set appropriate connection and query timeouts
4. **Qualify table names**: Use `schema.table` for tables that exist in several schemas (a bare ambiguous name raises)
5. **Test Connections**: Validate database connectivity before production use
6. **Monitor Performance**: Use logging to monitor query execution times

## Troubleshooting

### Common Issues

**Import Error for pyodbc**:
```bash
pip install "forklift-etl[sql]"   # or: pip install pyodbc
```
pyodbc also needs the unixODBC runtime library (`libodbc`) on the system.

**Driver Not Found**:
- Verify ODBC driver installation
- Check connection string driver name
- Use system-appropriate driver names

**Connection Timeouts**:
- Increase connection_timeout in configuration
- Verify network connectivity to database
- Check database server availability

**Schema Discovery Issues**:
- Verify user permissions for metadata queries
- Check schema/database names in connection
- Names are matched exactly first and then case-insensitively; if two tables differ only by case, pass the exact spelling

**Performance Issues**:
- Adjust batch_size for memory constraints
- Tune fetch_size for network efficiency
- Consider query timeouts for long-running operations

## Dependencies

- **pyodbc**: ODBC database connectivity
- **pyarrow**: Efficient data processing and type conversion
- **logging**: Comprehensive logging throughout the package

## Backward Compatibility

The package maintains backward compatibility with existing Forklift SQL input functionality while providing a cleaner, more modular architecture. Legacy direct access patterns are supported through property delegation in the main handler class.
