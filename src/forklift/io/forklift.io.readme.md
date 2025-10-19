# Forklift I/O Package

The `forklift.io` package provides unified I/O capabilities for ForkliftCore, enabling seamless data streaming between local filesystem and Amazon S3 storage systems. This package is a critical component of the Forklift data processing framework, providing the infrastructure layer for reading and writing data across different storage backends.

## Package Overview

The I/O package serves as the foundation for data ingestion and output in the Forklift ecosystem. It abstracts away the complexity of working with different storage systems, allowing higher-level components to process data without needing to know whether the data resides locally or in cloud storage.

### Key Features

- **Unified Interface**: Single API for working with local files and S3 objects
- **Streaming Architecture**: Efficient memory usage through streaming I/O operations
- **Format Support**: Built-in support for CSV and Parquet formats
- **S3 Multipart Uploads**: Optimized handling of large files to S3
- **Cross-Platform Compatibility**: Works seamlessly across different storage backends

## Architecture Context

Within the broader Forklift application, this package sits at the infrastructure layer:

```
┌─────────────────────────────────────┐
│        Forklift Core Engine        │  ← High-level data processing
├─────────────────────────────────────┤
│      Data Transformations &        │  ← Business logic layer
│      Schema Validation              │
├─────────────────────────────────────┤
│          forklift.io                │  ← I/O abstraction layer (THIS PACKAGE)
├─────────────────────────────────────┤
│    Local FS    │      AWS S3        │  ← Storage backends
└─────────────────────────────────────┘
```

The package enables Forklift's core processing engine to work with data regardless of where it's stored, supporting hybrid cloud architectures and enabling data pipeline flexibility.

## Module Documentation

### `__init__.py`

The package initialization module that exposes the main public API. It provides a clean interface by importing and re-exporting the most commonly used classes and functions:

**Key Exports:**
- `S3StreamingClient`: Main S3 interaction client
- `S3Path`: S3 path parsing and manipulation utility
- `UnifiedIOHandler`: Unified interface for local/S3 operations
- `UnifiedCSVWriter`: CSV writer supporting both local and S3 outputs
- `S3ParquetWriter`: Parquet writer optimized for S3
- Helper functions: `is_s3_path()`, `get_s3_client()`, `create_parquet_writer()`

### `s3_streaming.py`

Core S3 streaming functionality built on top of boto3, providing efficient streaming I/O operations for Amazon S3.

#### Key Classes:

**`S3Path`**
- Utility class for parsing and manipulating S3 URIs
- Provides path operations similar to `pathlib.Path` but for S3
- Features: parent directory access, path joining, name extraction
- Validates S3 URI format and extracts bucket/key components

**`S3StreamingClient`**
- Main client for S3 operations with streaming capabilities
- Supports both default AWS credential chain and explicit credentials
- Key methods:
  - `exists()`: Check if S3 object exists
  - `get_size()`: Get object size in bytes
  - `open_for_read()`: Stream data from S3 with support for both text and binary modes
  - `open_for_write()`: Stream data to S3 using multipart uploads
  - `list_objects()`: Paginated listing of S3 objects

**`S3StreamingWriter`**
- Implements multipart upload for efficient large file uploads to S3
- Automatically manages upload parts based on configurable part size (default: 100MB)
- Supports both text and binary write modes
- Provides file-like interface with proper context manager support
- Handles upload completion and cleanup on errors

#### Key Functions:
- `is_s3_path()`: Utility to detect S3 URIs
- `get_s3_client()`: Factory function for creating configured S3 clients

### `unified_io.py`

Provides a unified interface that abstracts local filesystem and S3 operations, integrating with ForkliftCore's streaming architecture.

#### Key Classes:

**`UnifiedIOHandler`**
- Main interface for unified I/O operations across storage backends
- Automatically detects path type (local vs S3) and routes operations appropriately
- Key capabilities:
  - File existence checking across storage types
  - Size retrieval for optimization decisions
  - Unified file opening for read/write operations
  - CSV reading/writing with format consistency
  - Cross-storage file copying (local↔local, local↔S3, S3↔S3)

**`UnifiedCSVWriter`**
- Context manager for CSV writing that works with both local files and S3
- Maintains consistent CSV formatting across storage backends
- Handles encoding and delimiter configuration
- Integrates with Python's built-in `csv` module

**`S3ParquetWriter`**
- Specialized Parquet writer optimized for S3 output
- Uses temporary local files for Parquet generation, then uploads to S3
- Supports PyArrow schema definition and compression options
- Handles both table and batch writing modes
- Provides proper resource cleanup and error handling

#### Key Functions:
- `create_parquet_writer()`: Factory function that returns appropriate writer based on path type
- `get_s3_client()`: Wrapper for S3 client creation (used by tests for mocking)

## Usage Patterns

### Basic File Operations
```python
from forklift.io import UnifiedIOHandler

io_handler = UnifiedIOHandler()

# Works with both local files and S3
if io_handler.exists("s3://mybucket/data.csv"):
    size = io_handler.get_size("s3://mybucket/data.csv")
    
with io_handler.open_for_read("s3://mybucket/data.csv") as f:
    data = f.read()
```

### CSV Processing
```python
from forklift.io import UnifiedIOHandler

io_handler = UnifiedIOHandler()

# Read CSV from any location
for row in io_handler.csv_reader("s3://mybucket/input.csv"):
    process_row(row)

# Write CSV to any location
with io_handler.csv_writer("s3://mybucket/output.csv") as writer:
    writer.writerow(["column1", "column2"])
```

### Parquet Operations
```python
from forklift.io import create_parquet_writer
import pyarrow as pa

schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("name", pa.string())
])

# Automatically handles local vs S3 based on path
with create_parquet_writer("s3://mybucket/output.parquet", schema) as writer:
    writer.write_table(table)
```

## Integration with Forklift Core

This I/O package is designed to integrate seamlessly with Forklift's data processing pipeline:

1. **Data Ingestion**: The unified interface allows Forklift to read source data from any supported storage location
2. **Intermediate Processing**: Temporary files and streaming operations support large-scale data transformations
3. **Output Generation**: Results can be written to the most appropriate storage location based on deployment configuration
4. **Pipeline Flexibility**: The same processing logic works across development (local files) and production (S3) environments

## Performance Considerations

- **Streaming Architecture**: Minimizes memory usage by processing data in chunks
- **Multipart Uploads**: Large S3 uploads are optimized using parallel multipart uploads
- **Configurable Chunk Sizes**: Allows tuning for different network and storage conditions
- **Efficient Path Detection**: Fast S3 URI detection avoids unnecessary overhead for local operations

## Error Handling

The package provides robust error handling:
- S3 credential and permission errors are properly propagated
- Network failures during streaming operations are handled gracefully  
- Partial uploads are cleaned up automatically on failure
- File system errors maintain consistent behavior across storage types

This package forms the critical foundation that enables Forklift to operate as a cloud-native data processing framework while maintaining compatibility with traditional file-based workflows.
