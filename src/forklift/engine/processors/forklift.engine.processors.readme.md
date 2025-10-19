# Forklift Engine Processors

## Overview

The **Processors** module is a core component of the Forklift data processing engine that handles the actual transformation and processing of data after it has been imported. Processors are responsible for taking raw data and converting it into clean, validated output formats with comprehensive metadata and error handling.

## Role in the Forklift Ecosystem

Forklift follows a modular architecture with distinct responsibilities:

1. **Importers** (`/importers/`) - Handle reading data from various sources (CSV, Excel, SQL, etc.)
2. **Processors** (`/processors/`) - Transform and validate the imported data 
3. **Config** (`/config/`) - Manage configuration and settings
4. **Core Engine** (`forklift_core.py`) - Orchestrates the entire workflow

### Processing Pipeline

```
Raw Data → Importer → Processor → Validated Output
                         ↓
                    Metadata & Manifests
```

Processors sit between the raw imported data and the final output, providing:
- **Schema validation** and constraint checking
- **Batch processing** for memory-efficient handling of large datasets
- **Header detection** and column mapping
- **Error handling** with separate good/bad data streams
- **Output generation** in multiple formats (Parquet, JSON metadata, manifests)

## Processor Components

### Core Interfaces

#### `base.py`
Legacy abstract base class for processors. Contains a minimal interface definition that has been superseded by `base_processor.py`.

**Key Components:**
- `BaseProcessor` (ABC) - Abstract interface requiring `process()` method implementation

#### `base_processor.py` 
The primary abstract base class that defines the processor interface used throughout Forklift.

**Key Components:**
- `BaseProcessor` (ABC) - Main abstract base class for all data processors
- `process()` method signature - Takes `ImportConfig` and returns `ProcessingResults`

**Usage Pattern:**
```python
class MyProcessor(BaseProcessor):
    def process(self, config: ImportConfig) -> ProcessingResults:
        # Implementation here
        pass
```

### Main Processors

#### `csv_processor.py`
The primary processor implementation for CSV data processing. This is the most comprehensive processor in the system.

**Key Features:**
- **Streaming Processing** - Uses PyArrow for memory-efficient batch processing
- **S3 Integration** - Supports both local files and S3 input/output
- **Header Detection** - Automatic detection of header rows
- **Schema Validation** - Validates data against JSON schemas
- **Error Separation** - Splits valid and invalid data into separate output streams
- **Metadata Generation** - Creates comprehensive metadata about processed data
- **Manifest Creation** - Generates file manifests for output tracking

**Main Workflow:**
1. Initialize components (schema processor, header detector, batch processor)
2. Load schema from file (if provided)
3. Detect header row location and column names
4. Process data in streaming batches
5. Validate each batch against schema
6. Write valid/invalid data to separate Parquet files
7. Generate metadata and manifest files

**Key Methods:**
- `process()` - Main processing orchestration method
- `_detect_header_row()` - Determines header location
- `_validate_batch()` - Validates data against schema
- `_create_s3_manifest()` / `_create_s3_metadata()` - Output file generation

### Specialized Processing Components

#### `batch_processor.py`
Handles the core batch processing logic for streaming large datasets efficiently.

**Key Features:**
- **PyArrow Integration** - Creates streaming RecordBatch readers
- **Memory Management** - Processes data in configurable batch sizes
- **Column Mismatch Handling** - Deals with rows having different column counts
- **Footer Detection** - Stops processing when footers are detected
- **S3 Streaming** - Fallback processing for S3 inputs
- **Data Corruption Detection** - Identifies and handles corrupted data

**Key Methods:**
- `create_batch_reader()` - Creates PyArrow streaming reader for local files
- `create_s3_batch_reader()` - Unified interface for both local and S3 files
- `_handle_column_mismatch_reader()` - Handles inconsistent column counts
- `_convert_rows_to_batch()` - Converts row data to PyArrow RecordBatch
- `_create_filtered_file()` - Creates temporary files with footers removed

**Excess Column Handling Modes:**
- `REJECT` - Skip rows with extra columns
- `TRUNCATE` - Remove excess columns from rows  
- `PASSTHROUGH` - Keep all columns, extending schema as needed

#### `header_detector.py`
Specialized component for detecting and extracting header information from CSV files.

**Key Features:**
- **Multiple Detection Modes** - AUTO, PRESENT, ABSENT
- **Comment Row Handling** - Skips rows matching comment patterns
- **Footer Detection** - Stops processing when footers are found
- **S3 Support** - Works with both local files and S3 objects
- **Pattern Matching** - Uses regex patterns for flexible detection

**Header Modes:**
- `PRESENT` - Header expected at first non-comment row
- `ABSENT` - No header present, use schema or generate names
- `AUTO` - Automatically detect header by analyzing content patterns

**Key Methods:**
- `detect_header_row()` - Main header detection orchestration
- `_find_first_data_row()` - Locates first non-comment data row
- `_auto_detect_header()` - Analyzes multiple rows to identify header
- `_looks_like_header()` - Determines if a row appears to be a header
- `should_stop_for_footer()` - Footer detection logic

#### `schema_processor.py`
Manages schema loading, conversion, and validation operations.

**Key Features:**
- **JSON Schema Support** - Loads and parses JSON schema files
- **PyArrow Conversion** - Converts JSON schemas to PyArrow schemas
- **S3 Schema Loading** - Supports schemas stored in S3
- **Metadata Configuration** - Extracts metadata generation settings from schema
- **Row Hash Configuration** - Handles primary key and hash configuration

**Key Methods:**
- `load_schema()` - Loads schema from file (local or S3)
- `_json_schema_to_pyarrow()` - Converts JSON schema to PyArrow format
- `_json_type_to_pyarrow()` - Maps JSON types to PyArrow data types
- `get_column_names_from_schema()` - Extracts column names from schema
- `get_metadata_config()` - Gets metadata generation configuration

**Supported Schema Extensions:**
- `x-rowHash` - Row hash and primary key configuration
- `x-metadata-generation` - Metadata collection settings

### Empty/Placeholder Files

#### `batch_converter.py`
Currently empty - likely intended for future batch conversion functionality.

#### `validator.py`
Currently empty - likely intended for future advanced validation logic.

## Configuration Integration

Processors work closely with the configuration system (`ImportConfig`) to:
- **Processing Parameters** - Batch size, encoding, delimiters
- **Validation Settings** - Schema file paths, validation modes
- **Output Configuration** - File paths, compression, metadata generation
- **Error Handling** - How to handle validation failures and malformed data

## Error Handling and Logging

The processors implement comprehensive error handling:
- **Graceful Degradation** - Continue processing when possible
- **Error Separation** - Invalid data written to separate bad rows file
- **Detailed Reporting** - Processing statistics and error details in results
- **Resource Cleanup** - Proper cleanup of temporary files and connections

## Performance Considerations

- **Streaming Architecture** - Memory-efficient processing of large files
- **Batch Processing** - Configurable batch sizes for optimal performance
- **PyArrow Integration** - High-performance columnar processing
- **S3 Optimization** - Efficient streaming for cloud storage
- **Lazy Loading** - Components initialized only when needed

## Future Extensions

The processor architecture is designed to be extensible:
- New processor types can be added by implementing `BaseProcessor`
- Additional validation logic can be added to `validator.py`
- Batch conversion utilities can be implemented in `batch_converter.py`
- New output formats can be supported by extending existing processors
