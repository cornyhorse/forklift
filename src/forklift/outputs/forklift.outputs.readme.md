# Forklift Outputs Package

The `forklift.outputs` package is responsible for the final stage of the Forklift data processing pipeline. After data has been read, validated, and processed, this package handles writing the results to various output formats with comprehensive metadata and data catalog integration.

## Package Overview

This package serves as the **output layer** in Forklift's data processing architecture:

```
Data Flow: Inputs → Processing → Validation → Outputs
           ↑                                    ↑
      readers.py                        outputs package
```

The outputs package transforms processed PyArrow data into production-ready formats with enterprise-grade metadata, manifests, and data lineage tracking.

## Architecture & Components

### Core Components

#### 1. **OutputConfig** (`config.py`)
Configuration class that controls all output operations:

```python
@dataclass
class OutputConfig:
    compression: str = "snappy"        # Parquet compression algorithm
    create_manifest: bool = True       # Generate manifest files for data catalogs
    create_metadata: bool = True       # Generate processing metadata
    row_group_size: int = 50000       # Parquet row group optimization
```

**Key Features:**
- Configurable compression algorithms (snappy, gzip, lz4, etc.)
- Toggle manifest and metadata generation
- Performance tuning via row group sizing
- Extensible for future output format configurations

#### 2. **ParquetOutputHandler** (`parquet.py`)
High-performance Parquet file writer with streaming capabilities:

**Key Features:**
- **Streaming writes**: Handles large datasets without memory overflow
- **Multiple writers**: Manages separate output streams (valid/invalid data)
- **Configurable compression**: Optimizes file size vs. performance
- **Row group optimization**: Controls internal Parquet structure for query performance
- **Batch processing**: Writes data in configurable batch sizes

**Usage Pattern:**
```python
handler = ParquetOutputHandler(config)
writer = handler.create_writer(output_path, schema)
handler.write_batch(writer, record_batch)
handler.close_all_writers()
```

#### 3. **ManifestGenerator** (`manifest.py`)
Creates data catalog-compatible manifest files:

**Key Features:**
- **Data catalog integration**: Compatible with Databricks, Apache Iceberg
- **File metadata**: Tracks file sizes, record counts, timestamps
- **JSON format**: Standardized manifest format for modern data systems
- **Automated discovery**: Enables automatic dataset discovery in data catalogs

**Generated Manifest Structure:**
```json
{
  "format_version": "1.0",
  "files": [
    {
      "file_path": "output_001.parquet",
      "file_size": 1048576,
      "record_count": 10000
    }
  ],
  "created_at": "2025-10-19T...",
  "total_files": 1,
  "total_size": 1048576
}
```

#### 4. **MetadataGenerator** (`metadata.py`)
Comprehensive processing statistics and lineage tracking:

**Key Features:**
- **Processing summary**: Input/output record counts, validation results
- **Column-level statistics**: Data types, column counts, schema information
- **Configuration tracking**: Preserves processing configuration for reproducibility
- **Data lineage**: Links input sources to output files
- **Quality metrics**: Validation results and data quality indicators

**Generated Metadata Structure:**
```json
{
  "processing_summary": {
    "total_records": 100000,
    "valid_records": 99950,
    "invalid_records": 50
  },
  "input_config": { ... },
  "column_statistics": {
    "output.parquet": {
      "num_columns": 15,
      "num_rows": 99950,
      "column_names": ["id", "name", ...],
      "column_types": ["int64", "string", ...]
    }
  }
}
```

## Integration with Forklift Pipeline

### Data Processing Flow

1. **Input Stage** (`forklift.inputs`): Reads data from various sources
2. **Processing Stage** (`forklift.processors`): Validates and transforms data
3. **Output Stage** (`forklift.outputs`): **← This package**
   - Writes processed data to Parquet files
   - Generates data catalog manifests
   - Creates processing metadata
   - Maintains data lineage information

### Integration Points

#### With Processing Engine
```python
# Engine provides processed data and statistics
output_handler = ParquetOutputHandler(output_config)
manifest_gen = ManifestGenerator()
metadata_gen = MetadataGenerator()

# Write data and generate metadata
writer = output_handler.create_writer(path, schema)
output_handler.write_batch(writer, processed_data)
manifest_gen.create_manifest(output_dir, output_files)
metadata_gen.create_metadata(output_dir, processing_stats)
```

#### With CLI Interface
The outputs package is configured through CLI parameters:
- `--compression`: Sets ParquetOutputHandler compression
- `--no-manifest`: Disables manifest generation
- `--no-metadata`: Disables metadata generation
- `--row-group-size`: Controls Parquet optimization

## Enterprise Features

### Data Catalog Integration
- **Manifest files** enable automatic dataset discovery
- **Metadata files** provide data lineage and quality metrics
- **Standardized formats** work with modern data platforms

### Performance Optimization
- **Streaming writes** handle datasets larger than memory
- **Configurable row groups** optimize query performance
- **Compression options** balance storage vs. processing speed

### Quality Assurance
- **Comprehensive metadata** tracks processing statistics
- **Column-level analytics** monitor data quality
- **Configuration preservation** ensures reproducible results

## Usage Examples

### Basic Usage
```python
from forklift.outputs import OutputConfig, ParquetOutputHandler

config = OutputConfig(compression="snappy", row_group_size=100000)
handler = ParquetOutputHandler(config)
```

### Enterprise Pipeline
```python
from forklift.outputs import (
    OutputConfig, 
    ParquetOutputHandler, 
    ManifestGenerator, 
    MetadataGenerator
)

# Configure output settings
config = OutputConfig(
    compression="lz4",           # Fast compression
    create_manifest=True,        # Enable data catalog
    create_metadata=True,        # Enable lineage tracking
    row_group_size=50000        # Optimize for queries
)

# Initialize components
output_handler = ParquetOutputHandler(config)
manifest_gen = ManifestGenerator()
metadata_gen = MetadataGenerator()

# Process and write data
writer = output_handler.create_writer(output_path, schema)
for batch in processed_batches:
    output_handler.write_batch(writer, batch)

output_handler.close_all_writers()

# Generate metadata and manifests
manifest_gen.create_manifest(output_dir, output_files)
metadata_gen.create_metadata(output_dir, processing_statistics)
```

## File Organization

```
outputs/
├── __init__.py           # Public API exports
├── config.py             # OutputConfig class
├── parquet.py           # ParquetOutputHandler
├── manifest.py          # ManifestGenerator
├── metadata.py          # MetadataGenerator
└── forklift.outputs.readme.md  # This documentation
```

## Dependencies

- **PyArrow**: High-performance Parquet reading/writing
- **pathlib**: Modern path handling
- **json**: Manifest and metadata serialization
- **datetime**: Timestamp generation for metadata

## Future Enhancements

The outputs package is designed for extensibility:
- Additional output formats (Delta Lake, Iceberg)
- Advanced compression algorithms
- Cloud storage optimization
- Real-time streaming outputs
- Advanced data catalog integrations
