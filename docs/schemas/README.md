# Forklift Extended JSON Schema (x-attributes) Documentation

## Overview
This directory contains comprehensive documentation for Forklift's extended JSON schema attributes (x-attributes) that provide powerful data processing, validation, transformation, and quality management capabilities beyond standard JSON schema functionality.

## Complete Feature Set

### Core Data Integrity Features

#### [x-primaryKey](./X_PRIMARY_KEY_DOCUMENTATION.md)
Primary key configuration and constraint enforcement with customizable violation handling.

**Key Features:**
- Single and composite primary key support
- Uniqueness enforcement with configurable actions
- Null value handling policies
- Integration with bad rows processing

#### [x-uniqueConstraints](./X_UNIQUE_CONSTRAINTS_DOCUMENTATION.md)
Additional unique constraints beyond primary keys for complex business rules.

**Key Features:**
- Multi-column unique constraints
- Conditional constraints based on business rules
- Case-sensitive and case-insensitive options
- Comprehensive violation reporting

#### [x-constraintHandling](./X_CONSTRAINT_HANDLING_DOCUMENTATION.md)
Comprehensive error handling and data quality issue management.

**Key Features:**
- Multiple error handling strategies (fail-fast, bad rows, ignore, transform)
- Detailed bad rows output with original data and error context
- Configurable validation options and error collection
- Integration with all constraint types

### Data Type and Validation Features

#### [x-special-type](./X_SPECIAL_TYPE_DOCUMENTATION.md)
Specialized data type handling for common structured formats.

**Supported Types:**
- **Personal Identifiers:** SSN, phone numbers, email addresses
- **Geographic:** ZIP codes (5-digit, 9-digit, permissive), IP addresses (IPv4, IPv6, auto-detect)
- **Network:** MAC addresses with format standardization
- **Validation & Normalization:** Automatic format detection and standardization

#### [x-transformations](./X_TRANSFORMATIONS_DOCUMENTATION.md)
Advanced data transformation and standardization capabilities.

**Transformation Categories:**
- **String Cleaning:** Unicode normalization, whitespace handling, quote standardization
- **Case Transformation:** Business-aware case conversion with exceptions
- **Numeric Cleaning:** Thousands separators, decimal handling, NaN processing
- **Money Type:** Currency symbol removal, parentheses negative notation
- **DateTime Parsing:** Multiple format support, fuzzy parsing, timezone handling
- **File-Type Specific:** FWF, SQL, and CSV-specific transformations

### Data Enhancement Features

#### [x-calculatedColumns](./X_CALCULATED_COLUMNS_DOCUMENTATION.md)
Dynamic column generation including constants, expressions, and computed fields.

**Column Types:**
- **Constants:** Static values for data lineage, versioning, partitioning
- **Expressions:** SQL-like expressions for data combination and transformation
- **Calculated Fields:** Pre-built functions for common calculations (age, string operations, etc.)
- **Partitioning & Indexing:** Optimization hints for downstream systems

#### [x-rowHash](./X_ROW_HASH_DOCUMENTATION.md)
Row-level hash generation and metadata for change detection and data integrity.

**Features:**
- **Multiple Hash Algorithms:** MD5, SHA1, SHA256, SHA384, SHA512
- **Input/Output Hashing:** Track changes made during processing
- **Metadata Columns:** Source URI, ingestion timestamps, row numbering
- **Change Detection:** Support for CDC and data quality monitoring

### Privacy and Security Features

#### [x-pii](./X_PII_DOCUMENTATION.md)
Comprehensive PII identification, classification, and protection.

**PII Categories:**
- **Direct Identifiers:** Names, SSNs, emails requiring strong protection
- **Quasi-Identifiers:** Birth dates, ZIP codes that can identify when combined
- **Sensitive Information:** Financial data, medical information requiring protection
- **System Identifiers:** Technical IDs with linking capability

**Masking Methods:**
- **Hash Masking:** Cryptographic hashing for complete anonymization
- **Generalization:** Broader categories preserving utility
- **Range/Category:** Ranges for sensitive numeric data
- **Redaction:** Complete or partial value removal

### File Format Specific Features

#### [x-csv](./X_CSV_DOCUMENTATION.md)
Advanced CSV processing with robust parsing and type mapping.

**Features:**
- **Encoding Detection:** Multi-encoding support with priority ordering
- **Delimiter Detection:** Automatic delimiter detection and custom format support
- **Header/Footer Handling:** Flexible header detection and footer exclusion
- **Null Handling:** Global and per-column null value configuration
- **Type Mapping:** Explicit Parquet type mapping with inference fallback

#### [x-fwf](./X_FWF_DOCUMENTATION.md)
Fixed-width file processing with precise field positioning.

**Features:**
- **Field Definition:** Exact positioning with start/length specifications
- **Alignment & Padding:** Left/right/center alignment with custom padding
- **Type Conversion:** Direct mapping to Parquet types
- **Conditional Processing:** Record-type based field definitions
- **Legacy Support:** Mainframe and legacy system compatibility

### Data Quality and Analysis Features

#### [x-metadata-generation](./X_METADATA_GENERATION_DOCUMENTATION.md)
Automatic metadata analysis and statistics generation.

**Analysis Types:**
- **Enum Detection:** Automatic categorical data identification
- **Statistical Analysis:** Comprehensive numeric statistics with quantiles and outlier detection
- **String Analysis:** Length statistics and pattern analysis
- **Performance Optimization:** Configurable sampling for large datasets

#### [x-dataQuality](./X_DATA_QUALITY_DOCUMENTATION.md)
Comprehensive data quality assessment and enforcement.

**Quality Dimensions:**
- **Completeness:** Null value analysis and required field coverage
- **Validity:** Format validation and business rule compliance
- **Uniqueness:** Duplicate detection and constraint validation
- **Consistency:** Cross-field validation and relationship checking
- **Statistical Quality:** Outlier detection and distribution analysis

### Data Integration Features

#### [x-columnMapping](./X_COLUMN_MAPPING_DOCUMENTATION.md)
Advanced column name mapping and standardization.

**Mapping Types:**
- **Global Mappings:** Universal column name transformations
- **Table-Specific Mappings:** Context-aware mappings per table/file
- **Pattern Mappings:** Regex-based transformations for systematic changes
- **Standardization:** Case conversion, special character handling, length limits

## Feature Integration Matrix

| Feature | Primary Key | Unique Constraints | Constraint Handling | Special Types | Transformations | Calculated Columns | PII | Row Hash | Metadata Gen | Data Quality | Column Mapping |
|---------|-------------|-------------------|-------------------|---------------|-----------------|-------------------|-----|----------|--------------|--------------|----------------|
| **Primary Key** | - | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Unique Constraints** | ✓ | - | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Constraint Handling** | ✓ | ✓ | - | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Special Types** | ✓ | ✓ | ✓ | - | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Transformations** | ✓ | ✓ | ✓ | ✓ | - | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Calculated Columns** | ✓ | ✓ | ✓ | ✓ | ✓ | - | ✓ | ✓ | ✓ | ✓ | ✓ |
| **PII** | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | - | ✓ | ✓ | ✓ | ✓ |
| **Row Hash** | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | - | ✓ | ✓ | ✓ |
| **Metadata Generation** | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | - | ✓ | ✓ |
| **Data Quality** | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | - | ✓ |
| **Column Mapping** | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | - |

## Common Use Case Scenarios

### Enterprise Data Integration
```json
{
  "x-columnMapping": {"globalMappings": {"emp_id": "employee_id"}},
  "x-transformations": {"stringCleaning": {"stripWhitespace": true}},
  "x-primaryKey": {"columns": ["employee_id"], "enforceUniqueness": true},
  "x-dataQuality": {"enabled": true, "qualityThresholds": {"validity": {"minimum": 0.95}}}
}
```

### Financial Data Processing
```json
{
  "x-special-type": "ssn",
  "x-pii": {"isPII": true, "category": "direct_identifier"},
  "x-transformations": {"moneyType": {"currencySymbols": ["$"]}},
  "x-constraintHandling": {"errorMode": "fail_fast"},
  "x-dataQuality": {"qualityThresholds": {"validity": {"minimum": 0.995}}}
}
```

### Legacy System Migration
```json
{
  "x-fwf": {"fields": [{"name": "id", "start": 1, "length": 10}]},
  "x-columnMapping": {"patternMappings": [{"pattern": "^(.+)_dt$", "replacement": "${1}_date"}]},
  "x-calculatedColumns": {"constants": [{"name": "migration_batch", "value": "2024_Q3"}]},
  "x-rowHash": {"enabled": true, "sourceUriEnabled": true}
}
```

### Data Quality Monitoring
```json
{
  "x-metadata-generation": {"enabled": true, "enum_detection": {"enabled": true}},
  "x-dataQuality": {"statisticalChecks": {"outlierDetection": {"enabled": true}}},
  "x-rowHash": {"enabled": true, "algorithm": "sha256"},
  "x-constraintHandling": {"errorMode": "bad_rows", "badRowsOutput": {"createSummary": true}}
}
```

## Getting Started

1. **Start Simple**: Begin with basic features like `x-primaryKey` and `x-constraintHandling`
2. **Add Data Quality**: Implement `x-dataQuality` and `x-metadata-generation` for insights
3. **Enhance Processing**: Add `x-transformations` and `x-special-type` for data standardization
4. **Advanced Features**: Implement `x-calculatedColumns`, `x-rowHash`, and `x-pii` as needed
5. **File-Specific**: Use `x-csv` or `x-fwf` based on your data sources

## Performance Considerations

- **Memory Usage**: Features like constraint validation and row hashing require memory proportional to data size
- **Processing Speed**: Each feature adds overhead; enable only needed functionality
- **Parallelization**: Most features support parallel processing for large datasets
- **Sampling**: Use sampling options for statistical analysis on very large files

## Best Practices

1. **Feature Selection**: Enable only the features you need to minimize overhead
2. **Configuration Testing**: Test configurations with representative data samples
3. **Error Handling**: Always configure appropriate constraint handling for your use case
4. **Documentation**: Document business rules and transformation logic
5. **Monitoring**: Set up monitoring for data quality metrics and processing performance
6. **Version Control**: Track changes to schema configurations over time

## Support and Troubleshooting

Each feature documentation includes:
- Detailed configuration examples
- Common use cases and patterns
- Integration guidance with other features
- Performance optimization tips
- Troubleshooting common issues
- Best practices and recommendations

For complex scenarios involving multiple features, refer to the integration examples in each feature's documentation and the cross-reference matrix above to understand feature interactions.
