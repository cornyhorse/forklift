# x-rowHash Documentation

## Overview
The `x-rowHash` extension provides comprehensive row-level hash generation and metadata column capabilities for change detection, data integrity verification, and audit trail creation. This feature enables tracking of data lineage, detecting changes between processing runs, and adding processing metadata to output files.

## Schema Structure
```json
{
  "x-rowHash": {
    "description": "Configuration for generating row-level hash columns and metadata for change detection and data integrity",
    "enabled": false,
    "columnName": "row_hash",
    "algorithm": "sha256",
    "includeColumns": null,
    "excludeColumns": [],
    "nullValue": "NULL",
    "separator": "||",
    "inputHashEnabled": false,
    "inputHashColumnName": "_input_hash",
    "sourceUriEnabled": false,
    "sourceUriColumnName": "_source_uri",
    "ingestedAtEnabled": false,
    "ingestedAtColumnName": "_ingested_at_utc",
    "rowNumberEnabled": false,
    "sourceRowNumberColumnName": "_rownum_in_source_file",
    "processingRowNumberColumnName": "_rownum",
    "description_detail": "Row hash and metadata columns disabled by default. When enabled, can generate: output row hash (SHA256 default), input row hash, source URI, ingestion timestamp, and row numbers. Supports MD5, SHA1, SHA256, SHA384, SHA512 algorithms."
  }
}
```

## Configuration Properties

### Row Hash Generation

#### `enabled`
- **Type**: Boolean
- **Description**: Enable or disable row hash generation
- **Default**: `false`
- **Implementation**: When `true`, generates hash column for each row

#### `columnName`
- **Type**: String
- **Description**: Name of the generated hash column
- **Default**: `"row_hash"`
- **Implementation**: Column added to output with computed hash values

#### `algorithm`
- **Type**: String
- **Description**: Cryptographic algorithm for hash generation
- **Values**: `"md5"`, `"sha1"`, `"sha256"`, `"sha384"`, `"sha512"`
- **Default**: `"sha256"`
- **Recommendation**: Use SHA256 or higher for security and collision resistance

#### `includeColumns`
- **Type**: Array of strings or null
- **Description**: Specific columns to include in hash calculation
- **Default**: `null` (include all columns)
- **Implementation**: When specified, only listed columns are used for hash

#### `excludeColumns`
- **Type**: Array of strings
- **Description**: Columns to exclude from hash calculation
- **Default**: `[]`
- **Implementation**: Specified columns are not included in hash computation
- **Use Cases**: Exclude timestamp columns, processing metadata, or frequently changing fields

#### `nullValue`
- **Type**: String
- **Description**: String representation for null values in hash calculation
- **Default**: `"NULL"`
- **Implementation**: Null values converted to this string before hashing

#### `separator`
- **Type**: String
- **Description**: Separator between field values in hash input
- **Default**: `"||"`
- **Implementation**: Fields concatenated with this separator before hashing

### Input Hash Tracking

#### `inputHashEnabled`
- **Type**: Boolean
- **Description**: Generate hash of original input row before transformations
- **Default**: `false`
- **Use Cases**: Track changes made during processing, detect transformation impacts

#### `inputHashColumnName`
- **Type**: String
- **Description**: Column name for input hash
- **Default**: `"_input_hash"`

### Source Metadata

#### `sourceUriEnabled`
- **Type**: Boolean
- **Description**: Add column with source file/database URI
- **Default**: `false`
- **Use Cases**: Data lineage tracking, source identification

#### `sourceUriColumnName`
- **Type**: String
- **Description**: Column name for source URI
- **Default**: `"_source_uri"`

### Processing Metadata

#### `ingestedAtEnabled`
- **Type**: Boolean
- **Description**: Add timestamp column showing when data was processed
- **Default**: `false`
- **Use Cases**: Audit trails, processing time tracking

#### `ingestedAtColumnName`
- **Type**: String
- **Description**: Column name for ingestion timestamp
- **Default**: `"_ingested_at_utc"`
- **Format**: UTC timestamp in ISO 8601 format

### Row Numbering

#### `rowNumberEnabled`
- **Type**: Boolean
- **Description**: Add row number columns
- **Default**: `false`

#### `sourceRowNumberColumnName`
- **Type**: String
- **Description**: Column name for original source file row number
- **Default**: `"_rownum_in_source_file"`
- **Implementation**: 1-based numbering from source file

#### `processingRowNumberColumnName`
- **Type**: String
- **Description**: Column name for processing order row number
- **Default**: `"_rownum"`
- **Implementation**: Sequential numbering during processing

## Implementation Details

### Hash Calculation Process
1. **Column Selection**: Determine which columns to include based on configuration
2. **Value Preparation**: Convert values to strings, handle nulls
3. **Concatenation**: Join values with specified separator
4. **Hash Generation**: Apply specified algorithm to concatenated string
5. **Encoding**: Convert hash to hexadecimal string representation

### Hash Input Format
```
column1_value||column2_value||NULL||column4_value
```

### Change Detection Workflow
1. **Initial Load**: Generate hashes for all rows
2. **Subsequent Loads**: Generate hashes for new data
3. **Comparison**: Compare hashes to detect:
   - New records (hash not in previous dataset)
   - Changed records (same key, different hash)
   - Unchanged records (same hash)
   - Deleted records (hash missing from new dataset)

### Performance Considerations
- **Hash Algorithm Speed**: MD5 fastest, SHA512 slowest
- **Column Selection**: Fewer columns = faster processing
- **Memory Usage**: Hash comparison requires storing previous hashes
- **String Concatenation**: Large text fields increase processing time

## Use Cases

### Change Data Capture (CDC)
```json
{
  "x-rowHash": {
    "enabled": true,
    "algorithm": "sha256",
    "excludeColumns": ["last_modified", "_ingested_at_utc"],
    "ingestedAtEnabled": true
  }
}
```

### Data Quality Monitoring
```json
{
  "x-rowHash": {
    "enabled": true,
    "inputHashEnabled": true,
    "algorithm": "sha256",
    "sourceUriEnabled": true,
    "rowNumberEnabled": true
  }
}
```

### Audit Trail Creation
```json
{
  "x-rowHash": {
    "enabled": true,
    "algorithm": "sha512",
    "ingestedAtEnabled": true,
    "sourceUriEnabled": true,
    "rowNumberEnabled": true,
    "excludeColumns": ["processing_timestamp"]
  }
}
```

### Performance-Optimized Configuration
```json
{
  "x-rowHash": {
    "enabled": true,
    "algorithm": "md5",
    "includeColumns": ["id", "name", "status", "amount"],
    "ingestedAtEnabled": false,
    "rowNumberEnabled": false
  }
}
```

## Integration Examples

### With Primary Key Validation
```json
{
  "x-primaryKey": {
    "columns": ["customer_id"],
    "enforceUniqueness": true
  },
  "x-rowHash": {
    "enabled": true,
    "excludeColumns": ["customer_id"],
    "description": "Hash excludes primary key to focus on data changes"
  }
}
```

### With PII Masking
```json
{
  "x-pii": {
    "fields": {
      "ssn": {"isPII": true, "category": "direct_identifier"}
    }
  },
  "x-rowHash": {
    "enabled": true,
    "excludeColumns": ["ssn"],
    "description": "Hash excludes PII fields to avoid privacy issues"
  }
}
```

### With Calculated Columns
```json
{
  "x-calculatedColumns": {
    "constants": [
      {"name": "batch_id", "value": "batch_001"}
    ]
  },
  "x-rowHash": {
    "enabled": true,
    "excludeColumns": ["batch_id", "_ingested_at_utc"],
    "ingestedAtEnabled": true,
    "description": "Hash excludes processing metadata"
  }
}
```

## Output Example

With full metadata enabled:

| customer_id | name | email | row_hash | _input_hash | _source_uri | _ingested_at_utc | _rownum_in_source_file | _rownum |
|-------------|------|-------|----------|-------------|-------------|------------------|------------------------|---------|
| 1 | John Doe | john@example.com | a1b2c3d4... | x9y8z7w6... | file:///data/customers.csv | 2024-08-26T10:30:00Z | 2 | 1 |
| 2 | Jane Smith | jane@example.com | e5f6g7h8... | v5u4t3s2... | file:///data/customers.csv | 2024-08-26T10:30:00Z | 3 | 2 |

## Best Practices

### Algorithm Selection
- **MD5**: Fast, suitable for change detection in trusted environments
- **SHA1**: Deprecated for security, avoid for new implementations
- **SHA256**: Good balance of security and performance, recommended default
- **SHA384/SHA512**: Maximum security, use for sensitive data or compliance requirements

### Column Selection Strategy
1. **Include Business Data**: Focus on columns that represent actual data changes
2. **Exclude Metadata**: Remove processing timestamps, batch IDs, etc.
3. **Exclude Volatile Fields**: Remove frequently changing non-business fields
4. **Include Key Fields**: Consider including primary/foreign keys for context

### Change Detection Workflow
1. **Baseline Creation**: Generate hashes for initial dataset
2. **Hash Storage**: Store hashes in metadata database or comparison files
3. **Incremental Processing**: Compare new hashes against stored baseline
4. **Action Planning**: Define actions for new, changed, and deleted records

### Performance Optimization
1. **Algorithm Choice**: Use fastest algorithm that meets security requirements
2. **Column Minimization**: Include only necessary columns in hash
3. **Batch Processing**: Process hashes in batches for large datasets
4. **Parallel Processing**: Leverage multiple cores for hash generation

### Security Considerations
1. **Algorithm Security**: Use SHA256 or higher for production systems
2. **Salt Usage**: Consider adding salt for additional security (custom implementation)
3. **Hash Storage**: Protect stored hashes if they could reveal sensitive information
4. **Access Control**: Limit access to hash comparison results

## Troubleshooting

### Common Issues
1. **Hash Instability**: Different hash values for same logical data
   - **Cause**: Inconsistent null handling, floating-point precision
   - **Solution**: Standardize null representation, round floating-point values

2. **Performance Issues**: Slow hash generation
   - **Cause**: Large text fields, complex algorithm, too many columns
   - **Solution**: Optimize column selection, use faster algorithm

3. **Memory Usage**: High memory consumption during processing
   - **Cause**: Storing too many hashes for comparison
   - **Solution**: Implement streaming comparison, process in batches

### Debugging Tips
1. **Hash Verification**: Compare hash inputs to identify differences
2. **Performance Profiling**: Measure hash generation time per row
3. **Column Impact Analysis**: Test hash generation with different column sets
4. **Algorithm Comparison**: Benchmark different algorithms with actual data
