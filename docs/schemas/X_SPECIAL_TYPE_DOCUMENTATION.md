# x-special-type Documentation

## Overview
The `x-special-type` extension provides specialized data type handling for common data formats that require validation, normalization, and standardization beyond basic JSON schema types. This feature enables automatic processing of structured data types like SSNs, ZIP codes, phone numbers, email addresses, and IP addresses.

## Supported Special Types

### Personal Identifiers

#### `ssn` - Social Security Number
- **Pattern**: `^\\d{3}-\\d{2}-\\d{4}$`
- **Format**: XXX-XX-XXXX
- **Validation**: Validates 9-digit SSN with proper hyphen placement
- **Normalization**: Converts various formats to standard XXX-XX-XXXX format
- **Privacy**: Integrates with PII masking features

```json
{
  "ssn": {
    "type": "string",
    "x-special-type": "ssn",
    "pattern": "^\\d{3}-\\d{2}-\\d{4}$",
    "description": "Social Security Number in XXX-XX-XXXX format"
  }
}
```

### Geographic Identifiers

#### `zip-permissive` - Flexible ZIP Code
- **Pattern**: `^\\d{5}(-\\d{4})?$`
- **Formats**: XXXXX or XXXXX-XXXX
- **Validation**: Accepts both 5-digit and ZIP+4 formats
- **Normalization**: Preserves original format

```json
{
  "zip_code": {
    "type": "string",
    "x-special-type": "zip-permissive",
    "pattern": "^\\d{5}(-\\d{4})?$",
    "description": "ZIP code in XXXXX or XXXXX-XXXX format"
  }
}
```

#### `zip-5` - 5-Digit ZIP Code
- **Pattern**: `^\\d{5}$`
- **Format**: XXXXX
- **Validation**: Strictly validates 5-digit ZIP codes
- **Normalization**: Strips ZIP+4 extensions if present

```json
{
  "zip_5": {
    "type": "string",
    "x-special-type": "zip-5",
    "pattern": "^\\d{5}$",
    "description": "5-digit ZIP code"
  }
}
```

#### `zip-9` - ZIP+4 Code
- **Pattern**: `^\\d{5}-\\d{4}$`
- **Format**: XXXXX-XXXX
- **Validation**: Requires full 9-digit ZIP+4 format
- **Normalization**: Adds hyphen if missing

```json
{
  "zip_9": {
    "type": "string",
    "x-special-type": "zip-9",
    "pattern": "^\\d{5}-\\d{4}$",
    "description": "9-digit ZIP+4 code in XXXXX-XXXX format"
  }
}
```

### Communication Identifiers

#### `phone` - Phone Number
- **Pattern**: `^(1?\\(\\d{3}\\) \\d{3}-\\d{4}|\\d{10,11})$`
- **Formats**: 
  - (XXX) XXX-XXXX
  - 1(XXX) XXX-XXXX
  - XXXXXXXXXX
  - 1XXXXXXXXXX
- **Validation**: Validates US phone number formats
- **Normalization**: Converts to standard (XXX) XXX-XXXX format

```json
{
  "phone_number": {
    "type": "string",
    "x-special-type": "phone",
    "pattern": "^(1?\\(\\d{3}\\) \\d{3}-\\d{4}|\\d{10,11})$",
    "description": "US phone number in (XXX) XXX-XXXX or 1(XXX) XXX-XXXX format"
  }
}
```

#### `email` - Email Address
- **Format**: Standard email format per RFC 5322
- **Validation**: Comprehensive email validation including domain checking
- **Normalization**: 
  - Converts to lowercase
  - Trims whitespace
  - Validates domain format
- **Features**:
  - Domain extraction for analytics
  - Disposable email detection
  - Corporate vs personal email classification

```json
{
  "email_address": {
    "type": "string",
    "x-special-type": "email",
    "format": "email",
    "description": "Email address with validation and normalization"
  }
}
```

### Network Identifiers

#### `ipv4` - IPv4 Address
- **Pattern**: `^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$`
- **Format**: XXX.XXX.XXX.XXX
- **Validation**: Validates IPv4 dotted decimal notation
- **Normalization**: Removes leading zeros from octets
- **Features**:
  - Range validation (0-255 per octet)
  - Private/public IP classification
  - Geographic IP location lookup (optional)

```json
{
  "ipv4_address": {
    "type": "string",
    "x-special-type": "ipv4",
    "pattern": "^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$",
    "description": "IPv4 address in dotted decimal notation"
  }
}
```

#### `ipv6` - IPv6 Address
- **Format**: Standard IPv6 format per RFC 4291
- **Validation**: Validates IPv6 address format including compression
- **Normalization**: 
  - Expands compressed notation
  - Converts to lowercase
  - Removes leading zeros in groups
- **Features**:
  - Support for :: compression
  - Mixed IPv4/IPv6 notation
  - Link-local address detection

```json
{
  "ipv6_address": {
    "type": "string",
    "x-special-type": "ipv6",
    "description": "IPv6 address with normalization and compression"
  }
}
```

#### `ip` - Universal IP Address
- **Format**: Auto-detects IPv4 or IPv6
- **Validation**: Automatically determines IP version and validates accordingly
- **Normalization**: Applies appropriate normalization based on detected version
- **Features**:
  - Automatic IPv4/IPv6 detection
  - Unified processing for mixed IP data
  - Version tagging in metadata

```json
{
  "ip_address": {
    "type": "string",
    "x-special-type": "ip",
    "description": "IP address (IPv4 or IPv6) with auto-detection"
  }
}
```

#### `mac-address` - MAC Address
- **Pattern**: `^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$`
- **Formats**: 
  - XX:XX:XX:XX:XX:XX (colon-separated)
  - XX-XX-XX-XX-XX-XX (dash-separated)
- **Validation**: Validates 6-octet MAC address format
- **Normalization**: Converts to uppercase colon-separated format
- **Features**:
  - OUI (Organizationally Unique Identifier) lookup
  - Vendor identification
  - Format standardization

```json
{
  "mac_address": {
    "type": "string",
    "x-special-type": "mac-address",
    "pattern": "^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$",
    "description": "MAC address in colon or dash separated format"
  }
}
```

## Implementation Details

### Validation Pipeline
1. **Format Recognition**: Detect input format using regex patterns
2. **Validation**: Verify data meets type-specific requirements
3. **Normalization**: Convert to standardized format
4. **Enhancement**: Add metadata (e.g., domain extraction, IP classification)
5. **Error Handling**: Route invalid data according to constraint handling rules

### Normalization Features
- **Consistent Formatting**: Standardize output format across all records
- **Case Normalization**: Apply appropriate case rules per data type
- **Whitespace Handling**: Trim and normalize whitespace
- **Symbol Standardization**: Use consistent punctuation and separators

### Integration with Other Features
- **PII Detection**: Special types automatically flagged for PII handling
- **Constraint Validation**: Invalid special types trigger constraint violations
- **Metadata Generation**: Type-specific statistics and pattern analysis
- **Transformations**: Enhanced cleaning rules for each special type

## Configuration Examples

### Basic Special Type Usage
```json
{
  "properties": {
    "customer_ssn": {
      "type": "string",
      "x-special-type": "ssn",
      "description": "Customer Social Security Number"
    },
    "shipping_zip": {
      "type": "string", 
      "x-special-type": "zip-permissive",
      "description": "Shipping ZIP code"
    }
  }
}
```

### Special Types with Enhanced Validation
```json
{
  "properties": {
    "contact_email": {
      "type": "string",
      "x-special-type": "email",
      "format": "email",
      "x-validation": {
        "check_mx_record": true,
        "block_disposable": true,
        "normalize_case": true
      }
    }
  }
}
```

### Special Types with PII Integration
```json
{
  "properties": {
    "employee_ssn": {
      "type": "string",
      "x-special-type": "ssn",
      "x-pii": {
        "category": "direct_identifier",
        "masking_required": true
      }
    }
  }
}
```

## Performance Considerations

1. **Regex Performance**: Complex patterns may impact processing speed
2. **Normalization Overhead**: Format conversion adds processing time
3. **Validation Complexity**: Some types require external lookups (MX records, GeoIP)
4. **Memory Usage**: Pattern compilation and caching considerations

## Best Practices

1. **Choose Appropriate Types**: Use most specific type available (zip-5 vs zip-permissive)
2. **Combine with Constraints**: Use with constraint handling for robust error management
3. **Document Expectations**: Clear descriptions help data providers
4. **Test with Real Data**: Validate patterns work with actual data samples
5. **Monitor Validation Rates**: Track success/failure rates for each special type
6. **Consider Performance**: Balance validation thoroughness with processing speed

## Error Handling

When special type validation fails:
- **Pattern Mismatch**: Data doesn't match expected format
- **Invalid Values**: Data matches pattern but fails semantic validation
- **Normalization Errors**: Unable to convert to standard format
- **Enhancement Failures**: Optional features (lookups) fail

These errors integrate with the `x-constraintHandling` system for consistent error management across all data quality issues.
