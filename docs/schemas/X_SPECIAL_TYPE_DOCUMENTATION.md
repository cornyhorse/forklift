# x-special-type Documentation

## Overview
The `x-special-type` extension provides specialized data type handling for common data formats that require validation, normalization, and standardization beyond basic JSON schema types. This feature enables automatic processing of structured data types like SSNs, ZIP codes, phone numbers, email addresses, and IP addresses.

## How special types are applied

The formatters live in `forklift.utils.transformations.format`; `SchemaBasedTransformer` (`forklift.processors.transformations`) adds the matching formatter automatically for every property that carries `x-special-type`, with the default options listed below. `import_csv` itself does not run them (see the [transformations documentation](./X_TRANSFORMATIONS_DOCUMENTATION.md)). A value that fails validation becomes NULL (or stays unchanged with `allow_invalid`); nothing is looked up externally: there is no MX, GeoIP, OUI/vendor, disposable-address or private/public-address check.

The same formatters are available per column as `ssn_formatting`, `zip_code_formatting`, `phone_number_formatting`, `email_formatting`, `ip_address_formatting` and `mac_address_formatting` with these options:

| Type | Options (defaults) |
| --- | --- |
| `ssn` | `format_with_dashes` (true), `zero_pad` (true), `validate` (true), `allow_invalid` (false) |
| `zip-*` | `zip_type` (`zip-permissive`), `format_with_dash` (true), `zero_pad` (true), `validate` (true), `allow_invalid` (false) |
| `phone` | `format_style` (`us-standard`, `international`, `digits-only`, `preserve`), `min_digits` (10), `max_digits` (11), `include_country_code` (false), `use_parentheses` (true), `use_dashes` (true), `use_dots` (false), `validate`, `allow_invalid` |
| `email` | `normalize_case` (true), `strip_whitespace` (true), `normalize_domain` (true), `validate_format` (true), `allow_invalid` (false) |
| `ipv4` / `ipv6` / `ip` | `ip_version` (`ipv4`, `ipv6`, `both`), `normalize_ipv6` (true), `compress_ipv6` (true), `validate` (true), `allow_invalid` (false) |
| `mac-address` | `format_style` (`colon`, `dash`, `dot`, `none`), `case_style` (`lower`, `upper`, `preserve`), `zero_pad` (true), `validate` (true), `allow_invalid` (false) |

`zero_pad` is applied before validation, so it restores leading zeros that a numeric column dropped (`"2134"` is ZIP `02134`, `"12345678"` is SSN `012-34-5678`), and a float rendering such as `"2134.0"` is read as `2134`.

## Supported Special Types

### Personal Identifiers

#### `ssn` - Social Security Number
- **Pattern**: `^\\d{3}-\\d{2}-\\d{4}$`
- **Format**: XXX-XX-XXXX
- **Validation**: Exactly 9 digits after separators are removed; letters make the value invalid
- **Normalization**: Converts various formats (`123456789`, `123 45 6789`) to standard XXX-XX-XXXX; with `zero_pad` shorter digit strings are padded to 9 digits first
- **Privacy**: The formatter standardizes the value; it does not mask it (use the PII masking features for that)

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
- **Validation**: Accepts 5-digit and 9-digit ZIP codes (anything else is invalid)
- **Normalization**: Up to 5 digits are padded to 5; 6-9 digits are padded to 9 and written as XXXXX-XXXX (`format_with_dash`)

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
- **Normalization**: Strips ZIP+4 extensions if present (keeps the first 5 digits); shorter values are zero-padded

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
- **Validation**: Requires 9 digits (shorter values are zero-padded first)
- **Normalization**: Adds hyphen if missing (`format_with_dash`)

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
- **Validation**: `min_digits`-`max_digits` digits (10-11 by default, counted after a leading country code `1` is removed); letters make the value invalid. A number that is not 10 digits long after the leading `1` is removed is returned as bare digits instead of being formatted
- **Normalization**: Converts to standard (XXX) XXX-XXXX format (`us-standard`)
- **International**: A number with an explicit `+` country code other than `+1` is validated against the E.164 length limits (7-15 digits including the country code) and written as `+<digits>`; `+1` is never added to it

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
- **Format**: `local@domain` with a dot-atom local part and a dotted domain whose last label is alphabetic
- **Validation**: Syntax only, no DNS lookups: rejects doubled, leading or trailing dots in the local part, empty or hyphen-edged domain labels, and addresses beyond the RFC 5321 length limits (64-character local part, 254 characters in total)
- **Normalization**:
  - Converts to lowercase
  - Trims whitespace (only when `strip_whitespace` is on)
  - Removes trailing dots from the domain

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
- **Validation**: Validates IPv4 dotted decimal notation with Python's `ipaddress` module (0-255 per octet; whether leading zeros such as `010` are accepted depends on the Python version)
- **Normalization**: The text is kept as it is

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
- **Validation**: Validates IPv6 address format including `::` compression
- **Normalization**: With `normalize_ipv6` the address is written in the canonical compressed lowercase form (`compress_ipv6=true`, the default) or fully expanded (`compress_ipv6=false`); with `normalize_ipv6=false` the text is kept

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
- **Features**: Automatic IPv4/IPv6 detection, so one column can hold both

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
  - also accepted when formatting: `0011.2233.4455`, space-separated and compact `001122334455`; unpadded octets (`0:1a:2b:3:4:5`) when `zero_pad` is on
- **Validation**: Exactly 12 hexadecimal digits; short or long input is rejected, never padded or truncated into a different address
- **Normalization**: The automatic formatter writes lower-case colon-separated octets (`format_style` `colon`, `case_style` `lower`); set `case_style: "upper"` for upper case

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
1. **Format Recognition**: Strip separators and extract the digits / octets
2. **Validation**: Verify data meets type-specific requirements
3. **Normalization**: Convert to standardized format
4. **Error Handling**: A value that fails becomes NULL (or is kept with `allow_invalid`)

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

### Special Types with Tuned Options
```json
{
  "properties": {
    "contact_email": {
      "type": "string",
      "x-special-type": "email",
      "format": "email"
    }
  },
  "x-transformations": {
    "column_transformations": {
      "contact_email": {
        "email_formatting": { "enabled": true, "normalize_case": true, "allow_invalid": true }
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
3. **Validation Complexity**: All checks are local (no network lookups)
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

These errors integrate with the `x-constraintHandling` system for consistent error management across all data quality issues.
