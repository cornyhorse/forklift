# x-special-type Documentation

## Overview
The `x-special-type` extension provides specialized data type handling for common data formats that require validation, normalization, and standardization beyond basic JSON schema types. This feature enables automatic processing of structured data types like SSNs, ZIP codes, phone numbers, email addresses, and IP addresses.

## How special types are applied

The formatters live in `forklift.utils.transformations.format`. `import_csv` (also `read_csv` and `forklift ingest --input-kind csv`) adds the matching formatter automatically for every property that carries `x-special-type` and applies it to the **text of the file, before the types are applied**, with the default options listed below (`SchemaBasedTransformer` does the same outside the engine; see the [transformations documentation](./X_TRANSFORMATIONS_DOCUMENTATION.md)). Excel, SQL and fixed-width imports do not apply it. Nothing is looked up externally: there is no MX, GeoIP, OUI/vendor, disposable-address or private/public-address check.

- **Invalid values**: a value that fails validation becomes NULL and is counted as `INVALID_SPECIAL_VALUE:<column>` in `results.validation_summary` (the CLI prints it under `Findings by the schema extensions:`). The row is kept; it is rejected only if the column is `required` (`required_value_missing`, and the bad row shows the NULL) or the NULL breaks another rule
- **Order**: the explicit `x-transformations` steps of the column run first (so a `regex_replace` can strip a prefix such as `SSN: `), the automatic formatter last. The null markers of `x-csv` are applied to the text before both
- **Column names**: the property is matched by the header name; a property declared only under a renamed column's new name is not applied (the import warns)
- **The options of the automatic step cannot be changed.** To use other options, leave `x-special-type` off the property and configure the explicit step (`ssn_formatting`, `zip_code_formatting`, `phone_number_formatting`, `email_formatting`, `ip_address_formatting`, `mac_address_formatting`) in `x-transformations.column_transformations`. An explicit step on a column that also has `x-special-type` runs first and the automatic step then runs on its result, so `"allow_invalid": true` there does not keep an invalid value
- **Constraints see the formatted value**: a `pattern` next to `x-special-type` (as in the shipped standard) is checked against the formatted text, and NULL passes it

The same formatters are available per column as `ssn_formatting`, `zip_code_formatting`, `phone_number_formatting`, `email_formatting`, `ip_address_formatting` and `mac_address_formatting` with these options:

| Type | Options (defaults) |
| --- | --- |
| `ssn` | `format_with_dashes` (true), `zero_pad` (true), `validate` (true), `allow_invalid` (false) |
| `zip-*` | `zip_type` (`zip-permissive`), `format_with_dash` (true), `zero_pad` (true), `validate` (true), `allow_invalid` (false) |
| `phone` | `format_style` (`us-standard`, `international`, `digits-only`, `preserve`), `min_digits` (10), `max_digits` (11), `include_country_code` (false), `use_parentheses` (true), `use_dashes` (true), `use_dots` (false), `validate`, `allow_invalid` |
| `email` | `normalize_case` (true), `strip_whitespace` (true), `normalize_domain` (true), `validate_format` (true), `allow_invalid` (false) |
| `ipv4` / `ipv6` / `ip` | `ip_version` (`ipv4`, `ipv6`, `both`), `normalize_ipv6` (true), `compress_ipv6` (true), `validate` (true), `allow_invalid` (false) |
| `mac-address` | `format_style` (`colon`, `dash`, `dot`, `none`), `case_style` (`lower`, `upper`, `preserve`), `zero_pad` (true), `validate` (true), `allow_invalid` (false) |

`zero_pad` restores leading zeros that a numeric column dropped, but with `validate` on (the automatic step) the number of digits is checked *before* padding, except for `zip-5`: `"2134"` is ZIP `02134` for `zip-5`, while a short SSN, `zip-9` or `zip-permissive` value is invalid. With `"validate": false` in an explicit step the same input is padded (`"2134"` is `02134`, `"12345678"` is SSN `012-34-5678`). A float rendering such as `"2134.0"` is read as `2134`.

### Example

```
ssn,zip,phone,email,ip,mac
123456789,2134,5551234567, Ann@Example.COM ,2001:DB8:0:0:0:0:0:1,00-1A-2B-3C-4D-5E
not-an-ssn,abcde,12,bad,999.1.1.1,xyz
111-22-3333,12345-6789,(555) 123-4567,b@x.org,10.0.0.1,0:1a:2b:3:4:5
```

with the properties `ssn` (`x-special-type: ssn`), `zip` (`zip-permissive`), `phone` (`phone`), `email` (`email`), `ip` (`ip`) and `mac` (`mac-address`), all `"type": "string"`, gives

| ssn | zip | phone | email | ip | mac |
| --- | --- | --- | --- | --- | --- |
| 123-45-6789 | (null) | (555) 123-4567 | ann@example.com | 2001:db8::1 | 00:1a:2b:3c:4d:5e |
| (null) | (null) | (null) | (null) | (null) | (null) |
| 111-22-3333 | 12345-6789 | (555) 123-4567 | b@x.org | 10.0.0.1 | 00:1a:2b:03:04:05 |

and `results.validation_summary` is `{'INVALID_SPECIAL_VALUE:ssn': 1, 'INVALID_SPECIAL_VALUE:zip': 2, 'INVALID_SPECIAL_VALUE:phone': 1, 'INVALID_SPECIAL_VALUE:email': 1, 'INVALID_SPECIAL_VALUE:ip': 1, 'INVALID_SPECIAL_VALUE:mac': 1}`. The first ZIP (`2134`) is NULL because a `zip-permissive` value needs 5 or 9 digits (use `zip-5` to pad it to `02134`).

## Supported Special Types

### Personal Identifiers

#### `ssn` - Social Security Number
- **Pattern**: `^\\d{3}-\\d{2}-\\d{4}$`
- **Format**: XXX-XX-XXXX
- **Validation**: Exactly 9 digits after separators are removed; letters make the value invalid
- **Normalization**: Converts various formats (`123456789`, `123 45 6789`, `123456789.0`) to standard XXX-XX-XXXX; a value with fewer than 9 digits is invalid in the automatic step (see `zero_pad` above)
- **Privacy**: The formatter standardizes the value; it does not mask it (masking is not implemented, see [x-pii](./X_PII_DOCUMENTATION.md))

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
- **Validation**: Accepts 5-digit and 9-digit ZIP codes (anything else, a 4-digit value included, is invalid)
- **Normalization**: 5 digits are kept; 9 digits are written as XXXXX-XXXX (`format_with_dash`), so `123456789` becomes `12345-6789`

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
- **Validation**: Requires exactly 9 digits (`12345678` and `12345` are invalid in the automatic step; with `validate: false` shorter values are zero-padded)
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
- **Normalization**: Converts to standard (XXX) XXX-XXXX format (`us-standard`); 11 digits that start with the country code `1` are written `1(XXX) XXX-XXXX` (`+1 555 123 4567` too)
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
- **PII Detection**: `x-pii` is documentation only; a special type is not flagged as PII and nothing is masked
- **Constraint Validation**: An invalid special value becomes NULL, which passes the per-property constraints (`pattern`, `maxLength`, ...); use `required` or an `x-validation` rule (`required: true`) to reject the rows
- **Metadata Generation**: The output metadata describes the formatted values
- **Transformations**: The explicit steps of the column run first, the automatic formatter last

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
Leave `x-special-type` off the property and configure the explicit step; here invalid addresses are kept as they are instead of becoming NULL:
```json
{
  "properties": {
    "contact_email": {
      "type": "string"
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

### Special Types with PII Marking
`x-pii` is documentation only (see the [x-pii documentation](./X_PII_DOCUMENTATION.md)): the column is formatted, not masked, and the import warns that `x-pii` is not applied.
```json
{
  "properties": {
    "employee_ssn": {
      "type": "string",
      "x-special-type": "ssn"
    }
  },
  "x-pii": {
    "fields": {
      "employee_ssn": {"isPII": true, "category": "direct_identifier"}
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
2. **Combine with Rules**: add `required` or an `x-validation` rule when rows with an invalid value must be rejected (the formatter itself only produces NULL)
3. **Document Expectations**: Clear descriptions help data providers
4. **Test with Real Data**: Validate patterns work with actual data samples
5. **Monitor Validation Rates**: watch `INVALID_SPECIAL_VALUE:<column>` in `results.validation_summary`
6. **Consider Performance**: Balance validation thoroughness with processing speed

## Error Handling

When special type validation fails:
- **Invalid Values**: the value becomes NULL (or, with an explicit step and `allow_invalid`, stays unchanged) and is counted as `INVALID_SPECIAL_VALUE:<column>`; the row is kept
- **Missing Columns**: a property with `x-special-type` whose column is not in the file is skipped with a warning
- **Configuration Errors**: an unknown option in an explicit `*_formatting` step raises `ValueError` before any output is written
- **Rows**: rejected only through `required` (reason `required_value_missing`) or another rule that sees the NULL
