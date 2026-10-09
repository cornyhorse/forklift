# x-transformations Documentation

## Overview
The `x-transformations` extension provides comprehensive data transformation and standardization capabilities for processing files with advanced data cleaning, normalization, and formatting options. This feature supports file-type-specific transformations and field-level customization.

## How the code reads this extension

The transformers live in `forklift.utils.transformations` (`DataTransformer`, `create_transformation_from_config`) and run on PyArrow arrays: nulls stay `None`, no pandas is involved, and string transformers return the same Arrow string type they were given. The processor that applies them to a batch, `SchemaBasedTransformer` (`forklift.processors.transformations`), reads the extension in the form below (**per column, snake_case option names, `enabled` must be true**):

```json
{
  "x-transformations": {
    "column_transformations": {
      "customer_name": {
        "string_cleaning": { "enabled": true, "case_transform": "title", "strip_whitespace": true }
      },
      "salary": {
        "money_conversion": { "enabled": true, "currency_symbols": ["$"], "thousands_separator": "," },
        "numeric_cleaning": { "enabled": true, "target_type": "double" }
      },
      "birth_date": {
        "datetime": { "enabled": true, "mode": "specify_formats", "formats": ["%Y-%m-%d", "%m/%d/%Y"], "target_type": "date" }
      }
    }
  }
}
```

Available `transform_type` keys: `string_cleaning`, `regex_replace`, `string_replace`, `string_padding`, `string_trimming`, `html_xml_cleaning`, `money_conversion`, `numeric_cleaning`, `datetime`, `ssn_formatting`, `zip_code_formatting`, `phone_number_formatting`, `email_formatting`, `ip_address_formatting` and `mac_address_formatting`. Each accepts the fields of the matching config class (`forklift/utils/transformations/configs.py`); **an unknown option raises `ValueError` listing the valid ones** instead of being ignored. Properties marked with `x-special-type` (`ssn`, `zip-permissive`/`zip-5`/`zip-9`, `phone`, `email`, `ipv4`/`ipv6`/`ip`, `mac-address`) get the matching formatter automatically. If a column's transformation raises, the processor reports it as a validation result (`SCHEMA_TRANSFORMATION_ERROR`) and leaves the column unchanged; a value that cannot be parsed by a transformer becomes NULL.

The generator (`forklift generate-schema`) writes suggestions in this same `column_transformations` form. The camelCase block names used in the rest of this document (`stringCleaning`, `caseTransformation`, `numericCleaning`, `moneyType`, `dateTimeParsing`, `fieldSpecific`) are the schema-standard vocabulary; the processor does not read them, so use the snake_case column form when you want the transformations applied. The option descriptions below apply to both spellings (`normalizeQuotes` is `normalize_quotes`, and so on).

> **`import_csv` does not run transformations.** The engine entry points (`import_csv`, `read_csv`) apply the schema's types, null markers and `required` list; they do not execute `x-transformations`. Apply the transformers (or `SchemaBasedTransformer`) to your Arrow data explicitly.

## Schema Structure
```json
{
  "x-transformations": {
    "description": "Data transformation and standardization configuration",
    "stringCleaning": {
      "normalizeQuotes": true,
      "normalizeDashes": true,
      "normalizeSpaces": true,
      "collapseWhitespace": true,
      "stripWhitespace": true,
      "removeZeroWidth": true,
      "removeControlChars": true,
      "preserveNewlines": true,
      "unicodeNormalize": "NFKC"
    },
    "caseTransformation": {
      "caseTransform": "title",
      "fixCaseIssues": true,
      "titleCaseExceptions": ["of", "the", "and", "or", "but"],
      "customCaseMappings": {
        "ca": "CA",
        "ny": "NY",
        "usa": "USA"
      }
    },
    "numericCleaning": {
      "thousandsSeparator": ",",
      "decimalSeparator": ".",
      "allowNaN": true,
      "nanValues": ["", "N/A", "NULL"],
      "stripWhitespace": true
    },
    "moneyType": {
      "currencySymbols": ["$", "€", "£"],
      "thousandsSeparator": ",",
      "decimalSeparator": ".",
      "parenthesesNegative": true
    },
    "dateTimeParsing": {
      "mode": "specify_formats",
      "allowFuzzy": false,
      "targetType": "date",
      "formats": ["%Y-%m-%d", "%m/%d/%Y"]
    }
  }
}
```

## Transformation Categories

### String Cleaning
Comprehensive text normalization and cleaning options.

#### `normalizeQuotes`
- **Type**: Boolean
- **Description**: Converts various quote characters to standard ASCII quotes
- **Implementation**: Replaces smart quotes, backticks, and Unicode quotes with " and '
- **Default**: `true`

#### `normalizeDashes`
- **Type**: Boolean  
- **Description**: Standardizes various dash and hyphen characters
- **Implementation**: Converts em-dashes, en-dashes, horizontal bars and the Unicode minus sign to the standard ASCII hyphen (-)
- **Default**: `true`

#### `normalizeSpaces`
- **Type**: Boolean
- **Description**: Converts various space characters to standard ASCII space
- **Implementation**: Replaces non-breaking spaces and the Unicode space characters (en/em/thin/figure spaces, ideographic space ...) with a standard space. Tabs are controlled by `removeTabs` / `tabReplacement` instead
- **Default**: `true`

#### `collapseWhitespace`
- **Type**: Boolean
- **Description**: Collapses multiple consecutive whitespace characters to single spaces
- **Implementation**: Replaces sequences of whitespace with single space character
- **Default**: `true`

#### `stripWhitespace`
- **Type**: Boolean
- **Description**: Removes leading and trailing whitespace
- **Implementation**: Applies trim() operation to string values
- **Default**: `true`

#### `removeZeroWidth`
- **Type**: Boolean
- **Description**: Removes zero-width Unicode characters
- **Implementation**: Strips zero-width spaces, joiners, and non-joiners
- **Default**: `true`

#### `removeControlChars`
- **Type**: Boolean
- **Description**: Removes ASCII control characters (except newlines/tabs if preserved)
- **Implementation**: Filters out ASCII and C1 control characters
- **Default**: `true`

#### `preserveNewlines`
- **Type**: Boolean
- **Description**: Whether to preserve newline characters during cleaning
- **Implementation**: Excludes \n and \r from control character removal when true
- **Default**: `true`

#### `unicodeNormalize`
- **Type**: String
- **Description**: Unicode normalization form to apply
- **Values**: `"NFC"`, `"NFD"`, `"NFKC"`, `"NFKD"`, `null`
- **Implementation**: Applies specified Unicode normalization
- **Default**: `"NFKC"`
- **Caution**: NFKC is lossy: it folds compatibility characters (full-width digits, ligatures, superscripts, `½` becomes `1⁄2`). Set it to `null` to keep the original characters

#### Other string cleaning options
- **`fixEncodingErrors`** (default `true`): repairs mojibake, i.e. UTF-8 text that was decoded as cp1252/latin-1 (`CafÃ©` becomes `Café`). Text is only touched if it contains a typical mojibake pair and the round trip (re-encode with the wrong codec, decode as UTF-8) succeeds without producing implausible characters; otherwise it is left unchanged, so legitimate letters such as the `Â` in `Âge` are never deleted
- **`removeTabs`** (default `false`) / **`tabReplacement`** (default `" "`) / **`preserveTabs`** (default `false`): tab handling
- **`removeAccents`** (default `false`) and **`asciiOnly`** (default `false`, implies `removeAccents`): `asciiOnly` drops every non-ASCII character and is lossy
- **`acronyms`**: words to force to upper case (`["NASA", "API"]`)

### Case Transformation
Advanced case normalization with business logic support.

#### `caseTransform`
- **Type**: String
- **Description**: Primary case transformation to apply
- **Values**:
  - `"upper"`: Convert to uppercase
  - `"lower"`: Convert to lowercase
  - `"title"`: Convert to title case word by word (`don't` becomes `Don't`, `1st` stays `1st`, `O'Brien` is kept)
  - `"proper"`: First character upper case, the rest lower case
  - `null` (omitted): No transformation
- **Default**: `null`

#### `fixCaseIssues`
- **Type**: Boolean
- **Description**: Automatically fix common case problems
- **Implementation**: Corrects all-caps words, improper capitalization
- **Default**: `false`

#### `titleCaseExceptions`
- **Type**: Array of strings
- **Description**: Words to keep lowercase in title case transformation
- **Default**: `["a", "an", "and", "as", "at", "but", "by", "for", "if", "in", "nor", "of", "on", "or", "so", "the", "to", "up", "yet"]`

#### `customCaseMappings`
- **Type**: Object
- **Description**: Custom word-specific case mappings
- **Implementation**: Exact word replacements after case transformation
- **Example**: `{"ca": "CA", "ny": "NY", "usa": "USA"}`

#### `caseMappingMode`
- **Type**: String
- **Description**: How to apply custom case mappings
- **Values**:
  - `"exact"`: The whole value equals the key
  - `"contains"`: The key occurs in the value (all occurrences are replaced)
  - `"startswith"` / `"endswith"`: The value starts or ends with the key
- **Default**: `"exact"`

### Numeric Cleaning
Standardization of numeric data formats.

#### `thousandsSeparator`
- **Type**: String
- **Description**: Expected thousands separator character
- **Implementation**: Removes specified character from numeric strings
- **Default**: `","`, unless only `decimalSeparator` is set to `","` (then `"."`). An empty string means "no grouping character"

#### `decimalSeparator`
- **Type**: String
- **Description**: Expected decimal separator character
- **Implementation**: Converts to standard decimal point if different; when it is not `"."`, a literal `.` in the text makes the value unparseable instead of being read as a decimal point
- **Default**: `"."`, unless only `thousandsSeparator` is set to `"."` (then `","`)
- **Pairing rule**: Setting only one of the two separators selects the matching counterpart (`decimal_separator=","` implies `thousands_separator="."` and vice versa). Setting both to the same non-empty value raises `ValueError` when the configuration is created

#### `allowNaN`
- **Type**: Boolean
- **Description**: Whether unparseable values are tolerated
- **Implementation**: With `true`, text that is not a number, `NaN`/`Infinity` text and values that overflow the target type become NULL. With `false`, such a value raises `ValueError` naming the row number (never the cell content)
- **Default**: `true`

#### `nanValues`
- **Type**: Array of strings
- **Description**: String values to treat as NaN/null
- **Default**: `["", "N/A", "NA", "NULL", "null", "NaN", "nan", "#N/A", "#NULL!"]`

#### `stripWhitespace`
- **Type**: Boolean
- **Description**: Remove whitespace from numeric strings before parsing
- **Default**: `true`

#### `target_type` (`numeric_cleaning` only)
- **Type**: String
- **Description**: Exact Arrow type of the result: `int8`..`int64`, `uint8`..`uint64`, `float32`, `float64`/`double` (`int`/`integer`/`bigint` mean `int64`). An unknown name raises `ValueError` when the transformation is created
- **Implementation**: Values are parsed as decimals, so large integers stay exact. Integer targets only accept integral values (`"3.0"` and `"1e3"` become 3 and 1000, `"3.9"` becomes NULL) and values outside the type's range become NULL
- **Default**: `"double"`

### Money Type Cleaning
Specialized handling for currency and monetary values.

#### `currencySymbols`
- **Type**: Array of strings
- **Description**: Currency symbols to remove during parsing
- **Default**: `["$", "€", "£", "¥", "₹", "₽", "¢"]`

#### `thousandsSeparator`
- **Type**: String
- **Description**: Thousands separator in monetary values
- **Default**: `","` (same pairing rule as numeric cleaning)

#### `decimalSeparator`
- **Type**: String
- **Description**: Decimal separator in monetary values
- **Default**: `"."` (same pairing rule as numeric cleaning)

#### `parenthesesNegative`
- **Type**: Boolean
- **Description**: Whether parentheses indicate negative values
- **Implementation**: Converts "(100.00)" to -100.0
- **Default**: `true`

The result of money conversion is a `float64` column; text that is not a number (or is `NaN`/`Infinity`) becomes NULL.

### DateTime Parsing
Advanced date and time parsing with format detection.

#### `mode`
- **Type**: String
- **Description**: DateTime parsing strategy
- **Values**:
  - `"enforce"`: Only the single `format` is accepted
  - `"specify_formats"`: Use only the listed `formats` (requires a non-empty list)
  - `"common_formats"`: Try the common formats, then dateutil
- **Default**: `"common_formats"`

#### `format`
- **Type**: String
- **Description**: The one strptime format required in `enforce` mode. `%z` accepts `Z`, `+0500` and `+05:00`

#### `allowFuzzy`
- **Type**: Boolean
- **Description**: Enable fuzzy date parsing for ambiguous formats
- **Default**: `false`

#### `fromEpoch`
- **Type**: Boolean
- **Description**: The values must be epoch timestamps (10, 13, 16 or 19 digits for seconds, milliseconds, microseconds, nanoseconds)
- **Default**: `false`
- **Note**: Without `fromEpoch`, plain digit strings of exactly those lengths are still recognised as epochs in `common_formats` mode, but **not** when an explicit `format` / `formats` is given: then only those formats are tried (`%Y%m%d%H` parses `2024010112`, a 10-digit phone-like id is never read as an epoch)

#### `toEpoch`
- **Type**: String
- **Description**: Return an epoch instead of a date: `seconds` (float64) or `milliseconds` / `microseconds` / `nanoseconds` (int64). Takes precedence over `targetType`

#### `targetType`
- **Type**: String
- **Description**: Target data type for parsed dates
- **Values**: `"datetime"` (`timestamp[us, tz=UTC]`), `"date"` (`date32`), `"timestamp"` (float64 epoch seconds), `"string"` (ISO text, or `outputFormat` as strftime pattern)
- **Default**: `"datetime"`

#### `dayfirst`
- **Type**: Boolean
- **Description**: Resolve ambiguous numeric dates (`03-04-2024`) as day-month-year (3 April) when `true`, month-day-year when `false`. `coerce_date`, `coerce_datetime` and `parse_date` share this keyword and one resolution order
- **Default**: `true`

#### `timezone`
- **Type**: String
- **Description**: IANA zone (`America/New_York`) the parsed values are converted to before the target type is applied; naive values are taken as UTC. It decides the calendar date for `targetType` `date` and the text for `string`; `datetime` results are always stored as UTC instants. An unknown name raises `ValueError` when the configuration is created (validated with `zoneinfo`, falling back to `pytz`), it does not null every row later

#### `formats`
- **Type**: Array of strings
- **Description**: Specific date formats to try when mode is "specify_formats"
- **Format**: Python strftime format codes
- **Example**: `["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"]`

A cell that cannot be parsed (or whose result does not fit the target type) becomes NULL.

### HTML/XML Cleaning (`html_xml_cleaning`)
Options `strip_tags` (default `true`), `decode_entities` (default `true`) and `preserve_whitespace` (default `false`).

This is **text extraction, not a security sanitizer**. Tags are removed first with the standard library's HTML tokenizer (quoted `>` inside attributes, comments and doctypes are handled like a browser would), `<script>` and `<style>` content is dropped, and a `<` that does not start a tag (`a < b`) stays text. Entities are decoded afterwards, once, and the decoded text is never re-read as markup. The result is plain text that can still contain `<`, `>` and `&` (`5 &lt; 6` becomes `5 < 6`), so escape it for the context where you use it (HTML, SQL, shell).

### Format Transformations
`ssn_formatting`, `zip_code_formatting`, `phone_number_formatting`, `email_formatting`, `ip_address_formatting` and `mac_address_formatting` validate and normalise structured identifiers; a value that fails validation becomes NULL (or is kept unchanged with `allow_invalid`). Notes:

- **`zero_pad`** (SSN, ZIP, MAC): padding is applied *before* validation, so it restores leading zeros dropped by a numeric column (`"2134"` becomes ZIP `02134`, `"12345678"` becomes SSN `012-34-5678`); a float rendering such as `"2134.0"` is read as `2134`. For MAC addresses it pads single-digit octets (`0:1a:2b:3:4:5`); input that is not exactly 12 hex digits is rejected, never padded or truncated.
- **Phone** (`format_style`: `us-standard`, `international`, `digits-only`, `preserve`): a number with a `+` country code other than `+1` is validated against E.164 length limits (7-15 digits) and written as `+<digits>`; `+1` is never added to it.
- **Email**: lower-cases (`normalize_case`), optional whitespace strip, trailing dots removed from the domain; validation rejects doubled, leading or trailing dots in the local part and empty or hyphen-edged domain labels.

## Field-Specific Transformations

Transformations are attached to individual columns (see "How the code reads this extension" above):

```json
{
  "x-transformations": {
    "column_transformations": {
      "customer_name": {
        "string_cleaning": { "enabled": true, "case_transform": "title" }
      },
      "salary": {
        "money_conversion": { "enabled": true },
        "numeric_cleaning": { "enabled": true, "target_type": "double" }
      },
      "birth_date": {
        "datetime": { "enabled": true, "mode": "specify_formats", "formats": ["%Y-%m-%d", "%m/%d/%Y"], "target_type": "date" }
      }
    }
  }
}
```

The transformations of one column run in the order they are listed. The `fwfSpecific` and `sqlSpecific` option blocks that earlier versions of this document described are not implemented.

## Implementation Details

### Processing Pipeline
1. **Special types**: columns with `x-special-type` get their formatter
2. **Column transformations**: each column's enabled transformations run in order on the batch column
3. **Result**: the column is replaced in the batch; a transformation that raises is reported as a validation result and the column is left unchanged

### Performance
- Transformers work on whole Arrow arrays; string cleaning, regex replacement and the format validators iterate the values in Python, so they cost more than Arrow compute kernels
- `regex_replace` patterns are compiled when the configuration is created (a bad pattern raises `ValueError`). They run on Python's `re`, which has no timeout, so only use patterns from trusted schemas

### Error Handling
- **Unparseable values** (numbers, dates, formatted identifiers) become NULL; with `allow_invalid` a format transformer keeps the original text
- **Invalid configuration** (unknown option, bad timezone, bad regex, equal separators) raises `ValueError` when the transformation is created
- **No cell values in errors**: exception messages carry row numbers and option names, not data

## Usage Examples

### Basic String Cleaning
```json
{
  "x-transformations": {
    "stringCleaning": {
      "normalizeQuotes": true,
      "collapseWhitespace": true,
      "stripWhitespace": true,
      "unicodeNormalize": "NFKC"
    }
  }
}
```

### Business Name Standardization
```json
{
  "x-transformations": {
    "caseTransformation": {
      "caseTransform": "title",
      "titleCaseExceptions": ["of", "the", "and", "LLC", "Inc"],
      "customCaseMappings": {
        "llc": "LLC",
        "inc": "Inc",
        "corp": "Corp"
      }
    }
  }
}
```

### Financial Data Processing
```json
{
  "x-transformations": {
    "moneyType": {
      "currencySymbols": ["$"],
      "thousandsSeparator": ",",
      "parenthesesNegative": true
    },
    "numericCleaning": {
      "allowNaN": false,
      "nanValues": ["", "N/A", "--"]
    }
  }
}
```

### Multi-Format Date Handling
```json
{
  "x-transformations": {
    "dateTimeParsing": {
      "mode": "specify_formats",
      "formats": [
        "%Y-%m-%d",
        "%m/%d/%Y", 
        "%d/%m/%Y",
        "%Y%m%d"
      ],
      "targetType": "date"
    }
  }
}
```

(In the per-column form: `"datetime": {"enabled": true, "mode": "specify_formats", "formats": [...], "target_type": "date"}`.)

## Best Practices

1. **Start Conservative**: Begin with basic transformations and add complexity as needed
2. **Test with Real Data**: Validate transformation rules with actual data samples
3. **Document Business Rules**: Clearly document why specific transformations are applied
4. **Monitor Transformation Success**: Track rates of successful transformations
5. **Field-Specific Rules**: Use field-specific configurations for complex requirements
6. **Performance Testing**: Benchmark transformation performance with large datasets
7. **Preserve Originals**: Consider keeping original values for audit trails
