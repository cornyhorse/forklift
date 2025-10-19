# Forklift Format Transformations

## Overview

The `forklift.utils.transformations.format` module provides specialized data formatters for standardizing and validating common data formats within the Forklift data processing ecosystem. These formatters are built on a common base architecture and integrate seamlessly with Forklift's PyArrow-based data pipeline.

## Role in Forklift Ecosystem

Format transformations are a critical component of Forklift's data cleaning and standardization pipeline. They:

- **Standardize Data Formats**: Convert various input formats to consistent, standardized outputs
- **Validate Data Quality**: Ensure data meets specified format requirements
- **Integrate with Processors**: Work seamlessly with Forklift's main data processors
- **Support Schema Compliance**: Help ensure processed data matches expected schema formats
- **Enable Data Interoperability**: Standardize formats for downstream systems and analytics

## Architecture

All format transformers inherit from `BaseFormatter` and follow a consistent pattern:
- Configuration-driven behavior through typed config objects
- PyArrow integration for efficient columnar processing
- Null-safe operations with proper handling of missing values
- Validation capabilities with configurable error handling

## Module Files

### `base.py`
**Core Infrastructure**

Provides the foundational `BaseFormatter` abstract base class that all format transformers inherit from. Defines the standard interface for:
- Configuration-based initialization
- Single value formatting (`format_value`)
- Columnar data processing (`apply_formatting`)
- PyArrow integration with automatic type casting
- Null-safe value handling

### `email.py`
**Email Address Formatting**

Handles email address standardization and validation:
- **Normalization**: Case normalization (lowercase)
- **Whitespace Handling**: Strips leading/trailing whitespace
- **Domain Cleaning**: Removes trailing dots from domains
- **Validation**: Optional email format validation
- **Use Cases**: User data processing, contact information standardization

### `network.py`
**Network Address Formatting**

Provides formatters for network-related identifiers:
- **IP Address Formatting**: IPv4 and IPv6 address standardization
- **MAC Address Formatting**: Hardware address normalization
- **Validation**: Network address format validation
- **Use Cases**: Network logs, device inventories, security data

### `phone.py`
**Phone Number Formatting**

Standardizes phone number formats across different input styles:
- **Digit Extraction**: Removes non-numeric characters except plus signs
- **Format Styles**: Multiple output formats (e.g., (XXX) XXX-XXXX, XXX-XXX-XXXX)
- **International Support**: Handles country codes and international formats
- **Validation**: Configurable validation for letter detection and format compliance
- **Use Cases**: Customer data, contact information, telecommunications data

### `postal.py`
**Postal Code Formatting**

Handles postal and ZIP code standardization:
- **ZIP Code Formatting**: US ZIP and ZIP+4 code standardization
- **International Support**: Postal code formats for various countries
- **Validation**: Format compliance checking
- **Padding**: Zero-padding for numeric postal codes
- **Use Cases**: Address data, shipping information, geographic analysis

### `ssn.py`
**Social Security Number Formatting**

Provides secure SSN formatting capabilities:
- **Format Standardization**: XXX-XX-XXXX format
- **Masking Options**: Partial masking for privacy (XXX-XX-1234)
- **Validation**: SSN format and basic validity checks
- **Security Considerations**: Designed with privacy and security in mind
- **Use Cases**: HR systems, financial data, identity verification

### `transformer.py`
**Format Transformation Orchestrator**

Central coordinator that manages all format transformers:
- **Factory Pattern**: Creates appropriate formatters based on configuration
- **Unified Interface**: Single entry point for all format transformations
- **Configuration Management**: Handles complex transformation configurations
- **Batch Processing**: Efficiently processes multiple format transformations
- **Integration Point**: Primary interface used by Forklift processors

## Usage Examples

### Basic Email Formatting
```python
from forklift.utils.transformations.format.email import EmailFormatter
from forklift.utils.transformations.configs import EmailConfig

config = EmailConfig(normalize_case=True, strip_whitespace=True)
formatter = EmailFormatter(config)
result = formatter.format_value("  User@EXAMPLE.COM  ")  # Returns "user@example.com"
```

### Phone Number Standardization
```python
from forklift.utils.transformations.format.phone import PhoneNumberFormatter
from forklift.utils.transformations.configs import PhoneNumberConfig

config = PhoneNumberConfig(format_style="standard")
formatter = PhoneNumberFormatter(config)
result = formatter.format_value("(555) 123-4567")  # Returns standardized format
```

### Batch Processing with Transformer
```python
from forklift.utils.transformations.format.transformer import FormatTransformer

# Configure multiple format transformations
configs = {
    'email_column': EmailConfig(normalize_case=True),
    'phone_column': PhoneNumberConfig(format_style="standard")
}

transformer = FormatTransformer(configs)
# Apply to PyArrow table columns
```

## Integration with Forklift

Format transformers integrate with Forklift's main processing pipeline through:
1. **Processor Configuration**: Specified in processing schemas and configurations
2. **DataTransformer**: Used by the main DataTransformer class
3. **Pipeline Stages**: Applied during data cleaning and standardization phases
4. **Schema Validation**: Ensure outputs match expected schema formats

## Performance Considerations

- **Vectorized Operations**: Uses PyArrow for efficient columnar processing
- **Memory Efficient**: Processes data in chunks to manage memory usage
- **Type Safety**: Leverages PyArrow's type system for safe transformations
- **Null Handling**: Optimized null value processing without unnecessary operations

## Configuration

All formatters use typed configuration objects that provide:
- **Type Safety**: Compile-time validation of configuration parameters
- **Documentation**: Self-documenting configuration options
- **Defaults**: Sensible default values for common use cases
- **Extensibility**: Easy to extend for custom formatting requirements
