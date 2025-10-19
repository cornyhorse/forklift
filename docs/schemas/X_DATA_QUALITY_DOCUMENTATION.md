# x-dataQuality Documentation

## Overview
The `x-dataQuality` extension provides comprehensive data quality assessment, monitoring, and enforcement capabilities. This feature enables automatic detection of data quality issues, statistical analysis of data characteristics, and enforcement of business rules to ensure data meets quality standards before processing.

## Schema Structure
```json
{
  "x-dataQuality": {
    "description": "Data quality assessment and enforcement configuration",
    "enabled": true,
    "qualityThresholds": {
      "completeness": {
        "minimum": 0.95,
        "action": "warn",
        "description": "At least 95% of required fields must be populated"
      },
      "validity": {
        "minimum": 0.98,
        "action": "fail",
        "description": "At least 98% of data must pass validation rules"
      },
      "uniqueness": {
        "minimum": 0.99,
        "action": "warn",
        "description": "At least 99% of unique constraint checks must pass"
      }
    },
    "fieldQualityRules": {
      "email_address": {
        "rules": ["not_null", "email_format", "domain_valid"],
        "severity": "error"
      },
      "phone_number": {
        "rules": ["not_null", "phone_format", "length_check"],
        "severity": "warning"
      },
      "birth_date": {
        "rules": ["not_null", "date_format", "reasonable_range"],
        "parameters": {
          "min_date": "1900-01-01",
          "max_date": "2024-12-31"
        }
      }
    },
    "crossFieldValidation": [
      {
        "name": "start_end_date_order",
        "rule": "start_date <= end_date",
        "fields": ["start_date", "end_date"],
        "severity": "error"
      }
    ],
    "statisticalChecks": {
      "outlierDetection": {
        "enabled": true,
        "method": "iqr",
        "threshold": 1.5,
        "action": "flag"
      },
      "distributionChecks": {
        "enabled": true,
        "baseline": "historical",
        "tolerance": 0.15
      }
    },
    "reporting": {
      "generateReport": true,
      "outputFormat": "json",
      "includeDetails": true,
      "scoringMethod": "weighted"
    }
  }
}
```

## Configuration Properties

### Quality Thresholds

#### `qualityThresholds`
- **Type**: Object
- **Description**: Define minimum quality standards and actions for different quality dimensions

##### Completeness Threshold
```json
{
  "completeness": {
    "minimum": 0.95,
    "action": "warn",
    "description": "At least 95% of required fields must be populated"
  }
}
```

##### Validity Threshold
```json
{
  "validity": {
    "minimum": 0.98,
    "action": "fail",
    "description": "At least 98% of data must pass validation rules"
  }
}
```

##### Uniqueness Threshold
```json
{
  "uniqueness": {
    "minimum": 0.99,
    "action": "warn",
    "description": "At least 99% of unique constraint checks must pass"
  }
}
```

#### Threshold Actions
- **`warn`**: Log warning and continue processing
- **`fail`**: Stop processing if threshold not met
- **`flag`**: Mark data quality issues but continue
- **`ignore`**: Continue processing regardless of threshold

### Field-Level Quality Rules

#### `fieldQualityRules`
- **Type**: Object with field names as keys
- **Description**: Define specific quality rules for individual fields

#### Available Quality Rules

##### Basic Validation Rules
- **`not_null`**: Field must not be null or empty
- **`not_empty`**: Field must contain non-whitespace content
- **`required`**: Field must be present and populated
- **`length_check`**: Field length within specified range
- **`pattern_match`**: Field matches specified regex pattern

##### Format Validation Rules
- **`email_format`**: Valid email address format
- **`phone_format`**: Valid phone number format
- **`date_format`**: Valid date format
- **`numeric_format`**: Valid numeric format
- **`url_format`**: Valid URL format
- **`ssn_format`**: Valid SSN format

##### Business Logic Rules
- **`domain_valid`**: Email domain exists and is valid
- **`reasonable_range`**: Value within reasonable business range
- **`lookup_valid`**: Value exists in reference lookup table
- **`checksum_valid`**: Field passes checksum validation
- **`credit_card_valid`**: Valid credit card number (Luhn algorithm)

##### Data Type Rules
- **`integer_type`**: Value is a valid integer
- **`decimal_type`**: Value is a valid decimal number
- **`boolean_type`**: Value is a valid boolean
- **`date_type`**: Value is a valid date
- **`timestamp_type`**: Value is a valid timestamp

#### Field Rule Configuration
```json
{
  "fieldQualityRules": {
    "customer_email": {
      "rules": ["not_null", "email_format", "domain_valid"],
      "severity": "error",
      "parameters": {
        "blocked_domains": ["tempmail.com", "10minutemail.com"]
      }
    },
    "customer_age": {
      "rules": ["not_null", "integer_type", "reasonable_range"],
      "severity": "warning",
      "parameters": {
        "min_value": 0,
        "max_value": 150
      }
    },
    "order_amount": {
      "rules": ["not_null", "decimal_type", "reasonable_range"],
      "severity": "error",
      "parameters": {
        "min_value": 0.01,
        "max_value": 1000000.00,
        "decimal_places": 2
      }
    }
  }
}
```

### Cross-Field Validation

#### `crossFieldValidation`
- **Type**: Array of validation objects
- **Description**: Rules that validate relationships between multiple fields

#### Cross-Field Rule Types
- **Date Ordering**: Start date before end date
- **Conditional Requirements**: Field required when another field has specific value
- **Mathematical Relationships**: Sum/difference/ratio constraints
- **Logical Consistency**: Business rule consistency checks

```json
{
  "crossFieldValidation": [
    {
      "name": "hire_before_termination",
      "rule": "hire_date <= termination_date OR termination_date IS NULL",
      "fields": ["hire_date", "termination_date"],
      "severity": "error",
      "description": "Termination date must be after hire date"
    },
    {
      "name": "discount_percentage_limit",
      "rule": "discount_percentage <= 100 AND (discount_amount <= order_total * discount_percentage / 100)",
      "fields": ["discount_percentage", "discount_amount", "order_total"],
      "severity": "warning",
      "description": "Discount amount must not exceed calculated percentage"
    },
    {
      "name": "conditional_address_requirement",
      "rule": "country = 'US' IMPLIES state IS NOT NULL",
      "fields": ["country", "state"],
      "severity": "error",
      "description": "State is required for US addresses"
    }
  ]
}
```

### Statistical Quality Checks

#### `statisticalChecks`
- **Type**: Object
- **Description**: Configure statistical analysis for data quality assessment

##### Outlier Detection
```json
{
  "outlierDetection": {
    "enabled": true,
    "method": "iqr",
    "threshold": 1.5,
    "action": "flag",
    "fields": ["salary", "order_amount", "account_balance"]
  }
}
```

**Methods**:
- **`iqr`**: Interquartile Range method
- **`zscore`**: Z-score statistical method
- **`isolation_forest`**: Machine learning isolation forest
- **`local_outlier_factor`**: Local outlier factor algorithm

##### Distribution Checks
```json
{
  "distributionChecks": {
    "enabled": true,
    "baseline": "historical",
    "tolerance": 0.15,
    "fields": ["customer_age", "order_value"],
    "tests": ["kolmogorov_smirnov", "chi_square"]
  }
}
```

**Baseline Types**:
- **`historical`**: Compare against historical data patterns
- **`expected`**: Compare against predefined expected distributions
- **`rolling`**: Compare against rolling window averages

### Quality Reporting

#### `reporting`
- **Type**: Object
- **Description**: Configure quality assessment reporting

```json
{
  "reporting": {
    "generateReport": true,
    "outputFormat": "json",
    "outputPath": "auto",
    "includeDetails": true,
    "includeRecommendations": true,
    "scoringMethod": "weighted",
    "weights": {
      "completeness": 0.3,
      "validity": 0.4,
      "uniqueness": 0.2,
      "consistency": 0.1
    }
  }
}
```

## Quality Dimensions

### Completeness
Measures the extent to which data is present and populated.

**Metrics**:
- Null value percentage
- Empty string percentage
- Missing value patterns
- Required field coverage

### Validity
Measures the extent to which data conforms to defined formats and rules.

**Metrics**:
- Format validation pass rate
- Business rule compliance
- Data type conformity
- Range validation success

### Uniqueness
Measures the extent to which data values are unique where required.

**Metrics**:
- Duplicate record percentage
- Primary key uniqueness
- Unique constraint violations
- Deduplication effectiveness

### Consistency
Measures the extent to which data is consistent across fields and records.

**Metrics**:
- Cross-field validation success
- Referential integrity compliance
- Business rule consistency
- Data relationship validity

### Accuracy
Measures the extent to which data correctly represents real-world values.

**Metrics**:
- Lookup table validation
- Format accuracy
- Checksum validation
- Domain verification

## Implementation Details

### Quality Assessment Pipeline
1. **Field-Level Checks**: Validate individual field quality rules
2. **Cross-Field Validation**: Validate relationships between fields
3. **Statistical Analysis**: Perform outlier detection and distribution checks
4. **Threshold Evaluation**: Compare results against defined thresholds
5. **Action Execution**: Execute defined actions based on quality scores
6. **Report Generation**: Generate comprehensive quality reports

### Performance Optimization
- **Parallel Processing**: Quality checks run in parallel where possible
- **Sampling**: Statistical checks can use sampling for large datasets
- **Caching**: Rule compilation and reference data caching
- **Incremental Checks**: Only check changed data in incremental loads

### Memory Management
- **Streaming Validation**: Process large datasets without loading into memory
- **Batch Processing**: Process quality checks in configurable batches
- **Resource Limits**: Configurable memory and CPU limits for quality checks

## Usage Examples

### E-commerce Data Quality
```json
{
  "x-dataQuality": {
    "enabled": true,
    "qualityThresholds": {
      "completeness": {"minimum": 0.95, "action": "warn"},
      "validity": {"minimum": 0.98, "action": "fail"}
    },
    "fieldQualityRules": {
      "customer_email": {
        "rules": ["not_null", "email_format", "domain_valid"],
        "severity": "error"
      },
      "order_amount": {
        "rules": ["not_null", "decimal_type", "reasonable_range"],
        "severity": "error",
        "parameters": {"min_value": 0.01, "max_value": 100000}
      },
      "product_sku": {
        "rules": ["not_null", "pattern_match", "length_check"],
        "severity": "error",
        "parameters": {
          "pattern": "^[A-Z]{2}\\d{6}$",
          "min_length": 8,
          "max_length": 8
        }
      }
    },
    "crossFieldValidation": [
      {
        "name": "order_date_shipped_date",
        "rule": "order_date <= shipped_date OR shipped_date IS NULL",
        "fields": ["order_date", "shipped_date"],
        "severity": "error"
      }
    ]
  }
}
```

### Financial Data Quality
```json
{
  "x-dataQuality": {
    "enabled": true,
    "qualityThresholds": {
      "completeness": {"minimum": 0.99, "action": "fail"},
      "validity": {"minimum": 0.995, "action": "fail"},
      "uniqueness": {"minimum": 0.999, "action": "fail"}
    },
    "fieldQualityRules": {
      "account_number": {
        "rules": ["not_null", "pattern_match", "checksum_valid"],
        "severity": "error",
        "parameters": {"pattern": "^\\d{10,12}$"}
      },
      "transaction_amount": {
        "rules": ["not_null", "decimal_type", "reasonable_range"],
        "severity": "error",
        "parameters": {
          "min_value": -1000000,
          "max_value": 1000000,
          "decimal_places": 2
        }
      },
      "currency_code": {
        "rules": ["not_null", "lookup_valid"],
        "severity": "error",
        "parameters": {"lookup_table": "valid_currencies"}
      }
    },
    "statisticalChecks": {
      "outlierDetection": {
        "enabled": true,
        "method": "zscore",
        "threshold": 3.0,
        "action": "flag",
        "fields": ["transaction_amount"]
      }
    }
  }
}
```

### HR Data Quality
```json
{
  "x-dataQuality": {
    "enabled": true,
    "fieldQualityRules": {
      "employee_id": {
        "rules": ["not_null", "pattern_match"],
        "severity": "error",
        "parameters": {"pattern": "^EMP\\d{6}$"}
      },
      "hire_date": {
        "rules": ["not_null", "date_format", "reasonable_range"],
        "severity": "error",
        "parameters": {
          "min_date": "1950-01-01",
          "max_date": "2024-12-31"
        }
      },
      "salary": {
        "rules": ["not_null", "decimal_type", "reasonable_range"],
        "severity": "warning",
        "parameters": {"min_value": 20000, "max_value": 500000}
      }
    },
    "crossFieldValidation": [
      {
        "name": "manager_hierarchy",
        "rule": "manager_id != employee_id",
        "fields": ["employee_id", "manager_id"],
        "severity": "error"
      }
    ]
  }
}
```

## Integration with Other Features

### With Constraint Handling
```json
{
  "x-dataQuality": {
    "enabled": true,
    "qualityThresholds": {
      "validity": {"minimum": 0.95, "action": "bad_rows"}
    }
  },
  "x-constraintHandling": {
    "errorMode": "bad_rows",
    "badRowsOutput": {
      "enabled": true,
      "includeErrorDetails": true
    }
  }
}
```

### With Transformations
```json
{
  "x-transformations": {
    "stringCleaning": {
      "stripWhitespace": true,
      "normalizeSpaces": true
    }
  },
  "x-dataQuality": {
    "fieldQualityRules": {
      "customer_name": {
        "rules": ["not_empty", "length_check"],
        "parameters": {"min_length": 2, "max_length": 100}
      }
    }
  }
}
```

### With Metadata Generation
```json
{
  "x-metadata-generation": {
    "enabled": true,
    "statistics": {
      "numeric": {"enabled": true},
      "string": {"enabled": true}
    }
  },
  "x-dataQuality": {
    "statisticalChecks": {
      "distributionChecks": {
        "enabled": true,
        "baseline": "historical"
      }
    }
  }
}
```

## Best Practices

### Rule Design
1. **Start Simple**: Begin with basic rules and add complexity gradually
2. **Business Focus**: Align rules with actual business requirements
3. **Performance Impact**: Consider the performance impact of complex rules
4. **Maintainability**: Design rules that are easy to understand and maintain

### Threshold Setting
1. **Realistic Targets**: Set achievable quality thresholds
2. **Gradual Improvement**: Start with current quality levels and improve over time
3. **Business Impact**: Consider the business impact of different threshold levels
4. **Regular Review**: Periodically review and adjust thresholds

### Error Handling
1. **Appropriate Actions**: Choose actions that match business needs
2. **Escalation**: Design escalation paths for quality issues
3. **Monitoring**: Set up monitoring for quality trends
4. **Feedback Loops**: Create feedback mechanisms to improve data sources

### Performance Optimization
1. **Selective Checking**: Apply quality checks only where needed
2. **Sampling**: Use sampling for statistical checks on large datasets
3. **Parallel Processing**: Enable parallel processing for independent checks
4. **Resource Management**: Monitor and manage resource usage

## Quality Report Output

The data quality assessment generates comprehensive reports:

```json
{
  "quality_assessment": {
    "overall_score": 0.94,
    "dimension_scores": {
      "completeness": 0.96,
      "validity": 0.93,
      "uniqueness": 0.99,
      "consistency": 0.87
    },
    "field_quality": {
      "customer_email": {
        "score": 0.98,
        "issues": ["2% failed domain validation"],
        "total_records": 10000,
        "valid_records": 9800
      }
    },
    "threshold_compliance": {
      "completeness": "PASS",
      "validity": "FAIL",
      "uniqueness": "PASS"
    },
    "recommendations": [
      "Improve email domain validation",
      "Review data entry processes for validity issues"
    ]
  }
}
```

This comprehensive documentation covers all the major x- attributes and their implementations in your Forklift project, providing detailed information about each feature's functionality, configuration options, and usage examples.
