# x-uniqueConstraints Documentation

## Overview
The `x-uniqueConstraints` extension provides configuration for additional unique constraints beyond the primary key. This feature enables enforcement of business rules requiring unique combinations of columns, supporting complex data integrity requirements and multi-column uniqueness validation.

## Schema Structure
```json
{
  "x-uniqueConstraints": [
    {
      "name": "unique_email",
      "columns": ["email_address"],
      "description": "Email addresses must be unique across all customers"
    },
    {
      "name": "unique_customer_order",
      "columns": ["customer_id", "order_date", "order_number"],
      "description": "Each customer can only have one order with the same number on the same date"
    },
    {
      "name": "unique_ssn_active",
      "columns": ["ssn", "status"],
      "condition": "status = 'active'",
      "description": "Active records must have unique SSN"
    }
  ]
}
```

## Configuration Properties

### Constraint Definition

#### `name` (required)
- **Type**: String
- **Description**: Unique identifier for the constraint
- **Implementation**: Used in error messages and violation reports
- **Naming Convention**: Descriptive name indicating purpose (e.g., "unique_email", "unique_customer_product")

#### `columns` (required)
- **Type**: Array of strings
- **Description**: List of column names that must be unique together
- **Implementation**: Combination of values across all listed columns must be unique
- **Examples**:
  - Single column: `["email"]`
  - Multi-column: `["customer_id", "product_id", "order_date"]`

#### `description` (optional)
- **Type**: String
- **Description**: Human-readable explanation of the business rule
- **Use**: Documentation and error reporting

#### `condition` (optional)
- **Type**: String
- **Description**: SQL-like condition that must be met for constraint to apply
- **Implementation**: Constraint only enforced when condition is true
- **Examples**:
  - `"status = 'active'"`: Only enforce for active records
  - `"amount > 0"`: Only enforce for positive amounts
  - `"end_date IS NULL"`: Only enforce for current records

#### `ignoreNulls` (optional)
- **Type**: Boolean
- **Description**: Whether to ignore NULL values in uniqueness checking
- **Default**: `false`
- **Implementation**: 
  - When `true`: NULL values don't participate in uniqueness validation
  - When `false`: NULL values are treated as regular values for uniqueness

#### `caseSensitive` (optional)
- **Type**: Boolean
- **Description**: Whether string comparisons should be case-sensitive
- **Default**: `true`
- **Implementation**: Controls how string values are compared for uniqueness

## Constraint Types

### Single Column Constraints
Enforce uniqueness on individual columns.

```json
{
  "x-uniqueConstraints": [
    {
      "name": "unique_email",
      "columns": ["email_address"],
      "description": "Email addresses must be unique"
    },
    {
      "name": "unique_employee_id",
      "columns": ["employee_id"],
      "description": "Employee IDs must be unique"
    }
  ]
}
```

### Multi-Column Constraints
Enforce uniqueness on combinations of columns.

```json
{
  "x-uniqueConstraints": [
    {
      "name": "unique_order_line",
      "columns": ["order_id", "line_number"],
      "description": "Each order can only have one line with the same line number"
    },
    {
      "name": "unique_employee_department_role",
      "columns": ["employee_id", "department", "role"],
      "description": "Employee can only have one role per department"
    }
  ]
}
```

### Conditional Constraints
Apply uniqueness only when certain conditions are met.

```json
{
  "x-uniqueConstraints": [
    {
      "name": "unique_active_username",
      "columns": ["username"],
      "condition": "status = 'active'",
      "description": "Active users must have unique usernames"
    },
    {
      "name": "unique_current_assignment",
      "columns": ["employee_id", "project_id"],
      "condition": "end_date IS NULL",
      "description": "Employee can only be assigned to each project once at a time"
    }
  ]
}
```

### Case-Insensitive Constraints
For systems where case differences should not matter.

```json
{
  "x-uniqueConstraints": [
    {
      "name": "unique_username_case_insensitive",
      "columns": ["username"],
      "caseSensitive": false,
      "description": "Usernames must be unique regardless of case"
    }
  ]
}
```

## Implementation Details

### Validation Process
1. **Constraint Registration**: Load constraint definitions during schema processing
2. **Data Collection**: Track values for constrained columns during processing
3. **Uniqueness Checking**: Validate uniqueness as data is processed
4. **Violation Detection**: Identify rows that violate constraints
5. **Error Handling**: Route violations according to constraint handling configuration

### Memory Management
- **Hash-based Tracking**: Use efficient hash tables to track seen combinations
- **Memory Limits**: Configurable limits to prevent memory exhaustion
- **Streaming Validation**: Process large datasets without loading all data into memory

### Performance Optimization
- **Index Usage**: Create temporary indexes for fast uniqueness checking
- **Batch Validation**: Validate constraints in batches for better performance
- **Early Termination**: Stop processing when constraint violations exceed thresholds

## Error Handling Integration

### With x-constraintHandling
Unique constraint violations integrate with the constraint handling system:

```json
{
  "x-constraintHandling": {
    "uniqueConstraintViolations": "bad_rows",
    "badRowsOutput": {
      "enabled": true,
      "includeErrorDetails": true
    }
  },
  "x-uniqueConstraints": [
    {
      "name": "unique_email",
      "columns": ["email_address"]
    }
  ]
}
```

### Violation Output Format
When violations occur, detailed information is provided:

```json
{
  "original_data": {
    "customer_id": 1001,
    "email_address": "john@example.com",
    "status": "active"
  },
  "errors": [
    {
      "constraint_type": "unique_constraint",
      "constraint_name": "unique_email",
      "columns": ["email_address"],
      "violation_type": "duplicate_value",
      "conflicting_values": ["john@example.com"],
      "message": "Duplicate value found for unique constraint 'unique_email'",
      "first_occurrence_row": 245
    }
  ]
}
```

## Usage Examples

### E-commerce System
```json
{
  "x-uniqueConstraints": [
    {
      "name": "unique_customer_email",
      "columns": ["email_address"],
      "description": "Customer email addresses must be unique"
    },
    {
      "name": "unique_order_line_item",
      "columns": ["order_id", "product_id", "variant_id"],
      "description": "Each order can contain each product variant only once"
    },
    {
      "name": "unique_active_cart",
      "columns": ["customer_id"],
      "condition": "cart_status = 'active'",
      "description": "Customer can only have one active cart"
    }
  ]
}
```

### HR Management System
```json
{
  "x-uniqueConstraints": [
    {
      "name": "unique_employee_badge",
      "columns": ["badge_number"],
      "ignoreNulls": true,
      "description": "Badge numbers must be unique (excluding temporary employees without badges)"
    },
    {
      "name": "unique_manager_department",
      "columns": ["department_id"],
      "condition": "role = 'manager'",
      "description": "Each department can only have one manager"
    },
    {
      "name": "unique_current_position",
      "columns": ["employee_id", "position_title"],
      "condition": "position_end_date IS NULL",
      "description": "Employee can only hold one current position"
    }
  ]
}
```

### Financial System
```json
{
  "x-uniqueConstraints": [
    {
      "name": "unique_account_number",
      "columns": ["account_number"],
      "description": "Account numbers must be globally unique"
    },
    {
      "name": "unique_daily_transaction",
      "columns": ["account_id", "transaction_date", "reference_number"],
      "description": "Each account can only have one transaction per reference per day"
    },
    {
      "name": "unique_active_loan",
      "columns": ["customer_id", "loan_type"],
      "condition": "loan_status IN ('active', 'pending')",
      "description": "Customer can only have one active loan per type"
    }
  ]
}
```

### Multi-Tenant System
```json
{
  "x-uniqueConstraints": [
    {
      "name": "unique_tenant_username",
      "columns": ["tenant_id", "username"],
      "caseSensitive": false,
      "description": "Usernames must be unique within each tenant"
    },
    {
      "name": "unique_tenant_resource",
      "columns": ["tenant_id", "resource_name", "resource_type"],
      "description": "Resource names must be unique per type within tenant"
    }
  ]
}
```

## Best Practices

### Constraint Design
1. **Business Logic First**: Design constraints based on actual business rules
2. **Performance Impact**: Consider the performance impact of complex multi-column constraints
3. **Null Handling**: Carefully consider whether nulls should participate in uniqueness
4. **Case Sensitivity**: Choose appropriate case sensitivity for your use case

### Naming Conventions
1. **Descriptive Names**: Use names that clearly indicate the constraint purpose
2. **Consistent Prefixing**: Use consistent prefixes like "unique_" for all unique constraints
3. **Column Indication**: Include key column names in constraint names when helpful

### Error Handling Strategy
1. **Appropriate Actions**: Choose the right constraint violation handling mode
2. **Error Details**: Enable detailed error reporting for troubleshooting
3. **Monitoring**: Set up monitoring for constraint violation rates
4. **Business Impact**: Consider the business impact of different violation handling strategies

### Performance Optimization
1. **Column Order**: Place most selective columns first in multi-column constraints
2. **Index Strategy**: Consider creating indexes for frequently checked constraints
3. **Batch Size**: Optimize batch sizes for constraint validation
4. **Memory Limits**: Set appropriate memory limits for large datasets

## Integration Considerations

### With Primary Keys
- Unique constraints complement primary key constraints
- Avoid duplicate constraints (unique constraint on primary key columns)
- Consider composite constraints that include primary key columns

### With Foreign Keys
- Unique constraints often support foreign key relationships
- Ensure referenced columns have appropriate unique constraints
- Consider cascading constraint violations

### With Transformations
- Apply constraints after data transformations
- Consider how transformations might affect uniqueness
- Validate constraint compatibility with transformation rules

## Monitoring and Maintenance

### Constraint Violation Tracking
1. **Rate Monitoring**: Track violation rates over time
2. **Pattern Analysis**: Analyze common violation patterns
3. **Source Analysis**: Identify data sources with high violation rates
4. **Trend Detection**: Monitor for increasing violation trends

### Performance Monitoring
1. **Validation Time**: Track time spent on constraint validation
2. **Memory Usage**: Monitor memory consumption during validation
3. **Processing Impact**: Measure impact on overall processing performance
4. **Optimization Opportunities**: Identify constraints that could be optimized
