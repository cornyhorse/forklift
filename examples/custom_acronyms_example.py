# Custom Acronyms Example
# This example demonstrates how to use domain-specific acronyms in string cleaning configurations.

from src.forklift.utils.transformations import DataTransformer, StringCleaningConfig
import pyarrow as pa

# Example 1: Healthcare/Medical Domain
healthcare_config = StringCleaningConfig(
    fix_case_issues=True,
    acronyms=[
        "EMR",
        "EHR",
        "ICU",
        "ER",
        "OR",
        "MRI",
        "CT",
        "PET",
        "ECG",
        "EKG",
        "IV",
        "RN",
        "MD",
        "DO",
    ],
)

# Example 2: Financial Domain
finance_config = StringCleaningConfig(
    fix_case_issues=True,
    acronyms=[
        "APR",
        "ROI",
        "KYC",
        "AML",
        "GAAP",
        "SEC",
        "FDIC",
        "IRA",
        "HSA",
        "ETF",
        "IPO",
        "REIT",
    ],
)

# Example 3: Technology Domain
tech_config = StringCleaningConfig(
    fix_case_issues=True,
    acronyms=[
        "API",
        "SDK",
        "IDE",
        "CLI",
        "GUI",
        "ORM",
        "JWT",
        "OAuth",
        "SAML",
        "REST",
        "GraphQL",
        "CRUD",
    ],
)

# Test data
test_data = [
    "THE EMR SYSTEM IN THE ICU NEEDS API INTEGRATION",
    "NASA AND THE FBI",  # Default acronyms still work
    "ROI ANALYSIS FOR ETF INVESTMENTS",
    "hello world",
]

transformer = DataTransformer()
column = pa.array(test_data)

print("Healthcare Domain:")
result = transformer.apply_string_cleaning(column, healthcare_config)
for input_val, output_val in zip(test_data, result.to_pylist()):
    print(f"  {repr(input_val)} → {repr(output_val)}")

print("\nFinance Domain:")
result = transformer.apply_string_cleaning(column, finance_config)
for input_val, output_val in zip(test_data, result.to_pylist()):
    print(f"  {repr(input_val)} → {repr(output_val)}")

print("\nTechnology Domain:")
result = transformer.apply_string_cleaning(column, tech_config)
for input_val, output_val in zip(test_data, result.to_pylist()):
    print(f"  {repr(input_val)} → {repr(output_val)}")

print("\n" + "=" * 60)
print("SCHEMA CONFIGURATION EXAMPLES")
print("=" * 60)

# Schema Configuration Examples
print(
    """
Healthcare Schema Configuration:
{
  "fields": [
    {
      "name": "medical_notes",
      "type": "string",
      "x-transformations": {
        "string_cleaning": {
          "enabled": true,
          "fix_case_issues": true,
          "normalize_quotes": true,
          "collapse_whitespace": true,
          "strip_whitespace": true,
          "acronyms": ["EMR", "EHR", "ICU", "ER", "OR", "MRI", "CT", "PET", "ECG", "EKG", "IV", "RN", "MD", "DO"]
        }
      }
    }
  ]
}

Financial Schema Configuration:
{
  "fields": [
    {
      "name": "financial_description", 
      "type": "string",
      "x-transformations": {
        "string_cleaning": {
          "enabled": true,
          "fix_case_issues": true,
          "normalize_quotes": true,
          "collapse_whitespace": true,
          "strip_whitespace": true,
          "acronyms": ["APR", "ROI", "KYC", "AML", "GAAP", "SEC", "FDIC", "IRA", "HSA", "ETF", "IPO", "REIT"]
        }
      }
    }
  ]
}

This allows for domain-specific acronym handling while maintaining the default set for common acronyms like NASA, FBI, etc.
"""
)
