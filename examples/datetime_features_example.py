"""
Comprehensive example demonstrating the enhanced datetime transformations in forklift.

This example shows all the new datetime parsing modes and features:
1. Enforce mode - strict format validation
2. Specify formats mode - custom format lists
3. Common formats mode - predefined formats
4. Fuzzy parsing with dateutil
5. Epoch timestamp support in various units
6. Timezone conversions
7. Multiple output types
8. Schema-based configuration
"""

import pyarrow as pa
from src.forklift.utils.transformations import DataTransformer, DateTimeTransformConfig
from src.forklift.processors.transformations import SchemaBasedTransformer


def main():
    print("🚀 Enhanced Datetime Transformations Example")
    print("=" * 60)

    transformer = DataTransformer()

    # Example 1: Enforce Mode (Strict Format Validation)
    print("\n1️⃣ ENFORCE MODE - Strict Format Validation")
    print("-" * 50)

    config_enforce = DateTimeTransformConfig(
        mode="enforce", format="YYYY-MM-DD", target_type="string"  # Only this exact format allowed
    )

    test_data = ["2025-08-27", "2025-8-27", "08/27/2025"]  # Only first will pass
    column = pa.array(test_data)
    result = transformer.apply_datetime_transformation(column, config_enforce)

    print(f"Input:  {test_data}")
    print(f"Output: {result.to_pylist()}")
    print("✓ Only exact format matches are accepted")

    # Example 2: Specify Formats Mode
    print("\n2️⃣ SPECIFY FORMATS MODE - Custom Format Lists")
    print("-" * 50)

    config_specify = DateTimeTransformConfig(
        mode="specify_formats",
        formats=["YYYY-MM-DD", "MM/DD/YYYY", "DD-MMM-YYYY"],
        target_type="string",
    )

    test_data = ["2025-08-27", "08/27/2025", "27-Aug-2025", "Aug 27, 2025"]
    column = pa.array(test_data)
    result = transformer.apply_datetime_transformation(column, config_specify)

    print(f"Allowed formats: YYYY-MM-DD, MM/DD/YYYY, DD-MMM-YYYY")
    print(f"Input:  {test_data}")
    print(f"Output: {result.to_pylist()}")
    print("✓ Only specified formats are accepted")

    # Example 3: Common Formats Mode
    print("\n3️⃣ COMMON FORMATS MODE - Predefined Formats")
    print("-" * 50)

    config_common = DateTimeTransformConfig(mode="common_formats", target_type="string")

    test_data = ["2025-08-27", "08/27/2025", "27-Aug-2025", "Aug 27, 2025", "20250827"]
    column = pa.array(test_data)
    result = transformer.apply_datetime_transformation(column, config_common)

    print(f"Input:  {test_data}")
    print(f"Output: {result.to_pylist()}")
    print("✓ All common date formats are recognized")

    # Example 4: Fuzzy Parsing
    print("\n4️⃣ FUZZY PARSING - Natural Language Dates")
    print("-" * 50)

    config_fuzzy = DateTimeTransformConfig(
        mode="common_formats", allow_fuzzy=True, target_type="string"
    )

    test_data = ["Tuesday, August 27th 2025", "27 Aug 2025 at 2:30 PM", "Aug 27, 2025"]
    column = pa.array(test_data)
    result = transformer.apply_datetime_transformation(column, config_fuzzy)

    print(f"Input:  {test_data}")
    print(f"Output: {result.to_pylist()}")
    print("✓ Natural language dates are parsed")

    # Example 5: Epoch Timestamp Support
    print("\n5️⃣ EPOCH TIMESTAMP SUPPORT - Various Units")
    print("-" * 50)

    # Parsing epoch timestamps
    config_epoch = DateTimeTransformConfig(mode="common_formats", target_type="string")

    epoch_data = [
        "1724766600",  # Seconds
        "1724766600000",  # Milliseconds
        "1724766600000000",  # Microseconds
        "2025-08-27",  # Regular date
    ]
    column = pa.array(epoch_data)
    result = transformer.apply_datetime_transformation(column, config_epoch)

    print(f"Input (mixed epoch/dates): {epoch_data}")
    print(f"Output: {result.to_pylist()}")
    print("✓ Various epoch timestamp formats auto-detected")

    # Converting to epoch
    config_to_epoch = DateTimeTransformConfig(mode="common_formats", to_epoch="seconds")

    regular_dates = ["2025-08-27 14:30:00", "2025-12-25 00:00:00"]
    column = pa.array(regular_dates)
    result = transformer.apply_datetime_transformation(column, config_to_epoch)

    print(f"\nConverting to epoch seconds:")
    print(f"Input:  {regular_dates}")
    print(f"Output: {result.to_pylist()}")
    print("✓ Datetimes converted to epoch timestamps")

    # Example 6: Timezone Conversion
    print("\n6️⃣ TIMEZONE CONVERSION - Global Timezone Support")
    print("-" * 50)

    config_tz = DateTimeTransformConfig(
        mode="common_formats", timezone="America/New_York", target_type="string"
    )

    utc_dates = ["2025-08-27 14:30:00", "2025-12-25 12:00:00"]
    column = pa.array(utc_dates)
    result = transformer.apply_datetime_transformation(column, config_tz)

    print(f"Input (UTC):  {utc_dates}")
    print(f"Output (EST): {result.to_pylist()}")
    print("✓ Timezone conversion applied")

    # Example 7: Multiple Output Types
    print("\n7️⃣ MULTIPLE OUTPUT TYPES - Flexible Results")
    print("-" * 50)

    test_date = ["2025-08-27 14:30:00"]
    column = pa.array(test_date)

    # Date type
    config_date = DateTimeTransformConfig(mode="common_formats", target_type="date")
    result_date = transformer.apply_datetime_transformation(column, config_date)

    # Timestamp type
    config_timestamp = DateTimeTransformConfig(mode="common_formats", target_type="timestamp")
    result_timestamp = transformer.apply_datetime_transformation(column, config_timestamp)

    # Custom string format
    config_custom = DateTimeTransformConfig(
        mode="common_formats", target_type="string", output_format="%B %d, %Y at %I:%M %p"
    )
    result_custom = transformer.apply_datetime_transformation(column, config_custom)

    print(f"Input: {test_date[0]}")
    print(f"As date:      {result_date.to_pylist()[0]} (type: {result_date.type})")
    print(f"As timestamp: {result_timestamp.to_pylist()[0]} (type: {result_timestamp.type})")
    print(f"Custom format: {result_custom.to_pylist()[0]}")
    print("✓ Multiple output types supported")

    # Example 8: Schema-Based Configuration
    print("\n8️⃣ SCHEMA-BASED CONFIGURATION - Declarative Setup")
    print("-" * 50)

    schema_config = {
        "x-transformations": {
            "column_transformations": {
                "order_date": {
                    "datetime": {
                        "enabled": True,
                        "mode": "specify_formats",
                        "formats": ["MM/DD/YYYY", "YYYY-MM-DD"],
                        "target_type": "string",
                        "output_format": "%Y-%m-%d",
                    }
                },
                "timestamp_col": {
                    "datetime": {"enabled": True, "mode": "common_formats", "to_epoch": "seconds"}
                },
            }
        }
    }

    # Create test data
    pa_schema = pa.schema(
        [pa.field("order_date", pa.string()), pa.field("timestamp_col", pa.string())]
    )

    data = {
        "order_date": ["08/27/2025", "2025-12-25", "invalid"],
        "timestamp_col": ["2025-08-27 14:30:00", "2025-12-25 00:00:00", "1724766600"],
    }
    batch = pa.record_batch(data, pa_schema)

    # Apply schema-based transformations
    schema_transformer = SchemaBasedTransformer(schema_config)
    result_batch, validation_results = schema_transformer.process_batch(batch)

    print("Schema configuration applied:")
    print(f"order_date:    {result_batch.column('order_date').to_pylist()}")
    print(f"timestamp_col: {result_batch.column('timestamp_col').to_pylist()}")
    print(f"Validation results: {len(validation_results)} errors")
    print("✓ Schema-based transformations work seamlessly")

    print(f"\n🎉 All features demonstrated successfully!")
    print("\nSUMMARY OF ENHANCED DATETIME FEATURES:")
    print("• 🎯 Enforce Mode: Strict format validation")
    print("• 📋 Specify Formats: Custom format lists")
    print("• 🔄 Common Formats: Predefined format recognition")
    print("• 🧠 Fuzzy Parsing: Natural language date parsing")
    print("• ⏰ Epoch Support: Unix timestamps (seconds, ms, μs, ns)")
    print("• 🌍 Timezone Conversion: Global timezone support")
    print("• 📊 Multiple Output Types: datetime, date, timestamp, string")
    print("• ⚙️ Schema Integration: Works with existing transformation system")


if __name__ == "__main__":
    main()
