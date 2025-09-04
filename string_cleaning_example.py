"""
Example demonstrating comprehensive string cleaning transformations in forklift.

This shows the new string cleaning capabilities including:
1. Smart quotes → ASCII conversion
2. Em/en dash → hyphen normalization
3. Non-breaking space → regular space conversion
4. Multiple whitespace collapse
5. Zero-width character removal
6. Control character removal
7. Unicode normalization
8. Accent removal and ASCII-only conversion
9. Case issue fixing
10. Encoding error correction
"""

import pyarrow as pa
from src.forklift.utils.data_transformations import DataTransformer, StringCleaningConfig
from src.forklift.processors.transformations import SchemaBasedTransformer

def main():
    print("🧹 Comprehensive String Cleaning Features")
    print("=" * 60)

    transformer = DataTransformer()

    # Example 1: Smart Quotes and Special Characters
    print("\n1️⃣ SMART QUOTES & SPECIAL CHARACTERS")
    print("-" * 50)

    config_quotes = StringCleaningConfig(
        normalize_quotes=True,
        normalize_dashes=True,
        normalize_spaces=True,
        collapse_whitespace=False,
        strip_whitespace=False
    )

    # Using Unicode escape sequences to avoid encoding issues
    test_data = [
        "\u201CHello\u201D",  # Smart double quotes
        "\u2018It\u2019s working\u2019",  # Smart single quotes
        "Hello\u2014world",   # Em dash
        "Hello\u2013world",   # En dash
        "Hello\u00A0world",   # Non-breaking space
    ]
    column = pa.array(test_data)
    result = transformer.apply_string_cleaning(column, config_quotes)

    print("Input (with smart quotes/dashes/spaces):")
    for i, item in enumerate(test_data):
        print(f"  {i+1}. {repr(item)}")
    print("\nOutput (normalized to ASCII):")
    for i, item in enumerate(result.to_pylist()):
        print(f"  {i+1}. {repr(item)}")
    print("✓ Special characters normalized to ASCII equivalents")

    # Example 2: Whitespace Handling
    print("\n2️⃣ WHITESPACE NORMALIZATION")
    print("-" * 50)

    config_whitespace = StringCleaningConfig(
        collapse_whitespace=True,
        strip_whitespace=True,
        remove_tabs=False,
        tab_replacement="    "  # Convert tabs to 4 spaces
    )

    test_data = [
        "  Hello    world  ",     # Multiple spaces with leading/trailing
        "Hello\t\tworld",         # Tabs
        "Hello\n\n\nworld",       # Multiple newlines
        "   Too   many   spaces   "
    ]
    column = pa.array(test_data)
    result = transformer.apply_string_cleaning(column, config_whitespace)

    print("Input (messy whitespace):")
    for i, item in enumerate(test_data):
        print(f"  {i+1}. {repr(item)}")
    print("\nOutput (cleaned whitespace):")
    for i, item in enumerate(result.to_pylist()):
        print(f"  {i+1}. {repr(item)}")
    print("✓ Whitespace normalized and collapsed")

    # Example 3: Zero-width and Control Characters
    print("\n3️⃣ INVISIBLE CHARACTER REMOVAL")
    print("-" * 50)

    config_invisible = StringCleaningConfig(
        remove_zero_width=True,
        remove_control_chars=True,
        preserve_newlines=True,
        preserve_tabs=False
    )

    test_data = [
        "Hello\u200Bworld",       # Zero-width space
        "Hello\u200Cworld",       # Zero-width non-joiner
        "\uFEFFHello world",      # BOM at start
        "Hello\x01\x02world",     # Control characters
        "Hello\nworld",           # Newline (should be preserved)
    ]
    column = pa.array(test_data)
    result = transformer.apply_string_cleaning(column, config_invisible)

    print("Input (with invisible characters):")
    for i, item in enumerate(test_data):
        print(f"  {i+1}. {repr(item)}")
    print("\nOutput (invisible chars removed):")
    for i, item in enumerate(result.to_pylist()):
        print(f"  {i+1}. {repr(item)}")
    print("✓ Zero-width and control characters removed")

    # Example 4: Unicode and Accent Handling
    print("\n4️⃣ UNICODE & ACCENT NORMALIZATION")
    print("-" * 50)

    config_unicode = StringCleaningConfig(
        unicode_normalize="NFKC",
        remove_accents=True,
        ascii_only=False
    )

    test_data = [
        "café",                   # Accented characters
        "naïve",
        "résumé",
        "piñata",
        "Zürich"
    ]
    column = pa.array(test_data)
    result = transformer.apply_string_cleaning(column, config_unicode)

    print("Input (with accents):")
    for i, item in enumerate(test_data):
        print(f"  {i+1}. {item}")
    print("\nOutput (accents removed):")
    for i, item in enumerate(result.to_pylist()):
        print(f"  {i+1}. {item}")
    print("✓ Accents and diacritics removed")

    # Example 5: Case Fixing
    print("\n5️⃣ CASE ISSUE CORRECTION")
    print("-" * 50)

    config_case = StringCleaningConfig(
        fix_case_issues=True,
        collapse_whitespace=True,
        strip_whitespace=True
    )

    test_data = [
        "HELLO WORLD",
        "THE QUICK BROWN FOX",
        "NASA AND THE FBI",
        "WELCOME TO THE UNITED STATES OF AMERICA"
    ]
    column = pa.array(test_data)
    result = transformer.apply_string_cleaning(column, config_case)

    print("Input (ALL CAPS):")
    for i, item in enumerate(test_data):
        print(f"  {i+1}. {item}")
    print("\nOutput (proper case):")
    for i, item in enumerate(result.to_pylist()):
        print(f"  {i+1}. {item}")
    print("✓ Case issues corrected with proper article handling")

    # Example 6: ASCII-Only Conversion
    print("\n6️⃣ ASCII-ONLY CONVERSION")
    print("-" * 50)

    config_ascii = StringCleaningConfig(
        ascii_only=True,
        strip_whitespace=True
    )

    test_data = [
        "café",
        "Hello 世界",  # Chinese characters
        "Москва",      # Cyrillic
        "Mix of ASCII and 中文"
    ]
    column = pa.array(test_data)
    result = transformer.apply_string_cleaning(column, config_ascii)

    print("Input (mixed Unicode):")
    for i, item in enumerate(test_data):
        print(f"  {i+1}. {item}")
    print("\nOutput (ASCII-only):")
    for i, item in enumerate(result.to_pylist()):
        print(f"  {i+1}. {repr(item)}")
    print("✓ Non-ASCII characters removed")

    # Example 7: Comprehensive Cleaning
    print("\n7️⃣ COMPREHENSIVE CLEANING")
    print("-" * 50)

    config_comprehensive = StringCleaningConfig(
        normalize_quotes=True,
        normalize_dashes=True,
        normalize_spaces=True,
        collapse_whitespace=True,
        strip_whitespace=True,
        remove_zero_width=True,
        remove_control_chars=True,
        preserve_newlines=True,
        unicode_normalize="NFKC",
        fix_encoding_errors=True
    )

    # Messy text with multiple issues
    messy_text = (
        "  \u201CHello\u201D   \u2013   this\u200Bis\u00A0messy   "
    )
    test_data = [messy_text]
    column = pa.array(test_data)
    result = transformer.apply_string_cleaning(column, config_comprehensive)

    print(f"Input (very messy): {repr(messy_text)}")
    print(f"Output (cleaned):   {repr(result.to_pylist()[0])}")
    print("✓ Multiple issues cleaned simultaneously")

    # Example 8: Schema-Based Configuration
    print("\n8️⃣ SCHEMA-BASED CONFIGURATION")
    print("-" * 50)

    schema_config = {
        "x-transformations": {
            "column_transformations": {
                "customer_name": {
                    "string_cleaning": {
                        "enabled": True,
                        "normalize_quotes": True,
                        "normalize_dashes": True,
                        "collapse_whitespace": True,
                        "strip_whitespace": True,
                        "fix_case_issues": True,
                        "remove_zero_width": True
                    }
                },
                "description": {
                    "string_cleaning": {
                        "enabled": True,
                        "normalize_quotes": True,
                        "collapse_whitespace": True,
                        "strip_whitespace": True,
                        "remove_control_chars": True,
                        "preserve_newlines": True
                    }
                }
            }
        }
    }

    # Create test data
    pa_schema = pa.schema([
        pa.field("customer_name", pa.string()),
        pa.field("description", pa.string())
    ])

    data = {
        "customer_name": [
            "  \u201CJOHN   SMITH\u201D  ",
            "MARY\u2014JANE  WATSON"
        ],
        "description": [
            "\u201CHigh quality\u201D product\x01\x02",
            "Great\u00A0service\n\nHighly recommend"
        ]
    }
    batch = pa.record_batch(data, pa_schema)

    # Apply schema-based transformations
    schema_transformer = SchemaBasedTransformer(schema_config)
    result_batch, validation_results = schema_transformer.process_batch(batch)

    print("Schema configuration applied:")
    print("customer_name column:")
    for i, name in enumerate(result_batch.column('customer_name').to_pylist()):
        print(f"  {i+1}. {repr(name)}")
    print("description column:")
    for i, desc in enumerate(result_batch.column('description').to_pylist()):
        print(f"  {i+1}. {repr(desc)}")
    print(f"Validation results: {len(validation_results)} errors")
    print("✓ Schema-based transformations work seamlessly")

    print(f"\n🎉 All string cleaning features demonstrated successfully!")
    print("\nSUMMARY OF STRING CLEANING FEATURES:")
    print("• 📝 Smart Quotes: Convert curly quotes to ASCII")
    print("• ➖ Dash Normalization: Em/en dashes → hyphens")
    print("• 🔲 Space Normalization: Non-breaking spaces → regular")
    print("• 📏 Whitespace Collapse: Multiple spaces → single")
    print("• 👻 Zero-width Removal: Invisible characters removed")
    print("• 🔧 Control Char Removal: With newline/tab preservation")
    print("• 🌐 Unicode Normalization: NFKC/NFC/NFD forms")
    print("• 🔤 Case Fixing: ALL CAPS → Title Case")
    print("• 🌍 Accent Removal: café → cafe")
    print("• 🔤 ASCII-only: Remove non-ASCII characters")
    print("• 🛠️ Encoding Fixes: Fix mojibake errors")
    print("• ⚙️ Schema Integration: Declarative configuration")

if __name__ == "__main__":
    main()
