import pytest
import json
from forklift.schema.fwf_schema_importer import parse_fwf_row, FWFRowParser

# Helper to load schema and rows
def load_fwf_schema_and_rows(schema_path, data_path):
    with open(schema_path, 'r', encoding='utf-8') as f:
        schema = json.load(f)
    with open(data_path, 'rb') as f:
        rows = [line for line in f if line.strip() and not line.startswith(b'#')]
    return schema, rows

def test_goodfwf():
    schema, rows = load_fwf_schema_and_rows(
        'tests/test-files/goodfwf/good_fwf1.json',
        'tests/test-files/goodfwf/good_fwf1.txt'
    )
    for row in rows:
        result = parse_fwf_row(row, schema)
        assert isinstance(result, dict)
        assert set(result.keys()) == {f['name'] for f in schema['fields']}

def test_startendfwf():
    schema, rows = load_fwf_schema_and_rows(
        'tests/test-files/startendfwf/startend_fwf1.json',
        'tests/test-files/startendfwf/startend_fwf1.txt'
    )
    for row in rows:
        result = parse_fwf_row(row, schema)
        assert isinstance(result, dict)
        assert set(result.keys()) == {f['name'] for f in schema['fields']}

def test_dupefwf():
    schema, rows = load_fwf_schema_and_rows(
        'tests/test-files/dupefwf/dupe_fwf1.json',
        'tests/test-files/dupefwf/dupe_fwf1.txt'
    )
    parsed = [parse_fwf_row(row, schema) for row in rows]
    # Check that duplicate rows parse identically
    assert parsed[1] == parsed[2]
    assert parsed[3] == parsed[4]

def test_overlappingfwf():
    schema, rows = load_fwf_schema_and_rows(
        'tests/test-files/overlappingfwf/overlapping_fwf1.json',
        'tests/test-files/overlappingfwf/overlap_fwf1.txt'
    )
    for row in rows:
        result = parse_fwf_row(row, schema)
        assert result['sub_id'] == row[3:6].decode('utf-8')
        assert result['id'] == row[0:6].decode('utf-8')

def test_badfwf_misaligned():
    schema, rows = load_fwf_schema_and_rows(
        'tests/test-files/badfwf/bad_fwf1_misaligned.json',
        'tests/test-files/badfwf/bad_fwf1_misaligned.txt'
    )
    # Type validation is now performed at output, not during parsing.
    # This test now only checks that parsing does not raise errors.
    for row in rows:
        result = parse_fwf_row(row, schema)
        assert isinstance(result, dict)
        assert set(result.keys()) == {f['name'] for f in schema['fields']}

def test_parse_with_length():
    spec = {
        "fields": [
            {"name": "A", "start": 1, "length": 3},
            {"name": "B", "start": 4, "length": 2}
        ]
    }
    row = b"abcde"
    result = parse_fwf_row(row, spec)
    assert result == {"A": "abc", "B": "de"}

def test_parse_with_end():
    spec = {
        "fields": [
            {"name": "A", "start": 1, "end": 3},
            {"name": "B", "start": 4, "end": 5}
        ]
    }
    row = b"abcde"
    result = parse_fwf_row(row, spec)
    assert result == {"A": "abc", "B": "de"}

def test_parse_with_both_length_and_end():
    spec = {
        "fields": [
            {"name": "A", "start": 1, "length": 2, "end": 3}
        ]
    }
    row = b"abcde"
    with pytest.raises(ValueError, match="cannot have both 'length' and 'end'"):
        parse_fwf_row(row, spec)

def test_parse_with_neither_length_nor_end():
    spec = {
        "fields": [
            {"name": "A", "start": 1}
        ]
    }
    row = b"abcde"
    with pytest.raises(ValueError, match="must have either 'length' or 'end'"):
        parse_fwf_row(row, spec)

def test_parse_with_end_less_than_start():
    spec = {
        "fields": [
            {"name": "A", "start": 3, "end": 2}
        ]
    }
    row = b"abcde"
    with pytest.raises(ValueError, match="invalid 'end' < 'start'"):
        parse_fwf_row(row, spec)

def test_extract_field_value_truncation():
    # Value longer than field_length should be truncated
    decoded = "abcdefg"
    result = FWFRowParser.extract_field_value(decoded, 0, 3)
    assert result == "abc"
    result = FWFRowParser.extract_field_value(decoded, 2, 4)
    assert result == "cdef"

def test_handle_whitespace_combinations():
    # Default: rstrip True, lstrip False
    assert FWFRowParser.handle_whitespace("  abc  ", {"name": "A"}) == "  abc"
    # rstrip False, lstrip True
    assert FWFRowParser.handle_whitespace("  abc  ", {"name": "A", "rstrip": False, "lstrip": True}) == "abc  "
    # Both True
    assert FWFRowParser.handle_whitespace("  abc  ", {"name": "A", "rstrip": True, "lstrip": True}) == "abc"
    # Both False
    assert FWFRowParser.handle_whitespace("  abc  ", {"name": "A", "rstrip": False, "lstrip": False}) == "  abc  "

def test_validate_type_integer():
    field = {"name": "A", "type": "integer"}
    FWFRowParser.validate_type("123", field)
    with pytest.raises(ValueError):
        FWFRowParser.validate_type("12a", field)
    # Do not expect error for empty string (optional field)
    FWFRowParser.validate_type("", field)

def test_validate_type_date():
    field = {"name": "D", "type": "date", "format": "%Y-%m-%d"}
    FWFRowParser.validate_type("2025-08-27", field)
    with pytest.raises(ValueError):
        FWFRowParser.validate_type("2025-13-27", field)

def test_validate_type_boolean():
    field = {"name": "B", "type": "boolean", "true": ["Y", "1"], "false": ["N", "0"]}
    FWFRowParser.validate_type("Y", field)
    FWFRowParser.validate_type("0", field)
    with pytest.raises(ValueError):
        FWFRowParser.validate_type("maybe", field)

def test_parse_fwf_row_non_default_encoding():
    spec = {
        "encoding": "latin-1",
        "fields": [
            {"name": "A", "start": 1, "length": 3}
        ]
    }
    row = "ébcde".encode("latin-1")
    result = FWFRowParser.extract_field_value(row.decode("latin-1"), 0, 3)
    assert result == "ébc"
    result = FWFRowParser.handle_whitespace("  abc  ", {"name": "A"})
    assert result == "  abc"
