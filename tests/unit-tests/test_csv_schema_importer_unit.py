import pytest
from pathlib import Path
from forklift.schema.csv_schema_importer import CsvSchemaImporter

# 1. Path-based schema (reuse existing good schema to ensure file path branch executed)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOOD_SCHEMA_PATH = PROJECT_ROOT / 'tests' / 'test-files' / 'goodcsv' / 'good_csv1.json'


def test_path_input_reads_schema():
    imp = CsvSchemaImporter(str(GOOD_SCHEMA_PATH))
    d = imp.as_dict()
    assert isinstance(d, dict)
    assert 'x-csv' in d

# 2. Dict-based schema with case (standardize + dedupe) and provided header mode + delimiter decoding (valid \t)
DICT_SCHEMA = {
    'type': 'object',
    'properties': {
        'First Name': {'type': 'string'},
        'First Name_1': {'type': 'string'},
        'Amount ($)': {'type': 'number'}
    },
    'x-csv': {
        'case': {'standardizeNames': 'postgres', 'dedupeNames': 'suffix'},
        'delimiter': '\\t',
        'quotechar': '"',
        'nulls': {'global': ['NA', '']},
        'header': {'mode': 'provided', 'columns': ['First Name', 'First Name', 'Amount ($)']},
        'encodingPriority': ['utf-8']
    }
}


def test_dict_input_and_standardize_and_provided_header():
    imp = CsvSchemaImporter(DICT_SCHEMA)
    derived = imp.derive_reader_options()
    # Delimiter decoded to real tab
    assert derived['delimiter'] == '\t'
    # Provided header stored in _provided_header_columns & has_header False
    assert derived['has_header'] is False
    assert derived['_provided_header_columns'] == ['First Name', 'First Name', 'Amount ($)']
    # Encoding priority applied
    assert derived['encoding'] == 'utf-8'
    # Null values captured
    assert set(derived['null_values']) == {'NA', ''}
    # Standardize & dedupe
    cols = ['First Name', 'First Name', 'Amount ($)']
    std = imp.standardize_and_dedupe(cols)
    # Postgres standardization: spaces & symbols -> underscores, lowercase
    assert std[0].startswith('first_name')
    assert std[0] == 'first_name'
    assert std[1] == 'first_name_1'  # deduped
    assert std[2] == 'amount'

# 3. TypeError on invalid schema type

def test_invalid_schema_type_raises():
    with pytest.raises(TypeError):
        CsvSchemaImporter(123)  # type: ignore[arg-type]

# 4. Delimiter 'auto' should not appear; no encodingPriority
AUTO_SCHEMA = {
    'type': 'object',
    'properties': {},
    'x-csv': {
        'delimiter': 'auto',
        'nulls': {'global': []}
    }
}


def test_auto_delimiter_not_added_and_no_encoding():
    imp = CsvSchemaImporter(AUTO_SCHEMA)
    derived = imp.derive_reader_options()
    assert 'delimiter' not in derived
    assert 'encoding' not in derived

# 5. Delimiter invalid escape sequence triggers except path retaining original string
BAD_ESCAPE_SCHEMA = {
    'type': 'object',
    'properties': {},
    'x-csv': {
        'delimiter': '\\uXYZ1',  # invalid escape => UnicodeDecodeError
    }
}


def test_bad_escape_delimiter_fallback():
    imp = CsvSchemaImporter(BAD_ESCAPE_SCHEMA)
    derived = imp.derive_reader_options()
    # Fallback retains original literal
    assert derived['delimiter'] == '\\uXYZ1'

# 6. standardize_and_dedupe no case config (returns unchanged)
NO_CASE_SCHEMA = {
    'type': 'object',
    'properties': {'A': {}, 'A_1': {}},
    'x-csv': {
        'delimiter': ',',
    }
}


def test_no_case_returns_original():
    imp = CsvSchemaImporter(NO_CASE_SCHEMA)
    cols = ['A', 'A']
    assert imp.standardize_and_dedupe(cols) == cols  # no dedupe suffix because not enabled

# 7. get_field_map returns properties

def test_get_field_map():
    imp = CsvSchemaImporter({'type': 'object', 'properties': {'foo': {'type': 'string'}}, 'x-csv': {}})
    assert imp.get_field_map() == {'foo': {'type': 'string'}}

SIMPLE_SCHEMA = {
    'type': 'object',
    'required': ['a'],
    'additionalProperties': False,
    'properties': {'a': {'type': 'integer'}, 'b': {'type': 'string'}},
    'x-csv': {
        'delimiter': ';',  # simple branch (no backslash)
        'header': {'mode': 'provided', 'cols': ['a','b']},  # use 'cols' variant
        'nulls': {'global': ['NULL']}
    }
}

def test_simple_schema_cols_variant_and_properties_access():
    imp = CsvSchemaImporter(SIMPLE_SCHEMA)
    derived = imp.derive_reader_options()
    assert derived['delimiter'] == ';'
    assert derived['_provided_header_columns'] == ['a','b']
    assert derived['has_header'] is False
    # Access stored attributes to execute their lines
    assert imp.required == ['a']
    assert imp.additional_properties is False
    assert imp.get_field_map() == {'a': {'type': 'integer'}, 'b': {'type': 'string'}}

# Schema with no case/dedupe to hit else branch of _standardize_column_name
NO_STD_SCHEMA = {
    'type': 'object', 'properties': {'Col A': {}}, 'x-csv': {'delimiter': ','}
}

def test_standardize_no_case_branch():
    imp = CsvSchemaImporter(NO_STD_SCHEMA)
    # Should return original names unchanged
    assert imp.standardize_and_dedupe(['Col A']) == ['Col A']
