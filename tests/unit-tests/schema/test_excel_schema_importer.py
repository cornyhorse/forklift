import sys
import os
import pytest
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../src')))
from forklift.schema.excel_schema_importer import ExcelSchemaImporter
from forklift.utils.dedupe import dedupe_column_names

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), '../../test-files/excel/excel_all_sheets.by_name.json')

@pytest.fixture
def importer():
    return ExcelSchemaImporter(SCHEMA_PATH)

def test_schema_loading(importer):
    assert importer.schema['title'] == 'Excel (by sheet name) rows'
    assert 'properties' in importer.schema

def test_excel_options(importer):
    opts = importer.get_excel_options()
    assert 'workbook' in opts
    assert 'sheets' in opts
    assert opts['valuesOnly'] is True
    assert opts['dateSystem'] == '1900'

def test_sheet_options(importer):
    sheets = importer.get_sheet_options()
    assert isinstance(sheets, list)
    assert any('select' in s for s in sheets)

def test_nulls(importer):
    nulls = importer.get_nulls()
    assert '' in nulls
    assert 'NA' in nulls

def test_standardize_column_name(importer):
    name = 'Amount USD'
    std = importer.standardize_column_name(name)
    assert std == 'amount_usd'

def test_standardize_column_name_default(importer):
    importer.standardize_names = None
    name = 'Amount USD'
    std = importer.standardize_column_name(name)
    assert std == 'Amount USD'

def test_standardize_column_name_edge(importer):
    # Very long name, special chars
    name = 'A'*70 + '!@#$%^&*()'
    std = importer.standardize_column_name(name)
    assert len(std) <= 63
    # Only special chars
    name2 = '!@#$%^&*()'
    std2 = importer.standardize_column_name(name2)
    assert std2 == ''

def test_dedupe_column_names(importer):
    names = ['name', 'name', 'amount', 'name']
    deduped = dedupe_column_names(names)
    assert deduped == ['name', 'name_1', 'amount', 'name_2']

def test_dedupe_column_names_default(importer):
    importer.dedupe_names = None
    names = ['id', 'name', 'amount']
    deduped = dedupe_column_names(names)
    assert deduped == names

def test_dedupe_column_names_empty(importer):
    importer.dedupe_names = 'suffix'
    names = []
    deduped = dedupe_column_names(names)
    assert deduped == []

def test_dedupe_column_names_suffix_edge(importer):
    importer.dedupe_names = 'suffix'
    names = ['name', 'name', 'name_1', 'name', 'name_1']
    deduped = dedupe_column_names(names)
    assert len(set(deduped)) == len(deduped)
    assert deduped[0] == 'name'
    assert deduped[1].startswith('name_')

def test_dedupe_column_names_suffix_existing_suffix(importer):
    importer.dedupe_names = 'suffix'
    names = ['name', 'name_1', 'name', 'name_1', 'name_2', 'name']
    deduped = dedupe_column_names(names)
    # Should dedupe all, no KeyError, and all names unique
    assert len(set(deduped)) == len(deduped)
    assert deduped[0] == 'name'
    assert deduped[1].startswith('name_')
    assert deduped[2].startswith('name_')
    assert deduped[3].startswith('name_')
    assert deduped[4].startswith('name_')
    assert deduped[5].startswith('name_')

def test_dedupe_column_names_suffix_branch_new_name(importer):
    importer.dedupe_names = 'suffix'
    # This input will force the deduplication logic to generate a new_name that is not in seen_counts
    names = ['name', 'name_1', 'name', 'name_1', 'name_2', 'name', 'name_1', 'name_3', 'name']
    deduped = dedupe_column_names(names)
    # All deduped names should be unique
    assert len(set(deduped)) == len(deduped)
    # There should be several suffixed names
    assert any(n.startswith('name_') for n in deduped)

def test_dedupe_column_names_suffix_force_new_name_init(importer):
    importer.dedupe_names = 'suffix'
    # This input is crafted to force the deduplication logic to generate a new_name that is not in seen_counts
    # and to enter the branch initializing seen_counts[new_name]
    names = ['name', 'name_1', 'name_1', 'name', 'name_2', 'name_1', 'name_2', 'name_3', 'name_2', 'name']
    deduped = dedupe_column_names(names)
    # All deduped names should be unique
    assert len(set(deduped)) == len(deduped)
    # There should be several suffixed names
    assert any(n.startswith('name_') for n in deduped)

def test_build_column_field_mapping(importer):
    columns = ['id', 'name', 'amount_usd', 'qty']
    mapping = importer.build_column_field_mapping(columns)
    assert mapping['id'] == 'id'
    assert mapping['amount_usd'] == 'amount_usd'
    assert mapping['qty'] == 'qty'

def test_build_column_field_mapping_case_insensitive(importer):
    # Add a field to field_map for case-insensitive match
    importer.field_map['SpecialField'] = {}
    columns = ['specialfield', 'SPECIALFIELD', 'SpecialField']
    mapping = importer.build_column_field_mapping(columns)
    # All should map to 'SpecialField'
    for col in columns:
        std_col = importer.standardize_column_name(col)
        deduped_col = dedupe_column_names([std_col])[0]
        assert mapping[deduped_col] == 'SpecialField'

def test_build_column_field_mapping_fallback(importer):
    columns = ['ID', 'NAME', 'AMOUNT_USD']
    mapping = importer.build_column_field_mapping(columns)
    # The keys in mapping will be standardized (lowercased) column names
    assert 'id' in mapping and mapping['id'] == 'id'
    assert 'name' in mapping and mapping['name'] == 'name'
    assert 'amount_usd' in mapping and mapping['amount_usd'] == 'amount_usd'

def test_build_column_field_mapping_empty(importer):
    columns = []
    mapping = importer.build_column_field_mapping(columns)
    assert mapping == {}

def test_validate_row(importer):
    valid_row = {
        'id': 1,
        'name': 'Alice',
        'email': 'alice@example.com',
        'signup_date': '2025-08-27',
        'active': True,
        'amount_usd': 100.0,
        'country': 'US',
        'status': 'active',
        'discount_pct': 10,
        'notes': '',
        'product': 'Widget',
        'qty': 5,
        'unit_price': 20.0,
        'total': 100.0,
        'price_formula': 100.0,
        'label': 'A',
        'serial': 12345,
        'amount': 100.0
    }
    errors = importer.validate_row(valid_row)
    assert errors is None
    invalid_row = valid_row.copy()
    del invalid_row['id']
    errors = importer.validate_row(invalid_row)
    assert errors is not None
    assert any('id' in e for e in errors)

def test_validate_row_empty(importer):
    errors = importer.validate_row({})
    assert errors is not None

def test_validate_row_invalid(importer):
    invalid_row = {'id': 'not-an-int'}
    errors = importer.validate_row(invalid_row)
    assert errors is not None
    assert any('is not of type' in e for e in errors)

def test_coerce_types_no_coerce(importer):
    row = {'id': 1, 'active': 'TRUE'}
    coerced = importer.coerce_types(row, None)
    assert coerced == row
    coerced = importer.coerce_types(row, {})
    assert coerced == row

def test_coerce_types_missing_field(importer):
    sheet_opts = {'coerce': {'booleans': {'true': ['TRUE'], 'false': ['FALSE']}}}
    row = {'id': 1, 'active': 'TRUE', 'extra': 'TRUE'}
    coerced = importer.coerce_types(row, sheet_opts)
    assert coerced['active'] is True
    assert coerced['extra'] == 'TRUE'  # not coerced, not in schema

def test_coerce_types_non_string(importer):
    sheet_opts = {'coerce': {'booleans': {'true': ['TRUE'], 'false': ['FALSE']}}}
    row = {'id': 1, 'active': True}
    coerced = importer.coerce_types(row, sheet_opts)
    assert coerced['active'] is True

def test_coerce_types_dates_branch(importer):
    sheet_opts = {'coerce': {'dates': {'formats': ['%Y/%m/%d']}}}
    row = {'id': 1, 'event_date': '2025/08/27'}
    coerced = importer.coerce_types(row, sheet_opts)
    assert coerced == row  # date coercion not implemented, should be unchanged

def test_coerce_types_no_coercion(importer):
    row = {'active': 'True', 'id': 1}
    coerced = importer.coerce_types(row)
    assert coerced == row

def test_coerce_types_booleans(importer):
    sheet_opts = {'coerce': {'booleans': {'true': ['True', 'yes'], 'false': ['False', 'no']}}}
    row = {'active': 'True', 'id': 1, 'name': 'Bob'}
    # Patch field_map for test
    importer.field_map['active'] = {'type': 'boolean'}
    coerced = importer.coerce_types(row, sheet_opts)
    assert coerced['active'] is True
    row2 = {'active': 'no'}
    coerced2 = importer.coerce_types(row2, sheet_opts)
    assert coerced2['active'] is False

def test_get_field_map(importer):
    field_map = importer.get_field_map()
    assert isinstance(field_map, dict)
    assert 'id' in field_map
