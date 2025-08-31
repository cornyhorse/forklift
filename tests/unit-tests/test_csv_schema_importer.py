from pathlib import Path
import pytest
import polars as pl

from forklift import read_csv
from forklift.schema.csv_schema_importer import CsvSchemaImporter

# Determine project root and test-files directory
# __file__ -> tests/unit-tests/test_csv_schema_importer.py
# parents: [0]=unit-tests, [1]=tests, [2]=forklift (repo root)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEST_FILES = PROJECT_ROOT / "tests" / "test-files"
GOODCSV_DIR = TEST_FILES / "goodcsv"
DUPECSV_DIR = TEST_FILES / "dupecsv"
BADTSV_DIR = TEST_FILES / "badtsv"

GOOD_SCHEMA = GOODCSV_DIR / "good_csv1.json"
GOOD_DATA = GOODCSV_DIR / "good_csv1.txt"
DUPE_SCHEMA = DUPECSV_DIR / "dupe_csv1.json"
DUPE_DATA = DUPECSV_DIR / "dupe_csv1.txt"
BADTSV_SCHEMA = BADTSV_DIR / "badtsv1.json"
BADTSV_DATA = BADTSV_DIR / "badtsv1.txt"


def test_csv_schema_importer_derivation_goodcsv():
    importer = CsvSchemaImporter(str(GOOD_SCHEMA))
    derived = importer.derive_reader_options()
    # Encoding may not be specified in this schema; default not enforced
    # Delimiter explicitly set to comma
    assert derived.get("delimiter") == ","
    # Quote char included
    assert derived.get("quote_char") == '"'
    # Escape char intentionally omitted (not supported) even though present in schema
    assert "escape_char" not in derived
    # Nulls list present
    assert derived.get("null_values") == [""]


def test_read_csv_with_forklift_schema_goodcsv():
    df = read_csv(str(GOOD_DATA), forklift_schema=str(GOOD_SCHEMA))
    assert isinstance(df, pl.DataFrame)
    # Good dataset has 20 rows
    assert df.shape[0] == 20
    expected_cols = [
        "id","name","email","signup_date","active","amount_usd","country","status","discount_pct","notes"
    ]
    assert df.columns == expected_cols


def test_read_csv_with_forklift_schema_dupecsv():
    df = read_csv(str(DUPE_DATA), forklift_schema=str(DUPE_SCHEMA))
    # Duplicate dataset has > 20 rows (24 entries)
    assert df.shape[0] >= 24


def test_read_csv_forklift_schema_conflict_new_columns():
    with pytest.raises(ValueError):
        read_csv(str(GOOD_DATA), forklift_schema=str(GOOD_SCHEMA), new_columns=["x"])  # conflict


def test_read_csv_forklift_schema_conflict_dtypes():
    with pytest.raises(ValueError):
        read_csv(str(GOOD_DATA), forklift_schema=str(GOOD_SCHEMA), dtypes={"id": pl.Int64})


def test_read_csv_forklift_schema_conflict_schema():
    with pytest.raises(ValueError):
        read_csv(str(GOOD_DATA), forklift_schema=str(GOOD_SCHEMA), schema={"id": pl.Int64})


def test_read_csv_user_override_precedence_encoding():
    # Provide encoding override (dataset is UTF-8, override should be honored without error)
    df = read_csv(str(GOOD_DATA), forklift_schema=str(GOOD_SCHEMA), encoding="utf-8")
    assert df.shape[0] == 20


def test_read_tsv_with_provided_header_schema():
    df = read_csv(str(BADTSV_DATA), forklift_schema=str(BADTSV_SCHEMA))
    # Expect at least the number of data rows before footer (16); footer may be included -> >=16
    assert df.shape[0] >= 16
    # Provided header columns from schema
    expected_cols = ["order_id","customer","order_date","region","amount","fulfilled"]
    assert df.columns == expected_cols
