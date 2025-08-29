import polars as pl
from forklift.preprocessors.type_coercion import TypeCoercion

def test_type_coercion_vectorized_happy_and_errors():
    tc = TypeCoercion(types={
        "active": {"type": "boolean"},
        "signup_date": {"type": "string", "format": "date"},
        "amount": {"type": "number"},
        "name": {"type": "string"},
    })
    df = pl.DataFrame([
        {"active": "YES", "signup_date": "2024-03-01", "amount": "10.50", "name": "Amy"},
        {"active": "no", "signup_date": "not-a-date", "amount": "xx", "name": "Bob"},
    ])
    out = tc.process_dataframe(df)
    # One bad row should be filtered out
    assert out.height == 1
    row = out.to_dicts()[0]
    assert row["active"] is True
    assert row["signup_date"].isoformat() == "2024-03-01"  # normalized ISO date
    assert row["amount"] == 10.50
    assert row["name"] == "Amy"
    # Errors captured
    assert hasattr(tc, "_df_errors")
    assert len(tc._df_errors) == 1
