import pytest
from forklift.engine.registry import get_input_cls, get_output_cls, get_preprocessors


def test_registry_lookups_positive():
    assert get_input_cls("csv").__name__.lower().endswith("input")
    assert get_output_cls("parquet").__name__.lower().endswith("output")
    names = [p.__class__.__name__.lower() for p in get_preprocessors(["type_coercion"])]
    assert any("typecoercion" in n for n in names)


def test_registry_unknowns_raise():
    with pytest.raises(KeyError):
        get_input_cls("bogus")
    with pytest.raises(KeyError):
        get_output_cls("bogus")
    # preprocessors() returns [] for unknowns per your mapping logic
    assert get_preprocessors(["nope"]) == []
