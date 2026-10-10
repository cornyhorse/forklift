"""The removed ``forklift.utils.row_validation`` module fails loudly for legacy callers."""

import pytest

from forklift.utils import row_validation


class TestRemovedRowValidation:
    def test_module_exports_nothing(self):
        assert row_validation.__all__ == []

    @pytest.mark.parametrize(
        "function",
        [
            row_validation.validate_row_against_schema,
            row_validation.validate_dataframe_against_schema,
        ],
    )
    def test_legacy_function_raises_with_migration_hint(self, function):
        with pytest.raises(RuntimeError, match="Use the 'type_coercion' preprocessor") as excinfo:
            function({"id": 1}, schema={"fields": []})

        assert str(excinfo.value) == row_validation.REMOVAL_MESSAGE
