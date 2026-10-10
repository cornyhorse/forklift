"""``standardizeNames`` styles applied by ``forklift.schema.naming.apply_name_style``."""

import pytest

from forklift.schema.naming import apply_name_style

NAMES = ["User ID", "customerName"]


class TestApplyNameStyle:
    @pytest.mark.parametrize(
        "method, expected",
        [
            ("postgres", ["user_id", "customername"]),
            ("snake_case", ["user_id", "customer_name"]),
            ("camelCase", ["userId", "customerName"]),
        ],
    )
    def test_known_styles_rename_every_name(self, method, expected):
        assert apply_name_style(NAMES, method) == expected

    @pytest.mark.parametrize("method", [None, "", "kebab-case"])
    def test_unknown_or_missing_style_keeps_the_names(self, method):
        names = list(NAMES)

        result = apply_name_style(names, method)

        assert result is names
        assert result == ["User ID", "customerName"]
