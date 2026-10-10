"""``transformations.format_transformations`` re-exports the ``format`` package (legacy path)."""

import pytest

from forklift.utils.transformations import format as format_package
from forklift.utils.transformations import format_transformations


class TestLegacyImportPath:
    @pytest.mark.parametrize("name", format_transformations.__all__)
    def test_name_is_the_same_object_as_in_the_format_package(self, name):
        assert getattr(format_transformations, name) is getattr(format_package, name)

    def test_all_formatters_are_exported(self):
        assert sorted(format_transformations.__all__) == sorted(format_package.__all__)
