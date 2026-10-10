"""ColumnMapper: naming conventions on names without letters or digits."""

from __future__ import annotations

import pyarrow as pa

from forklift.processors.column_mapper import ColumnMapper, ColumnMappingConfig


class TestSnakeCaseWithoutWords:
    def test_names_made_only_of_separators_are_kept(self):
        mapper = ColumnMapper(ColumnMappingConfig(naming_convention="snake_case"))

        assert mapper.output_names(["__", "!!", "First Name"]) == {
            "__": "__",
            "!!": "!!",
            "First Name": "first_name",
        }

    def test_the_batch_is_renamed_the_same_way(self):
        mapper = ColumnMapper(ColumnMappingConfig(naming_convention="snake_case"))

        batch, results = mapper.process_batch(pa.RecordBatch.from_pydict({"--": [1], "aB": [2]}))

        assert batch.schema.names == ["--", "a_b"] and results == []


class TestUppercase:
    def test_names_are_upper_cased(self):
        mapper = ColumnMapper(ColumnMappingConfig(naming_convention="UPPERCASE"))

        assert mapper.output_names(["first name"]) == {"first name": "FIRST NAME"}
