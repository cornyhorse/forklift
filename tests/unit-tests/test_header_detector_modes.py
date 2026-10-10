"""Tests for HeaderDetector's header modes and footer detection."""

import pytest

from forklift.engine.config import HeaderMode, ImportConfig
from forklift.engine.processors import HeaderDetector
from forklift.io import UnifiedIOHandler


def _detect(tmp_path, text, **config):
    path = tmp_path / "in.csv"
    path.write_text(text, encoding="utf-8")
    detector = HeaderDetector(
        ImportConfig(input_path=path, output_path=tmp_path / "out", **config),
        UnifiedIOHandler(),
    )
    return detector.detect_header_row(path)


class TestAbsentHeaderWithoutSchema:
    def test_columns_are_named_after_the_width_of_the_first_data_row(self, tmp_path):
        result = _detect(tmp_path, "# exported\n\n1,2,3\n4,5,6\n", header_mode=HeaderMode.ABSENT)

        assert result == (-1, ["col_1", "col_2", "col_3"])

    def test_empty_file_has_no_columns(self, tmp_path):
        assert _detect(tmp_path, "", header_mode=HeaderMode.ABSENT) == (-1, [])

    def test_search_window_with_only_comments_is_an_error(self, tmp_path):
        with pytest.raises(ValueError, match="No header row found within the first 2 rows"):
            _detect(
                tmp_path,
                "# one\n# two\n# three\n1,2\n",
                header_mode=HeaderMode.ABSENT,
                header_search_rows=2,
            )


class TestPresentHeader:
    def test_row_of_blank_cells_above_the_header_is_skipped(self, tmp_path):
        result = _detect(tmp_path, " , \nname,amount\nx,1\n")

        assert result == (1, ["name", "amount"])


class TestAutoHeader:
    def test_numeric_rows_before_the_header_are_skipped(self, tmp_path):
        result = _detect(tmp_path, "1,,2\nname,,amount\n3,,4\n", header_mode=HeaderMode.AUTO)

        assert result == (1, ["name", "", "amount"])

    def test_first_row_is_the_header_when_no_row_looks_like_one(self, tmp_path):
        result = _detect(tmp_path, "1,2\n3,4\n", header_mode=HeaderMode.AUTO)

        assert result == (0, ["1", "2"])


class TestFooterPatternColumn:
    @pytest.fixture
    def detector(self, tmp_path):
        config = ImportConfig(
            input_path=tmp_path / "in.csv",
            output_path=tmp_path / "out",
            footer_detection={"column_index": 2, "patterns": [r"^Total"]},
        )
        return HeaderDetector(config, UnifiedIOHandler())

    def test_row_shorter_than_the_pattern_column_is_not_a_footer(self, detector):
        assert detector.should_stop_for_footer(["Total", "5"]) is False

    def test_pattern_in_the_configured_column_is_a_footer(self, detector):
        assert detector.should_stop_for_footer(["", "", " Total: 5"]) is True
