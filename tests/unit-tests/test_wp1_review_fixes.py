"""Regression tests for the WP1 review fixes.

Covers the transformation package (``forklift.utils.transformations``), the date parser, the
column-name utilities and ``TransformationAnalyzer``. Real pyarrow is used throughout.
"""

import contextlib
import datetime
import os
import random
import re
import signal
import sys
import threading
import time
from pathlib import Path

import pyarrow as pa
import pytest

from forklift.schema.types.transformations import TransformationAnalyzer
from forklift.utils.column_name_utilities import (
    dedupe_column_names,
    standardize_postgres_column_name,
)
from forklift.utils.date_parser import coerce_date, coerce_datetime, parse_date
from forklift.utils.date_parser.epoch import datetime_to_epoch, is_epoch_timestamp
from forklift.utils.date_parser.format_utils import matches_format_exact, normalize_format
from forklift.utils.transformations import create_transformation_from_config
from forklift.utils.transformations.base import DataTransformer
from forklift.utils.transformations.configs import (
    DateTimeTransformConfig,
    EmailConfig,
    HTMLXMLConfig,
    IPAddressConfig,
    MACAddressConfig,
    MoneyTypeConfig,
    NumericCleaningConfig,
    PhoneNumberConfig,
    RegexReplaceConfig,
    SSNConfig,
    StringCleaningConfig,
    StringPaddingConfig,
    StringReplaceConfig,
    ZipCodeConfig,
)
from forklift.utils.transformations.format.email import EmailFormatter
from forklift.utils.transformations.format.network import IPAddressFormatter, MACAddressFormatter
from forklift.utils.transformations.format.phone import PhoneNumberFormatter
from forklift.utils.transformations.format.postal import ZipCodeFormatter
from forklift.utils.transformations.format.ssn import SSNFormatter

SRC = Path(__file__).resolve().parents[2] / "src" / "forklift"
UTC = datetime.timezone.utc


@contextlib.contextmanager
def time_limit(seconds=10):
    """Turn an endless loop into a test failure instead of a hung test run (Unix, main thread)."""
    if not hasattr(signal, "SIGALRM") or threading.current_thread() is not threading.main_thread():
        yield
        return

    def on_alarm(signum, frame):
        raise TimeoutError("operation did not terminate")

    previous = signal.signal(signal.SIGALRM, on_alarm)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


@pytest.fixture
def transformer():
    return DataTransformer()


STRING_TYPES = [pa.string(), pa.large_string()]


# --------------------------------------------------------------------------------------------
# 1. pandas removal and null handling
# --------------------------------------------------------------------------------------------

PANDAS_FREE_FILES = [
    "utils/transformations/string_transformations.py",
    "utils/transformations/html_xml_transformations.py",
    "utils/transformations/numeric_transformations.py",
    "utils/transformations/datetime_transformations.py",
    "utils/transformations/format/base.py",
    "schema/types/transformations.py",
]


class TestNoPandas:
    @pytest.mark.parametrize("relative_path", PANDAS_FREE_FILES)
    def test_module_does_not_use_pandas(self, relative_path):
        source = (SRC / relative_path).read_text()
        assert "import pandas" not in source
        assert "from pandas" not in source
        assert "to_pandas" not in source

    @pytest.mark.parametrize("arrow_type", STRING_TYPES)
    def test_nulls_survive_every_string_transformer(self, transformer, arrow_type):
        column = pa.array(["  Hello <b>World</b>  ", None, "x"], type=arrow_type)
        assert transformer.apply_string_cleaning(column, StringCleaningConfig()).to_pylist() == [
            "Hello <b>World</b>",
            None,
            "x",
        ]
        assert transformer.apply_html_xml_cleaning(column, HTMLXMLConfig()).to_pylist() == [
            "Hello World",
            None,
            "x",
        ]
        assert transformer.apply_string_trimming(column).to_pylist()[1] is None
        assert transformer.apply_regex_replace(
            column, RegexReplaceConfig("x", "y")
        ).to_pylist() == ["  Hello <b>World</b>  ", None, "y"]
        assert transformer.apply_string_replace(
            column, StringReplaceConfig("x", "y")
        ).to_pylist() == ["  Hello <b>World</b>  ", None, "y"]
        assert transformer.apply_string_padding(
            column, StringPaddingConfig(width=3, fillchar="0")
        ).to_pylist()[1:] == [None, "00x"]

    def test_numeric_money_and_format_transformers_handle_nulls(self, transformer):
        column = pa.array(["$1,000.50", None, "2"])
        assert transformer.apply_money_conversion(column, MoneyTypeConfig()).to_pylist() == [
            1000.5,
            None,
            2.0,
        ]
        assert transformer.apply_numeric_cleaning(
            column.slice(1), NumericCleaningConfig()
        ).to_pylist() == [None, 2.0]
        phones = pa.array(["5551234567", None])
        assert transformer.apply_phone_number_formatting(
            phones, PhoneNumberConfig()
        ).to_pylist() == ["(555) 123-4567", None]

    def test_analyzer_does_not_need_pandas_and_sees_large_string(self):
        column = pa.chunked_array(
            [pa.array(["1,234.56", None, "2,567.89"], type=pa.large_string())]
        )
        suggestions = TransformationAnalyzer.analyze_column_for_transformations(
            "amount", column, pa.large_string()
        )
        assert "numeric_cleaning" in suggestions
        assert "string_trimming" in suggestions


# --------------------------------------------------------------------------------------------
# 2. Arrow type preservation
# --------------------------------------------------------------------------------------------


class TestTypePreservation:
    def test_trimming_result_is_still_accepted_by_the_next_transformation(self, transformer):
        column = pa.array(["  abc  ", None], type=pa.large_string())
        trimmed = transformer.apply_string_trimming(column)
        assert trimmed.type == pa.large_string()
        # used to be silently skipped because only ``string`` was accepted
        upper = transformer.apply_string_cleaning(
            trimmed, StringCleaningConfig(case_transform="upper")
        )
        assert upper.to_pylist() == ["ABC", None]
        padded = transformer.apply_string_padding(
            trimmed, StringPaddingConfig(width=5, fillchar="*")
        )
        assert padded.to_pylist() == ["**abc", None]

    @pytest.mark.parametrize("arrow_type", STRING_TYPES)
    def test_every_string_transformer_keeps_the_input_type(self, transformer, arrow_type):
        column = pa.array(["a ", None], type=arrow_type)
        results = [
            transformer.apply_string_trimming(column),
            transformer.apply_string_cleaning(column, StringCleaningConfig()),
            transformer.apply_regex_replace(column, RegexReplaceConfig("a", "b")),
            transformer.apply_string_replace(column, StringReplaceConfig("a", "b")),
            transformer.apply_string_padding(column, StringPaddingConfig(width=4)),
            transformer.apply_html_xml_cleaning(column, HTMLXMLConfig()),
            transformer.apply_phone_number_formatting(column, PhoneNumberConfig()),
            transformer.apply_email_formatting(column, EmailConfig()),
        ]
        assert [r.type for r in results] == [arrow_type] * len(results)

    @pytest.mark.parametrize("arrow_type", STRING_TYPES)
    def test_all_null_and_all_invalid_columns_stay_strings(self, transformer, arrow_type):
        all_null = pa.array([None, None], type=arrow_type)
        assert (
            transformer.apply_string_cleaning(all_null, StringCleaningConfig()).type == arrow_type
        )
        assert transformer.apply_html_xml_cleaning(all_null, HTMLXMLConfig()).type == arrow_type
        assert (
            transformer.apply_regex_replace(all_null, RegexReplaceConfig("a", "b")).type
            == arrow_type
        )
        invalid = pa.array(["not a phone", "also not"], type=arrow_type)
        formatted = transformer.apply_phone_number_formatting(invalid, PhoneNumberConfig())
        assert formatted.type == arrow_type
        assert formatted.to_pylist() == [None, None]

    def test_chunked_input_returns_a_plain_array(self, transformer):
        chunked = pa.chunked_array([["a "], [" b"]])
        assert isinstance(transformer.apply_string_trimming(chunked), pa.Array)
        assert isinstance(
            transformer.apply_string_replace(chunked, StringReplaceConfig("a", "z")), pa.Array
        )

    def test_numeric_and_datetime_results_have_explicit_types_when_all_null(self, transformer):
        nulls = pa.array([None, None], type=pa.string())
        assert (
            transformer.apply_numeric_cleaning(nulls, NumericCleaningConfig()).type == pa.float64()
        )
        assert transformer.apply_money_conversion(nulls, MoneyTypeConfig()).type == pa.float64()
        bad = pa.array(["nope", "never"])
        result = transformer.apply_datetime_transformation(bad, DateTimeTransformConfig())
        assert result.type == pa.timestamp("us", tz="UTC")
        assert result.to_pylist() == [None, None]


# --------------------------------------------------------------------------------------------
# 3. Money / numeric separators and integer targets
# --------------------------------------------------------------------------------------------


class TestSeparators:
    def test_only_decimal_separator_set_gets_matching_thousands_separator(self, transformer):
        config = MoneyTypeConfig(decimal_separator=",")
        assert (config.thousands_separator, config.decimal_separator) == (".", ",")
        column = pa.array(["12,50", "1.234,56", "$ 7,5"])
        assert transformer.apply_money_conversion(column, config).to_pylist() == [
            12.5,
            1234.56,
            7.5,
        ]

    def test_only_thousands_separator_set_gets_matching_decimal_separator(self):
        config = NumericCleaningConfig(thousands_separator=".")
        assert (config.thousands_separator, config.decimal_separator) == (".", ",")
        config = NumericCleaningConfig(thousands_separator=" ")
        assert (config.thousands_separator, config.decimal_separator) == (" ", ".")

    def test_defaults_unchanged(self):
        for config in (MoneyTypeConfig(), NumericCleaningConfig()):
            assert (config.thousands_separator, config.decimal_separator) == (",", ".")

    @pytest.mark.parametrize("config_class", [MoneyTypeConfig, NumericCleaningConfig])
    def test_same_explicit_separator_for_both_raises(self, config_class):
        with pytest.raises(ValueError, match="must differ"):
            config_class(thousands_separator=",", decimal_separator=",")

    def test_empty_thousands_separator_does_not_skip_decimal_conversion(self, transformer):
        config = MoneyTypeConfig(thousands_separator="", decimal_separator=",")
        assert transformer.apply_money_conversion(pa.array(["12,50"]), config).to_pylist() == [
            12.5
        ]
        numeric = NumericCleaningConfig(thousands_separator="", decimal_separator=",")
        assert transformer.apply_numeric_cleaning(pa.array(["3,25"]), numeric).to_pylist() == [
            3.25
        ]

    def test_foreign_decimal_separator_is_not_read_as_a_decimal_point(self, transformer):
        # decimal_separator="," -> a literal "." is not a number (it was silently read as 1.5)
        config = MoneyTypeConfig(thousands_separator=" ", decimal_separator=",")
        assert transformer.apply_money_conversion(pa.array(["1.5"]), config).to_pylist() == [None]

    def test_factory_applies_the_same_resolution(self):
        transform = create_transformation_from_config(
            "money_conversion", {"decimal_separator": ","}
        )
        assert transform(pa.array(["12,50"])).to_pylist() == [12.5]


class TestIntegerTargets:
    def test_large_integers_are_exact(self, transformer):
        column = pa.array(["9007199254740993", "-9223372036854775808"])
        result = transformer.apply_numeric_cleaning(column, NumericCleaningConfig(), "int64")
        assert result.to_pylist() == [9007199254740993, -9223372036854775808]

    def test_non_integral_values_become_null(self, transformer):
        column = pa.array(["3.9", "3.0", "1e3", "-0.5"])
        result = transformer.apply_numeric_cleaning(column, NumericCleaningConfig(), "int64")
        assert result.to_pylist() == [None, 3, 1000, None]

    @pytest.mark.parametrize(
        "target,arrow_type",
        [
            ("int8", pa.int8()),
            ("int16", pa.int16()),
            ("int32", pa.int32()),
            ("int64", pa.int64()),
            ("uint8", pa.uint8()),
            ("uint32", pa.uint32()),
            ("float32", pa.float32()),
            ("double", pa.float64()),
        ],
    )
    def test_target_type_produces_that_arrow_type(self, transformer, target, arrow_type):
        result = transformer.apply_numeric_cleaning(
            pa.array(["1", None]), NumericCleaningConfig(), target
        )
        assert result.type == arrow_type

    def test_out_of_range_values_are_null_and_do_not_abort_the_batch(self, transformer):
        column = pa.array(["127", "128", "-129", "1e30", "5"])
        result = transformer.apply_numeric_cleaning(column, NumericCleaningConfig(), "int8")
        assert result.to_pylist() == [127, None, None, None, 5]
        assert transformer.apply_numeric_cleaning(
            pa.array(["-1", "255", "256"]), NumericCleaningConfig(), "uint8"
        ).to_pylist() == [None, 255, None]

    def test_absurd_exponent_does_not_blow_up(self, transformer):
        with time_limit(5):
            result = transformer.apply_numeric_cleaning(
                pa.array(["1e999999999", "7"]), NumericCleaningConfig(), "int64"
            )
        assert result.to_pylist() == [None, 7]

    @pytest.mark.parametrize("target", ["int64", "double", "float32"])
    def test_nan_and_infinity_text_become_null(self, transformer, target):
        column = pa.array(["NaN", "Infinity", "-Infinity", "inf", "nan", "2"])
        result = transformer.apply_numeric_cleaning(column, NumericCleaningConfig(), target)
        assert result.to_pylist() == [None, None, None, None, None, 2]

    def test_float_overflow_becomes_null(self, transformer):
        column = pa.array(["1e30", "1e39", "1e999"])
        assert transformer.apply_numeric_cleaning(
            column, NumericCleaningConfig(), "float32"
        ).to_pylist()[1:] == [None, None]
        assert transformer.apply_numeric_cleaning(
            column, NumericCleaningConfig(), "double"
        ).to_pylist() == [1e30, 1e39, None]

    def test_allow_nan_false_raises_without_echoing_the_value(self, transformer):
        config = NumericCleaningConfig(allow_nan=False)
        with pytest.raises(ValueError) as excinfo:
            transformer.apply_numeric_cleaning(pa.array(["1", "TopSecret42"]), config, "int64")
        assert "TopSecret42" not in str(excinfo.value)
        assert "row 1" in str(excinfo.value)
        with pytest.raises(ValueError):
            transformer.apply_numeric_cleaning(pa.array(["3.5"]), config, "int64")

    def test_unknown_target_type_is_an_error(self, transformer):
        with pytest.raises(ValueError, match="Unsupported numeric target_type"):
            transformer.apply_numeric_cleaning(pa.array(["1"]), NumericCleaningConfig(), "decimal")
        with pytest.raises(ValueError, match="Unsupported numeric target_type"):
            create_transformation_from_config("numeric_cleaning", {"target_type": "nope"})

    def test_money_nan_infinity_and_overflow_become_null(self, transformer):
        column = pa.array(["NaN", "Infinity", "$1e999999999", "$1,5e3", "5"])
        result = transformer.apply_money_conversion(column, MoneyTypeConfig())
        assert result.type == pa.float64()
        assert result.to_pylist() == [None, None, None, 15000.0, 5.0]

    def test_non_string_numeric_input_still_works(self, transformer):
        result = transformer.apply_numeric_cleaning(
            pa.array([1, None, 3]), NumericCleaningConfig(), "int32"
        )
        assert result.to_pylist() == [1, None, 3]


# --------------------------------------------------------------------------------------------
# 4. HTML / XML text extraction
# --------------------------------------------------------------------------------------------


def clean_html(text, **options):
    column = pa.array([text])
    return (
        DataTransformer().apply_html_xml_cleaning(column, HTMLXMLConfig(**options)).to_pylist()[0]
    )


class TestHtmlXml:
    def test_escaped_comparison_operators_are_text_not_tags(self):
        assert clean_html("a &lt; b and c &gt; d") == "a < b and c > d"
        assert clean_html("5 &lt; 6 and 7 &gt; 6") == "5 < 6 and 7 > 6"

    def test_decoded_text_is_not_reinterpreted_as_markup(self):
        assert clean_html("&lt;b&gt;bold&lt;/b&gt;") == "<b>bold</b>"
        assert clean_html("1 &amp;lt; 2") == "1 &lt; 2"  # decoded exactly once

    def test_real_tags_are_removed_and_entities_decoded(self):
        assert clean_html("<p>Fish &amp; <b>chips</b></p>") == "Fish & chips"

    def test_plain_less_than_followed_by_space_stays_text(self):
        assert clean_html("a < b and c > d") == "a < b and c > d"
        assert clean_html("1 <3 2") == "1 <3 2"

    def test_script_and_style_content_is_dropped(self):
        assert clean_html("a<script>alert('x')</script>b<style>p{color:red}</style>c") == "abc"

    def test_unclosed_tag_at_end_is_dropped(self):
        assert clean_html("hello <img src=x onerror=alert(1)//") == "hello"

    def test_quoted_gt_inside_attribute(self):
        assert clean_html('<a title="x>y" href="/u">z</a>') == "z"

    def test_comments_and_declarations_are_dropped(self):
        assert clean_html("x <!-- a > b --> y") == "x y"
        assert clean_html("x <!-- never closed") == "x"
        assert clean_html("<!DOCTYPE html><?xml version='1.0'?><p>t</p>") == "t"

    def test_cdata_content_is_kept_literally(self):
        assert clean_html("<r><![CDATA[a < b & c <i>]]></r>") == "a < b & c <i>"
        assert clean_html("<![CDATA[&lt;]]>") == "&lt;"

    def test_unterminated_cdata_is_dropped(self):
        assert clean_html("a<![CDATA[x<y]]>b<![CDATA[z") == "ax<yb"

    @pytest.mark.parametrize(
        "hostile",
        ["<![CDATA[" * 20000, "<!--" * 50000, "<a " * 100000, "<![CDATA[x]]>" * 20000],
    )
    def test_hostile_input_is_processed_in_linear_time(self, hostile):
        with time_limit(10):
            clean_html(hostile)

    def test_decode_entities_off_keeps_entities(self):
        assert clean_html("<b>AT&amp;T &lt; x</b> &copy;", decode_entities=False) == (
            "AT&amp;T &lt; x &copy;"
        )

    def test_strip_tags_off_only_decodes(self):
        assert clean_html("<b>AT&amp;T</b>", strip_tags=False) == "<b>AT&T</b>"

    def test_partial_entity_at_end(self):
        assert clean_html("t &amp") == "t &"

    def test_whitespace_handling(self):
        assert clean_html("<p>a</p>\n\n<p> b </p>") == "a b"
        assert clean_html("a\n b", preserve_whitespace=True) == "a\n b"

    def test_documented_as_text_extraction_not_a_sanitizer(self):
        from forklift.utils.transformations import html_xml_transformations as module

        assert "not a security sanitizer" in module.__doc__
        readme = (
            SRC / "utils/transformations/forklift.utils.transformations.readme.md"
        ).read_text()
        assert "not a security sanitizer" in readme
        assert "Remove potentially harmful markup" not in readme


# --------------------------------------------------------------------------------------------
# 5. Mojibake repair
# --------------------------------------------------------------------------------------------


class TestMojibake:
    @pytest.fixture
    def fix(self, transformer):
        return transformer._fix_encoding_errors

    @pytest.mark.parametrize(
        "broken,fixed",
        [
            ("Donâ€™t", "Don’t"),
            ("CafÃ©", "Café"),
            ("naÃ¯ve", "naïve"),
            ("a â€” b", "a — b"),  # the em dash used to be eaten by the table order
            ("â€œquotedâ€\x9d", "“quoted”"),
            ("MÃ¼nchen", "München"),
            ("Â\xa0x", "\xa0x"),
        ],
    )
    def test_real_mojibake_is_repaired(self, fix, broken, fixed):
        assert fix(broken) == fixed

    @pytest.mark.parametrize(
        "text",
        ["Âge", "IRMÃ DO", "Ã", "plain ascii", "Café", "Weiß“", "Ö”", "Ñandú"],
    )
    def test_legitimate_text_is_left_alone(self, fix, text):
        assert fix(text) == text

    def test_text_that_does_not_round_trip_is_unchanged(self, fix):
        assert fix("Café Ã©x€") == "Café Ã©x€"  # mixed -> cannot round trip
        assert fix('â€"') == 'â€"'

    def test_default_string_cleaning_no_longer_destroys_letters(self, transformer):
        column = pa.array(["Âge", "IRMÃ DO", "Donâ€™t"])
        result = transformer.apply_string_cleaning(column, StringCleaningConfig())
        assert result.to_pylist() == ["Âge", "IRMÃ DO", "Don't"]


# --------------------------------------------------------------------------------------------
# 6. Datetime transformations
# --------------------------------------------------------------------------------------------


class TestDateTimeTransformer:
    def apply(self, values, **options):
        config = DateTimeTransformConfig(**options)
        return DataTransformer().apply_datetime_transformation(values, config)

    def test_invalid_timezone_raises_at_construction(self):
        with pytest.raises(ValueError, match="Unknown timezone"):
            DateTimeTransformConfig(timezone="America/New_Yrok")
        with pytest.raises(ValueError):
            create_transformation_from_config("datetime", {"timezone": "Nowhere/Land"})
        DateTimeTransformConfig(timezone="America/New_York")  # valid names still work

    def test_timezone_conversion_applies(self):
        result = self.apply(
            pa.array(["2024-01-01 00:30:00"]), target_type="string", timezone="America/New_York"
        )
        assert result.to_pylist() == ["2023-12-31T19:30:00-05:00"]

    def test_pytz_fallback_when_zoneinfo_is_unavailable(self, monkeypatch):
        from forklift.utils.transformations._timezones import resolve_timezone

        pytz = pytest.importorskip("pytz")
        monkeypatch.setitem(sys.modules, "zoneinfo", None)
        assert resolve_timezone("America/New_York").zone == "America/New_York"
        with pytest.raises(ValueError):
            resolve_timezone("America/New_Yrok")
        monkeypatch.setitem(sys.modules, "pytz", None)
        with pytest.raises(ImportError, match="zoneinfo"):
            resolve_timezone("America/New_York")
        assert pytz is not None

    def test_integer_epochs_with_nulls(self):
        result = self.apply(pa.array([1700000000, None, 1700000001]), from_epoch=True)
        assert result.to_pylist() == [
            datetime.datetime(2023, 11, 14, 22, 13, 20, tzinfo=UTC),
            None,
            datetime.datetime(2023, 11, 14, 22, 13, 21, tzinfo=UTC),
        ]

    def test_whole_number_float_epochs(self):
        result = self.apply(pa.array([1700000000.0, float("nan")]), from_epoch=True)
        assert result.to_pylist()[0] == datetime.datetime(2023, 11, 14, 22, 13, 20, tzinfo=UTC)
        assert result.to_pylist()[1] is None

    def test_timestamp_target_treats_naive_datetimes_as_utc(self, monkeypatch):
        monkeypatch.setenv("TZ", "America/Los_Angeles")
        time.tzset()
        try:
            result = self.apply(pa.array(["2024-01-01 10:00:00"]), target_type="timestamp")
        finally:
            monkeypatch.undo()
            time.tzset()
        assert result.to_pylist() == [1704103200.0]

    def test_date_target_with_to_epoch_does_not_overflow(self):
        result = self.apply(
            pa.array(["2024-01-01 10:00:00", "bad"]), target_type="date", to_epoch="nanoseconds"
        )
        assert result.type == pa.int64()
        assert result.to_pylist() == [1704103200000000000, None]

    def test_epoch_beyond_int64_is_null_not_a_batch_failure(self):
        result = self.apply(
            pa.array(["2300-01-01", "2024-01-01"]), to_epoch="nanoseconds", target_type="datetime"
        )
        assert result.to_pylist() == [None, 1704067200000000000]

    def test_programming_errors_are_not_swallowed(self, monkeypatch):
        def broken(*args, **kwargs):
            raise TypeError("programming error")

        monkeypatch.setattr(
            "forklift.utils.transformations.datetime_transformations.coerce_datetime", broken
        )
        with pytest.raises(TypeError):
            self.apply(pa.array(["2024-01-01"]))

    def test_unparseable_values_are_null(self):
        result = self.apply(pa.array(["2024-01-01", "garbage", "", None]), target_type="date")
        assert result.to_pylist() == [datetime.date(2024, 1, 1), None, None, None]

    def test_dayfirst_option(self):
        values = pa.array(["03-04-2024"])
        assert self.apply(values, target_type="date").to_pylist() == [datetime.date(2024, 4, 3)]
        assert self.apply(values, target_type="date", dayfirst=False).to_pylist() == [
            datetime.date(2024, 3, 4)
        ]


# --------------------------------------------------------------------------------------------
# 7. Date parser
# --------------------------------------------------------------------------------------------


class TestDateParser:
    def test_shadowed_module_file_is_gone(self):
        assert not (SRC / "utils" / "date_parser.py").exists()
        assert (SRC / "utils" / "date_parser" / "__init__.py").exists()

    @pytest.mark.parametrize(
        "schema_format,expected,value",
        [
            ("YYYY-MM-DD HH:mm:ss.SSS", "%Y-%m-%d %H:%M:%S.%f", "2024-01-02 03:04:05.678"),
            ("YYYYMMDDHHmmss", "%Y%m%d%H%M%S", "20240102030405"),
            ("DD MM YYYY HH:mm:ss", "%d %m %Y %H:%M:%S", "02 01 2024 03:04:05"),
            ("YYYY-MM-DDTHH:mm:ssZ", "%Y-%m-%dT%H:%M:%SZ", "2024-01-02T03:04:05Z"),
            ("DD/MM/YYYY HH:MM:SS", "%d/%m/%Y %H:%M:%S", "02/01/2024 03:04:05"),
            ("DD de MMMM de YYYY", "%d de %B de %Y", "02 de January de 2024"),
            ("HH:mm", "%H:%M", "03:04"),
        ],
    )
    def test_schema_tokens_produce_valid_strptime_formats(self, schema_format, expected, value):
        assert normalize_format(schema_format) == expected
        parsed = coerce_datetime(value, fmt=schema_format)
        assert (parsed.year, parsed.month) == (2024, 1) or schema_format == "HH:mm"
        if "HH" in schema_format and "mm" in schema_format.lower():
            assert (parsed.hour, parsed.minute) == (3, 4)

    def test_month_and_minute_are_told_apart_by_context(self):
        parsed = coerce_datetime("2024-02-03 04:05:06", fmt="YYYY-MM-DD HH:MM:SS")
        assert (parsed.month, parsed.day, parsed.hour, parsed.minute) == (2, 3, 4, 5)
        parsed = coerce_datetime("20240203040506", fmt="yyyymmddHHMMSS")
        assert (parsed.month, parsed.minute, parsed.second) == (2, 5, 6)

    def test_fractional_seconds_token(self):
        parsed = coerce_datetime("2024-01-02 03:04:05.678", fmt="YYYY-MM-DD HH:mm:ss.SSS")
        assert parsed.microsecond == 678000

    def test_malformed_format_never_leaks_re_error(self):
        for bad_format in ["%", "%Y%Y", "%Y(", "(%Y"]:
            with pytest.raises(ValueError):
                coerce_datetime("2024", fmt=bad_format)
            assert parse_date("2024", fmt=bad_format) is False
        assert coerce_datetime("2024", formats=["%Y(", "%Y"]).year == 2024

    def test_explicit_format_is_not_hijacked_by_epoch_detection(self):
        assert coerce_date("2024010112", fmt="%Y%m%d%H") == "2024-01-01"
        assert coerce_datetime("2024010112", fmt="%Y%m%d%H") == datetime.datetime(2024, 1, 1, 12)
        assert parse_date("2024010112", fmt="%Y%m%d%H") is True
        # a phone-like ID is an epoch only when nobody asked for a format
        with pytest.raises(ValueError):
            coerce_datetime("2125551234", fmt="%Y%m%d%H")
        with pytest.raises(ValueError):
            coerce_date("1700000000", formats=["%Y-%m-%d"])

    def test_from_epoch_flag_still_forces_epoch_parsing(self):
        parsed = coerce_datetime("1700000000", fmt="%Y", from_epoch=True)
        assert parsed == datetime.datetime(2023, 11, 14, 22, 13, 20, tzinfo=UTC)

    def test_auto_detection_without_format_is_unchanged_and_ascii_only(self):
        assert coerce_datetime("1700000000").year == 2023
        assert coerce_date("1700000000000") == "2023-11-14"
        assert not is_epoch_timestamp("١٧٠٠٠٠٠٠٠٠")
        assert not is_epoch_timestamp("0700000000")

    def test_nanosecond_epochs_use_integer_math(self):
        parsed = coerce_datetime("1700000000123456789")
        assert parsed == datetime.datetime(2023, 11, 14, 22, 13, 20, 123456, tzinfo=UTC)
        # floats cannot hold this: spacing of float64 near 1.7e18 is 256
        assert (
            coerce_datetime("1700000000999999999", to_epoch="nanoseconds") == 1700000000999999000
        )
        value = datetime.datetime(2262, 4, 11, 23, 47, 16, 854775, tzinfo=UTC)
        assert datetime_to_epoch(value, "nanoseconds") == 9223372036854775000
        assert datetime_to_epoch(value, "microseconds") == 9223372036854775
        assert (
            datetime_to_epoch(datetime.datetime(1969, 12, 31, 23, 59, 59, 500000), "seconds") == 0
        )
        assert datetime_to_epoch(datetime.datetime(1969, 12, 31, 23, 59, 58), "seconds") == -2

    @pytest.mark.parametrize("junk", ["12", "Mon", "Mar", "2024", "10:30", "2024-05", "March 3"])
    def test_dateutil_fallback_rejects_text_without_full_date(self, junk):
        assert parse_date(junk) is False
        with pytest.raises(ValueError):
            coerce_date(junk)
        with pytest.raises(ValueError):
            coerce_datetime(junk)

    def test_dateutil_fallback_still_accepts_full_dates(self):
        assert coerce_date("Mar 3 2024") == "2024-03-03"
        assert coerce_datetime("3 March 2024 10:30") == datetime.datetime(2024, 3, 3, 10, 30)
        assert coerce_datetime("January 1, 2025 2:30 PM", fuzzy=True).hour == 14

    def test_date_and_datetime_agree_on_ambiguous_dates(self):
        assert coerce_date("03-04-2024") == "2024-04-03"
        assert coerce_datetime("03-04-2024").date().isoformat() == "2024-04-03"
        assert coerce_date("03-04-2024", dayfirst=False) == "2024-03-04"
        assert coerce_datetime("03-04-2024", dayfirst=False).date().isoformat() == "2024-03-04"
        assert coerce_date("03/04/24", dayfirst=False) == "2024-03-04"
        assert parse_date("03-04-2024", dayfirst=False) is True

    @pytest.mark.parametrize("dayfirst", [True, False])
    def test_iso_dates_are_never_read_year_day_month(self, dayfirst):
        # dateutil swaps month and day for ISO text when dayfirst=True; year-first text is exempt
        for value in [
            "2024-03-04T10:11:12+05:00",
            "2024-03-04 10:11:12 UTC",
            "2024/03/04 10:11 Z",
        ]:
            parsed = coerce_datetime(value, dayfirst=dayfirst)
            assert (parsed.month, parsed.day) == (3, 4), value
            assert coerce_date(value, dayfirst=dayfirst) == "2024-03-04", value

    def test_date_and_datetime_agree_for_many_inputs(self):
        for value in [
            "2024-03-04",
            "04/03/2024",
            "2024-03-04 10:11:12",
            "2024-03-04T10:11:12Z",
            "2024-03-04T10:11:12+05:00",
            "4 Mar 2024",
            "20240304",
            "1700000000",
        ]:
            for dayfirst in (True, False):
                assert (
                    coerce_date(value, dayfirst=dayfirst)
                    == coerce_datetime(value, dayfirst=dayfirst).date().isoformat()
                ), value

    @pytest.mark.parametrize("value", ["2024-01-02T03:04:05Z", "2024-01-02T03:04:05+05:00"])
    def test_z_and_offsets_match_percent_z(self, value):
        fmt = "%Y-%m-%dT%H:%M:%S%z"
        assert matches_format_exact(value, fmt)
        assert parse_date(value, fmt=fmt) is True
        parsed = coerce_datetime(value, fmt=fmt)
        assert parsed.tzinfo is not None
        assert parsed.utcoffset() in (datetime.timedelta(0), datetime.timedelta(hours=5))

    def test_exact_matching_still_rejects_unpadded_fields(self):
        assert not matches_format_exact("2024-1-02T03:04:05Z", "%Y-%m-%dT%H:%M:%S%z")
        assert not matches_format_exact("2025-8-27", "%Y-%m-%d")
        assert matches_format_exact("2025-08-27 10:00:00.5", "%Y-%m-%d %H:%M:%S.%f")

    def test_parse_date_with_formats_agrees_with_coerce_date(self):
        formats = ["%Y-%m-%d", "%d/%m/%Y"]
        for value in [
            "2025-08-27",
            "27/08/2025",
            "Aug 27, 2025",
            "20250827",
            "nonsense",
            "03/13/2025",
        ]:
            try:
                coerce_date(value, formats=formats)
                expected = True
            except ValueError:
                expected = False
            assert parse_date(value, formats=formats) is expected, value

    def test_error_messages_do_not_contain_the_value(self):
        for call in (
            lambda: coerce_date("SECRET-VALUE-1"),
            lambda: coerce_datetime("SECRET-VALUE-2"),
            lambda: coerce_datetime("SECRET-VALUE-3", fmt="%Y-%m-%d"),
            lambda: coerce_datetime("SECRET-VALUE-4", formats=["%Y-%m-%d"]),
            lambda: coerce_datetime("SECRET-VALUE-5", from_epoch=True),
        ):
            with pytest.raises(ValueError) as excinfo:
                call()
            assert "SECRET" not in str(excinfo.value)


# --------------------------------------------------------------------------------------------
# 8. Column name utilities
# --------------------------------------------------------------------------------------------


class TestColumnNames:
    def test_empty_names_terminate(self):
        with time_limit():
            assert dedupe_column_names(["", "", ""]) == ["", "_1", "_2"]
            assert dedupe_column_names(["", "", "", ""]) == ["", "_1", "_2", "_3"]

    def test_previously_endless_combinations_terminate(self):
        with time_limit():
            for names in (["", "_1", "", ""], ["a_1", "", "", ""], ["_1", "_1", "_1"]):
                result = dedupe_column_names(names)
                assert len(result) == len(names)
                assert len(set(result)) == len(result)

    def test_existing_results_are_unchanged(self):
        assert dedupe_column_names(["a", "a"]) == ["a", "a_1"]
        assert dedupe_column_names(["", ""]) == ["", "_1"]
        assert dedupe_column_names(["col", "col", "col_1", "col"]) == [
            "col",
            "col_1",
            "col_1_1",
            "col_2",
        ]
        assert dedupe_column_names(["name", "name", "name"], method="prefix") == [
            "name",
            "1_name",
            "2_name",
        ]

    @pytest.mark.parametrize("method", ["suffix", "prefix"])
    def test_randomized_termination_uniqueness_and_length(self, method):
        rng = random.Random(20240601)
        alphabet = ["", "a", "a_1", "_1", "a_2", "b", "_2", "a_1_1", "_1_1", "x_9_y_1", "1", "_"]
        with time_limit(20):
            for _ in range(3000):
                names = [rng.choice(alphabet) for _ in range(rng.randint(0, 12))]
                result = dedupe_column_names(list(names), method=method)
                assert len(result) == len(names), names
                assert len(set(result)) == len(result), (names, result)

    def test_randomized_with_max_length(self):
        rng = random.Random(7)
        alphabet = ["", "a", "a" * 63, "a" * 62 + "_", "b" * 70, "a_1"]
        with time_limit(20):
            for method in ("suffix", "prefix"):
                for _ in range(1000):
                    names = [rng.choice(alphabet) for _ in range(rng.randint(0, 10))]
                    result = dedupe_column_names(names, method=method, max_length=63)
                    assert len(result) == len(names)
                    assert len(set(result)) == len(result), (names, result)
                    assert all(len(name) <= 63 for name in result), (names, result)

    def test_max_length_keeps_postgres_names_within_the_limit(self):
        long_name = standardize_postgres_column_name("a" * 100)
        assert len(long_name) == 63
        result = dedupe_column_names([long_name] * 3, max_length=63)
        assert [len(name) for name in result] == [63, 63, 63]
        assert result[1].endswith("_1") and result[2].endswith("_2")
        # without a limit the historical behaviour is kept (suffix simply extends the name)
        assert dedupe_column_names([long_name] * 2)[1] == long_name + "_1"

    def test_accented_headers_are_transliterated(self):
        assert standardize_postgres_column_name("Café Crème") == "cafe_creme"
        assert standardize_postgres_column_name("Größe") == "grosse"
        assert standardize_postgres_column_name("Ünïcödé") == "unicode"

    def test_untransliterable_headers_do_not_collapse_to_empty(self):
        first = standardize_postgres_column_name("名前")
        second = standardize_postgres_column_name("住所")
        assert first.startswith("col_") and second.startswith("col_")
        assert first != second
        assert first == standardize_postgres_column_name("名前")  # stable
        assert standardize_postgres_column_name("名前", index=3) == "col_3"

    def test_headers_without_letters_or_digits_stay_empty(self):
        assert standardize_postgres_column_name("") == ""
        assert standardize_postgres_column_name("@#$") == ""

    def test_end_to_end_non_ascii_headers_stay_unique(self):
        headers = ["名前", "住所", "Café", "Cafe", "", ""]
        standardized = [standardize_postgres_column_name(h) for h in headers]
        result = dedupe_column_names(standardized, max_length=63)
        assert len(set(result)) == len(headers)
        assert all(len(name) <= 63 for name in result)


# --------------------------------------------------------------------------------------------
# 9-12. Format transformers
# --------------------------------------------------------------------------------------------


class TestSsnAndZip:
    def test_ssn_zero_pad_works_with_validate(self):
        formatter = SSNFormatter(SSNConfig(zero_pad=True, validate=True))
        assert formatter.format_value("12345678") == "012-34-5678"
        assert formatter.format_value("123456789") == "123-45-6789"

    def test_ssn_validation_still_checks_the_padded_length(self):
        formatter = SSNFormatter(SSNConfig(zero_pad=True, validate=True))
        with pytest.raises(ValueError, match="exactly 9 digits"):
            formatter.format_value("1234567890")
        no_pad = SSNFormatter(SSNConfig(zero_pad=False, validate=True))
        with pytest.raises(ValueError, match="exactly 9 digits"):
            no_pad.format_value("12345678")

    def test_float_suffix_is_an_integer(self):
        assert SSNFormatter(SSNConfig()).format_value("123456789.0") == "123-45-6789"
        assert SSNFormatter(SSNConfig()).format_value("12345678.0") == "012-34-5678"
        assert ZipCodeFormatter(ZipCodeConfig(zip_type="zip-5")).format_value("2134.0") == "02134"
        assert ZipCodeFormatter(ZipCodeConfig()).format_value("02134.00") == "02134"

    def test_zip_zero_pad_runs_before_validation(self):
        zip9 = ZipCodeFormatter(ZipCodeConfig(zip_type="zip-9", zero_pad=True, validate=True))
        assert zip9.format_value("21345678") == "02134-5678"
        permissive = ZipCodeFormatter(ZipCodeConfig(zero_pad=True, validate=True))
        assert permissive.format_value("501") == "00501"
        assert permissive.format_value("5010001") == "00501-0001"

    def test_zip_validation_without_padding_still_rejects(self):
        zip9 = ZipCodeFormatter(ZipCodeConfig(zip_type="zip-9", zero_pad=False, validate=True))
        with pytest.raises(ValueError):
            zip9.format_value("21345678")
        permissive = ZipCodeFormatter(ZipCodeConfig(zero_pad=False, validate=True))
        with pytest.raises(ValueError):
            permissive.format_value("501")

    def test_float_suffix_through_the_column_api(self, transformer):
        column = pa.array(["2134.0", "90210", None])
        result = transformer.apply_zip_code_formatting(column, ZipCodeConfig(zip_type="zip-5"))
        assert result.to_pylist() == ["02134", "90210", None]


class TestMac:
    def fmt(self, value, **options):
        return MACAddressFormatter(MACAddressConfig(**options)).format_value(value)

    def test_short_input_is_rejected_not_fabricated(self):
        with pytest.raises(ValueError):
            self.fmt("aa:bb:cc")
        with pytest.raises(ValueError):
            self.fmt("aabbcc")
        with pytest.raises(ValueError):
            self.fmt("11223344556")

    def test_too_many_digits_are_rejected_not_truncated(self):
        with pytest.raises(ValueError):
            self.fmt("0011223344556")
        with pytest.raises(ValueError):
            self.fmt("00:11:22:33:44:55:66")

    def test_unpadded_octets_are_parsed_per_octet(self):
        assert self.fmt("0:1a:2b:3:4:5") == "00:1a:2b:03:04:05"
        assert self.fmt("0-1A-2B-3-4-5", format_style="dash", case_style="upper") == (
            "00-1A-2B-03-04-05"
        )
        with pytest.raises(ValueError):
            self.fmt("0:1a:2b:3:4:5", zero_pad=False)

    def test_supported_notations(self):
        assert self.fmt("0011.2233.4455") == "00:11:22:33:44:55"
        assert self.fmt("00 11 22 33 44 55") == "00:11:22:33:44:55"
        assert self.fmt("AABBCCDDEEFF", format_style="dot") == "aabb.ccdd.eeff"
        assert self.fmt("00:11:22:33:44:55", format_style="none") == "001122334455"

    def test_garbage_and_mixed_separators_are_rejected(self):
        for bad in ["00:11:22:33:44:gg", "00:11-22:33-44:55", "00:11:22:33:44", "zz"]:
            with pytest.raises(ValueError):
                self.fmt(bad)

    def test_validate_false_passes_unparseable_text_through(self):
        assert self.fmt("aa:bb:cc", validate=False) == "aa:bb:cc"

    def test_invalid_values_are_null_or_kept(self, transformer):
        column = pa.array(["aa:bb:cc", "001122334455"])
        assert transformer.apply_mac_address_formatting(
            column, MACAddressConfig()
        ).to_pylist() == [
            None,
            "00:11:22:33:44:55",
        ]
        kept = transformer.apply_mac_address_formatting(
            column, MACAddressConfig(allow_invalid=True)
        )
        assert kept.to_pylist() == ["aa:bb:cc", "00:11:22:33:44:55"]


class TestIpv6:
    def test_normalize_ipv6_false_leaves_text_alone(self):
        original = "2001:0DB8:0000:0000:0000:0000:0000:0001"
        assert IPAddressFormatter(IPAddressConfig(normalize_ipv6=False)).format_value(
            original
        ) == (original)
        assert IPAddressFormatter(IPAddressConfig()).format_value(original) == "2001:db8::1"
        expanded = IPAddressFormatter(IPAddressConfig(compress_ipv6=False)).format_value("::1")
        assert expanded == "0000:0000:0000:0000:0000:0000:0000:0001"

    def test_validation_still_applies_without_normalization(self):
        formatter = IPAddressFormatter(IPAddressConfig(normalize_ipv6=False, ip_version="ipv6"))
        with pytest.raises(ValueError):
            formatter.format_value("not an ip")


class TestPhone:
    def fmt(self, value, **options):
        return PhoneNumberFormatter(PhoneNumberConfig(**options)).format_value(value)

    @pytest.mark.parametrize("style", ["us-standard", "international"])
    def test_foreign_numbers_are_kept_as_e164(self, style):
        assert self.fmt("+44 20 7946 0958", format_style=style) == "+442079460958"
        assert self.fmt("+49 30 123456", format_style=style) == "+4930123456"

    def test_foreign_numbers_never_get_a_plus_one(self):
        assert self.fmt(
            "+44 20 7946 0958", format_style="international", include_country_code=True
        ) == ("+442079460958")
        assert self.fmt("+49 30 123456", format_style="digits-only") == "4930123456"
        assert self.fmt("+44 20 7946 0958", format_style="preserve") == "+44 20 7946 0958"

    def test_foreign_number_length_is_validated_as_e164(self):
        with pytest.raises(ValueError, match="International phone number"):
            self.fmt("+44 123")
        with pytest.raises(ValueError, match="International phone number"):
            self.fmt("+" + "4" * 16)
        with pytest.raises(ValueError, match="invalid country code"):
            self.fmt("+0123456789")
        assert self.fmt("+44 123", validate=False) == "+44123"

    def test_nanp_numbers_unchanged(self):
        assert self.fmt("+1 (555) 123-4567") == "1(555) 123-4567"
        assert self.fmt("555.123.4567") == "(555) 123-4567"
        assert self.fmt("1-555-123-4567", include_country_code=True) == "1(555) 123-4567"
        assert self.fmt("5551234567", format_style="international", include_country_code=True) == (
            "+1 5551234567"
        )
        assert self.fmt("5551234567", format_style="digits-only", include_country_code=True) == (
            "15551234567"
        )

    def test_no_invented_country_code_for_other_lengths(self):
        options = dict(format_style="international", include_country_code=True, validate=False)
        assert self.fmt("5551234", **options) == "5551234"
        assert self.fmt(
            "5551234", format_style="digits-only", include_country_code=True, validate=False
        ) == ("5551234")

    def test_float_suffix(self):
        assert self.fmt("5551234567.0") == "(555) 123-4567"

    def test_use_dots_and_use_dashes(self):
        assert self.fmt("5551234567", use_parentheses=False, use_dots=True) == "555.123.4567"
        assert self.fmt(
            "5551234567", use_parentheses=False, use_dots=True, include_country_code=True
        ) == ("1.555.123.4567")
        assert self.fmt("5551234567", use_dots=True) == "(555) 123.4567"
        assert self.fmt("5551234567", use_parentheses=False, use_dashes=False) == "555 123 4567"
        assert self.fmt("5551234567", use_dashes=False) == "(555) 123 4567"
        assert self.fmt("5551234567", use_parentheses=False) == "555-123-4567"

    def test_preserve_style_is_not_modified_by_use_dots(self):
        assert self.fmt("555-123-4567", format_style="preserve", use_dots=True) == "555-123-4567"


class TestEmail:
    def fmt(self, value, **options):
        return EmailFormatter(EmailConfig(**options)).format_value(value)

    @pytest.mark.parametrize(
        "bad",
        [
            "a..b@x.com",
            ".a@x.com",
            "a.@x.com",
            "a@.x.com",
            "a@x..com",
            "a@-x.com",
            "a@x-.com",
            "a@x.c",
            "a@@x.com",
            "a b@x.com",
            "@x.com",
            "a@",
        ],
    )
    def test_invalid_addresses_are_rejected(self, bad):
        with pytest.raises(ValueError):
            self.fmt(bad)

    @pytest.mark.parametrize(
        "good",
        ["a@x.com", "first.last+tag@sub.example.co.uk", "a_b%c@x-y.org", "1@2.io"],
    )
    def test_valid_addresses_are_accepted(self, good):
        assert self.fmt(good) == good

    def test_normalisation_unchanged(self):
        assert self.fmt("  USER@Example.COM. ") == "user@example.com"

    def test_length_limits(self):
        assert self.fmt("a" * 64 + "@x.com") == "a" * 64 + "@x.com"
        with pytest.raises(ValueError):
            self.fmt("a" * 65 + "@x.com")
        with pytest.raises(ValueError):
            self.fmt("a@" + "b" * 64 + ".com")

    def test_strip_whitespace_false_is_honoured(self):
        with pytest.raises(ValueError):
            self.fmt(" a@x.com ", strip_whitespace=False)
        assert self.fmt(" a@x.com ", strip_whitespace=False, validate_format=False) == " a@x.com "
        assert self.fmt(" a@x.com ", strip_whitespace=True) == "a@x.com"
        with pytest.raises(ValueError, match="Empty"):
            self.fmt("   ", strip_whitespace=False)
        with pytest.raises(ValueError):  # a trailing newline is not part of an address
            self.fmt("a@x.com\n", strip_whitespace=False)


# --------------------------------------------------------------------------------------------
# 13. Title case, regex / string replace
# --------------------------------------------------------------------------------------------


class TestTitleCase:
    @pytest.fixture
    def title(self, transformer):
        config = StringCleaningConfig(case_transform="title")

        def apply(text):
            return transformer.apply_string_cleaning(pa.array([text]), config).to_pylist()[0]

        return apply

    @pytest.mark.parametrize(
        "text,expected",
        [
            ("don't stop", "Don't Stop"),
            ("o'brien's pub", "O'Brien's Pub"),
            ("1st place", "1st Place"),
            ("the 22nd and 3rd", "The 22nd And 3rd"),
            ("l'oréal paris", "L'Oréal Paris"),
            ("mary-jane o'neil", "Mary-Jane O'Neil"),
            ("(hello) world", "(Hello) World"),
            ("MCDONALD'S", "Mcdonald's"),
        ],
    )
    def test_title_case(self, title, text, expected):
        assert title(text) == expected

    def test_fix_case_issues_uses_the_same_rules(self, transformer):
        config = StringCleaningConfig(fix_case_issues=True)
        result = transformer.apply_string_cleaning(pa.array(["DON'T STOP O'BRIEN"]), config)
        assert result.to_pylist() == ["Don't Stop O'Brien"]


class TestRegexAndReplace:
    @pytest.mark.parametrize("pattern", ["(", "[a-", "*abc", "(?P<n>a)(?P<n>b)"])
    def test_bad_regex_raises_value_error_at_config_time(self, pattern):
        with pytest.raises(ValueError, match="Invalid regex_replace"):
            RegexReplaceConfig(pattern, "x")
        with pytest.raises(ValueError, match="Invalid regex_replace"):
            create_transformation_from_config(
                "regex_replace", {"pattern": pattern, "replacement": "x"}
            )

    def test_bad_replacement_template_raises_at_config_time(self):
        with pytest.raises(ValueError, match="Invalid regex_replace"):
            RegexReplaceConfig("(a)", r"\2")
        with pytest.raises(ValueError, match="Invalid regex_replace"):
            RegexReplaceConfig("a", "x", flags="not-flags")

    def test_valid_regex_uses_python_re_semantics(self, transformer):
        config = RegexReplaceConfig(r"(\w+)@(\w+)", r"\2 at \1", flags=re.IGNORECASE)
        result = transformer.apply_regex_replace(pa.array(["Joe@Example"]), config)
        assert result.to_pylist() == ["Example at Joe"]

    def test_lack_of_regex_timeout_is_documented(self):
        assert "no timeout" in RegexReplaceConfig.__doc__
        readme = (
            SRC / "utils/transformations/forklift.utils.transformations.readme.md"
        ).read_text()
        assert "no timeout" in readme

    def test_string_replace_is_literal_and_honours_count(self, transformer):
        column = pa.array(["a.b.c", "(x)", None])
        dots = transformer.apply_string_replace(column, StringReplaceConfig(".", "-"))
        assert dots.to_pylist() == ["a-b-c", "(x)", None]
        limited = transformer.apply_string_replace(column, StringReplaceConfig(".", "-", count=1))
        assert limited.to_pylist() == ["a-b.c", "(x)", None]
        parens = transformer.apply_string_replace(column, StringReplaceConfig("(", "["))
        assert parens.to_pylist() == ["a.b.c", "[x)", None]
        unchanged = transformer.apply_string_replace(
            column, StringReplaceConfig(".", "-", count=0)
        )
        assert unchanged.to_pylist() == ["a.b.c", "(x)", None]

    def test_string_replace_with_empty_needle_terminates(self, transformer):
        with time_limit(5):
            result = transformer.apply_string_replace(
                pa.array(["ab"]), StringReplaceConfig("", "-")
            )
        assert result.to_pylist() == ["-a-b-"]

    def test_padding_and_trimming_semantics_are_unchanged(self, transformer):
        column = pa.array(["ab", None, "abcd"])
        padded = lambda side: transformer.apply_string_padding(  # noqa: E731
            column, StringPaddingConfig(width=5, fillchar="*", side=side)
        ).to_pylist()
        assert padded("left") == ["***ab", None, "*abcd"]
        assert padded("right") == ["ab***", None, "abcd*"]
        assert padded("both") == [
            value.center(5, "*") if value else None for value in column.to_pylist()
        ]
        assert padded("weird") == ["***ab", None, "*abcd"]
        trimmed = transformer.apply_string_trimming(pa.array(["xxhixx", " hi "]), "left", "x")
        assert trimmed.to_pylist() == ["hixx", " hi "]
        assert transformer.apply_string_trimming(pa.array(["  hi  "])).to_pylist() == ["hi"]
        assert transformer.apply_string_trimming(pa.array([" hi "]), chars="").to_pylist() == [
            " hi "
        ]

    def test_padding_config_validation(self):
        with pytest.raises(ValueError):
            StringPaddingConfig(width=5, fillchar="ab")
        with pytest.raises(ValueError):
            StringPaddingConfig(width=5, fillchar="")


# --------------------------------------------------------------------------------------------
# 14. Factory: unknown keys
# --------------------------------------------------------------------------------------------


class TestFactoryUnknownKeys:
    def test_typo_raises_and_lists_valid_keys(self):
        with pytest.raises(ValueError) as excinfo:
            create_transformation_from_config("ssn_formatting", {"zeropad": False})
        message = str(excinfo.value)
        assert "zeropad" in message
        assert "zero_pad" in message and "format_with_dashes" in message

    @pytest.mark.parametrize(
        "transform_type,config",
        [
            ("regex_replace", {"pattern": "a", "replacement": "b", "flag": 1}),
            ("string_replace", {"old": "a", "new": "b", "cnt": 1}),
            ("money_conversion", {"decimal_seperator": ","}),
            ("numeric_cleaning", {"target": "int64"}),
            ("string_padding", {"width": 3, "fill": "0"}),
            ("string_trimming", {"sides": "left"}),
            ("html_xml_cleaning", {"strip_tag": True}),
            ("datetime", {"timezones": "UTC"}),
            ("string_cleaning", {"normalise_quotes": True}),
            ("zip_code_formatting", {"zip": "zip-5"}),
            ("phone_number_formatting", {"style": "x"}),
            ("email_formatting", {"lowercase": True}),
            ("ip_address_formatting", {"version": "ipv4"}),
            ("mac_address_formatting", {"style": "dash"}),
        ],
    )
    def test_every_transformation_type_rejects_unknown_keys(self, transform_type, config):
        with pytest.raises(ValueError, match="Unknown option"):
            create_transformation_from_config(transform_type, config)

    def test_valid_configs_still_work(self):
        transform = create_transformation_from_config(
            "ssn_formatting", {"enabled": True, "zero_pad": False, "validate": False}
        )
        assert transform(pa.array(["12345"])).to_pylist() == ["12-34-5"]
        trim = create_transformation_from_config(
            "string_trimming", {"enabled": True, "side": "left", "chars": "x"}
        )
        assert trim(pa.array(["xxa"])).to_pylist() == ["a"]
        numeric = create_transformation_from_config(
            "numeric_cleaning", {"target_type": "int16", "allow_nan": True}
        )
        assert numeric(pa.array(["12"])).type == pa.int16()


# --------------------------------------------------------------------------------------------
# 15. Documentation of lossy defaults
# --------------------------------------------------------------------------------------------


def test_lossy_defaults_are_documented():
    doc = StringCleaningConfig.__doc__
    assert "lossy" in doc and "NFKC" in doc and "ascii_only" in doc
    assert StringCleaningConfig().unicode_normalize == "NFKC"  # defaults deliberately unchanged
    assert StringCleaningConfig().ascii_only is False


def test_environment_is_not_left_modified():
    # guards the TZ juggling in the timestamp test
    assert os.environ.get("TZ") != "America/Los_Angeles"
