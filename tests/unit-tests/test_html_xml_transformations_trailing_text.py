"""What ``html_xml_transformations`` does with text the HTML tokenizer leaves unfinished."""

import pyarrow as pa
import pytest

from forklift.utils.transformations.configs import HTMLXMLConfig
from forklift.utils.transformations.html_xml_transformations import (
    HTMLXMLTransformer,
    extract_text,
)


def clean(values, **options):
    column = pa.array(values)
    return HTMLXMLTransformer().apply_html_xml_cleaning(column, HTMLXMLConfig(**options))


class TestTrailingLessThan:
    @pytest.mark.parametrize("decode_entities", [True, False])
    def test_lone_trailing_less_than_is_kept_as_text(self, decode_entities):
        assert extract_text("a <", decode_entities=decode_entities) == "a <"

    def test_unterminated_trailing_tag_is_dropped(self):
        assert extract_text("hello <img src=x onerror=alert(1)//") == "hello "


class TestTrailingPartialEntity:
    def test_partial_entity_is_decoded(self):
        assert clean(["Fish <b>and</b> chips &amp"]).to_pylist() == ["Fish and chips &"]

    def test_partial_entity_is_kept_verbatim_without_decoding(self):
        result = clean(["Fish <b>and</b> chips &amp"], decode_entities=False)

        assert result.to_pylist() == ["Fish and chips &amp"]


class TestTextWithoutMarkup:
    def test_entities_are_kept_when_decoding_is_off(self):
        result = clean(["a &amp; b", None], decode_entities=False)

        assert result.to_pylist() == ["a &amp; b", None]
        assert result.type == pa.string()

    def test_entities_are_decoded_by_default(self):
        assert clean(["a &amp; b"]).to_pylist() == ["a & b"]
