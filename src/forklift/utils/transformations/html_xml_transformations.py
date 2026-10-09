"""HTML/XML transformation utilities.

This module provides HTML/XML tag removal and entity decoding capabilities.

**This is text extraction, not a security sanitizer.** The output is plain text in which ``<``,
``>`` and ``&`` can legitimately appear (``"5 &lt; 6"`` becomes ``"5 < 6"``). Never insert the
result into an HTML page, a SQL statement or a shell command without escaping it for that context.

Rules, in order:

1. Tags are removed first, using the standard library's ``html.parser`` tokenizer (so quoted
   ``>`` inside attributes, comments, processing instructions and doctypes are handled the way a
   browser handles them). ``<script>`` and ``<style>`` elements are dropped together with their
   content. A ``<`` that does not start a tag (``"a < b"``, ``"1 <3 2"``) stays text.
2. Character references (``&lt;``, ``&amp;``, ``&#169;``) are decoded afterwards and only once.
   Decoded text is never interpreted as markup again, so ``"a &lt; b and c &gt; d"`` keeps all of
   its words.
3. ``<![CDATA[...]]>`` sections contribute their content as literal text.
4. A tag, comment or CDATA section that is still open at the end of the value is dropped (as
   browsers do), so ``"hello <img src=x onerror=alert(1)//"`` becomes ``"hello"``.
5. Unless ``preserve_whitespace`` is set, runs of whitespace collapse to one space and the result
   is stripped.
"""

from __future__ import annotations

import html
import re
from html.parser import HTMLParser
from typing import List

import pyarrow as pa

from ._arrow_utils import is_string_like, string_array
from .configs import HTMLXMLConfig

_CDATA_OPEN = "<![CDATA["
_CDATA_CLOSE = "]]>"
_SKIPPED_ELEMENTS = frozenset({"script", "style"})


class _TextExtractor(HTMLParser):
    """Collects the text of a document, skipping tags, comments and script/style content."""

    def __init__(self) -> None:
        # convert_charrefs=True: text reaches handle_data already decoded, as plain data that is
        # never re-parsed, which is what keeps "&lt;b&gt;" from being treated as a tag.
        super().__init__(convert_charrefs=True)
        self.parts: List[str] = []
        self._skip_depth = 0

    @property
    def skipping(self) -> bool:
        return self._skip_depth > 0

    def handle_starttag(self, tag, attrs):
        if tag in _SKIPPED_ELEMENTS:
            self._skip_depth += 1

    def handle_endtag(self, tag):
        if tag in _SKIPPED_ELEMENTS and self._skip_depth:
            self._skip_depth -= 1

    def handle_data(self, data):
        if not self._skip_depth:
            self.parts.append(data)


def _escape_cdata(markup: str, protect_entities: bool) -> str:
    """Prepare markup for the parser: CDATA content becomes escaped literal text.

    With ``protect_entities`` every remaining ``&`` is escaped so the parser's entity decoding
    hands the original text back (used when ``decode_entities`` is off).

    Sections are located with ``str.find`` (linear). A regular expression with a lazy ``.*?``
    would rescan the rest of the value for every unterminated ``<![CDATA[`` (quadratic time).
    """

    def outside(text: str) -> str:
        return text.replace("&", "&amp;") if protect_entities else text

    pieces = []
    position = 0
    while True:
        start = markup.find(_CDATA_OPEN, position)
        if start < 0:
            break
        end = markup.find(_CDATA_CLOSE, start + len(_CDATA_OPEN))
        if end < 0:
            break  # unterminated: leave the rest to the parser, which drops it
        pieces.append(outside(markup[position:start]))
        pieces.append(html.escape(markup[start + len(_CDATA_OPEN) : end], quote=False))
        position = end + len(_CDATA_CLOSE)
    pieces.append(outside(markup[position:]))
    return "".join(pieces)


def extract_text(markup: str, decode_entities: bool = True) -> str:
    """Remove tags from ``markup`` and return its text (see the module docstring for the rules)."""
    parser = _TextExtractor()
    parser.feed(_escape_cdata(markup, protect_entities=not decode_entities))
    # Deliberately no parser.close(): whatever the tokenizer could not complete is still in
    # parser.rawdata, which lets us drop an unterminated tag/comment instead of emitting it.
    pending = parser.rawdata
    if pending and not parser.skipping:
        if pending == "<":
            parser.parts.append("<")
        elif not pending.startswith("<"):
            # trailing text that ends in a partial entity such as "t &amp". When decode_entities is
            # off every "&" was escaped above, so unescaping restores the original text exactly.
            parser.parts.append(html.unescape(pending))
    return "".join(parser.parts)


class HTMLXMLTransformer:
    """Specialized transformer for HTML/XML operations (text extraction, not sanitising)."""

    def apply_html_xml_cleaning(self, column: pa.Array, config: HTMLXMLConfig) -> pa.Array:
        """Remove HTML/XML tags and decode entities."""
        if not is_string_like(column.type):
            return column

        transformed_values = []

        for value in column.to_pylist():
            if value is None:
                transformed_values.append(None)
                continue

            str_value = value

            if config.strip_tags and "<" in str_value:
                str_value = extract_text(str_value, decode_entities=config.decode_entities)
            elif config.decode_entities:
                # No markup to strip: decoding is all that is left to do.
                str_value = html.unescape(str_value)

            # Handle whitespace
            if not config.preserve_whitespace:
                str_value = re.sub(r"\s+", " ", str_value).strip()

            transformed_values.append(str_value)

        return string_array(transformed_values, column.type)
