"""HTML/XML transformation utilities.

This module provides HTML/XML tag removal and entity decoding capabilities.
"""

from __future__ import annotations

import html
import re
import pyarrow as pa
import pandas as pd

from .configs import HTMLXMLConfig


class HTMLXMLTransformer:
    """Specialized transformer for HTML/XML operations."""

    def apply_html_xml_cleaning(self, column: pa.Array, config: HTMLXMLConfig) -> pa.Array:
        """Remove HTML/XML tags and decode entities."""
        if not pa.types.is_string(column.type):
            return column

        pandas_series = column.to_pandas()
        transformed_values = []

        for value in pandas_series:
            if pd.isna(value) or value is None:
                transformed_values.append(value)
                continue

            str_value = str(value)

            # Decode HTML entities
            if config.decode_entities:
                str_value = html.unescape(str_value)

            # Strip HTML/XML tags
            if config.strip_tags:
                str_value = re.sub(r'<[^>]+>', '', str_value)

            # Handle whitespace
            if not config.preserve_whitespace:
                str_value = re.sub(r'\s+', ' ', str_value).strip()

            transformed_values.append(str_value)

        return pa.array(transformed_values)
