"""Comprehensive string cleaning operations."""

from __future__ import annotations
import re
import unicodedata
import pyarrow as pa
import pandas as pd

from ..configs import StringCleaningConfig
from .normalization_operations import NormalizationOperations
from .case_operations import CaseOperations


class CleaningOperations:
    """Handles comprehensive string cleaning operations."""

    @staticmethod
    def apply_string_cleaning(column: pa.Array, config: StringCleaningConfig) -> pa.Array:
        """Apply comprehensive string cleaning operations."""
        if not pa.types.is_string(column.type):
            return column

        pandas_series = column.to_pandas()
        transformed_values = []

        for value in pandas_series:
            if pd.isna(value) or value is None:
                transformed_values.append(value)
                continue

            str_value = str(value)

            # Fix common encoding errors FIRST
            if config.fix_encoding_errors:
                str_value = NormalizationOperations.fix_encoding_errors(str_value)

            # Unicode normalization
            if config.unicode_normalize:
                try:
                    str_value = unicodedata.normalize(config.unicode_normalize, str_value)
                except ValueError:
                    pass

            # Smart quotes and special characters
            if config.normalize_quotes:
                str_value = NormalizationOperations.normalize_quotes(str_value)

            if config.normalize_dashes:
                str_value = NormalizationOperations.normalize_dashes(str_value)

            if config.normalize_spaces:
                str_value = NormalizationOperations.normalize_spaces(str_value)

            # Zero-width and control characters
            if config.remove_zero_width:
                replace_with_space = config.collapse_whitespace
                str_value = NormalizationOperations.remove_zero_width_chars(str_value, replace_with_space=replace_with_space)

            # Tab handling
            if config.remove_tabs:
                str_value = str_value.replace('\t', '')
            elif '\t' in str_value:
                explicit_tab_replacement = (
                    config.tab_replacement != " " or config.collapse_whitespace
                )

                if explicit_tab_replacement:
                    str_value = str_value.replace('\t', config.tab_replacement)
                elif config.remove_control_chars and not config.preserve_tabs:
                    pass
                else:
                    str_value = str_value.replace('\t', config.tab_replacement)

            if config.remove_control_chars:
                preserve_tabs_for_removal = config.preserve_tabs
                str_value = NormalizationOperations.remove_control_chars(str_value, config.preserve_newlines, preserve_tabs_for_removal)

            # Whitespace handling
            if config.collapse_whitespace:
                if config.tab_replacement != " " and len(config.tab_replacement) > 1:
                    placeholder = "\uE000"
                    str_value = str_value.replace(config.tab_replacement, placeholder)
                    str_value = re.sub(r'\s+', ' ', str_value)
                    str_value = str_value.replace(placeholder, config.tab_replacement)
                else:
                    str_value = re.sub(r'\s+', ' ', str_value)

            if config.strip_whitespace:
                str_value = str_value.strip()

            # Accent and ASCII handling
            if config.remove_accents or config.ascii_only:
                str_value = NormalizationOperations.remove_accents(str_value)

            if config.ascii_only:
                str_value = NormalizationOperations.to_ascii_only(str_value)

            # Case handling
            if config.fix_case_issues:
                str_value = CaseOperations.fix_case_issues(str_value, config.title_case_exceptions, config.acronyms)

            if config.case_transform == 'upper':
                str_value = str_value.upper()
            elif config.case_transform == 'lower':
                str_value = str_value.lower()
            elif config.case_transform in {'title', 'proper'}:
                if config.case_transform == 'title':
                    parts = re.split(r'(\s+|-)', str_value)
                    transformed_parts = [part.title() if part.strip() else part for part in parts]
                    str_value = ''.join(transformed_parts)
                else:  # proper
                    str_value = str_value[0].upper() + str_value[1:].lower() if str_value else str_value

            # Custom case mapping
            if config.custom_case_mapping:
                for key, mapped_value in config.custom_case_mapping.items():
                    if config.case_mapping_mode == 'exact' and str_value == key:
                        str_value = mapped_value
                        break
                    elif config.case_mapping_mode == 'startswith' and str_value.startswith(key):
                        str_value = mapped_value + str_value[len(key):]
                        break
                    elif config.case_mapping_mode == 'endswith' and str_value.endswith(key):
                        str_value = str_value[:-len(key)] + mapped_value
                        break
                    elif config.case_mapping_mode == 'contains' and key in str_value:
                        str_value = str_value.replace(key, mapped_value)

            # Acronym handling
            if config.acronyms:
                for acronym in config.acronyms:
                    pattern = r'\b' + re.escape(acronym.lower()) + r'\b'
                    str_value = re.sub(pattern, acronym.upper(), str_value, flags=re.IGNORECASE)

            transformed_values.append(str_value)

        return pa.array(transformed_values)
