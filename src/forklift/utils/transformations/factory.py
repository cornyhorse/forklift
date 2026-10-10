"""Factory functions for creating transformations from configuration.

This module provides utility functions to create transformation functions
from schema configuration dictionaries.
"""

from __future__ import annotations

from typing import Any, Callable, Dict

import pyarrow as pa

from .base import DataTransformer
from .configs import (
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
from .numeric_transformations import resolve_numeric_target_type


def _reject_unknown_keys(transform_type: str, config_dict: Dict[str, Any], valid_keys) -> None:
    """Raise ``ValueError`` naming the valid options if ``config_dict`` has unknown keys."""
    unknown = sorted(str(key) for key in set(config_dict) - set(valid_keys))
    if unknown:
        raise ValueError(
            f"Unknown option(s) {unknown} for transformation '{transform_type}'. "
            f"Valid options: {sorted(valid_keys)}"
        )


def create_transformation_from_config(
    transform_type: str, config: Dict[str, Any]
) -> Callable[[pa.Array], pa.Array]:
    """Create a transformation function from schema configuration.

    Args:
        transform_type: Type of transformation
        config: Configuration dictionary from schema

    Returns:
        Transformation function that can be applied to PyArrow Arrays
    """
    transformer = DataTransformer()

    # Helper function to validate the config keys and return the ones the class accepts
    def filter_config_for_class(config_class, config_dict):
        """Return the config entries for ``config_class``; unknown keys raise ``ValueError``.

        A misspelled option (``{"zeropad": False}``) used to be dropped silently, so the
        transformation ran with the default instead of the intended setting.
        """
        valid_fields = set(config_class.__dataclass_fields__)  # every config class is a dataclass
        _reject_unknown_keys(transform_type, config_dict, valid_fields)
        return {k: v for k, v in config_dict.items() if k in valid_fields}

    # Remove 'enabled' from config since it's not part of any transformation config
    clean_config = {k: v for k, v in config.items() if k != "enabled"}

    if transform_type == "regex_replace":
        filtered_config = filter_config_for_class(RegexReplaceConfig, clean_config)
        regex_config = RegexReplaceConfig(**filtered_config)
        return lambda col: transformer.apply_regex_replace(col, regex_config)

    elif transform_type == "string_replace":
        filtered_config = filter_config_for_class(StringReplaceConfig, clean_config)
        replace_config = StringReplaceConfig(**filtered_config)
        return lambda col: transformer.apply_string_replace(col, replace_config)

    elif transform_type == "money_conversion":
        filtered_config = filter_config_for_class(MoneyTypeConfig, clean_config)
        money_config = MoneyTypeConfig(**filtered_config)
        return lambda col: transformer.apply_money_conversion(col, money_config)

    elif transform_type == "numeric_cleaning":
        # Extract target_type before filtering since it's not part of NumericCleaningConfig
        target_type = clean_config.pop("target_type", "double")
        resolve_numeric_target_type(target_type)  # unknown names fail now, not mid-batch
        filtered_config = filter_config_for_class(NumericCleaningConfig, clean_config)
        numeric_config = NumericCleaningConfig(**filtered_config)
        return lambda col: transformer.apply_numeric_cleaning(col, numeric_config, target_type)

    elif transform_type == "string_padding":
        filtered_config = filter_config_for_class(StringPaddingConfig, clean_config)
        padding_config = StringPaddingConfig(**filtered_config)
        return lambda col: transformer.apply_string_padding(col, padding_config)

    elif transform_type == "string_trimming":
        # string_trimming doesn't use a config class, so validate the keys manually
        _reject_unknown_keys(transform_type, clean_config, {"side", "chars"})
        side = clean_config.get("side", "both")
        chars = clean_config.get("chars", None)
        return lambda col: transformer.apply_string_trimming(col, side, chars)

    elif transform_type == "html_xml_cleaning":
        filtered_config = filter_config_for_class(HTMLXMLConfig, clean_config)
        html_config = HTMLXMLConfig(**filtered_config)
        return lambda col: transformer.apply_html_xml_cleaning(col, html_config)

    elif transform_type == "datetime":
        filtered_config = filter_config_for_class(DateTimeTransformConfig, clean_config)
        datetime_config = DateTimeTransformConfig(**filtered_config)
        return lambda col: transformer.apply_datetime_transformation(col, datetime_config)

    elif transform_type == "string_cleaning":
        filtered_config = filter_config_for_class(StringCleaningConfig, clean_config)
        string_cleaning_config = StringCleaningConfig(**filtered_config)
        return lambda col: transformer.apply_string_cleaning(col, string_cleaning_config)

    elif transform_type == "ssn_formatting":
        filtered_config = filter_config_for_class(SSNConfig, clean_config)
        ssn_config = SSNConfig(**filtered_config)
        return lambda col: transformer.apply_ssn_formatting(col, ssn_config)

    elif transform_type == "zip_code_formatting":
        filtered_config = filter_config_for_class(ZipCodeConfig, clean_config)
        zip_config = ZipCodeConfig(**filtered_config)
        return lambda col: transformer.apply_zip_code_formatting(col, zip_config)

    elif transform_type == "phone_number_formatting":
        filtered_config = filter_config_for_class(PhoneNumberConfig, clean_config)
        phone_config = PhoneNumberConfig(**filtered_config)
        return lambda col: transformer.apply_phone_number_formatting(col, phone_config)

    elif transform_type == "email_formatting":
        filtered_config = filter_config_for_class(EmailConfig, clean_config)
        email_config = EmailConfig(**filtered_config)
        return lambda col: transformer.apply_email_formatting(col, email_config)

    elif transform_type == "ip_address_formatting":
        filtered_config = filter_config_for_class(IPAddressConfig, clean_config)
        ip_config = IPAddressConfig(**filtered_config)
        return lambda col: transformer.apply_ip_address_formatting(col, ip_config)

    elif transform_type == "mac_address_formatting":
        filtered_config = filter_config_for_class(MACAddressConfig, clean_config)
        mac_config = MACAddressConfig(**filtered_config)
        return lambda col: transformer.apply_mac_address_formatting(col, mac_config)

    else:
        raise ValueError(f"Unknown transformation type: {transform_type}")
