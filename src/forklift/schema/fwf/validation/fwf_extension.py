"""FWF extension validation functionality."""

from __future__ import annotations
from typing import Any, Dict, List


class FwfExtensionValidator:
    """Validates x-fwf extension structure and values."""

    @staticmethod
    def validate(fwf_ext: Dict[str, Any]) -> List[str]:
        """Validate x-fwf extension structure and values.

        Args:
            fwf_ext: The x-fwf extension dictionary to validate

        Returns:
            List of validation error messages
        """
        errors = []

        if not fwf_ext:
            errors.append("Missing required 'x-fwf' extension")
            return errors

        # Validate encoding
        encoding = fwf_ext.get("encoding", "utf-8")
        valid_encodings = {"utf-8", "utf-8-sig", "latin-1", "cp1252", "ascii"}
        if encoding not in valid_encodings:
            errors.append(f"Invalid encoding '{encoding}', must be one of {valid_encodings}")

        # Validate header and footer rows
        header_rows = fwf_ext.get("headerRows", 0)
        if not isinstance(header_rows, int) or header_rows < 0:
            errors.append("headerRows must be a non-negative integer")

        footer_rows = fwf_ext.get("footerRows", 0)
        if not isinstance(footer_rows, int) or footer_rows < 0:
            errors.append("footerRows must be a non-negative integer")

        # Validate trim configuration
        trim_config = fwf_ext.get("trim", {})
        if trim_config:
            if not isinstance(trim_config, dict):
                errors.append("trim configuration must be a dictionary")
            else:
                for field_name, should_trim in trim_config.items():
                    if not isinstance(should_trim, bool):
                        errors.append(f"trim.{field_name} must be a boolean")

        # Validate nulls configuration
        nulls_config = fwf_ext.get("nulls", {})
        if nulls_config:
            if "global" in nulls_config and not isinstance(nulls_config["global"], list):
                errors.append("x-fwf.nulls.global must be a list")
            if "perColumn" in nulls_config and not isinstance(nulls_config["perColumn"], dict):
                errors.append("x-fwf.nulls.perColumn must be a dictionary")

        # Validate case configuration
        case_cfg = fwf_ext.get("case")
        if case_cfg and isinstance(case_cfg, dict):
            standardize = case_cfg.get("standardizeNames")
            if standardize and standardize not in {"postgres", "snake_case", "camelCase"}:
                errors.append(f"Invalid standardizeNames value '{standardize}'")

            dedupe = case_cfg.get("dedupeNames")
            if dedupe and dedupe not in {"suffix", "prefix", "error"}:
                errors.append(f"Invalid dedupeNames value '{dedupe}'")

        return errors
