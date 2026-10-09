"""FWF extension validation functionality."""

from __future__ import annotations

from typing import Any, Dict, List

from ...validation_utils import normalize_encoding


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
        if not isinstance(fwf_ext, dict):
            errors.append("x-fwf must be an object")
            return errors

        # Validate encoding: any text encoding Python knows (cp037 EBCDIC, utf-16, iso-8859-1 ...)
        encoding = fwf_ext.get("encoding", "utf-8")
        if normalize_encoding(encoding) is None:
            errors.append(
                f"Invalid encoding '{encoding}', must be one of the text encodings known to"
                " Python (see the codecs module)"
            )

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
            if not isinstance(nulls_config, dict):
                errors.append("x-fwf.nulls must be an object")
            else:
                if "global" in nulls_config and not isinstance(nulls_config["global"], list):
                    errors.append("x-fwf.nulls.global must be a list")
                if "perColumn" in nulls_config and not isinstance(nulls_config["perColumn"], dict):
                    errors.append("x-fwf.nulls.perColumn must be a dictionary")

        # Validate case configuration
        case_cfg = fwf_ext.get("case")
        if case_cfg and isinstance(case_cfg, dict):
            standardize = case_cfg.get("standardizeNames")
            if standardize and not (
                isinstance(standardize, str)
                and standardize in {"postgres", "snake_case", "camelCase"}
            ):
                errors.append(f"Invalid standardizeNames value '{standardize}'")

            dedupe = case_cfg.get("dedupeNames")
            if dedupe and not (
                isinstance(dedupe, str) and dedupe in {"suffix", "prefix", "error"}
            ):
                errors.append(f"Invalid dedupeNames value '{dedupe}'")

        # The field layout must be a list (traditional) or an object (conditional)
        if "fields" in fwf_ext and not isinstance(fwf_ext["fields"], list):
            errors.append("x-fwf.fields must be an array")
        if "conditionalSchemas" in fwf_ext and not isinstance(fwf_ext["conditionalSchemas"], dict):
            errors.append("x-fwf.conditionalSchemas must be an object")

        return errors
