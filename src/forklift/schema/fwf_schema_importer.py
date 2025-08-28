from forklift.utils.date_parser import parse_date
import datetime


class FWFRowParser:
    @staticmethod
    def calculate_field_length(field):
        """
        Calculate the length of a field based on its specification.

        :param dict field: The field specification dictionary.
        :returns: The length of the field.
        :rtype: int
        :raises ValueError: If both 'length' and 'end' are specified, or neither is specified,
                            or if 'end' is less than 'start'.
        """
        # ...existing code...
        length = field.get("length")
        end = field.get("end")
        if length is not None and end is not None:
            raise ValueError(f"Field '{field['name']}' cannot have both 'length' and 'end'.")
        if length is not None:
            return length
        elif end is not None:
            field_length = end - field["start"] + 1
            if field_length < 1:
                raise ValueError(f"Field '{field['name']}' has invalid 'end' < 'start'.")
            return field_length
        else:
            raise ValueError(f"Field '{field['name']}' must have either 'length' or 'end'.")

    @staticmethod
    def extract_field_value(decoded_text, start, field_length):
        """
        Extract a substring from the decoded text representing the field value.

        :param str decoded_text: The decoded text line.
        :param int start: The starting index of the field (0-based).
        :param int field_length: The length of the field.
        :returns: The extracted field value as a string.
        :rtype: str
        """
        field_value = decoded_text[start:start + field_length]
        return field_value

    @staticmethod
    def handle_whitespace(field_value, field):
        """
        Handle whitespace stripping on the field value according to field specification.

        :param str field_value: The extracted field value.
        :param dict field: The field specification dictionary.
        :returns: The field value after applying whitespace handling.
        :rtype: str
        """
        if field.get("rstrip", True):
            field_value = field_value.rstrip()
        if field.get("lstrip", False):
            field_value = field_value.lstrip()
        return field_value

    @staticmethod
    def validate_type(field_value, field):
        """
        Validate the field value against the expected type specified in the field.

        :param str field_value: The field value to validate.
        :param dict field: The field specification dictionary.
        :raises ValueError: If the field value does not conform to the expected type.
        """
        field_type = field.get("type")
        if field_type == "integer":
            if field_value and not field_value.strip().isdigit():
                raise ValueError(f"Field '{field['name']}' expected integer, got '{field_value}'")
        elif field_type == "date":
            fmt = field.get("format")
            if not parse_date(field_value, fmt):
                raise ValueError(
                    f"Field '{field['name']}' expected date{f' {fmt}' if fmt else ''}, got '{field_value}'")
        elif field_type == "boolean":
            true_vals = field.get("true", ["Y", "1", "T", "True"])
            false_vals = field.get("false", ["N", "0", "F", "False"])
            if field_value not in true_vals and field_value not in false_vals:
                raise ValueError(f"Field '{field['name']}' expected boolean, got '{field_value}'")


def parse_fwf_row(raw_bytes: bytes, fwf_spec: dict) -> dict:
    """
    Parse a fixed-width formatted row from raw bytes according to the given specification.

    :param bytes raw_bytes: The raw byte string of the fixed-width row.
    :param dict fwf_spec: The specification dictionary defining fields and encoding.
    :returns: A dictionary mapping field names to their parsed string values.
    :rtype: dict
    """
    decoded_text = raw_bytes.decode(fwf_spec.get("encoding", "utf-8"), errors="replace").rstrip("\r\n")
    parsed_fields = {}
    for field in fwf_spec["fields"]:
        name = field["name"]
        start = field["start"] - 1
        field_length = FWFRowParser.calculate_field_length(field)
        field_value = FWFRowParser.extract_field_value(decoded_text, start, field_length)
        field_value = FWFRowParser.handle_whitespace(field_value, field)
        parsed_fields[name] = field_value
    return parsed_fields
