from forklift.utils.date_parser import parse_date
import datetime


class FWFRowParser:
    @staticmethod
    def calculate_field_length(field):
        """
        Calculate the length of a field based on the schema definition.

        Args:
            field (dict): Field specification containing 'start', and either 'length' or 'end'.

        Returns:
            int: The length of the field.

        Raises:
            ValueError: If both 'length' and 'end' are provided, or neither is provided, or 'end' < 'start'.
        """
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
        Extract the raw value of a field from the decoded text.

        Args:
            decoded_text (str): The decoded row string.
            start (int): Zero-based start index for the field.
            field_length (int): Number of characters to extract.

        Returns:
            str: The extracted field value.
        """
        field_value = decoded_text[start:start + field_length]
        return field_value

    @staticmethod
    def handle_whitespace(field_value, field):
        """
        Apply whitespace stripping to a field value based on schema options.

        Args:
            field_value (str): The raw field value.
            field (dict): Field specification, may include 'rstrip' and 'lstrip'.

        Returns:
            str: The field value after whitespace handling.
        """
        if field.get("rstrip", True):
            field_value = field_value.rstrip()
        if field.get("lstrip", False):
            field_value = field_value.lstrip()
        return field_value

    @staticmethod
    def validate_type(field_value, field):
        """
        Validate the type and format of a field value according to schema.

        Args:
            field_value (str): The processed field value.
            field (dict): Field specification, may include 'type', 'format', 'true', 'false'.

        Raises:
            ValueError: If the field value does not match the expected type or format.
        """
        field_type = field.get("type")
        if field_type == "integer":
            if field_value and not field_value.strip().isdigit():
                raise ValueError(f"Field '{field['name']}' expected integer, got '{field_value}'")
        elif field_type == "date":
            fmt = field.get("format")
            if not parse_date(field_value, fmt):
                raise ValueError(f"Field '{field['name']}' expected date{f' {fmt}' if fmt else ''}, got '{field_value}'")
        elif field_type == "boolean":
            true_vals = field.get("true", ["Y", "1", "T", "True"])
            false_vals = field.get("false", ["N", "0", "F", "False"])
            if field_value not in true_vals and field_value not in false_vals:
                raise ValueError(f"Field '{field['name']}' expected boolean, got '{field_value}'")


def parse_fwf_row(raw_bytes: bytes, fwf_spec: dict) -> dict:
    """
    Parse a single fixed-width formatted (FWF) row into a dictionary of field values.
    Only parses and cleans field values; type validation is performed at output.

    Args:
        raw_bytes (bytes): The raw bytes representing a single FWF row.
        fwf_spec (dict): Specification for parsing, including encoding and field definitions.
            Expected keys:
                - encoding (str, optional): Character encoding for decoding bytes. Defaults to 'utf-8'.
                - fields (list of dict): Each dict should have:
                    - name (str): Field name for output.
                    - start (int): 1-based start position in the row.
                    - length (int, optional): Number of characters in the field.
                    - end (int, optional): 1-based inclusive end position in the row.
                    - rstrip (bool, optional): Whether to right-strip whitespace. Defaults to True.
                    - lstrip (bool, optional): Whether to left-strip whitespace. Defaults to False.
            Note: For each field, either 'length' or 'end' must be provided, but not both. If both are provided, a ValueError is raised.
    Returns:
        dict: Mapping of field names to their parsed string values.
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
