import datetime


def parse_fwf_row(raw_bytes: bytes, fwf_spec: dict) -> dict:
    """
    Parse a single fixed-width formatted (FWF) row into a dictionary of field values.
    Performs basic type validation for integer, date, and boolean fields.

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
        length = field.get("length")
        end = field.get("end")
        if length is not None and end is not None:
            raise ValueError(f"Field '{name}' cannot have both 'length' and 'end'.")
        if length is not None:
            field_length = length
        elif end is not None:
            field_length = end - field["start"] + 1
            if field_length < 1:
                raise ValueError(f"Field '{name}' has invalid 'end' < 'start'.")
        else:
            raise ValueError(f"Field '{name}' must have either 'length' or 'end'.")
        field_value = decoded_text[start:start + field_length]
        # Ignore extra data after declared field length
        if len(field_value) > field_length:
            field_value = field_value[:field_length]
        print(f"DEBUG: Field '{name}' extracted as '{field_value}' from positions {start} to {start + field_length}")
        if field.get("rstrip", True):
            field_value = field_value.rstrip()
        if field.get("lstrip", False):
            field_value = field_value.lstrip()
        # Type validation
        field_type = field.get("type")
        if field_type == "integer":
            if field_value and not field_value.strip().isdigit():
                raise ValueError(f"Field '{name}' expected integer, got '{field_value}'")
        elif field_type == "date":
            fmt = field.get("format", "YYYYMMDD")
            if fmt == "YYYYMMDD":
                try:
                    datetime.datetime.strptime(field_value, "%Y%m%d")
                except Exception:
                    raise ValueError(f"Field '{name}' expected date YYYYMMDD, got '{field_value}'")
        elif field_type == "boolean":
            true_vals = field.get("true", ["Y", "1", "T", "True"])
            false_vals = field.get("false", ["N", "0", "F", "False"])
            if field_value not in true_vals and field_value not in false_vals:
                raise ValueError(f"Field '{name}' expected boolean, got '{field_value}'")
        parsed_fields[name] = field_value
    return parsed_fields
