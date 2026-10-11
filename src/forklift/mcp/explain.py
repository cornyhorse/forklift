"""What an error code or a finding means, what to do about it, and which page explains it.

The texts are general: the engine's own message (in the result) says what went wrong in this
run. Page names refer to the schema documentation resources (``forklift://docs/schemas/...``).
"""

from __future__ import annotations

from typing import Dict, List, Tuple

README = "README.md"
VALIDATION = "X_VALIDATION_DOCUMENTATION.md"
CONSTRAINTS = "X_CONSTRAINT_HANDLING_DOCUMENTATION.md"
PRIMARY_KEY = "X_PRIMARY_KEY_DOCUMENTATION.md"
UNIQUE = "X_UNIQUE_CONSTRAINTS_DOCUMENTATION.md"
CSV = "X_CSV_DOCUMENTATION.md"
SPECIAL_TYPE = "X_SPECIAL_TYPE_DOCUMENTATION.md"
DATA_QUALITY = "X_DATA_QUALITY_DOCUMENTATION.md"
COLUMN_MAPPING = "X_COLUMN_MAPPING_DOCUMENTATION.md"
STANDARDS = "SCHEMA_STANDARDS.md"

#: code -> (what it means, what to do next, pages)
ERROR_HELP: Dict[str, Tuple[str, List[str], List[str]]] = {
    "SPEC_INVALID": (
        "The job was refused before anything was read: an argument does not fit the job "
        "contract, or names something that cannot work (an unknown encoding, an unsupported "
        "format).",
        ["Fix the argument the message names and call the tool again."],
        [],
    ),
    "SCHEMA_INVALID": (
        "The schema cannot be used: it is not valid JSON Schema for forklift, or one of its "
        "x-... extensions is configured wrongly.",
        [
            "Read the documentation page of the extension the message names.",
            "Fix the schema and check it with validate_schema before running again.",
        ],
        [README, STANDARDS],
    ),
    "INPUT_UNREADABLE": (
        "The input could not be read as the format and options say: a missing file, no header "
        "row, malformed rows, or a workbook or sheet that cannot be opened.",
        [
            "Look at the first rows with preview (raw) to check the delimiter, quoting and "
            "header row.",
            "Set input_options (delimiter, quote_char, header_mode, header_search_rows, sheet) "
            "to match the file.",
        ],
        [CSV],
    ),
    "ENCODING_ERROR": (
        "The input has bytes that are not valid in the configured text encoding.",
        [
            "Set input_options.encoding to the encoding the file was written with (for "
            "example 'latin-1' or 'cp1252'), then preview to check the text.",
        ],
        [CSV],
    ),
    "COLUMN_MISSING": (
        "A column the schema needs (in required, a key, or an extension's rules) is not in the "
        "input's header.",
        [
            "Compare the header (preview, or validate_schema's columns_not_in_schema and "
            "schema_columns_not_in_input) with the schema's properties.",
            "Rename columns with x-columnMapping, or remove the column from the rules that "
            "need it.",
        ],
        [COLUMN_MAPPING, README],
    ),
    "BAD_ROWS_THRESHOLD_EXCEEDED": (
        "x-validation rejected a larger share of the rows than "
        "x-validation.badRowsHandling.maxBadRowsPercent allows, so the import stopped and no "
        "data file was kept; the rejected rows are kept in bad_rows.parquet.",
        [
            "Look at the rejected rows and their _rejection_reason (get_bad_rows) to see which "
            "rules rejected them.",
            "Fix the schema's rules (or the data), or raise maxBadRowsPercent, or set "
            "failOnExceedThreshold to false to keep the accepted rows.",
        ],
        [VALIDATION],
    ),
    "CONSTRAINT_VIOLATION": (
        "A constraint (x-primaryKey, x-uniqueConstraints or a per-property rule) was violated "
        "and x-constraintHandling.errorMode is fail_fast or fail_complete, which stop the "
        "import instead of setting the rows aside.",
        [
            "Find the offending rows (the message names the constraint and column).",
            "Fix the data or the constraint, or use errorMode bad_rows to set such rows aside.",
        ],
        [CONSTRAINTS, PRIMARY_KEY, UNIQUE],
    ),
    "LIMIT_EXCEEDED": (
        "A limit of the job (limits.max_rows, max_input_bytes or max_seconds) was reached.",
        ["Raise the limit that the message names, or run on a smaller input."],
        [],
    ),
    "PERMISSION_DENIED": (
        "The file system refused to read the input or to write an output.",
        ["Check the permissions of the input file and the output directory."],
        [],
    ),
    "TARGET_WRITE_FAILED": (
        "Writing the output failed after the input was read.",
        ["Check the free space and permissions of the output directory, then run again."],
        [],
    ),
    "CANCELLED": (
        "The job was cancelled before it finished.",
        ["Run it again if its result is still needed."],
        [],
    ),
    "INTERNAL": (
        "The engine failed unexpectedly; the message starts with the type of the error.",
        [
            "Run again; if it keeps failing, report the message (it holds no cell values) "
            "with the schema and the input's shape."
        ],
        [],
    ),
}

#: Finding or rejection rule -> pages that explain it
RULE_PAGES: Dict[str, List[str]] = {
    "VALIDATION_ERROR": [VALIDATION],
    "UNIQUE_VIOLATION": [UNIQUE, PRIMARY_KEY, CONSTRAINTS],
    "NULL_VIOLATION": [PRIMARY_KEY, CONSTRAINTS],
    "RANGE_VIOLATION": [CONSTRAINTS],
    "LENGTH_VIOLATION": [CONSTRAINTS],
    "PATTERN_VIOLATION": [CONSTRAINTS, DATA_QUALITY],
    "ENUM_VIOLATION": [CONSTRAINTS],
    "INVALID_SPECIAL_VALUE": [SPECIAL_TYPE],
    "type_conversion_failed": [README, CSV],
    "required_value_missing": [README],
    "too_many_fields": [CSV],
}

#: What the rejection reasons that are not CODE:column mean
REASON_MEANINGS: Dict[str, str] = {
    "type_conversion_failed": "a value could not be converted to its column's type",
    "required_value_missing": "a required column was empty or null",
    "too_many_fields": "the row had more fields than the header (excess_column_mode reject)",
}
