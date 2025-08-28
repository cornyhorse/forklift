"""
FWFInput: Reads fixed-width formatted (FWF) files and yields rows as dictionaries.

This module provides the FWFInput class, which uses a schema specification (fwf_spec)
to parse each row of a fixed-width file into a dictionary of field values. Type validation
is performed at the output stage, not during parsing.

:class FWFInput: Input class for FWF files.
:method iter_rows: Yields parsed rows as dictionaries using the provided fwf_spec.
"""
from .base import BaseInput
from ..schema.fwf_schema_importer import parse_fwf_row


class FWFInput(BaseInput):
    """
    Input class for FWF (fixed-width formatted) files.
    Uses a schema specification (fwf_spec) to parse each row into a dictionary of field values.
    """

    def iter_rows(self):
        """
        Iterate over rows in a fixed-width formatted (FWF) file and yield each as a dictionary.

        Uses the schema specification (fwf_spec) from self.opts to parse each row using parse_fwf_row.
        Type validation is performed at the output stage, not during parsing.

        :return: Yields dictionaries mapping field names to parsed string values for each row.
        :rtype: Iterator[dict]
        """
        spec = self.opts.get("fwf_spec")  # e.g., from JSON Schema: x-fwf
        with open(self.source, "rb") as fh:
            for raw in fh:
                row = parse_fwf_row(raw, spec)
                yield row

    def get_tables(self) -> list[dict]:
        """
        Return a list containing a single table dict for FWF input.
        Each dict contains 'name' (the source filename) and 'rows' (an iterable of row dicts).

        :return: List of table dicts for FWF input.
        :rtype: list[dict]
        """
        return [{
            "name": self.source,
            "rows": self.iter_rows()
        }]
