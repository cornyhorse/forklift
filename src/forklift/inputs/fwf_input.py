from .base import BaseInput
from ..schema.fwf_extensions import parse_fwf_row


class FWFInput(BaseInput):
    def iter_rows(self):
        spec = self.opts.get("fwf_spec")  # e.g., from JSON Schema: x-fwf
        with open(self.source, "rb") as fh:
            for raw in fh:
                row = parse_fwf_row(raw, spec)
                yield row
