import csv
from .base import BaseInput
from ..utils.encoding import open_text_auto
from ..utils.sampling import detect_header_and_dialect


class CSVInput(BaseInput):
    def iter_rows(self):
        fh, dialect, header, start_line = detect_header_and_dialect(self.source, **self.opts)
        with fh:
            reader = csv.DictReader(fh, fieldnames=header, dialect=dialect)
            # skip pre-header lines
            for _ in range(start_line):
                next(fh, None)
            for r in reader:
                if not any(v.strip() for v in r.values()):
                    continue
                yield r
