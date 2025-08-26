from __future__ import annotations
from typing import Dict, Any
import pyarrow as pa
import pyarrow.parquet as pq
from ..types import Row, RowResult
from ..io.parquet_writer import RollingParquetWriter


class PQOutput(BaseOutput):
    def open(self) -> None:
        self.writer = RollingParquetWriter(self.dest, schema_hint=self.schema, **self.opts)
        self.bad_rows = []

    def write(self, row: Row) -> None:
        self.writer.write(row)

    def quarantine(self, rr: RowResult) -> None:
        self.bad_rows.append({"row": rr.row, "error": str(rr.error)})

    def close(self) -> None:
        self.writer.close()
        if self.bad_rows:
            # write DLQ JSONL or CSV next to output
            self.writer.write_quarantine(self.bad_rows)
